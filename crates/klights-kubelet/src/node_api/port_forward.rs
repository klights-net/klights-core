use bytes::Bytes;
use klights_node_api::{
    BoundedByteStream, ByteFrame, ByteStreamBounds, ByteStreamError, ByteStreamFuture,
    NodePortForward, NodePortForwardChannel, NodePortForwardFrame, NodePortForwardFuture,
    NodePortForwardRequest, NodePortForwardRuntime, NodePortForwardSession,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

const PORT_FORWARD_FRAME_CAPACITY: usize = 64;
const PORT_FORWARD_BYTE_CAPACITY: usize = 256 * 1024;
const PORT_FORWARD_READ_BUFFER_SIZE: usize = 4096;

/// Build the local control-plane port from its private TCP runtime adapter.
pub fn local_node_port_forward(
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Arc<dyn NodePortForward> {
    Arc::new(LocalNodePortForward {
        runtime: Arc::new(TcpNodePortForwardRuntime { task_supervisor }),
    })
}

struct LocalNodePortForward {
    runtime: Arc<dyn NodePortForwardRuntime>,
}

impl NodePortForward for LocalNodePortForward {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        self.runtime.open_port_forward(request)
    }
}

struct TcpNodePortForwardRuntime {
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

struct TcpNodePortForwardSession {
    bounds: ByteStreamBounds,
    writers: Arc<Mutex<HashMap<usize, PortWriterRoute>>>,
    inbound_rx: Mutex<mpsc::Receiver<BudgetedPortFrame>>,
    budget: SessionBudget,
    cancel_token: CancellationToken,
    cancelled: AtomicBool,
}

#[derive(Clone)]
struct PortWriterRoute {
    generation: u64,
    sender: mpsc::Sender<BudgetedPortWrite>,
}

struct BudgetedPortWrite {
    data: Bytes,
    _budget: SessionBudgetPermit,
}

struct BudgetedPortFrame {
    frame: NodePortForwardFrame,
    _budget: SessionBudgetPermit,
}

struct SessionBudgetPermit {
    _frame: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Clone)]
struct SessionBudget {
    frames: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}

impl SessionBudget {
    fn new() -> Self {
        Self {
            frames: Arc::new(Semaphore::new(PORT_FORWARD_FRAME_CAPACITY)),
            bytes: Arc::new(Semaphore::new(PORT_FORWARD_BYTE_CAPACITY)),
        }
    }

    async fn acquire(
        &self,
        byte_count: usize,
        cancellation: &CancellationToken,
    ) -> Result<SessionBudgetPermit, ByteStreamError> {
        let byte_count = u32::try_from(byte_count)
            .ok()
            .filter(|count| {
                usize::try_from(*count)
                    .ok()
                    .is_some_and(|count| count <= PORT_FORWARD_BYTE_CAPACITY)
            })
            .ok_or_else(|| {
                ByteStreamError::failed("port-forward frame exceeds session byte budget")
            })?;
        let frame = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ByteStreamError::cancelled()),
            permit = self.frames.clone().acquire_owned() => permit,
        }
        .map_err(|_| ByteStreamError::closed("port-forward frame budget closed"))?;
        let bytes = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ByteStreamError::cancelled()),
            permit = self.bytes.clone().acquire_many_owned(byte_count) => permit,
        }
        .map_err(|_| ByteStreamError::closed("port-forward byte budget closed"))?;
        Ok(SessionBudgetPermit {
            _frame: frame,
            _bytes: bytes,
        })
    }
}

struct OpenCancellationGuard {
    token: CancellationToken,
    armed: bool,
}

impl OpenCancellationGuard {
    fn new(token: CancellationToken) -> Self {
        Self { token, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OpenCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

impl BoundedByteStream for TcpNodePortForwardSession {
    type Frame = NodePortForwardFrame;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn send_frame(&self, frame: NodePortForwardFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            let (port_index, channel, mut data) = frame.into_parts();
            if channel == NodePortForwardChannel::Error {
                return Ok(());
            }

            let writer = self.writers.lock().await.get(&port_index).cloned();
            let Some(writer) = writer else {
                // The WebSocket adapter historically ignored frames for an
                // unopened or unknown channel. Keep that behavior here.
                return Ok(());
            };
            if data.is_empty() {
                return Ok(());
            }
            while !data.is_empty() {
                let chunk = data.split_to(data.len().min(PORT_FORWARD_BYTE_CAPACITY));
                let budget = self.budget.acquire(chunk.len(), &self.cancel_token).await?;
                tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => return Err(ByteStreamError::cancelled()),
                    result = writer.sender.send(BudgetedPortWrite { data: chunk, _budget: budget }) => {
                        result.map_err(|_| ByteStreamError::closed(
                            "port-forward TCP stream writer closed",
                        ))?;
                    },
                }
            }
            Ok(())
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodePortForwardFrame>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            let mut inbound = self.inbound_rx.lock().await;
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => Err(ByteStreamError::cancelled()),
                frame = inbound.recv() => Ok(frame.map(|frame| frame.frame)),
            }
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancelled.swap(true, Ordering::AcqRel) {
                self.cancel_token.cancel();
                self.inbound_rx.get_mut().close();
                self.writers.lock().await.clear();
            }
            Ok(())
        })
    }
}

impl Drop for TcpNodePortForwardSession {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

impl NodePortForwardRuntime for TcpNodePortForwardRuntime {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        Box::pin(async move {
            let (target, ports) = request.into_parts();
            let (_, _, pod_ip) = target.into_parts();
            let (inbound_tx, inbound_rx) = mpsc::channel(PORT_FORWARD_FRAME_CAPACITY);
            let writers = Arc::new(Mutex::new(HashMap::new()));
            let mut setup_errors = Vec::new();
            let cancel_token = CancellationToken::new();
            let mut open_guard = OpenCancellationGuard::new(cancel_token.clone());
            let budget = SessionBudget::new();

            for (port_index, port) in ports.into_iter().enumerate() {
                let address = format!("{pod_ip}:{port}");
                match tokio::net::TcpStream::connect(&address).await {
                    Ok(stream) => {
                        tracing::debug!(
                            pod_ip,
                            port,
                            port_index,
                            "connected port-forward TCP stream"
                        );
                        let (mut reader, mut writer) = stream.into_split();
                        let (writer_tx, mut writer_rx) =
                            mpsc::channel::<BudgetedPortWrite>(PORT_FORWARD_FRAME_CAPACITY);
                        let generation = u64::try_from(port_index).unwrap_or(u64::MAX);
                        writers.lock().await.insert(
                            port_index,
                            PortWriterRoute {
                                generation,
                                sender: writer_tx,
                            },
                        );

                        let stream_cancel = cancel_token.clone();
                        let stream_tx = inbound_tx.clone();
                        let read_budget = budget.clone();
                        let write_budget = budget.clone();
                        let stream_writers = writers.clone();
                        if let Err(error) = self
                            .task_supervisor
                            .spawn_async(
                                klights_supervisor::TaskCategory::Others,
                                format!("pod_portforward_tcp_stream_{port_index}"),
                                async move {
                                    let reader_cancel = stream_cancel.clone();
                                    let reader_tx = stream_tx.clone();
                                    let read_loop = async move {
                                        let mut buffer =
                                            vec![0; PORT_FORWARD_READ_BUFFER_SIZE];
                                        loop {
                                            let result = tokio::select! {
                                                biased;
                                                _ = reader_cancel.cancelled() => break,
                                                result = reader.read(&mut buffer) => result,
                                            };
                                            match result {
                                                Ok(0) => {
                                                    tracing::debug!(
                                                        port_index,
                                                        "port-forward TCP stream reached EOF"
                                                    );
                                                    break;
                                                }
                                                Ok(length) => {
                                                    let data = Bytes::copy_from_slice(&buffer[..length]);
                                                    let Ok(budget) = read_budget
                                                        .acquire(data.len(), &reader_cancel)
                                                        .await
                                                    else {
                                                        break;
                                                    };
                                                    let frame = BudgetedPortFrame {
                                                        frame: NodePortForwardFrame::data(port_index, data),
                                                        _budget: budget,
                                                    };
                                                    let sent = tokio::select! {
                                                        biased;
                                                        _ = reader_cancel.cancelled() => false,
                                                        result = reader_tx.send(frame) => result.is_ok(),
                                                    };
                                                    if !sent {
                                                        break;
                                                    }
                                                }
                                                Err(error) => {
                                                    tracing::error!(
                                                        port_index,
                                                        %error,
                                                        "port-forward TCP read failed"
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                    };
                                    let writer_cancel = stream_cancel.clone();
                                    let writer_routes = stream_writers.clone();
                                    let write_loop = async move {
                                        while let Some(write) = tokio::select! {
                                            biased;
                                            _ = writer_cancel.cancelled() => None,
                                                data = writer_rx.recv() => data,
                                        } {
                                            let result = tokio::select! {
                                                biased;
                                                _ = writer_cancel.cancelled() => break,
                                                result = writer.write_all(&write.data) => result,
                                            };
                                            if let Err(error) = result {
                                                let data = Bytes::from(format!("Failed to write: {error}"));
                                                let budget = write_budget.acquire(data.len(), &writer_cancel).await;
                                                if let Ok(budget) = budget {
                                                    let frame = BudgetedPortFrame {
                                                        frame: NodePortForwardFrame::error(
                                                    port_index,
                                                            data,
                                                        ),
                                                        _budget: budget,
                                                    };
                                                    tokio::select! {
                                                        biased;
                                                        _ = writer_cancel.cancelled() => {},
                                                        _ = stream_tx.send(frame) => {},
                                                    }
                                                }
                                                let mut routes = writer_routes.lock().await;
                                                if routes.get(&port_index).is_some_and(|route| route.generation == generation) {
                                                    routes.remove(&port_index);
                                                }
                                                break;
                                            }
                                        }
                                    };

                                    // Keep both TCP halves live inside one
                                    // supervised task. A backpressured write
                                    // must not prevent the read half from
                                    // delivering peer data.
                                    tokio::join!(read_loop, write_loop);
                                    let mut routes = stream_writers.lock().await;
                                    if routes.get(&port_index).is_some_and(|route| route.generation == generation) {
                                        routes.remove(&port_index);
                                    }
                                },
                            )
                            .await
                        {
                            tracing::warn!(
                                port_index,
                                %error,
                                "failed to spawn port-forward TCP stream"
                            );
                            writers.lock().await.remove(&port_index);
                            setup_errors.push(NodePortForwardFrame::error(
                                port_index,
                                format!("Failed to start TCP stream: {error}").into_bytes(),
                            ));
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            pod_ip,
                            port,
                            port_index,
                            %error,
                            "failed to connect port-forward TCP stream"
                        );
                        setup_errors.push(NodePortForwardFrame::error(
                            port_index,
                            format!("Failed to connect: {error}").into_bytes(),
                        ));
                    }
                }
            }

            if setup_errors.is_empty() {
                drop(inbound_tx);
            } else {
                let setup_cancel = cancel_token.clone();
                let setup_budget = budget.clone();
                self.task_supervisor
                    .spawn_async(
                        klights_supervisor::TaskCategory::Others,
                        "pod_portforward_setup_errors",
                        async move {
                            for frame in setup_errors {
                                let budget = match setup_budget
                                    .acquire(frame.payload().len(), &setup_cancel)
                                    .await
                                {
                                    Ok(budget) => budget,
                                    Err(_) => break,
                                };
                                tokio::select! {
                                    biased;
                                    _ = setup_cancel.cancelled() => break,
                                    result = inbound_tx.send(BudgetedPortFrame { frame, _budget: budget }) => {
                                        if result.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        },
                    )
                    .await
                    .map_err(|error| {
                        klights_node_api::NodePortForwardSetupError::unavailable(format!(
                            "failed to start port-forward setup result delivery: {error}"
                        ))
                    })?;
            }

            open_guard.disarm();
            Ok(Box::new(TcpNodePortForwardSession {
                bounds: ByteStreamBounds::try_new_with_bytes(
                    PORT_FORWARD_FRAME_CAPACITY,
                    PORT_FORWARD_BYTE_CAPACITY,
                    PORT_FORWARD_FRAME_CAPACITY,
                    PORT_FORWARD_BYTE_CAPACITY,
                )
                .expect("port-forward stream bounds are non-zero"),
                writers,
                inbound_rx: Mutex::new(inbound_rx),
                budget,
                cancel_token,
                cancelled: AtomicBool::new(false),
            }) as Box<dyn NodePortForwardSession>)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_node_api::NodePortForwardTarget;
    use std::time::Duration;

    #[tokio::test]
    async fn sixty_five_failed_connects_return_a_session_without_setup_backpressure_deadlock() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let runtime = TcpNodePortForwardRuntime {
            task_supervisor: supervisor.clone(),
        };
        let request = NodePortForwardRequest::try_new(
            NodePortForwardTarget::try_new("default", "pod", "127.0.0.1").unwrap(),
            vec![0; 65],
        )
        .unwrap();

        let session = supervisor
            .timeout(
                "test_portforward_failed_connect_setup",
                Duration::from_secs(1),
                runtime.open_port_forward(request),
            )
            .await
            .unwrap()
            .expect("setup must not block behind its own bounded error channel")
            .unwrap();

        for expected_index in 0..65 {
            let frame = session
                .recv_frame()
                .await
                .unwrap()
                .expect("one error frame per failed connect");
            assert_eq!(frame.port_index(), expected_index);
            assert_eq!(frame.channel(), NodePortForwardChannel::Error);
        }
        assert!(session.recv_frame().await.unwrap().is_none());
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn cancellation_does_not_wait_for_a_backpressured_tcp_writer_queue() {
        let (writer_tx, mut writer_rx) = mpsc::channel(1);
        let cancel_token = CancellationToken::new();
        let budget = SessionBudget::new();
        let first_budget = budget.acquire(1, &cancel_token).await.unwrap();
        writer_tx
            .send(BudgetedPortWrite {
                data: Bytes::from_static(&[1]),
                _budget: first_budget,
            })
            .await
            .unwrap();
        let (_inbound_tx, inbound_rx) = mpsc::channel(1);
        let mut session = TcpNodePortForwardSession {
            bounds: ByteStreamBounds::try_new(64, 64).unwrap(),
            writers: Arc::new(Mutex::new(HashMap::from([(
                0,
                PortWriterRoute {
                    generation: 1,
                    sender: writer_tx,
                },
            )]))),
            inbound_rx: Mutex::new(inbound_rx),
            budget,
            cancel_token,
            cancelled: AtomicBool::new(false),
        };

        {
            let blocked = session.send_frame(NodePortForwardFrame::data(0, vec![2]));
            tokio::pin!(blocked);
            assert!(futures::poll!(blocked.as_mut()).is_pending());
        }

        session.cancel().await.unwrap();
        assert!(session.is_cancelled());
        assert_eq!(
            writer_rx.recv().await.unwrap().data,
            Bytes::from_static(&[1])
        );
        assert!(writer_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn tcp_reader_remains_live_while_writer_is_backpressured() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first_byte = [0_u8; 1];
            stream.read_exact(&mut first_byte).await.unwrap();
            stream.write_all(b"peer-response").await.unwrap();
            std::future::pending::<()>().await;
        });
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let runtime = TcpNodePortForwardRuntime {
            task_supervisor: supervisor.clone(),
        };
        let request = NodePortForwardRequest::try_new(
            NodePortForwardTarget::try_new("default", "pod", "127.0.0.1").unwrap(),
            vec![address.port()],
        )
        .unwrap();
        let mut session = runtime.open_port_forward(request).await.unwrap();

        // This exceeds the host's maximum TCP send buffer. The peer consumes
        // only one byte, keeping the write half blocked while it sends data in
        // the opposite direction.
        let frame = {
            let blocked_send =
                session.send_frame(NodePortForwardFrame::data(0, vec![7_u8; 8 * 1024 * 1024]));
            tokio::pin!(blocked_send);
            supervisor
                .timeout(
                    "test_portforward_full_duplex_under_backpressure",
                    Duration::from_secs(1),
                    async {
                        tokio::select! {
                            frame = session.recv_frame() => frame,
                            result = &mut blocked_send => panic!(
                                "large TCP write unexpectedly completed before peer response: {result:?}"
                            ),
                        }
                    },
                )
                .await
                .unwrap()
                .expect("TCP reads must progress while the write half is blocked")
                .unwrap()
                .expect("peer response frame")
        };
        assert_eq!(frame.channel(), NodePortForwardChannel::Data);
        assert_eq!(frame.data_bytes(), b"peer-response");

        session.cancel().await.unwrap();
        peer.abort();
        let _ = peer.await;
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn peer_write_half_close_does_not_stop_client_to_peer_forwarding() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read_half, mut write_half) = stream.into_split();
            write_half.shutdown().await.unwrap();
            let mut input = [0_u8; 4];
            read_half.read_exact(&mut input).await.unwrap();
            input
        });
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let runtime = TcpNodePortForwardRuntime {
            task_supervisor: supervisor.clone(),
        };
        let request = NodePortForwardRequest::try_new(
            NodePortForwardTarget::try_new("default", "pod", "127.0.0.1").unwrap(),
            vec![port],
        )
        .unwrap();
        let session = runtime.open_port_forward(request).await.unwrap();
        session
            .send_frame(NodePortForwardFrame::data(
                0,
                bytes::Bytes::from_static(b"ping"),
            ))
            .await
            .expect("peer read half remains writable after its write half closes");
        let received = supervisor
            .timeout(
                "test_portforward_peer_half_close",
                Duration::from_secs(1),
                peer,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&received, b"ping");
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
