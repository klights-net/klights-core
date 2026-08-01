use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use klights_node_api::{
    BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, ExecSetupError,
    NodeExec, NodeExecFrame, NodeExecFuture, NodeExecRequest, NodeExecRuntime, NodeExecSession,
    NodeExecSyncRequest, NodeExecSyncResult,
};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

const LOCAL_NODE_EXEC_FRAME_CAPACITY: usize = 128;

pub struct InProcessNodeExec {
    runtime: Arc<dyn NodeExecRuntime>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl InProcessNodeExec {
    pub fn new(
        runtime: Arc<dyn NodeExecRuntime>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            supervisor,
        })
    }
}

struct InProcessNodeExecSession {
    inbound: Mutex<mpsc::Receiver<NodeExecFrame>>,
    outbound: mpsc::Sender<NodeExecFrame>,
    cancelled: CancellationToken,
    locally_cancelled: AtomicBool,
}

fn in_process_exec_session_pair() -> (InProcessNodeExecSession, InProcessNodeExecSession) {
    let (api_to_runtime_tx, api_to_runtime_rx) = mpsc::channel(LOCAL_NODE_EXEC_FRAME_CAPACITY);
    let (runtime_to_api_tx, runtime_to_api_rx) = mpsc::channel(LOCAL_NODE_EXEC_FRAME_CAPACITY);
    let cancelled = CancellationToken::new();
    (
        InProcessNodeExecSession {
            inbound: Mutex::new(runtime_to_api_rx),
            outbound: api_to_runtime_tx,
            cancelled: cancelled.clone(),
            locally_cancelled: AtomicBool::new(false),
        },
        InProcessNodeExecSession {
            inbound: Mutex::new(api_to_runtime_rx),
            outbound: runtime_to_api_tx,
            cancelled,
            locally_cancelled: AtomicBool::new(false),
        },
    )
}

impl BoundedByteStream for InProcessNodeExecSession {
    type Frame = NodeExecFrame;

    fn bounds(&self) -> ByteStreamBounds {
        ByteStreamBounds::try_new(
            LOCAL_NODE_EXEC_FRAME_CAPACITY,
            LOCAL_NODE_EXEC_FRAME_CAPACITY,
        )
        .expect("local exec frame capacity is non-zero")
    }

    fn is_cancelled(&self) -> bool {
        self.locally_cancelled.load(Ordering::Acquire) || self.cancelled.is_cancelled()
    }

    fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            tokio::select! {
                _ = self.cancelled.cancelled() => Err(ByteStreamError::cancelled()),
                result = self.outbound.send(frame) => result.map_err(|_| {
                    ByteStreamError::closed("local node exec peer closed")
                }),
            }
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            let mut inbound = self.inbound.lock().await;
            tokio::select! {
                _ = self.cancelled.cancelled() => Err(ByteStreamError::cancelled()),
                frame = inbound.recv() => Ok(frame),
            }
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.locally_cancelled.swap(true, Ordering::AcqRel) {
                self.cancelled.cancel();
                self.inbound.get_mut().close();
            }
            Ok(())
        })
    }
}

impl NodeExec for InProcessNodeExec {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async move { Ok(self.runtime.exec_sync(request).await) })
    }

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async move {
            let (api_session, runtime_session) = in_process_exec_session_pair();
            let runtime = self.runtime.clone();
            self.supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Network,
                    "local_node_exec_runtime",
                    async move {
                        runtime
                            .exec_stream(request, Box::new(runtime_session))
                            .await;
                    },
                )
                .await
                .map_err(|error| ExecSetupError::unavailable(error.to_string()))?;
            Ok(Box::new(api_session) as Box<dyn NodeExecSession>)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_node_api::{
        ExecStreamChannel, ExecStreamOptions, ExecTerminalError, NodeExecTarget,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingExecRuntime {
        sync_requests: StdMutex<Vec<NodeExecSyncRequest>>,
        stream_requests: StdMutex<Vec<NodeExecRequest>>,
    }

    impl NodeExecRuntime for RecordingExecRuntime {
        fn exec_sync(
            &self,
            request: NodeExecSyncRequest,
        ) -> klights_node_api::NodeExecRuntimeFuture<'_, NodeExecSyncResult> {
            Box::pin(async move {
                self.sync_requests.lock().unwrap().push(request);
                NodeExecSyncResult::failed(
                    b"stdout".to_vec(),
                    b"stderr".to_vec(),
                    17,
                    ExecTerminalError::new("command failed"),
                )
            })
        }

        fn exec_stream(
            &self,
            request: NodeExecRequest,
            session: Box<dyn NodeExecSession>,
        ) -> klights_node_api::NodeExecRuntimeFuture<'_, ()> {
            Box::pin(async move {
                self.stream_requests.lock().unwrap().push(request);
                let stdin = session.recv_frame().await.unwrap().unwrap();
                assert_eq!(stdin.channel(), ExecStreamChannel::Stdin);
                session
                    .send_frame(NodeExecFrame::new(
                        ExecStreamChannel::Stdout,
                        stdin.data().to_vec(),
                        false,
                    ))
                    .await
                    .unwrap();

                let resize = session.recv_frame().await.unwrap().unwrap();
                assert_eq!(resize.channel(), ExecStreamChannel::Resize);
                session
                    .send_frame(NodeExecFrame::new(
                        ExecStreamChannel::Error,
                        br#"{"status":"Success"}"#.to_vec(),
                        true,
                    ))
                    .await
                    .unwrap();
            })
        }
    }

    fn exec_target() -> NodeExecTarget {
        NodeExecTarget::try_new("node-a", "default", "pod-a", "container-a").unwrap()
    }

    #[tokio::test]
    async fn in_process_exec_preserves_sync_and_stream_semantics() {
        let runtime = Arc::new(RecordingExecRuntime::default());
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_exec = InProcessNodeExec::new(runtime.clone(), supervisor.clone());

        let sync_request =
            NodeExecSyncRequest::try_new(exec_target(), vec!["false".into()], 12).unwrap();
        let result = node_exec.exec_sync(sync_request.clone()).await.unwrap();
        assert_eq!(result.stdout(), b"stdout");
        assert_eq!(result.stderr(), b"stderr");
        assert_eq!(result.exit_code(), 17);
        assert_eq!(result.terminal_error().unwrap().message(), "command failed");
        assert_eq!(
            runtime.sync_requests.lock().unwrap().as_slice(),
            &[sync_request]
        );

        let stream_request = NodeExecRequest::exec(
            exec_target(),
            vec!["sh".into()],
            ExecStreamOptions::new(true, true, false, true),
        );
        let mut session = node_exec.open_exec(stream_request.clone()).await.unwrap();
        assert_eq!(
            session.bounds(),
            ByteStreamBounds::try_new(
                LOCAL_NODE_EXEC_FRAME_CAPACITY,
                LOCAL_NODE_EXEC_FRAME_CAPACITY,
            )
            .unwrap()
        );
        session
            .send_frame(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                b"hello".to_vec(),
                false,
            ))
            .await
            .unwrap();
        session
            .send_frame(NodeExecFrame::new(
                ExecStreamChannel::Resize,
                br#"{"Width":120,"Height":40}"#.to_vec(),
                false,
            ))
            .await
            .unwrap();

        let stdout = tokio::time::timeout(Duration::from_secs(1), session.recv_frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            stdout,
            NodeExecFrame::new(ExecStreamChannel::Stdout, b"hello".to_vec(), false)
        );
        let terminal = tokio::time::timeout(Duration::from_secs(1), session.recv_frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(terminal.is_terminal());
        assert_eq!(
            runtime.stream_requests.lock().unwrap().as_slice(),
            &[stream_request]
        );

        session.cancel().await.unwrap();
        assert!(session.is_cancelled());
        supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn in_process_exec_session_is_bounded_and_cancellation_is_shared() {
        let (mut api, runtime) = in_process_exec_session_pair();
        for byte in 0..LOCAL_NODE_EXEC_FRAME_CAPACITY {
            api.send_frame(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                vec![byte as u8],
                false,
            ))
            .await
            .unwrap();
        }
        let blocked = tokio::time::timeout(
            Duration::from_millis(20),
            api.send_frame(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                b"blocked".to_vec(),
                false,
            )),
        )
        .await;
        assert!(
            blocked.is_err(),
            "frame beyond the bound did not backpressure"
        );

        api.cancel().await.unwrap();
        assert!(api.is_cancelled());
        assert!(runtime.is_cancelled());
        assert!(matches!(
            runtime.recv_frame().await,
            Err(ByteStreamError::Cancelled)
        ));
        api.cancel().await.unwrap();
    }
}
