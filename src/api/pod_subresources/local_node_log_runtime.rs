use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use klights_node_api::{
    BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeLogEvent,
    NodeLogFuture, NodeLogRequest, NodeLogResult, NodeLogRuntime, NodeLogTarget,
    NodeLogTerminalError,
};
use klights_supervisor::{TaskCategory, TaskSupervisor};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use super::logs::{
    LogQuery, PodLogFollowTermination, PodLogFollowWatchSource, build_log_output_bytes_at,
    build_pod_log_follow_event_cursor, follow_log_file_with_initial_query_at,
    follow_log_file_with_termination_watch_at,
};

const NODE_LOG_STREAM_FRAME_CHANNEL_CAPACITY: usize = 128;

/// Root-constructed local node-log adapter.
///
/// Replication consumes only the transport-neutral [`NodeLogRuntime`] port;
/// filesystem layout and API watch semantics remain owned by the pod-log
/// adapter.
pub(crate) struct LocalNodeLogRuntime {
    pod_logs_root: PathBuf,
    task_supervisor: Arc<TaskSupervisor>,
    clock: Arc<dyn klights_auth::clock::Clock>,
    pod_log_follow_watch: Option<PodLogFollowWatchSource>,
}

impl LocalNodeLogRuntime {
    #[cfg(test)]
    pub(crate) fn new(containerd_namespace: String, task_supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            pod_logs_root: std::env::temp_dir()
                .join("klights-api-tests")
                .join(containerd_namespace)
                .join("logs")
                .join("pods"),
            task_supervisor,
            clock: Arc::new(klights_auth::clock::SystemClock),
            pod_log_follow_watch: None,
        }
    }

    pub(crate) fn new_with_pod_event_store(
        pod_logs_root: PathBuf,
        task_supervisor: Arc<TaskSupervisor>,
        clock: Arc<dyn klights_auth::clock::Clock>,
        pod_log_follow_watch: PodLogFollowWatchSource,
    ) -> Self {
        Self {
            pod_logs_root,
            task_supervisor,
            clock,
            pod_log_follow_watch: Some(pod_log_follow_watch),
        }
    }

    fn log_path(&self, target: &NodeLogTarget) -> String {
        self.pod_logs_root
            .join(format!(
                "{}_{}_{}",
                target.namespace(),
                target.pod_name(),
                target.pod_uid()
            ))
            .join(target.container_name())
            .join("0.log")
            .to_string_lossy()
            .into_owned()
    }
}

impl NodeLogRuntime for LocalNodeLogRuntime {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        let (target, options) = request.into_parts();
        let log_path = self.log_path(&target);
        let (follow, tail_lines, timestamps, since_time, since_seconds, limit_bytes, previous) =
            options.into_parts();
        let is_previous = previous.as_deref() == Some("true");
        let params = LogQuery {
            container: Some(target.container_name().to_string()),
            follow,
            tail_lines,
            timestamps,
            since_seconds,
            since_time,
            limit_bytes,
            previous,
            insecure_skip_tls_verify_backend: false,
        };
        let operation_now = self.clock.now();

        Box::pin(async move {
            if is_previous {
                return Ok(NodeLogResult::success(Vec::new()));
            }
            match build_log_output_bytes_at(
                &log_path,
                &params,
                self.task_supervisor.as_ref(),
                operation_now,
            )
            .await
            {
                Ok(content) => Ok(NodeLogResult::success(content.to_vec())),
                Err(error) => Ok(NodeLogResult::failed(
                    Vec::new(),
                    NodeLogTerminalError::new(format!("{error:?}")),
                )),
            }
        })
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        let (target, options) = request.into_parts();
        let log_path = self.log_path(&target);
        let (follow, tail_lines, timestamps, since_time, since_seconds, limit_bytes, previous) =
            options.into_parts();
        let params = LogQuery {
            container: Some(target.container_name().to_string()),
            follow: Some(follow.unwrap_or_else(|| "true".to_string())),
            tail_lines,
            timestamps,
            since_seconds,
            since_time,
            limit_bytes,
            previous,
            insecure_skip_tls_verify_backend: false,
        };
        let namespace = target.namespace().to_string();
        let pod_name = target.pod_name().to_string();
        let pod_uid = target.pod_uid().to_string();
        let container_name = target.container_name().to_string();
        let operation_now = self.clock.now();

        if params.previous.as_deref() == Some("true") {
            return Box::pin(async move {
                let (tx, rx) = mpsc::channel(NODE_LOG_STREAM_FRAME_CHANNEL_CAPACITY);
                let _ = tx.send(NodeLogEvent::terminal()).await;
                Ok(Box::new(LocalPodLogStreamSession {
                    inbound_rx: Mutex::new(rx),
                    producer_cancel: CancellationToken::new(),
                    cancelled: AtomicBool::new(false),
                })
                    as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
            });
        }

        let pod_log_follow_watch = self.pod_log_follow_watch.clone();
        let task_supervisor = self.task_supervisor.clone();
        let producer_supervisor = task_supervisor.clone();
        let (tx, rx) = mpsc::channel(NODE_LOG_STREAM_FRAME_CHANNEL_CAPACITY);
        let log_tx = tx.clone();
        let producer_cancel = CancellationToken::new();
        let task_cancel = producer_cancel.clone();
        let log_task = async move {
            let mut inbound = if let Some(pod_log_follow_watch) = pod_log_follow_watch {
                let pod_events =
                    match build_pod_log_follow_event_cursor(&pod_log_follow_watch).await {
                        Ok(pod_events) => pod_events,
                        Err(error) => {
                            let _ = log_tx
                                .send(NodeLogEvent::failed(
                                    Vec::new(),
                                    NodeLogTerminalError::new(format!(
                                        "failed to open Pod log follow watch: {error}"
                                    )),
                                ))
                                .await;
                            return;
                        }
                    };
                let termination = PodLogFollowTermination::new(
                    pod_events,
                    namespace,
                    pod_name,
                    pod_uid,
                    container_name,
                    false,
                );
                Box::pin(follow_log_file_with_termination_watch_at(
                    log_path,
                    params,
                    producer_supervisor.clone(),
                    termination,
                    operation_now,
                ))
                    as Pin<
                        Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>,
                    >
            } else {
                Box::pin(follow_log_file_with_initial_query_at(
                    log_path,
                    params,
                    producer_supervisor.clone(),
                    operation_now,
                ))
            };
            loop {
                let item = tokio::select! {
                    biased;
                    _ = task_cancel.cancelled() => return,
                    item = inbound.next() => item,
                };
                let Some(item) = item else {
                    break;
                };
                match item {
                    Ok(log_content) => {
                        let send = log_tx.send(NodeLogEvent::data(log_content.to_vec()));
                        if tokio::select! {
                            biased;
                            _ = task_cancel.cancelled() => true,
                            result = send => result.is_err(),
                        } {
                            return;
                        }
                    }
                    Err(error) => {
                        let send = log_tx.send(NodeLogEvent::failed(
                            Vec::new(),
                            NodeLogTerminalError::new(error.to_string()),
                        ));
                        tokio::select! {
                            biased;
                            _ = task_cancel.cancelled() => {},
                            _ = send => {},
                        }
                        return;
                    }
                }
            }
            let send = log_tx.send(NodeLogEvent::terminal());
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => {},
                _ = send => {},
            }
        };

        Box::pin(async move {
            if let Err(error) = task_supervisor
                .spawn_async(TaskCategory::Network, "local_pod_log_follow", log_task)
                .await
            {
                let _ = tx
                    .send(NodeLogEvent::failed(
                        Vec::new(),
                        NodeLogTerminalError::new(error.to_string()),
                    ))
                    .await;
            }
            Ok(Box::new(LocalPodLogStreamSession {
                inbound_rx: Mutex::new(rx),
                producer_cancel,
                cancelled: AtomicBool::new(false),
            })
                as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
        })
    }
}

struct LocalPodLogStreamSession {
    inbound_rx: Mutex<mpsc::Receiver<NodeLogEvent>>,
    producer_cancel: CancellationToken,
    cancelled: AtomicBool,
}

impl BoundedByteStream for LocalPodLogStreamSession {
    type Frame = NodeLogEvent;

    fn bounds(&self) -> ByteStreamBounds {
        ByteStreamBounds::try_new(
            NODE_LOG_STREAM_FRAME_CHANNEL_CAPACITY,
            NODE_LOG_STREAM_FRAME_CHANNEL_CAPACITY,
        )
        .expect("log stream capacities are non-zero constants")
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn send_frame(&self, _frame: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move { Err(ByteStreamError::closed("pod log stream is receive-only")) })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            let frame = self.inbound_rx.lock().await.recv().await;
            match frame {
                Some(frame) => {
                    if frame.is_terminal() {
                        self.cancelled.store(true, Ordering::Release);
                        self.producer_cancel.cancel();
                    }
                    Ok(Some(frame))
                }
                None => {
                    self.cancelled.store(true, Ordering::Release);
                    self.producer_cancel.cancel();
                    Ok(None)
                }
            }
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancelled.swap(true, Ordering::AcqRel) {
                self.producer_cancel.cancel();
                self.inbound_rx.get_mut().close();
            }
            Ok(())
        })
    }
}

impl Drop for LocalPodLogStreamSession {
    fn drop(&mut self) {
        self.producer_cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_log_session_cancel_and_drop_stop_the_owned_producer() {
        for explicit_cancel in [false, true] {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let producer_cancel = CancellationToken::new();
            let mut session = LocalPodLogStreamSession {
                inbound_rx: Mutex::new(rx),
                producer_cancel: producer_cancel.clone(),
                cancelled: AtomicBool::new(false),
            };

            if explicit_cancel {
                session.cancel().await.unwrap();
                session.cancel().await.unwrap();
                assert!(session.is_cancelled());
            } else {
                drop(session);
            }
            assert!(producer_cancel.is_cancelled());
        }
    }
}
