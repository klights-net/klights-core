use std::sync::Arc;

use klights_node_api::{
    BoundedByteStream, NodeLog, NodeLogEvent, NodeLogFuture, NodeLogRequest, NodeLogResult,
    NodeLogRuntime, NodeLogTerminalError,
};

/// Root composition adapter from the node-local runtime capability to the
/// control-plane Pod log capability consumed by the native API service.
pub(crate) struct RuntimeNodeLogAdapter {
    runtime: Arc<dyn NodeLogRuntime>,
}

impl RuntimeNodeLogAdapter {
    pub(crate) fn new(runtime: Arc<dyn NodeLogRuntime>) -> Self {
        Self { runtime }
    }
}

pub(crate) fn pod_log_capabilities(
    local_http: Arc<dyn NodeLogRuntime>,
    local_websocket: Arc<dyn NodeLogRuntime>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    local_node_name: impl Into<String>,
) -> Arc<k8s_native_service::subresources::pod::logs::PodLogCapabilities> {
    Arc::new(
        k8s_native_service::subresources::pod::logs::PodLogCapabilities::new(
            Arc::new(RuntimeNodeLogAdapter::new(local_http)),
            Arc::new(RuntimeNodeLogAdapter::new(local_websocket)),
            task_supervisor,
            local_node_name,
        ),
    )
}

impl NodeLog for RuntimeNodeLogAdapter {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        Box::pin(async move {
            let result = self.runtime.read_logs(request).await?;
            let (content, terminal_error) = result.into_parts();
            match terminal_error {
                // The local runtime keeps its historical private Debug
                // representation for worker RPC consumers. The old local HTTP
                // handler exposed only LogReadError::client_message(), so the
                // root API adapter restores that exact public message.
                Some(error)
                    if error.message() == r#"Internal("failed to read container logs")"# =>
                {
                    Ok(NodeLogResult::failed(
                        content,
                        NodeLogTerminalError::new("failed to read container logs"),
                    ))
                }
                Some(error) => Ok(NodeLogResult::failed(content, error)),
                None => Ok(NodeLogResult::success(content)),
            }
        })
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        self.runtime.open_logs(request)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use klights_node_api::{
        ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeLogOptions, NodeLogTarget,
        NodeLogTerminalError,
    };

    use super::*;

    struct RecordedRuntime {
        requests: Mutex<Vec<NodeLogRequest>>,
    }

    impl RecordedRuntime {
        fn new() -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl NodeLogRuntime for RecordedRuntime {
        fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
            self.requests.lock().unwrap().push(request.clone());
            Box::pin(async move {
                Ok(NodeLogResult::failed(
                    b"partial\n".to_vec(),
                    NodeLogTerminalError::new("finite terminal"),
                ))
            })
        }

        fn open_logs(
            &self,
            request: NodeLogRequest,
        ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
            self.requests.lock().unwrap().push(request.clone());
            Box::pin(async move {
                Ok(Box::new(RecordedStream::new([
                    NodeLogEvent::data(b"follow\n".to_vec()),
                    NodeLogEvent::failed(Vec::new(), NodeLogTerminalError::new("follow terminal")),
                ]))
                    as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
            })
        }
    }

    struct RecordedStream {
        events: Mutex<VecDeque<NodeLogEvent>>,
        cancelled: bool,
    }

    impl RecordedStream {
        fn new(events: impl IntoIterator<Item = NodeLogEvent>) -> Self {
            Self {
                events: Mutex::new(events.into_iter().collect()),
                cancelled: false,
            }
        }
    }

    impl BoundedByteStream for RecordedStream {
        type Frame = NodeLogEvent;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(2, 2).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled
        }

        fn send_frame(&self, _frame: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
            Box::pin(async { Err(ByteStreamError::closed("receive-only test stream")) })
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
            Box::pin(async move { Ok(self.events.lock().unwrap().pop_front()) })
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.cancelled = true;
                Ok(())
            })
        }
    }

    fn request(follow: &str) -> NodeLogRequest {
        NodeLogRequest::new(
            NodeLogTarget::try_new("node-a", "default", "pod-a", "uid-a", "main").unwrap(),
            NodeLogOptions::new(
                Some(follow.to_string()),
                Some(20),
                Some("true".to_string()),
                Some("2026-08-02T00:00:00Z".to_string()),
                Some(30),
                Some(4096),
                Some("false".to_string()),
            ),
        )
    }

    #[tokio::test]
    async fn phase17c_root_node_log_adapter_preserves_finite_request_and_terminal_result() {
        let runtime = Arc::new(RecordedRuntime::new());
        let adapter = RuntimeNodeLogAdapter::new(runtime.clone());
        let expected = request("false");

        let result = adapter.read_logs(expected.clone()).await.unwrap();

        assert_eq!(runtime.requests.lock().unwrap().as_slice(), &[expected]);
        assert_eq!(result.content(), b"partial\n");
        assert_eq!(
            result.terminal_error().map(|error| error.message()),
            Some("finite terminal")
        );
    }

    struct LocalReadFailureRuntime;

    impl NodeLogRuntime for LocalReadFailureRuntime {
        fn read_logs(&self, _request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
            Box::pin(async {
                Ok(NodeLogResult::failed(
                    Vec::new(),
                    NodeLogTerminalError::new(r#"Internal("failed to read container logs")"#),
                ))
            })
        }

        fn open_logs(
            &self,
            _request: NodeLogRequest,
        ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
            Box::pin(async { unreachable!("finite failure test does not open a stream") })
        }
    }

    #[tokio::test]
    async fn phase17c_root_node_log_adapter_preserves_local_http_read_error_message() {
        let adapter = RuntimeNodeLogAdapter::new(Arc::new(LocalReadFailureRuntime));

        let result = adapter.read_logs(request("false")).await.unwrap();

        assert_eq!(
            result.terminal_error().map(|error| error.message()),
            Some("failed to read container logs")
        );
    }

    #[tokio::test]
    async fn phase17c_root_node_log_adapter_preserves_follow_request_and_stream_events() {
        let runtime = Arc::new(RecordedRuntime::new());
        let adapter = RuntimeNodeLogAdapter::new(runtime.clone());
        let expected = request("true");

        let mut stream = adapter.open_logs(expected.clone()).await.unwrap();
        let data = stream.recv_frame().await.unwrap().unwrap();
        let terminal = stream.recv_frame().await.unwrap().unwrap();

        assert_eq!(runtime.requests.lock().unwrap().as_slice(), &[expected]);
        assert_eq!(data.content(), b"follow\n");
        assert!(!data.is_terminal());
        assert!(terminal.is_terminal());
        assert_eq!(
            terminal.terminal_error().map(|error| error.message()),
            Some("follow terminal")
        );
        stream.cancel().await.unwrap();
        assert!(stream.is_cancelled());
    }

    #[test]
    fn phase17c_root_node_log_adapter_remains_a_node_log_port() {
        fn assert_port(_: &dyn NodeLog) {}

        assert_port(&RuntimeNodeLogAdapter::new(
            Arc::new(RecordedRuntime::new()),
        ));
    }
}
