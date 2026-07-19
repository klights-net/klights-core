use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Mutex;

use klights_node_api::{
    BoundedByteStream, ByteFrame, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeLog,
    NodeLogEvent, NodeLogFuture, NodeLogOptions, NodeLogRequest, NodeLogResult, NodeLogRuntime,
    NodeLogSetupError, NodeLogTarget, NodeLogTerminalError,
};

struct FakeLogStream {
    bounds: ByteStreamBounds,
    events: Mutex<VecDeque<NodeLogEvent>>,
    cancelled: bool,
}

impl FakeLogStream {
    fn new(bounds: ByteStreamBounds, events: impl IntoIterator<Item = NodeLogEvent>) -> Self {
        Self {
            bounds,
            events: Mutex::new(events.into_iter().collect()),
            cancelled: false,
        }
    }
}

impl BoundedByteStream for FakeLogStream {
    type Frame = NodeLogEvent;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn send_frame(&self, event: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.cancelled {
                return Err(ByteStreamError::cancelled());
            }
            if self.events.lock().unwrap().len() >= self.bounds.send_frames().get() {
                pending::<()>().await;
            }
            self.events.lock().unwrap().push_back(event);
            Ok(())
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
        Box::pin(async move {
            if self.cancelled {
                return Err(ByteStreamError::cancelled());
            }
            Ok(self.events.lock().unwrap().pop_front())
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            self.cancelled = true;
            Ok(())
        })
    }
}

struct FakeNodeLog;

impl NodeLog for FakeNodeLog {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        Box::pin(async move {
            if request.options().previous() == Some("true") {
                return Ok(NodeLogResult::success(Vec::new()));
            }
            Ok(NodeLogResult::success(b"finite\n".to_vec()))
        })
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        Box::pin(async move {
            let events = if request.options().previous() == Some("true") {
                vec![NodeLogEvent::terminal()]
            } else {
                vec![
                    NodeLogEvent::data(b"follow\n".to_vec()),
                    NodeLogEvent::terminal(),
                ]
            };
            Ok(Box::new(FakeLogStream::new(
                ByteStreamBounds::try_new(2, 2).unwrap(),
                events,
            ))
                as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
        })
    }
}

struct FakeNodeLogRuntime;

impl NodeLogRuntime for FakeNodeLogRuntime {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        FakeNodeLog.read_logs(request)
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        FakeNodeLog.open_logs(request)
    }
}

fn target() -> NodeLogTarget {
    NodeLogTarget::try_new("worker-a", "default", "logger", "pod-uid", "main")
        .expect("valid log target")
}

fn assert_control_plane_object_safe(_: &dyn NodeLog) {}
fn assert_runtime_object_safe(_: &dyn NodeLogRuntime) {}

#[test]
fn ports_are_object_safe_and_contract_values_are_send_sync() {
    assert_control_plane_object_safe(&FakeNodeLog);
    assert_runtime_object_safe(&FakeNodeLogRuntime);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NodeLogTarget>();
    assert_send_sync::<NodeLogOptions>();
    assert_send_sync::<NodeLogRequest>();
    assert_send_sync::<NodeLogResult>();
    assert_send_sync::<NodeLogEvent>();
    assert_send_sync::<NodeLogSetupError>();
    assert_send_sync::<NodeLogTerminalError>();
}

#[test]
fn request_preserves_every_existing_log_query_field() {
    let options = NodeLogOptions::new(
        Some("true".to_string()),
        Some(200),
        Some("true".to_string()),
        Some("2026-07-19T00:00:00Z".to_string()),
        Some(45),
        Some(4096),
        Some("false".to_string()),
    );
    let request = NodeLogRequest::new(target(), options.clone());

    assert_eq!(request.target().node_name(), "worker-a");
    assert_eq!(request.target().namespace(), "default");
    assert_eq!(request.target().pod_name(), "logger");
    assert_eq!(request.target().pod_uid(), "pod-uid");
    assert_eq!(request.target().container_name(), "main");
    assert_eq!(request.options(), &options);
    assert_eq!(options.follow(), Some("true"));
    assert_eq!(options.tail_lines(), Some(200));
    assert_eq!(options.timestamps(), Some("true"));
    assert_eq!(options.since_time(), Some("2026-07-19T00:00:00Z"));
    assert_eq!(options.since_seconds(), Some(45));
    assert_eq!(options.limit_bytes(), Some(4096));
    assert_eq!(options.previous(), Some("false"));
}

#[test]
fn request_rejects_empty_identity_without_runtime_or_transport_types() {
    for (field, result) in [
        (
            "log.node_name",
            NodeLogTarget::try_new("", "default", "logger", "pod-uid", "main"),
        ),
        (
            "log.namespace",
            NodeLogTarget::try_new("worker-a", "", "logger", "pod-uid", "main"),
        ),
        (
            "log.pod_name",
            NodeLogTarget::try_new("worker-a", "default", "", "pod-uid", "main"),
        ),
        (
            "log.pod_uid",
            NodeLogTarget::try_new("worker-a", "default", "logger", "", "main"),
        ),
        (
            "log.container_name",
            NodeLogTarget::try_new("worker-a", "default", "logger", "pod-uid", ""),
        ),
    ] {
        assert!(matches!(
            result,
            Err(NodeLogSetupError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn finite_and_follow_results_preserve_bytes_terminal_and_error_semantics() {
    let finite = NodeLogResult::failed(
        b"partial\n".to_vec(),
        NodeLogTerminalError::new("runtime read failed"),
    );
    assert_eq!(finite.content(), b"partial\n");
    assert_eq!(
        finite.terminal_error().map(NodeLogTerminalError::message),
        Some("runtime read failed")
    );

    let data = NodeLogEvent::data(b"chunk\n".to_vec());
    assert_eq!(data.payload(), b"chunk\n");
    assert!(!data.is_terminal());
    assert!(data.terminal_error().is_none());

    let terminal = NodeLogEvent::terminal();
    assert!(terminal.payload().is_empty());
    assert!(terminal.is_terminal());
    assert!(terminal.terminal_error().is_none());

    let final_data = NodeLogEvent::complete(b"final\n".to_vec());
    assert_eq!(final_data.payload(), b"final\n");
    assert!(final_data.is_terminal());

    let failed = NodeLogEvent::failed(
        b"partial tail\n".to_vec(),
        NodeLogTerminalError::new("follow failed"),
    );
    assert_eq!(failed.payload(), b"partial tail\n");
    assert!(failed.is_terminal());
    assert_eq!(
        failed.terminal_error().map(NodeLogTerminalError::message),
        Some("follow failed")
    );
}

#[test]
fn previous_true_freezes_existing_empty_finite_and_follow_behavior() {
    let request = NodeLogRequest::new(
        target(),
        NodeLogOptions::new(
            Some("true".to_string()),
            None,
            None,
            None,
            None,
            None,
            Some("true".to_string()),
        ),
    );

    let finite = poll_ready(FakeNodeLog.read_logs(request.clone())).unwrap();
    assert!(finite.content().is_empty());
    assert!(finite.terminal_error().is_none());

    let stream = poll_ready(FakeNodeLog.open_logs(request)).unwrap();
    let terminal = poll_ready(stream.recv_frame()).unwrap().unwrap();
    assert!(terminal.payload().is_empty());
    assert!(terminal.is_terminal());
    assert!(terminal.terminal_error().is_none());
}

fn poll_ready<T, E>(
    mut future: Pin<Box<dyn Future<Output = Result<T, E>> + Send + '_>>,
) -> Result<T, E> {
    use std::task::{Context, Poll, Waker};

    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("contract fake unexpectedly pending"),
    }
}
