use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Mutex;

use klights_node_api::{
    BoundedByteStream, ByteFrame, ByteStreamBounds, ByteStreamBoundsError, ByteStreamError,
    ByteStreamFuture, ExecSetupError, ExecStreamChannel, ExecStreamOptions, ExecTerminalError,
    NodeExec, NodeExecFrame, NodeExecFuture, NodeExecRequest, NodeExecRuntime,
    NodeExecRuntimeFuture, NodeExecSession, NodeExecSyncRequest, NodeExecSyncResult,
    NodeExecTarget,
};

struct FakeExecSession {
    bounds: ByteStreamBounds,
    frames: Mutex<VecDeque<NodeExecFrame>>,
    cancelled: bool,
}

impl FakeExecSession {
    fn new(bounds: ByteStreamBounds) -> Self {
        Self {
            bounds,
            frames: Mutex::new(VecDeque::new()),
            cancelled: false,
        }
    }
}

impl BoundedByteStream for FakeExecSession {
    type Frame = NodeExecFrame;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.cancelled {
                return Err(ByteStreamError::cancelled());
            }
            if self.frames.lock().unwrap().len() >= self.bounds.send_frames().get() {
                pending::<()>().await;
            }
            self.frames.lock().unwrap().push_back(frame);
            Ok(())
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
        Box::pin(async move {
            if self.cancelled {
                return Err(ByteStreamError::cancelled());
            }
            Ok(self.frames.lock().unwrap().pop_front())
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            self.cancelled = true;
            Ok(())
        })
    }
}

struct FakeNodeExec;

impl NodeExec for FakeNodeExec {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async move {
            let _ = request;
            Ok(NodeExecSyncResult::success(b"out".to_vec(), Vec::new(), 0))
        })
    }

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async move {
            let _ = request;
            Ok(Box::new(FakeExecSession::new(
                ByteStreamBounds::try_new(2, 3).unwrap(),
            )) as Box<dyn NodeExecSession>)
        })
    }
}

struct FakeRuntime;

impl NodeExecRuntime for FakeRuntime {
    fn exec_sync(
        &self,
        request: NodeExecSyncRequest,
    ) -> NodeExecRuntimeFuture<'_, NodeExecSyncResult> {
        Box::pin(async move {
            let _ = request;
            NodeExecSyncResult::success(Vec::new(), Vec::new(), 0)
        })
    }

    fn exec_stream(
        &self,
        request: NodeExecRequest,
        session: Box<dyn NodeExecSession>,
    ) -> NodeExecRuntimeFuture<'_, ()> {
        Box::pin(async move {
            let _ = (request, session);
        })
    }
}

fn assert_control_plane_object_safe(_: &dyn NodeExec) {}
fn assert_runtime_object_safe(_: &dyn NodeExecRuntime) {}

#[test]
fn ports_are_object_safe_and_contract_values_are_send_sync() {
    assert_control_plane_object_safe(&FakeNodeExec);
    assert_runtime_object_safe(&FakeRuntime);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NodeExecTarget>();
    assert_send_sync::<NodeExecSyncRequest>();
    assert_send_sync::<NodeExecSyncResult>();
    assert_send_sync::<NodeExecRequest>();
    assert_send_sync::<NodeExecFrame>();
    assert_send_sync::<ExecSetupError>();
    assert_send_sync::<ByteStreamError>();
    assert_send_sync::<ByteStreamBoundsError>();
    assert_send_sync::<ExecTerminalError>();
}

fn target() -> NodeExecTarget {
    NodeExecTarget::try_new("worker-a", "default", "shell", "containerd://abc")
        .expect("valid target")
}

#[test]
fn requests_are_validated_and_exec_attach_are_distinct() {
    for (field, result) in [
        (
            "exec.node_name",
            NodeExecTarget::try_new("", "default", "shell", "containerd://abc"),
        ),
        (
            "exec.namespace",
            NodeExecTarget::try_new("worker-a", "", "shell", "containerd://abc"),
        ),
        (
            "exec.pod_name",
            NodeExecTarget::try_new("worker-a", "default", "", "containerd://abc"),
        ),
        (
            "exec.container_id",
            NodeExecTarget::try_new("worker-a", "default", "shell", ""),
        ),
    ] {
        assert!(matches!(
            result,
            Err(ExecSetupError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }

    assert!(matches!(
        NodeExecSyncRequest::try_new(target(), vec!["true".into()], -1),
        Err(ExecSetupError::InvalidRequest {
            field: "exec.timeout_seconds",
            ..
        })
    ));

    let options = ExecStreamOptions::new(true, true, true, false);
    let exec = NodeExecRequest::exec(target(), vec!["sh".into()], options);
    assert!(!exec.is_attach());
    assert_eq!(exec.command(), &["sh"]);

    let attach = NodeExecRequest::attach(target(), options);
    assert!(attach.is_attach());
    assert!(attach.command().is_empty());
}

#[test]
fn byte_frames_preserve_channels_and_terminal_status_semantics() {
    let cases = [
        (ExecStreamChannel::Stdin, "stdin"),
        (ExecStreamChannel::Stdout, "stdout"),
        (ExecStreamChannel::Stderr, "stderr"),
        (ExecStreamChannel::Error, "error"),
        (ExecStreamChannel::Resize, "resize"),
    ];
    for (channel, wire_name) in cases {
        assert_eq!(channel.as_wire_name(), wire_name);
        assert_eq!(
            ExecStreamChannel::try_from_wire_name(wire_name),
            Some(channel)
        );
    }
    assert_eq!(ExecStreamChannel::try_from_wire_name("unknown"), None);

    let status = br#"{"status":"Failure","message":"boom"}"#.to_vec();
    let terminal = NodeExecFrame::new(ExecStreamChannel::Error, status, false);
    assert!(terminal.is_terminal());
    assert!(!NodeExecFrame::new(ExecStreamChannel::Stdout, b"ok".to_vec(), true).is_terminal());
    assert!(NodeExecFrame::new(ExecStreamChannel::Error, Vec::new(), true).is_terminal());

    for (payload, expected) in [
        (br#"{ "status" : "Success" }"#.as_slice(), true),
        (br#"{"status":"Failure"}"#.as_slice(), true),
        (br#"{"nested":{"status":"Failure"}}"#.as_slice(), false),
        (
            br#"{"message":"\\\"status\\\":\\\"Failure\\\""}"#.as_slice(),
            false,
        ),
        (br#"{"status":"Unknown"}"#.as_slice(), false),
        (br#"not-json"#.as_slice(), false),
    ] {
        assert_eq!(
            NodeExecFrame::new(ExecStreamChannel::Error, payload.to_vec(), false).is_terminal(),
            expected,
            "payload={}",
            String::from_utf8_lossy(payload)
        );
    }
}

#[test]
fn stream_contract_requires_finite_bounds_and_exposes_backpressure_and_cancellation() {
    for (send, receive, field) in [
        (0, 1, "byte_stream.send_frames"),
        (1, 0, "byte_stream.receive_frames"),
    ] {
        assert!(matches!(
            ByteStreamBounds::try_new(send, receive),
            Err(error) if error.field() == field
        ));
    }

    let bounds = ByteStreamBounds::try_new(1, 1).unwrap();
    let session = FakeExecSession::new(bounds);
    assert_eq!(session.bounds(), bounds);

    let first = session.send_frame(NodeExecFrame::new(
        ExecStreamChannel::Stdout,
        b"first".to_vec(),
        false,
    ));
    assert!(matches!(poll_once(first), std::task::Poll::Ready(Ok(()))));

    let blocked = session.send_frame(NodeExecFrame::new(
        ExecStreamChannel::Stdout,
        b"second".to_vec(),
        false,
    ));
    assert!(matches!(poll_once(blocked), std::task::Poll::Pending));

    let mut cancelled = FakeExecSession::new(bounds);
    assert!(matches!(
        poll_once(cancelled.cancel()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert!(cancelled.is_cancelled());
    assert!(matches!(
        poll_once(cancelled.recv_frame()),
        std::task::Poll::Ready(Err(ByteStreamError::Cancelled))
    ));
}

#[derive(Debug)]
struct NeutralFrame(Vec<u8>);

impl ByteFrame for NeutralFrame {
    fn payload(&self) -> &[u8] {
        &self.0
    }
}

struct NeutralSession {
    bounds: ByteStreamBounds,
    frame: Mutex<Option<NeutralFrame>>,
    cancelled: bool,
}

impl BoundedByteStream for NeutralSession {
    type Frame = NeutralFrame;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn send_frame(&self, frame: NeutralFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            *self.frame.lock().unwrap() = Some(frame);
            Ok(())
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NeutralFrame>> {
        Box::pin(async move { Ok(self.frame.lock().unwrap().take()) })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            self.cancelled = true;
            Ok(())
        })
    }
}

#[test]
fn bounded_byte_stream_is_reusable_without_an_exec_channel_enum() {
    fn assert_neutral_object_safe(_: &dyn BoundedByteStream<Frame = NeutralFrame>) {}

    let session = NeutralSession {
        bounds: ByteStreamBounds::try_new(1, 1).unwrap(),
        frame: Mutex::new(None),
        cancelled: false,
    };
    assert_neutral_object_safe(&session);
    assert!(matches!(
        poll_once(session.send_frame(NeutralFrame(b"neutral".to_vec()))),
        std::task::Poll::Ready(Ok(()))
    ));
    let frame = match poll_once(session.recv_frame()) {
        std::task::Poll::Ready(Ok(Some(frame))) => frame,
        other => panic!("expected neutral byte frame, got {other:?}"),
    };
    assert_eq!(frame.payload(), b"neutral");
    assert_eq!(
        ByteStreamError::cancelled().to_string(),
        "byte stream was cancelled"
    );
}

fn poll_once<T>(future: Pin<Box<dyn Future<Output = T> + Send + '_>>) -> std::task::Poll<T> {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut future = future;
    future.as_mut().poll(&mut context)
}

#[test]
fn unary_result_preserves_bytes_exit_code_and_terminal_error() {
    let error = ExecTerminalError::new("runtime unavailable");
    let result = NodeExecSyncResult::failed(Vec::new(), b"stderr".to_vec(), 126, error.clone());
    assert_eq!(result.stdout(), b"");
    assert_eq!(result.stderr(), b"stderr");
    assert_eq!(result.exit_code(), 126);
    assert_eq!(result.terminal_error(), Some(&error));

    let (stdout, stderr, exit_code, terminal_error) = result.into_parts();
    assert!(stdout.is_empty());
    assert_eq!(stderr, b"stderr");
    assert_eq!(exit_code, 126);
    assert_eq!(terminal_error.unwrap().message(), "runtime unavailable");
}

#[test]
fn setup_errors_keep_exact_validation_routing_collision_and_timeout_categories() {
    let cases = [
        (
            ExecSetupError::invalid("exec.node_name", "must not be empty"),
            "invalid exec.node_name: must not be empty",
        ),
        (
            ExecSetupError::unavailable("worker is disconnected"),
            "worker is disconnected",
        ),
        (
            ExecSetupError::duplicate_session("session collision"),
            "session collision",
        ),
        (
            ExecSetupError::timeout("exec timed out after 300s"),
            "exec timed out after 300s",
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
    }
}
