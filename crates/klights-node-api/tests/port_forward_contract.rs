use std::collections::VecDeque;
use std::future::{Future, pending};
use std::sync::Mutex;

use bytes::Bytes;
use klights_node_api::{
    BoundedByteStream, ByteFrame, ByteStreamBounds, ByteStreamError, ByteStreamFuture,
    NodePortForward, NodePortForwardChannel, NodePortForwardFrame, NodePortForwardFuture,
    NodePortForwardRequest, NodePortForwardRuntime, NodePortForwardSession,
    NodePortForwardSetupError, NodePortForwardTarget,
};

struct FakePortForwardSession {
    bounds: ByteStreamBounds,
    frames: Mutex<VecDeque<NodePortForwardFrame>>,
    cancelled: bool,
}

impl FakePortForwardSession {
    fn new(bounds: ByteStreamBounds) -> Self {
        Self {
            bounds,
            frames: Mutex::new(VecDeque::new()),
            cancelled: false,
        }
    }
}

impl BoundedByteStream for FakePortForwardSession {
    type Frame = NodePortForwardFrame;

    fn bounds(&self) -> ByteStreamBounds {
        self.bounds
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn send_frame(&self, frame: NodePortForwardFrame) -> ByteStreamFuture<'_, ()> {
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

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodePortForwardFrame>> {
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

struct FakePortForward;

impl NodePortForward for FakePortForward {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        Box::pin(async move {
            let _ = request;
            Ok(Box::new(FakePortForwardSession::new(
                ByteStreamBounds::try_new(64, 64).unwrap(),
            )) as Box<dyn NodePortForwardSession>)
        })
    }
}

impl NodePortForwardRuntime for FakePortForward {
    fn open_port_forward(
        &self,
        request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        NodePortForward::open_port_forward(self, request)
    }
}

fn target() -> NodePortForwardTarget {
    NodePortForwardTarget::try_new("default", "web", "10.42.0.17")
        .expect("valid port-forward target")
}

fn request() -> NodePortForwardRequest {
    NodePortForwardRequest::try_new(target(), vec![8080, 9090]).expect("valid port-forward request")
}

fn assert_control_plane_object_safe(_: &dyn NodePortForward) {}
fn assert_runtime_object_safe(_: &dyn NodePortForwardRuntime) {}

#[test]
fn ports_are_object_safe_and_contract_values_are_send_sync() {
    assert_control_plane_object_safe(&FakePortForward);
    assert_runtime_object_safe(&FakePortForward);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NodePortForwardTarget>();
    assert_send_sync::<NodePortForwardRequest>();
    assert_send_sync::<NodePortForwardChannel>();
    assert_send_sync::<NodePortForwardFrame>();
    assert_send_sync::<NodePortForwardSetupError>();
}

#[test]
fn request_preserves_target_and_ordered_port_indices() {
    let request = request();
    assert_eq!(request.target().namespace(), "default");
    assert_eq!(request.target().pod_name(), "web");
    assert_eq!(request.target().pod_ip(), "10.42.0.17");
    assert_eq!(request.ports(), &[8080, 9090]);

    let (target, ports) = request.into_parts();
    assert_eq!(
        target.into_parts(),
        ("default".into(), "web".into(), "10.42.0.17".into())
    );
    assert_eq!(ports, vec![8080, 9090]);
}

#[test]
fn request_validation_rejects_only_preexisting_invalid_setup_shapes() {
    for (field, result) in [
        (
            "port_forward.namespace",
            NodePortForwardTarget::try_new("", "web", "10.42.0.17"),
        ),
        (
            "port_forward.pod_name",
            NodePortForwardTarget::try_new("default", "", "10.42.0.17"),
        ),
        (
            "port_forward.pod_ip",
            NodePortForwardTarget::try_new("default", "web", ""),
        ),
    ] {
        assert!(matches!(
            result,
            Err(NodePortForwardSetupError::InvalidRequest { field: actual, .. }) if actual == field
        ));
    }

    assert!(matches!(
        NodePortForwardRequest::try_new(target(), Vec::new()),
        Err(NodePortForwardSetupError::InvalidRequest {
            field: "port_forward.ports",
            ..
        })
    ));

    // Port zero was accepted by the existing query parser and reached TCP
    // setup. The refactor must not silently introduce stricter behavior.
    assert_eq!(
        NodePortForwardRequest::try_new(target(), vec![0])
            .unwrap()
            .ports(),
        &[0]
    );
}

#[test]
fn request_port_count_preserves_the_u8_channel_boundary_without_wraparound() {
    let maximum = NodePortForwardRequest::try_new(target(), vec![8080; 128])
        .expect("128 ports map exactly to channels 0 through 255");
    assert_eq!(maximum.ports().len(), 128);

    assert!(matches!(
        NodePortForwardRequest::try_new(target(), vec![8080; 129]),
        Err(NodePortForwardSetupError::InvalidRequest {
            field: "port_forward.ports",
            ..
        })
    ));
}

#[test]
fn frames_preserve_port_index_channel_bytes_and_error_semantics() {
    let data = NodePortForwardFrame::data(1, Bytes::from_static(b"request"));
    assert_eq!(data.port_index(), 1);
    assert_eq!(data.channel(), NodePortForwardChannel::Data);
    assert_eq!(data.payload(), b"request");
    assert!(!data.is_error());

    let error = NodePortForwardFrame::error(0, Bytes::from_static(b"connection refused"));
    assert_eq!(error.port_index(), 0);
    assert_eq!(error.channel(), NodePortForwardChannel::Error);
    assert_eq!(error.payload(), b"connection refused");
    assert!(error.is_error());

    assert_eq!(
        error.into_parts(),
        (
            0,
            NodePortForwardChannel::Error,
            Bytes::from_static(b"connection refused")
        )
    );
}

#[test]
fn session_bounds_include_one_exact_shared_frame_and_byte_budget() {
    let bounds = ByteStreamBounds::try_new_with_bytes(64, 256 * 1024, 64, 256 * 1024)
        .expect("non-zero session budget");
    assert_eq!(bounds.send_frames().get(), 64);
    assert_eq!(bounds.send_bytes().get(), 256 * 1024);
    assert_eq!(bounds.receive_frames().get(), 64);
    assert_eq!(bounds.receive_bytes().get(), 256 * 1024);
}

#[test]
fn session_preserves_64_frame_bounds_backpressure_cancellation_and_close() {
    let bounds = ByteStreamBounds::try_new(64, 64).unwrap();
    let session = FakePortForwardSession::new(bounds);
    assert_eq!(session.bounds(), bounds);

    for index in 0..64 {
        assert!(matches!(
            poll_once(session.send_frame(NodePortForwardFrame::data(index, vec![index as u8]))),
            std::task::Poll::Ready(Ok(()))
        ));
    }
    assert!(matches!(
        poll_once(session.send_frame(NodePortForwardFrame::data(64, vec![64]))),
        std::task::Poll::Pending
    ));

    let closed = FakePortForwardSession::new(bounds);
    assert!(matches!(
        poll_once(closed.recv_frame()),
        std::task::Poll::Ready(Ok(None))
    ));

    let mut cancelled = FakePortForwardSession::new(bounds);
    assert!(matches!(
        poll_once(cancelled.cancel()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert!(cancelled.is_cancelled());
    assert!(matches!(
        poll_once(cancelled.cancel()),
        std::task::Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        poll_once(cancelled.recv_frame()),
        std::task::Poll::Ready(Err(ByteStreamError::Cancelled))
    ));
}

#[test]
fn focused_capability_opens_a_transport_neutral_session() {
    let session = poll_ready(NodePortForward::open_port_forward(
        &FakePortForward,
        request(),
    ))
    .expect("session opens");
    assert_eq!(session.bounds(), ByteStreamBounds::try_new(64, 64).unwrap());
    assert!(matches!(
        poll_once(session.recv_frame()),
        std::task::Poll::Ready(Ok(None))
    ));
}

fn poll_once<F: Future>(future: F) -> std::task::Poll<F::Output> {
    let mut future = Box::pin(future);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}

fn poll_ready<F: Future>(future: F) -> F::Output {
    match poll_once(future) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("future unexpectedly pending"),
    }
}
