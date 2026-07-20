use std::sync::Mutex;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use klights_reconcile_api::{
    ControllerReconcileSink, GcPodDeleteError, GcPodDeleteRequest, GcPodDeleteSink, ReconcileKey,
    ReconcileSinkError, ReconcileSinkFuture, ServiceReconcileKey, ServiceReconcileSink,
};
use klights_types::PodIdentity;

#[derive(Default)]
struct RecordingSink {
    controller_keys: Mutex<Vec<ReconcileKey>>,
    service_keys: Mutex<Vec<ServiceReconcileKey>>,
    pod_deletes: Mutex<Vec<PodIdentity>>,
}

impl ControllerReconcileSink for RecordingSink {
    fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.controller_keys.lock().unwrap().extend(keys);
            Ok(())
        })
    }
}

impl ServiceReconcileSink for RecordingSink {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.service_keys.lock().unwrap().extend(keys);
            Ok(())
        })
    }
}

impl GcPodDeleteSink for RecordingSink {
    fn request_gc_pod_delete(
        &self,
        request: GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        Box::pin(async move {
            self.pod_deletes
                .lock()
                .unwrap()
                .push(request.into_identity());
            Ok(())
        })
    }
}

fn assert_object_safe(
    _controller: &dyn ControllerReconcileSink,
    _service: &dyn ServiceReconcileSink,
    _gc: &dyn GcPodDeleteSink,
) {
}

#[test]
fn focused_reconcile_sinks_are_independently_object_safe() {
    let sink = RecordingSink::default();
    assert_object_safe(&sink, &sink, &sink);
}

#[test]
fn service_key_is_narrow_and_namespaced() {
    let key = ServiceReconcileKey::new("default", "api");
    assert_eq!(key.namespace(), "default");
    assert_eq!(key.name(), "api");
    assert_eq!(
        key.clone().into_reconcile_key(),
        ReconcileKey::namespaced("v1", "Service", "default", "api")
    );
}

#[test]
fn gc_request_preserves_uid_qualified_pod_identity() {
    let identity = PodIdentity::new("default", "web", "uid-1");
    let request = GcPodDeleteRequest::new(identity.clone());
    assert_eq!(request.identity(), &identity);

    let error = GcPodDeleteError::unavailable("leader unavailable");
    assert_eq!(error.to_string(), "leader unavailable");
}

#[test]
fn gc_errors_preserve_gone_identity_and_retry_categories() {
    let gone = GcPodDeleteError::not_found("Pod not found");
    let replacement = GcPodDeleteError::identity_changed("UID precondition failed");
    let unavailable = GcPodDeleteError::unavailable("leader unavailable");

    assert!(gone.is_gone_or_identity_changed());
    assert!(replacement.is_gone_or_identity_changed());
    assert!(!unavailable.is_gone_or_identity_changed());
}

#[test]
fn reconcile_errors_preserve_closed_unavailable_and_unsupported_categories() {
    for (error, expected) in [
        (ReconcileSinkError::closed("queue closed"), "queue closed"),
        (
            ReconcileSinkError::unavailable("dispatcher unavailable"),
            "dispatcher unavailable",
        ),
        (
            ReconcileSinkError::unsupported_key("Service keys use the focused sink"),
            "Service keys use the focused sink",
        ),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

fn poll_ready<T>(
    mut future: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + '_>>,
) -> T {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RAW, |_| {}, |_| {}, |_| {});
    const RAW: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
    // SAFETY: the no-op vtable never dereferences its null data pointer, and
    // these contract-test futures do not suspend.
    let waker = unsafe { Waker::from_raw(RAW) };
    match future.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("contract-test sink future unexpectedly suspended"),
    }
}

#[test]
fn one_sink_future_carries_each_complete_mutation_batch() {
    let sink = RecordingSink::default();
    let controller_keys = vec![
        ReconcileKey::cluster("v1", "Namespace", "one"),
        ReconcileKey::cluster("v1", "Namespace", "two"),
    ];
    poll_ready(sink.enqueue_reconcile_batch(controller_keys.clone())).unwrap();
    let service_keys = vec![
        ServiceReconcileKey::new("default", "one"),
        ServiceReconcileKey::new("default", "two"),
    ];
    poll_ready(sink.enqueue_service_reconcile_batch(service_keys.clone())).unwrap();

    assert_eq!(*sink.controller_keys.lock().unwrap(), controller_keys);
    assert_eq!(*sink.service_keys.lock().unwrap(), service_keys);
}
