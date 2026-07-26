use klights_reconcile_api::ReconcileFailureMetrics;

#[derive(Default)]
struct RecordingMetrics {
    cascade: std::sync::atomic::AtomicU64,
    namespace: std::sync::atomic::AtomicU64,
}

impl ReconcileFailureMetrics for RecordingMetrics {
    fn record_cascade_delete_failure(&self) {
        self.cascade
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_namespace_delete_failure(&self) {
        self.namespace
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[test]
fn reconcile_failure_metrics_are_narrow_monotonic_signals() {
    let metrics = RecordingMetrics::default();
    metrics.record_cascade_delete_failure();
    metrics.record_namespace_delete_failure();

    assert_eq!(
        metrics.cascade.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics.namespace.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}
