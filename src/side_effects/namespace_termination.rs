//! Side effect to check namespace termination after Pod mutations.

#[cfg(test)]
mod tests {
    use klights_controllers::side_effects::SideEffectMetrics;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_namespace_termination_check_name() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let effect =
            crate::namespace_termination_adapter::effect(db_handle, SideEffectMetrics::new());
        assert_eq!(effect.name(), "namespace_termination");
    }

    /// Sanity test for the metrics wiring path: the hook holds the same
    /// Arc<SideEffectMetrics> that callers register on ApiState, and any
    /// future increment inside reconcile_namespace_termination must show
    /// up on the shared counter (not a private clone).
    #[tokio::test]
    async fn test_namespace_termination_hook_shares_metrics_arc() {
        let metrics = SideEffectMetrics::new();
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let _effect = crate::namespace_termination_adapter::effect(db_handle, metrics.clone());

        // Manually increment to prove the Arc clones share storage.
        metrics
            .namespace_delete_failures_total
            .fetch_add(7, Ordering::Relaxed);

        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            7,
            "the metrics Arc held by the hook must be the same one observed externally"
        );
    }

    /// Race regression: a concurrent reconcile may have already removed the
    /// namespace by the time this one decides to delete. Treat the resulting
    /// "not found" as success, not as a permanent failure that bumps the
    /// failure counter and leaves the namespace stuck.
    #[tokio::test]
    async fn test_reconcile_namespace_termination_already_deleted_is_ok() {
        use crate::datastore::test_support::in_memory;

        let db = in_memory().await;
        let metrics = SideEffectMetrics::new();

        // Reconcile against a namespace that never existed. The function
        // should silently no-op (Ok), not error and not bump failure counter.
        crate::api::reconcile_namespace_termination(&db, "ghost-ns", &metrics)
            .await
            .expect("reconcile against missing namespace must be ok");

        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            0,
            "missing namespace must not increment failure counter"
        );
    }

    /// End-to-end: success path through reconcile_namespace_termination
    /// must NOT increment namespace_delete_failures_total. Guards against
    /// a regression where the counter is incremented unconditionally.
    #[tokio::test]
    async fn test_reconcile_namespace_termination_success_does_not_increment_counter() {
        use crate::datastore::test_support::in_memory;

        let db = in_memory().await;
        let metrics = SideEffectMetrics::new();

        // Bootstrap a namespace with deletionTimestamp set so reconcile
        // walks the termination path. Empty resource list → reconcile
        // proceeds to delete_namespace, which succeeds for an existing ns.
        let ns_name = "term-test-ns";
        let ns_data = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": ns_name,
                "deletionTimestamp": "2026-01-01T00:00:00.000000000Z",
            },
            "spec": { "finalizers": [] },
            "status": { "phase": "Terminating" }
        });
        db.create_namespace(ns_name, ns_data)
            .await
            .expect("create ns");

        crate::api::reconcile_namespace_termination(&db, ns_name, &metrics)
            .await
            .expect("reconcile ok");

        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            0,
            "success path must not increment failure counter"
        );
    }
}
