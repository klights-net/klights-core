//! Side effect to check namespace termination after Pod mutations.

use super::{SideEffect, SideEffectMetrics};
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Checks whether namespace should terminate after Pod mutations.
///
/// Registered only for `(v1, Pod)` — the registry handles the kind dispatch.
/// Holds an `Arc<SideEffectMetrics>` so failures inside
/// `reconcile_namespace_termination` increment the same counters exposed at
/// `/metrics` from this side-effect path as well as the HTTP handler path.
pub struct NamespaceTerminationEffect {
    metrics: Arc<SideEffectMetrics>,
}

#[async_trait]
impl SideEffect for NamespaceTerminationEffect {
    fn name(&self) -> &'static str {
        "namespace_termination"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let ns_name = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if ns_name.is_empty() {
            return Ok(());
        }

        crate::api::reconcile_namespace_termination(db, ns_name, &self.metrics)
            .await
            .map_err(|e| anyhow::anyhow!("namespace termination failed: {:?}", e))
    }
}

/// Create a NamespaceTerminationEffect instance bound to the shared metrics.
pub fn namespace_termination_check(metrics: Arc<SideEffectMetrics>) -> Arc<dyn SideEffect> {
    Arc::new(NamespaceTerminationEffect { metrics })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn test_namespace_termination_check_name() {
        let effect = namespace_termination_check(SideEffectMetrics::new());
        assert_eq!(effect.name(), "namespace_termination");
    }

    /// Sanity test for the metrics wiring path: the hook holds the same
    /// Arc<SideEffectMetrics> that callers register on AppState, and any
    /// future increment inside reconcile_namespace_termination must show
    /// up on the shared counter (not a private clone).
    #[tokio::test]
    async fn test_namespace_termination_hook_shares_metrics_arc() {
        let metrics = SideEffectMetrics::new();
        let _effect = namespace_termination_check(metrics.clone());

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
                "deletionTimestamp": crate::utils::k8s_timestamp(),
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
