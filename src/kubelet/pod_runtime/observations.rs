//! Actor-owned per-Pod runtime reconcile observations.
//!
//! Replaces the single `pending_runtime_reconcile_container_id: Option<String>`
//! field so that CRI events for multiple containers (or multiple events for the
//! same container) are all preserved until the next reconcile drains them.

use std::collections::BTreeSet;

/// All container IDs observed from CRI events for a single Pod, keyed by
/// Pod UID. The actor owns insertion and draining; the reconciler receives a
/// snapshot via `RuntimeReconcileHint::from_container_ids`.
///
/// Properties:
/// - Bounded: each actor holds at most one `RuntimeReconcileObservations` at
///   a time (one per active Pod UID).
/// - Actor-owned: only the lifecycle actor may insert or drain; the service
///   receives a snapshot and must not retain it across reconcile calls.
/// - No polling: observations accumulate as CRI events arrive; the reconciler
///   drains them in a single pass without spinning.
#[derive(Clone, Debug, Default)]
pub struct RuntimeReconcileObservations {
    pod_uid: String,
    container_ids: BTreeSet<String>,
    generation: u64,
}

impl RuntimeReconcileObservations {
    pub fn new(pod_uid: impl Into<String>) -> Self {
        Self {
            pod_uid: pod_uid.into(),
            container_ids: BTreeSet::new(),
            generation: 0,
        }
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    /// Record a CRI event container ID. Idempotent (BTreeSet dedup).
    /// Generation increments only for new (non-duplicate) inserts.
    pub fn observe(&mut self, container_id: impl Into<String>) {
        let id = container_id.into();
        if !id.is_empty() && self.container_ids.insert(id) {
            self.generation += 1;
        }
    }

    /// Observe multiple container IDs at once.
    pub fn observe_all(&mut self, ids: impl IntoIterator<Item = impl Into<String>>) {
        for id in ids {
            self.observe(id);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.container_ids.is_empty()
    }

    pub fn container_ids(&self) -> impl Iterator<Item = &str> {
        self.container_ids.iter().map(String::as_str)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Drain all container IDs and reset generation. Called after a
    /// successful reconcile pass so stale observations don't accumulate.
    pub fn drain(&mut self) -> BTreeSet<String> {
        self.generation = 0;
        std::mem::take(&mut self.container_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_accumulates_ids() {
        let mut obs = RuntimeReconcileObservations::new("pod-uid");
        obs.observe("ctr-a");
        obs.observe("ctr-b");
        obs.observe("ctr-a"); // dedup
        let ids: BTreeSet<_> = obs.container_ids().collect();
        assert_eq!(ids, ["ctr-a", "ctr-b"].iter().copied().collect());
        assert_eq!(obs.generation(), 2);
    }

    #[test]
    fn drain_clears_observations() {
        let mut obs = RuntimeReconcileObservations::new("pod-uid");
        obs.observe("ctr-x");
        let drained = obs.drain();
        assert!(drained.contains("ctr-x"));
        assert!(obs.is_empty());
        assert_eq!(obs.generation(), 0);
    }
}
