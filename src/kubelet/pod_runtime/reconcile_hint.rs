//! Deferred runtime-reconcile hint carried from CRI container events.
//!
//! Extracted from the `service.rs` hub to keep it under its size cap. The
//! `RuntimeReconcileHint` type is re-exported from `service` so the public
//! path `crate::kubelet::pod_runtime::service::RuntimeReconcileHint` stays
//! stable for callers in the lifecycle router, actor, and core state machine.

use std::collections::BTreeSet;

/// Hint carried from CRI container events into deferred runtime reconcile.
///
/// When a short-lived pod exits while startup finalization is still in
/// flight, the actor defers the CRI stop event and later runs a runtime
/// reconcile. By then the sandbox container listing may be empty or stale.
/// This hint carries ALL observed container IDs so the reconciler can read
/// concrete terminated status for every exited container instead of
/// synthesizing `Pending`/`ContainerCreating` — even for multi-container
/// pods where only a subset of containers appeared in the stale listing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeReconcileHint {
    container_ids: BTreeSet<String>,
}

impl RuntimeReconcileHint {
    /// No hint — used by callers that have no concrete container id.
    pub fn none() -> Self {
        Self {
            container_ids: BTreeSet::new(),
        }
    }

    /// Build a hint from a single CRI event container id. An empty id
    /// collapses to `none()` so callers can pass the raw event payload
    /// without a guard.
    pub fn from_container_id(container_id: impl Into<String>) -> Self {
        let container_id = container_id.into();
        if container_id.is_empty() {
            Self::none()
        } else {
            let mut ids = BTreeSet::new();
            ids.insert(container_id);
            Self { container_ids: ids }
        }
    }

    /// Build a hint from multiple container IDs (multi-container pods or
    /// multiple deferred CRI events). Empty IDs are filtered out.
    pub fn from_container_ids(ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let container_ids = ids
            .into_iter()
            .map(Into::into)
            .filter(|id: &String| !id.is_empty())
            .collect();
        Self { container_ids }
    }

    /// Iterate over all hinted container IDs.
    pub fn container_ids(&self) -> impl Iterator<Item = &str> {
        self.container_ids.iter().map(String::as_str)
    }

    /// True when no container IDs are hinted.
    pub fn is_empty(&self) -> bool {
        self.container_ids.is_empty()
    }

    /// Single-ID accessor kept for existing callers that only care about one
    /// container id. Returns `None` when the hint is empty or has multiple IDs.
    #[deprecated(note = "use container_ids() to support multi-container pods")]
    pub fn single_container_id(&self) -> Option<&str> {
        if self.container_ids.len() == 1 {
            self.container_ids.iter().next().map(String::as_str)
        } else {
            None
        }
    }
}
