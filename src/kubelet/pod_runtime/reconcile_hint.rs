//! Deferred runtime-reconcile hint carried from a CRI container event.
//!
//! Extracted from the `service.rs` hub to keep it under its size cap. The
//! `RuntimeReconcileHint` type is re-exported from `service` so the public
//! path `crate::kubelet::pod_runtime::service::RuntimeReconcileHint` stays
//! stable for callers in the lifecycle router, actor, and core state machine.

/// Hint carried from a CRI container event into deferred runtime reconcile.
///
/// When a short-lived pod exits while startup finalization is still in
/// flight, the actor defers the CRI stop event and later runs a runtime
/// reconcile. By then the sandbox container listing may be empty or stale.
/// This hint lets the reconciler read the concrete (terminated) container
/// status via the event's container id instead of synthesizing
/// `Pending`/`ContainerCreating`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeReconcileHint {
    pub container_id: Option<String>,
}

impl RuntimeReconcileHint {
    /// No hint — used by callers that have no concrete container id.
    pub fn none() -> Self {
        Self { container_id: None }
    }

    /// Build a hint from a CRI event container id. An empty id collapses to
    /// `none()` so callers can pass the raw event payload without a guard.
    pub fn from_container_id(container_id: impl Into<String>) -> Self {
        let container_id = container_id.into();
        if container_id.is_empty() {
            Self::none()
        } else {
            Self {
                container_id: Some(container_id),
            }
        }
    }
}
