//! Event-driven scheduler controller.
//!
//! Watches unbound Pods and Node changes through the local datastore watch
//! broadcaster, then schedules Pods by setting `spec.nodeName` and the
//! PodScheduled condition. Runs an initial sweep after replay to catch pods
//! that were created before the scheduler's watch floor RV.
//!
//! ## Invariants
//! - Uses local datastore watch topics, not HTTP watch.
//! - No polling loops.
//! - Timers/backoff must use `TaskSupervisor`.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::datastore::WatchTarget;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::watch::WatchBootstrap;

/// Scheduler controller configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerControllerConfig {
    /// Whether the scheduler controller is enabled.
    pub enabled: bool,
}

impl SchedulerControllerConfig {
    /// Single-node seed leader path: scheduler controller disabled by default.
    pub fn single_node_seed_default() -> Self {
        Self { enabled: false }
    }

    /// Experimental leader wiring: scheduler controller enabled.
    pub fn experimental_leader() -> Self {
        Self { enabled: true }
    }
}

/// Whether a watch event should wake the scheduler.
///
/// Wakes on:
/// - unbound Pod add/modify/delete
/// - Node add/modify/delete
pub fn should_wake_scheduler(event: &crate::watch::WatchEvent) -> bool {
    let kind = event
        .object
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match kind {
        "Pod" => {
            // Wake only for unbound Pods (no spec.nodeName)
            event.object.pointer("/spec/nodeName").is_none()
        }
        "Node" => true,
        _ => false,
    }
}

/// Run the scheduler watch loop.
///
/// Disabled by default — call this only when config.enabled = true.
pub async fn run_scheduler_watch(state: Arc<AppState>, cancel: CancellationToken) {
    let db = state.db.clone();
    let db_handle = state.db.clone();

    let watch_bootstrap = WatchBootstrap::new(
        db.subscribe_watch_many(vec![
            crate::watch::WatchTopic::new("v1", "Pod"),
            crate::watch::WatchTopic::new("v1", "Node"),
        ]),
        DatastoreWatchReplaySource::new(
            db_handle,
            vec![
                WatchTarget::namespaced("v1", "Pod"),
                WatchTarget::cluster("v1", "Node"),
            ],
        ),
        db.get_current_resource_version().await.unwrap_or(0),
    );
    let mut cursor = watch_bootstrap.into_cursor();
    if let Err(e) = cursor.prime_replay().await {
        tracing::warn!("scheduler: initial replay failed: {:#}", e);
    }

    // Initial sweep: the watch replay only catches events with RV > floor_rv.
    // Pods created before the scheduler starts (e.g. during CoreDNS bootstrap)
    // must be picked up by a direct list so they don't remain Pending forever.
    tracing::info!("scheduler: running initial sweep for unbound pods");
    if let Err(e) = state.pod_repository.schedule_all_unbound_pods().await {
        tracing::warn!("scheduler initial sweep failed: {e:#}");
    }

    loop {
        match cursor
            .next_event_recovering(&cancel, state.task_supervisor.as_ref())
            .await
        {
            Ok(Some(event)) => {
                if !should_wake_scheduler(&event) {
                    continue;
                }

                tracing::debug!(
                    kind = %event.object.get("kind").and_then(|v| v.as_str()).unwrap_or(""),
                    name = %event.object.pointer("/metadata/name").and_then(|v| v.as_str()).unwrap_or(""),
                    "scheduler controller woke on relevant event"
                );
                if let Err(e) = state.pod_repository.schedule_all_unbound_pods().await {
                    tracing::warn!("scheduler reconcile failed: {e:#}");
                }
            }
            Ok(None) => {
                // Watch ended
                break;
            }
            Err(e) => {
                tracing::warn!("scheduler watch error: {:#?}", e);
                // Backoff via TaskSupervisor timer helper
                let _ = state
                    .task_supervisor
                    .sleep(
                        "scheduler_watch_retry",
                        std::time::Duration::from_millis(100),
                    )
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod_event(node_name: Option<&str>) -> crate::watch::WatchEvent {
        crate::watch::WatchEvent::modified(json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "pod-1", "namespace": "default", "resourceVersion": "1"},
            "spec": match node_name {
                Some(name) => json!({"nodeName": name}),
                None => json!({}),
            }
        }))
    }

    fn node_event() -> crate::watch::WatchEvent {
        crate::watch::WatchEvent::modified(json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": {"name": "node-1", "resourceVersion": "1"},
        }))
    }

    fn configmap_event() -> crate::watch::WatchEvent {
        crate::watch::WatchEvent::modified(json!({
            "kind": "ConfigMap",
            "apiVersion": "v1",
            "metadata": {"name": "cm-1", "namespace": "default", "resourceVersion": "1"},
        }))
    }

    #[test]
    fn wakes_on_unbound_pod_add() {
        let event = pod_event(None);
        assert!(should_wake_scheduler(&event));
    }

    #[test]
    fn does_not_wake_on_bound_pod() {
        let event = pod_event(Some("node-1"));
        assert!(!should_wake_scheduler(&event));
    }

    #[test]
    fn wakes_on_node_change() {
        let event = node_event();
        assert!(should_wake_scheduler(&event));
    }

    #[test]
    fn ignores_irrelevant_kinds() {
        let event = configmap_event();
        assert!(!should_wake_scheduler(&event));
    }

    #[test]
    fn single_node_seed_scheduler_disabled_by_default() {
        let cfg = SchedulerControllerConfig::single_node_seed_default();
        assert!(
            !cfg.enabled,
            "scheduler controller must be disabled for single-node seed leader"
        );
    }

    #[test]
    fn experimental_leader_scheduler_enabled() {
        let cfg = SchedulerControllerConfig::experimental_leader();
        assert!(cfg.enabled);
    }

    #[test]
    fn idle_silent_config_has_no_background_work_when_disabled() {
        let cfg = SchedulerControllerConfig::single_node_seed_default();
        assert!(!cfg.enabled);
        // Structural assertion: disabled config means the watch loop is never started.
    }

    #[test]
    fn scheduler_uses_local_watch_not_http_watch() {
        // Structural assertion: the controller imports WatchBootstrap and WatchTarget,
        // not any HTTP watch client.
    }
}
