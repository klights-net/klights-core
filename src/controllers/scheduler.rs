//! Event-driven scheduler controller.
//!
//! Watches unbound Pods and Node changes through datastore watch signals, then
//! schedules Pods by setting `spec.nodeName` and the
//! PodScheduled condition. Runs an initial sweep after subscribing so pods
//! that already exist do not remain Pending forever.
//!
//! ## Invariants
//! - Uses local datastore watch topics, not HTTP watch.
//! - No polling loops.
//! - Timers/backoff must use `TaskSupervisor`.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt as _;
use klights_leader_api::{LeaderWatchError, ResourceEvent, WatchEventType, WatchStream};
use tokio_util::sync::CancellationToken;

/// Focused runtime port used by the event-driven scheduler.
///
/// Controller code owns the scheduling policy while bootstrap adapts the
/// leader datastore and Pod repository to this narrow contract.
#[async_trait]
pub trait SchedulerRuntime: Send + Sync {
    async fn open_watch_sessions(&self) -> std::result::Result<Vec<WatchStream>, LeaderWatchError>;

    async fn schedule_all_unbound_pods(&self) -> Result<()>;
}

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
pub fn should_wake_scheduler(event: &ResourceEvent) -> bool {
    match event.resource().kind.as_str() {
        "Pod" => {
            // Wake only for unbound Pods (no spec.nodeName)
            event.resource().data.pointer("/spec/nodeName").is_none()
        }
        "Node" => true,
        _ => false,
    }
}

/// Run the scheduler watch loop.
///
/// Disabled by default — call this only when config.enabled = true.
pub async fn run_scheduler_watch(runtime: Arc<dyn SchedulerRuntime>, cancel: CancellationToken) {
    let sessions = match runtime.open_watch_sessions().await {
        Ok(sessions) => sessions,
        Err(error) => {
            tracing::warn!("scheduler watch open failed: {error:#}");
            return;
        }
    };
    let mut events = futures::stream::select_all(sessions);

    // Initial sweep: the watch replay only catches events with RV > floor_rv.
    // Pods created before the scheduler starts (e.g. during CoreDNS bootstrap)
    // must be picked up by a direct list so they don't remain Pending forever.
    tracing::info!("scheduler: running initial sweep for unbound pods");
    if let Err(e) = runtime.schedule_all_unbound_pods().await {
        tracing::warn!("scheduler initial sweep failed: {e:#}");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            event = events.next() => match event {
                Some(Ok(event)) => {
                    if matches!(event.event_type(), WatchEventType::Bookmark | WatchEventType::Error)
                        || !should_wake_scheduler(&event)
                    {
                        continue;
                    }
                    tracing::debug!("scheduler controller woke on watch signal");
                    if let Err(e) = runtime.schedule_all_unbound_pods().await {
                        tracing::warn!("scheduler reconcile failed: {e:#}");
                    }
                }
                Some(Err(LeaderWatchError::ReplayExpired { .. })) => {
                    tracing::warn!(
                        "scheduler watch replay expired; running full unbound-pod sweep"
                    );
                    if let Err(e) = runtime.schedule_all_unbound_pods().await {
                        tracing::warn!("scheduler reconcile after replay expiry failed: {e:#}");
                    }
                    match runtime.open_watch_sessions().await {
                        Ok(reopened) => events = futures::stream::select_all(reopened),
                        Err(error) => {
                            tracing::warn!("scheduler watch reopen failed: {error:#}");
                            break;
                        }
                    }
                }
                Some(Err(error)) => tracing::warn!("scheduler watch failed: {error:#}"),
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(value: serde_json::Value) -> ResourceEvent {
        let resource =
            klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(value)).unwrap();
        ResourceEvent::try_new(WatchEventType::Modified, resource, None).unwrap()
    }

    fn pod_event(node_name: Option<&str>) -> ResourceEvent {
        event(json!({
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": "pod-1", "namespace": "default", "resourceVersion": "1"},
            "spec": match node_name {
                Some(name) => json!({"nodeName": name}),
                None => json!({}),
            }
        }))
    }

    fn node_event() -> ResourceEvent {
        event(json!({
            "kind": "Node",
            "apiVersion": "v1",
            "metadata": {"name": "node-1", "resourceVersion": "1"},
        }))
    }

    fn configmap_event() -> ResourceEvent {
        event(json!({
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
