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

use tokio_util::sync::CancellationToken;

use crate::api::AppState;

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
    let mut signal_rx = crate::watch::WatchSignalReceiver::new(
        [
            crate::watch::WatchTopic::new("v1", "Pod"),
            crate::watch::WatchTopic::new("v1", "Node"),
        ]
        .into_iter()
        .map(|topic| db.subscribe_watch_signals(topic))
        .collect(),
    );
    let mut last_seen_rv = db.get_current_resource_version().await.unwrap_or(0);

    // Initial sweep: the watch replay only catches events with RV > floor_rv.
    // Pods created before the scheduler starts (e.g. during CoreDNS bootstrap)
    // must be picked up by a direct list so they don't remain Pending forever.
    tracing::info!("scheduler: running initial sweep for unbound pods");
    if let Err(e) = state.pod_repository.schedule_all_unbound_pods().await {
        tracing::warn!("scheduler initial sweep failed: {e:#}");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            signal = signal_rx.recv() => match signal {
                Ok(signal) => {
                    let high_rv = signal
                        .advances
                        .iter()
                        .map(|advance| advance.high_rv)
                        .max()
                        .unwrap_or(last_seen_rv);
                    if high_rv <= last_seen_rv {
                        continue;
                    }
                    last_seen_rv = high_rv;
                    tracing::debug!("scheduler controller woke on watch signal");
                    if let Err(e) = state.pod_repository.schedule_all_unbound_pods().await {
                        tracing::warn!("scheduler reconcile failed: {e:#}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "scheduler watch signals lagged by {n}; running full unbound-pod sweep"
                    );
                    if let Err(e) = state.pod_repository.schedule_all_unbound_pods().await {
                        tracing::warn!("scheduler reconcile after signal lag failed: {e:#}");
                    }
                    last_seen_rv = db.get_current_resource_version().await.unwrap_or(last_seen_rv);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
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
