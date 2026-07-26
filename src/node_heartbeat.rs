use crate::datastore::WatchTarget;
use crate::kubelet::pod_watch_source::BoxedWatchReplaySource;
use crate::utils::k8s_microtime_now;
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WindowPolicy,
};
use anyhow::Result;
use async_trait::async_trait;
use klights_watch::WatchTopic;
use std::time::Duration;

fn is_node_heartbeat_event(event: &WatchEvent, node_name: &str) -> bool {
    if event.event_type == EventType::Bookmark || event.event_type == EventType::Deleted {
        return false;
    }
    let Some(kind) = event.object.get("kind").and_then(|k| k.as_str()) else {
        return false;
    };
    if kind != "Node" {
        return false;
    }
    event
        .object
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        == Some(node_name)
}

#[cfg(test)]
pub(crate) fn build_lease(node_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": node_name,
            "namespace": "kube-node-lease"
        },
        "spec": {
            "holderIdentity": node_name,
            "leaseDurationSeconds": klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS,
            "renewTime": k8s_microtime_now()
        }
    })
}

#[async_trait]
pub trait NodeHeartbeatWatchSource: Send + Sync {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver;

    fn replay_source(&self, targets: Vec<WatchTarget>) -> BoxedWatchReplaySource;

    async fn current_resource_version(&self) -> Result<i64>;
}

// Derived from the canonical node-lease cadence so the renewal timer and the
// staleness grace (GRACE = HEARTBEAT * MISSED) can never drift apart. Change
// the cadence in one place: node_lease_tracker::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS.
pub(crate) const NODE_HEARTBEAT_INTERVAL: Duration =
    Duration::from_secs(crate::node_lease_tracker::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS as u64);

/// Run the node heartbeat loop: renews the kube-node-lease every
/// NODE_HEARTBEAT_INTERVAL (and on Node watch events) via the memory-only
/// lease client (worker -> leader RPC, or the leader's local tracker). This
/// is the only production heartbeat entry point; it never writes a Lease to
/// cluster.db.
pub async fn run_heartbeat_with_lease_client(
    watch_source: std::sync::Arc<dyn NodeHeartbeatWatchSource>,
    lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    node_name: String,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
) {
    run_heartbeat_with_interval(
        watch_source,
        lease_client,
        node_name,
        cancel_token,
        task_supervisor,
        NODE_HEARTBEAT_INTERVAL,
    )
    .await;
}

pub(crate) async fn run_heartbeat_with_interval(
    watch_source: std::sync::Arc<dyn NodeHeartbeatWatchSource>,
    lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    node_name: String,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    heartbeat_interval: Duration,
) {
    tracing::info!("Starting node heartbeat for {}", node_name);

    // Memory-only heartbeat (T6): renew via the lease client (worker -> leader
    // RPC, or the leader's local NodeLeaseTracker). This path never writes a
    // Lease to cluster.db; the dead outbox/direct-db renewal helpers were
    // removed. The watch source is retained only to drive the Node watch
    // cursor below.
    if let Err(err) = renew_lease_with_client(lease_client.as_ref(), &node_name).await {
        tracing::warn!("Failed to send initial node heartbeat: {}", err);
    }

    // Event-driven heartbeat: renew the lease on node watch events.
    let topic = WatchTopic::new("v1", "Node");
    let mut cursor = SignalWatchCursor::new(
        watch_source.subscribe_watch_signals(topic.clone()),
        watch_source.replay_source(vec![WatchTarget::cluster("v1", "Node")]),
        topic,
        WatchDeliveryScope::Cluster,
        watch_source.current_resource_version().await.unwrap_or(0),
        WindowPolicy::default_watch_delivery(),
    );
    match cursor.prime_replay_or_expired().await {
        Ok(replayed) => {
            tracing::debug!(
                "Node heartbeat primed {} replay events before entering live watch",
                replayed
            );
        }
        Err(err) => {
            tracing::warn!(?err, "Node heartbeat initial replay failed");
        }
    }

    let mut next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
    loop {
        let delay = next_heartbeat.saturating_duration_since(tokio::time::Instant::now());
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Node heartbeat cancelled, shutting down");
                break;
            }
            sleep = task_supervisor.sleep("node_heartbeat_interval", delay) => {
                if let Err(err) = sleep {
                    tracing::warn!("Node heartbeat timer failed: {err:#}");
                }
                if let Err(err) = renew_lease_with_client(lease_client.as_ref(), &node_name).await {
                    tracing::warn!("Failed to send node heartbeat: {}", err);
                }
                next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
                tracing::debug!("Node heartbeat sent for {}", node_name);
            }
            event = cursor.next_event() => {
                match event {
                    Ok(event) if is_node_heartbeat_event(&event, &node_name) => {
                        if let Err(err) =
                            renew_lease_with_client(lease_client.as_ref(), &node_name).await
                        {
                            tracing::warn!("Failed to send node heartbeat: {}", err);
                        }
                        next_heartbeat = tokio::time::Instant::now() + heartbeat_interval;
                        tracing::debug!("Node heartbeat sent for {}", node_name);
                    }
                    Ok(_) => {}
                    Err(WatchCursorError::Closed) => {
                        tracing::warn!("Node heartbeat watch signal channel closed");
                        break;
                    }
                    Err(WatchCursorError::Expired) => {
                        tracing::warn!("Node heartbeat replay window expired; waiting for next signal");
                    }
                    Err(WatchCursorError::Replay(err)) => {
                        tracing::warn!("Node heartbeat replay failed: {err:#}");
                    }
                }
            }
        };
    }
}

async fn renew_lease_with_client(
    client: &dyn klights_leader_api::LeaderNodeLeaseRenewal,
    node_name: &str,
) -> Result<()> {
    let request = klights_leader_api::NodeLeaseRenewalRequest::try_new(
        node_name,
        k8s_microtime_now(),
        klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS,
    )?;
    client
        .renew_node_lease(request)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
}
