//! Per-node subnet allocation and peer-route reconciliation.
//!
//! Called once at klights startup. Allocates a /24 from the cluster CIDR for
//! the local node. After this function returns, `cni::add` can use the
//! node-local subnet for IPAM and the peer-route controller can install the
//! selected dataplane route type for known peers. Default encrypted peers use
//! WireGuard; explicit disabled-encryption peers use direct routes without an
//! extra overlay interface.

use anyhow::{Context, Result};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::controllers::annotations::{
    HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION, NodePeerMode, parse_node_peer_mode,
};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreBackend, DatastoreHandle, NodeSubnet, WatchTarget};
use crate::kubelet::outbox::Outbox;
use crate::networking::NodeEndpoint;
use crate::networking::dataplane_health::{DataplaneHealth, DataplaneHealthStatus};
use crate::networking::types::HostPortRange;
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WatchTopic,
    WindowPolicy,
};

/// Result of one [`sync_peer_routes`] pass, used to gate the local node's
/// readiness. A node is only Ready when every *Ready* peer has a dataplane
/// route installed; peers that are themselves NotReady are excluded so a
/// genuinely-down node does not keep the rest of the cluster NotReady forever.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerSyncOutcome {
    /// Total peers desired in the node_subnets table (excludes self).
    pub desired_peers: usize,
    /// Peers whose Node `Ready` condition is currently True.
    pub ready_peers: usize,
    /// Ready peers that have no dataplane route installed (missing metadata).
    pub unreachable_ready_peers: usize,
}

/// Map a peer-route sync outcome onto the local dataplane health. Connected
/// when every Ready peer is reachable (including the zero-peer single-node
/// case); Disconnected otherwise.
pub fn apply_peer_sync_outcome(health: &DataplaneHealth, outcome: &PeerSyncOutcome) {
    if outcome.unreachable_ready_peers == 0 {
        health.set_peers_connected();
    } else {
        health.set_peers_disconnected(format!(
            "Waiting for WireGuard dataplane connectivity to {} of {} ready peer(s)",
            outcome.unreachable_ready_peers, outcome.ready_peers
        ));
    }
}

/// Dispatch on cluster-visible dataplane metadata to choose the exact peer
/// endpoint. The safe default is encrypted WireGuard; explicit disabled
/// encryption is a typed direct route. Missing metadata is skipped instead of
/// falling back to VXLAN or host-port grafting.
async fn endpoint_for_peer(
    db: &dyn DatastoreBackend,
    peer: &NodeSubnet,
) -> Result<Option<NodeEndpoint>> {
    let Some(metadata) = db.get_node_dataplane(peer.node_name.as_ref()).await? else {
        tracing::warn!(
            node = %peer.node_name,
            "node_subnet: peer missing dataplane metadata, skipping route install"
        );
        return Ok(None);
    };

    match metadata.encryption {
        crate::networking::wireguard::DataplaneEncryption::Enabled => {
            let plan = crate::networking::wireguard::WireGuardPeerPlan::try_new(
                metadata,
                &peer.subnet.to_string(),
            )?;
            Ok(Some(NodeEndpoint::WireGuard(plan)))
        }
        crate::networking::wireguard::DataplaneEncryption::Disabled => {
            let plan = crate::networking::wireguard::UnencryptedPeerPlan::try_new(
                metadata,
                &peer.subnet.to_string(),
            )?;
            Ok(Some(NodeEndpoint::UnencryptedDirect(plan)))
        }
    }
}

/// Tracks every peer the controller has actually installed against the network
/// `PeerRouter`, keyed by node name. Stores both the projected `NodeSubnet`
/// (for change detection) and the exact `NodeEndpoint` variant we applied so
/// removal hits the same shape — root removal must not be issued against a
/// rootless endpoint or vice versa.
#[derive(Clone)]
pub struct AppliedPeer {
    pub subnet: NodeSubnet,
    pub endpoint: NodeEndpoint,
}

/// Allocate (or retrieve) the local node's /24 subnet.
///
/// F2-02 split: this owns node-local IPAM and metadata only. Peer route install
/// lives in [`sync_peer_routes`] so callers (rootless / hybrid) that have no
/// valid `PeerRouter` for the current mode can still allocate locally.
///
/// Idempotent: re-running finds the existing allocation in SQLite.
pub async fn ensure_local_node_subnet(
    db: &dyn DatastoreBackend,
    node_name: &str,
    cluster_cidr: &str,
    node_ip: &str,
) -> Result<NodeSubnet> {
    let subnet = db
        .allocate_node_subnet(node_name, cluster_cidr, node_ip)
        .await
        .context("Failed to allocate node subnet")?;

    tracing::info!(
        "node_subnet: node={} subnet={} vtep_ip={}",
        node_name,
        subnet.subnet,
        subnet.vtep_ip,
    );

    Ok(subnet)
}

/// Watch Node events and keep peer routes in sync with `node_subnets`.
///
/// Event-driven only: no polling loop. Uses WatchCursor replay to survive lag.
pub async fn run_peer_watch(
    state: std::sync::Arc<AppState>,
    dataplane_health: DataplaneHealth,
    cancel: CancellationToken,
) {
    run_peer_watch_with_components_inner(
        state.db.clone(),
        state.config.node_name.clone(),
        state.config.cluster_cidr.clone(),
        state.network.peering.clone(),
        state.task_supervisor.clone(),
        state.is_raft_leader_rx.clone(),
        Some(dataplane_health),
        Some(state.outbox.clone()),
        cancel,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub async fn run_peer_watch_with_components(
    db: DatastoreHandle,
    my_node_name: String,
    cluster_cidr: String,
    peering: std::sync::Arc<dyn crate::networking::PeerRouter>,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    dataplane_health: Option<DataplaneHealth>,
    outbox: Option<std::sync::Arc<Outbox>>,
    cancel: CancellationToken,
) {
    run_peer_watch_with_components_inner(
        db,
        my_node_name,
        cluster_cidr,
        peering,
        task_supervisor,
        None,
        dataplane_health,
        outbox,
        cancel,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn run_peer_watch_with_components_inner(
    db: DatastoreHandle,
    my_node_name: String,
    cluster_cidr: String,
    peering: std::sync::Arc<dyn crate::networking::PeerRouter>,
    _task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    raft_leader_proxy: Option<std::sync::Arc<crate::api::raft_proxy::RaftLeaderProxy>>,
    dataplane_health: Option<DataplaneHealth>,
    outbox: Option<std::sync::Arc<Outbox>>,
    cancel: CancellationToken,
) {
    let topic = WatchTopic::new("v1", "Node");
    let mut cursor = SignalWatchCursor::new(
        db.subscribe_watch_signals(topic.clone()),
        DatastoreWatchReplaySource::new(db.clone(), vec![WatchTarget::cluster("v1", "Node")]),
        topic,
        WatchDeliveryScope::Cluster,
        db.get_current_resource_version().await.unwrap_or(0),
        WindowPolicy::default_watch_delivery(),
    );
    if let Err(e) = cursor.prime_replay_or_expired().await {
        tracing::warn!(?e, "node_subnet: initial replay failed");
    }

    let mut applied: HashMap<String, AppliedPeer> = HashMap::new();
    // Track the last confirmed readiness so we only re-write the node's
    // conditions when health actually changes. Start empty to force one
    // persisted-state verification on watcher startup; unchanged conditions are
    // memoed after that, while a missing Node is not.
    let mut last_readiness: Option<DataplaneHealthStatus> = None;

    // Run an initial sync against an empty applied map so every peer known
    // in the datastore gets applied to the dataplane on watcher start.
    // This covers peers that bootstrap may have partially failed to apply
    // (e.g. transient WireGuard or FDB setup errors) — the watcher starts
    // from truth-in-store, not from bootstrap's local success/failure state.
    match sync_peer_routes(db.as_ref(), &my_node_name, peering.as_ref(), &mut applied).await {
        Ok(outcome) => {
            reconcile_local_readiness(
                db.as_ref(),
                outbox.as_deref(),
                &my_node_name,
                dataplane_health.as_ref(),
                &outcome,
                &mut last_readiness,
            )
            .await;
        }
        Err(e) => tracing::warn!("node_subnet: initial peer route sync failed: {:#}", e),
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("node_subnet: peer watch cancelled");
                break;
            }
            result = cursor.next_event() => match result {
            Ok(event) => {
                if !is_node_event(&event) {
                    continue;
                }
                if let Some(peer_name) = event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                {
                    let can_write_cluster_state = raft_leader_proxy
                        .as_ref()
                        .is_none_or(|proxy| proxy.is_leader());
                    if let Err(e) = reconcile_peer_node_event_cluster_state(
                        db.as_ref(),
                        &my_node_name,
                        &cluster_cidr,
                        &event,
                        can_write_cluster_state,
                    )
                    .await
                    {
                        tracing::warn!(
                            "node_subnet: peer {} cluster-state reconcile failed: {}",
                            peer_name,
                            e
                        );
                    }
                    match sync_peer_routes(
                        db.as_ref(),
                        &my_node_name,
                        peering.as_ref(),
                        &mut applied,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            reconcile_local_readiness(
                                db.as_ref(),
                                outbox.as_deref(),
                                &my_node_name,
                                dataplane_health.as_ref(),
                                &outcome,
                                &mut last_readiness,
                            )
                            .await;
                        }
                        Err(e) => tracing::warn!("node_subnet: peer sync failed: {}", e),
                    }
                }
            }
            Err(WatchCursorError::Closed) => {
                tracing::warn!("node_subnet: watch signal channel closed");
                break;
            }
            Err(WatchCursorError::Expired) => {
                tracing::warn!("node_subnet: watch replay window expired; peer routes will resync on the next signal");
            }
            Err(WatchCursorError::Replay(err)) => {
                tracing::warn!("node_subnet: watch replay failed: {err:#}");
            }
            }
        }
    }
}

async fn reconcile_peer_node_event_cluster_state(
    db: &dyn DatastoreBackend,
    my_node_name: &str,
    cluster_cidr: &str,
    event: &WatchEvent,
    can_write_cluster_state: bool,
) -> Result<()> {
    let Some(peer_name) = event
        .object
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
    else {
        return Ok(());
    };
    if peer_name == my_node_name {
        return Ok(());
    }
    if !can_write_cluster_state {
        tracing::debug!(
            node = %peer_name,
            "node_subnet: skipping peer cluster-state update on non-leader"
        );
        return Ok(());
    }

    match event.event_type {
        EventType::Deleted => {
            if let Err(e) = db.delete_node_subnet(peer_name).await {
                tracing::warn!(
                    "node_subnet: failed to delete subnet for peer {}: {}",
                    peer_name,
                    e
                );
            }
        }
        EventType::Added | EventType::Modified => {
            let Some(live_node) = db.get_resource("v1", "Node", None, peer_name).await? else {
                return Ok(());
            };
            if live_node
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty())
            {
                if let Err(e) = db.delete_node_subnet(peer_name).await {
                    tracing::warn!(
                        "node_subnet: failed to delete subnet for deleting peer {}: {}",
                        peer_name,
                        e
                    );
                }
                return Ok(());
            }

            if let Some(node_ip) = node_dataplane_ip(&live_node.data) {
                match db
                    .allocate_node_subnet(peer_name, cluster_cidr, &node_ip)
                    .await
                {
                    Err(e) => {
                        tracing::warn!(
                            "node_subnet: failed to allocate subnet for peer {}: {}",
                            peer_name,
                            e
                        );
                    }
                    _ => {
                        // F2-04: project mode + hostport-range annotations
                        // onto the peer row so sync_peer_routes can pick the
                        // right NodeEndpoint variant for each peer.
                        let (mode, hostport_range) = project_node_peer_attributes(&live_node.data);
                        if let Err(e) = db
                            .update_node_peer_attributes(peer_name, mode, hostport_range)
                            .await
                        {
                            tracing::warn!(
                                "node_subnet: failed to update peer {} mode/hostport-range: {}",
                                peer_name,
                                e
                            );
                        }
                    }
                }
            }
        }
        EventType::Bookmark | EventType::Error => {}
    }

    Ok(())
}

pub async fn sync_peer_routes(
    db: &dyn DatastoreBackend,
    my_node_name: &str,
    network: &dyn crate::networking::PeerRouter,
    applied: &mut HashMap<String, AppliedPeer>,
) -> Result<PeerSyncOutcome> {
    let desired_list = db
        .list_peer_subnets(my_node_name)
        .await
        .context("list_peer_subnets failed")?;
    let desired: HashMap<String, NodeSubnet> = desired_list
        .into_iter()
        .map(|peer| (peer.node_name.as_str().to_string(), peer))
        .collect();

    let mut outcome = PeerSyncOutcome {
        desired_peers: desired.len(),
        ready_peers: 0,
        unreachable_ready_peers: 0,
    };

    if desired.is_empty() && applied.is_empty() {
        return Ok(outcome);
    }

    for (name, peer) in &desired {
        // Only Ready peers gate our readiness — a NotReady peer cannot form a
        // tunnel anyway and must not wedge us NotReady indefinitely.
        let peer_ready = peer_node_is_ready(db, name).await;
        if peer_ready {
            outcome.ready_peers += 1;
        }
        let Some(endpoint) = endpoint_for_peer(db, peer).await? else {
            // Skipped peers must not appear in `applied` — otherwise a later
            // reconcile would treat them as stale and emit a phantom remove
            // against an endpoint we never applied.
            applied.remove(name);
            if peer_ready {
                outcome.unreachable_ready_peers += 1;
            }
            continue;
        };
        let needs_apply = applied
            .get(name)
            .map(|old| {
                let s = &old.subnet;
                s.subnet != peer.subnet
                    || s.vtep_ip != peer.vtep_ip
                    || s.node_ip != peer.node_ip
                    || s.mode != peer.mode
                    || s.hostport_range != peer.hostport_range
                    || old.endpoint != endpoint
            })
            .unwrap_or(true);
        if !needs_apply {
            continue;
        }
        if let Some(old) = applied.remove(name) {
            network
                .remove_peer_endpoint(&old.endpoint)
                .await
                .with_context(|| format!("replace peer {}", name))?;
        }
        network
            .apply_peer_endpoint(&endpoint)
            .await
            .with_context(|| format!("apply peer {}", name))?;
        applied.insert(
            name.clone(),
            AppliedPeer {
                subnet: peer.clone(),
                endpoint,
            },
        );
    }

    let stale: Vec<String> = applied
        .keys()
        .filter(|name| !desired.contains_key(*name))
        .cloned()
        .collect();
    for stale_name in stale {
        if stale_name == my_node_name {
            continue;
        }
        if let Some(applied_peer) = applied.remove(&stale_name) {
            // Remove using the EXACT endpoint variant we applied. Root removal
            // against a rootless endpoint (or vice versa) is undefined under
            // the current PeerRouter contract.
            network
                .remove_peer_endpoint(&applied_peer.endpoint)
                .await
                .with_context(|| format!("remove peer {}", stale_name))?;
        }
    }

    Ok(outcome)
}

/// Update local dataplane health from a peer-sync outcome and, if the combined
/// readiness changed, re-publish the node's `Ready`/`NetworkUnavailable`
/// conditions. No-op when health tracking is disabled (single-node test paths).
async fn reconcile_local_readiness(
    db: &dyn DatastoreBackend,
    outbox: Option<&Outbox>,
    my_node_name: &str,
    dataplane_health: Option<&DataplaneHealth>,
    outcome: &PeerSyncOutcome,
    last_readiness: &mut Option<DataplaneHealthStatus>,
) {
    let Some(health) = dataplane_health else {
        return;
    };
    apply_peer_sync_outcome(health, outcome);
    let new_status = health.status();
    if last_readiness.as_ref() == Some(&new_status) {
        return;
    }
    match crate::kubelet::node::refresh_node_network_conditions(db, outbox, my_node_name, health)
        .await
    {
        Ok(crate::kubelet::node::NodeNetworkRefreshResult::Updated) => {
            tracing::info!(
                node = %my_node_name,
                ready = new_status.is_healthy(),
                reason = new_status.reason().unwrap_or("Ready"),
                "node_subnet: dataplane readiness updated"
            );
            *last_readiness = Some(new_status);
        }
        Ok(crate::kubelet::node::NodeNetworkRefreshResult::Unchanged) => {
            *last_readiness = Some(new_status);
            tracing::debug!(
                node = %my_node_name,
                "node_subnet: readiness refresh skipped (conditions unchanged)"
            );
        }
        Ok(crate::kubelet::node::NodeNetworkRefreshResult::Missing) => {
            // Node not found — do NOT memo the readiness. A future re-sync
            // (triggered by a Node watch event) must retry.
            tracing::debug!(
                node = %my_node_name,
                "node_subnet: readiness refresh skipped (node not found)"
            );
        }
        Err(e) => {
            tracing::warn!(
                "node_subnet: failed to refresh node network conditions: {:#}",
                e
            );
            // Do NOT memo on error — a later re-sync must retry.
        }
    }
}

/// Read a peer Node's `Ready` condition. Missing node or non-True status => not
/// ready (and therefore excluded from readiness gating).
async fn peer_node_is_ready(db: &dyn DatastoreBackend, node_name: &str) -> bool {
    match db.get_resource("v1", "Node", None, node_name).await {
        Ok(Some(node)) => node_ready_condition_is_true(&node.data),
        Ok(None) => false,
        Err(e) => {
            tracing::warn!(
                node = %node_name,
                error = %e,
                "node_subnet: failed to read peer Node readiness; treating as NotReady"
            );
            false
        }
    }
}

fn node_ready_condition_is_true(node: &serde_json::Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .is_some_and(|conditions| {
            conditions.iter().any(|cond| {
                cond.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && cond.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
}

fn is_node_event(event: &WatchEvent) -> bool {
    if event.event_type == EventType::Bookmark {
        return false;
    }
    event.object.get("kind").and_then(|v| v.as_str()) == Some("Node")
}

fn node_dataplane_ip(node: &serde_json::Value) -> Option<String> {
    node_address(node, "ExternalIP").or_else(|| node_address(node, "InternalIP"))
}

fn node_address(node: &serde_json::Value, address_type: &str) -> Option<String> {
    node.pointer("/status/addresses")
        .and_then(|v| v.as_array())
        .and_then(|addrs| {
            addrs.iter().find_map(|addr| {
                if addr.get("type").and_then(|v| v.as_str()) == Some(address_type) {
                    addr.get("address")
                        .and_then(|v| v.as_str())
                        .filter(|value| !value.trim().is_empty())
                        .map(str::to_string)
                } else {
                    None
                }
            })
        })
}

/// F2-04: read `klights.io/mode` + `klights.io/hostport-range` annotations
/// from a Node object and project them into the persisted peer model. Falls
/// back to `Root` / `None` when annotations are missing or unparseable.
fn project_node_peer_attributes(node: &serde_json::Value) -> (NodePeerMode, Option<HostPortRange>) {
    let annotations = node.pointer("/metadata/annotations");
    let mode_str = annotations
        .and_then(|a| a.get(NODE_MODE_ANNOTATION))
        .and_then(|v| v.as_str());
    let mode = parse_node_peer_mode(mode_str).unwrap_or(NodePeerMode::Root);
    let range_str = annotations
        .and_then(|a| a.get(HOSTPORT_RANGE_ANNOTATION))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let hostport_range = range_str.and_then(|s| HostPortRange::parse(s).ok());
    (mode, hostport_range)
}

#[cfg(test)]
mod tests {
    use crate::controllers::annotations::NodePeerMode;
    use crate::networking::test_support::{MockNetworkProvider, NetworkCall};
    use serde_json::json;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_allocate_node_subnet_first_node_gets_first_24() {
        let db = crate::datastore::test_support::in_memory().await;
        let subnet = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");
        assert_eq!(subnet.vtep_ip.to_string(), "10.42.0.0");
        assert_eq!(subnet.node_ip.to_string(), "192.168.1.1");
        assert_eq!(subnet.node_name.as_str(), "node-a");
    }

    #[tokio::test]
    async fn test_allocate_node_subnet_second_node_gets_next_24() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        let subnet_b = db
            .allocate_node_subnet("node-b", "10.42.0.0/16", "192.168.1.2")
            .await
            .unwrap();
        assert_eq!(subnet_b.subnet.to_string(), "10.42.1.0/24");
        assert_eq!(subnet_b.vtep_ip.to_string(), "10.42.1.0");
    }

    #[tokio::test]
    async fn test_allocate_node_subnet_idempotent_for_existing_node() {
        let db = crate::datastore::test_support::in_memory().await;
        let first = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        let second = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        assert_eq!(first.subnet, second.subnet);
    }

    #[tokio::test]
    async fn test_get_node_subnet_returns_none_when_absent() {
        let db = crate::datastore::test_support::in_memory().await;
        let result = db.get_node_subnet("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_node_subnet_returns_record_after_allocation() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        let record = db.get_node_subnet("node-a").await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().node_ip.to_string(), "10.0.0.1");
    }

    #[tokio::test]
    async fn follower_peer_node_event_does_not_write_cluster_state() {
        use crate::datastore::DatastoreBackend;
        use crate::datastore::command::StorageCommand;
        use crate::datastore::replicated::{RaftProposer, ReplicatedDatastore, ReplicationMode};
        use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
        use std::sync::Arc;

        struct FollowerProposer;

        #[async_trait::async_trait]
        impl RaftProposer for FollowerProposer {
            async fn propose_command(&self, _command: StorageCommand) -> anyhow::Result<()> {
                anyhow::bail!("not the leader")
            }

            async fn propose_outbox_command(
                &self,
                _idempotency_key: &str,
                _operation: &str,
                _command: StorageCommand,
                _authoring_node: &str,
            ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
                Err(OutboxApplyError::Retryable("not the leader".to_string()))
            }
        }

        let inner: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let db = ReplicatedDatastore::new(
            inner,
            ReplicationMode::Raft {
                node_name: "mn-controlplane2".to_string(),
            },
        );
        db.set_raft_proposer(Arc::new(FollowerProposer));
        let event = crate::watch::WatchEvent::added(json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "mn-controlplane1"
            },
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "10.99.0.10"}
                ]
            }
        }));

        super::reconcile_peer_node_event_cluster_state(
            &db,
            "mn-controlplane2",
            "10.50.0.0/16",
            &event,
            false,
        )
        .await
        .expect("follower peer event must be a local no-op");

        assert!(
            db.get_node_subnet("mn-controlplane1")
                .await
                .unwrap()
                .is_none(),
            "follower peer watcher must not allocate peer subnets locally"
        );
        assert!(
            db.list_applied_outbox().await.unwrap().is_empty(),
            "follower peer watcher must not leave local raft proposal placeholders"
        );
    }

    #[tokio::test]
    async fn stale_peer_node_event_after_live_delete_does_not_allocate_subnet() {
        let db = crate::datastore::test_support::in_memory().await;
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "node-b"
            },
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "10.99.0.10"}
                ]
            }
        });
        db.create_resource("v1", "Node", None, "node-b", node.clone())
            .await
            .unwrap();
        let stale_event = crate::watch::WatchEvent::modified(node);
        db.delete_resource("v1", "Node", None, "node-b")
            .await
            .unwrap();

        super::reconcile_peer_node_event_cluster_state(
            &db,
            "node-a",
            "10.50.0.0/16",
            &stale_event,
            true,
        )
        .await
        .expect("stale peer Node event should be ignored after live delete");

        assert!(
            db.get_node_subnet("node-b").await.unwrap().is_none(),
            "stale Node events must not allocate peer subnet rows after live Node deletion"
        );
    }

    #[test]
    fn node_dataplane_ip_prefers_external_ip_for_peer_routing() {
        let node = serde_json::json!({
            "status": {
                "addresses": [
                    {"type": "Hostname", "address": "worker-a"},
                    {"type": "InternalIP", "address": "10.0.0.7"},
                    {"type": "ExternalIP", "address": "203.0.113.77"}
                ]
            }
        });

        assert_eq!(
            super::node_dataplane_ip(&node).as_deref(),
            Some("203.0.113.77")
        );
    }

    #[test]
    fn node_dataplane_ip_falls_back_to_internal_ip_when_external_missing() {
        let node = serde_json::json!({
            "status": {
                "addresses": [
                    {"type": "Hostname", "address": "worker-a"},
                    {"type": "InternalIP", "address": "10.0.0.7"}
                ]
            }
        });

        assert_eq!(super::node_dataplane_ip(&node).as_deref(), Some("10.0.0.7"));
    }

    /// F2-04: list_peer_subnets excludes self and includes all peer rows; the
    /// controller decides per-peer whether to install a route via endpoint_for_peer.
    #[tokio::test]
    async fn test_list_peer_subnets_excludes_self_and_includes_peer_rows() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        let peers = db.list_peer_subnets("node-a").await.unwrap();
        assert_eq!(peers.len(), 1, "self excluded, peer row included");
        assert_eq!(peers[0].node_name.as_str(), "node-b");
    }

    #[tokio::test]
    async fn sync_peer_routes_uses_wireguard_by_default_for_enabled_peer() {
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };

        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
                Some("10.0.0.2".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let network = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .expect("sync_peer_routes should succeed");

        let calls = network.calls();
        assert_eq!(calls.len(), 1);
        assert!(
            !calls
                .iter()
                .any(|call| matches!(call, NetworkCall::ApplyUnencryptedPeerEndpoint { .. })),
            "enabled WireGuard dataplane must not install direct pod-CIDR routes"
        );
        match &calls[0] {
            NetworkCall::ApplyWireGuardPeerEndpoint {
                node_name,
                endpoint,
                allowed_pod_cidr,
            } => {
                assert_eq!(node_name, "node-b");
                assert_eq!(endpoint, "10.0.0.2:51820");
                assert_eq!(allowed_pod_cidr, "10.42.1.0/24");
            }
            other => panic!("unexpected network call: {other:?}"),
        }
        assert_eq!(applied.len(), 1);
    }

    #[tokio::test]
    async fn sync_peer_routes_disabled_encryption_uses_typed_unencrypted_direct_route() {
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };

        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Disabled,
                None,
                Some("10.0.0.2".to_string()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let network = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .expect("sync_peer_routes should succeed");

        let calls = network.calls();
        assert!(calls.iter().any(|call| matches!(
            call,
            NetworkCall::ApplyUnencryptedPeerEndpoint {
                node_name,
                node_ip,
                allowed_pod_cidr
            } if node_name == "node-b" && node_ip == "10.0.0.2" && allowed_pod_cidr == "10.42.1.0/24"
        )));
        assert_eq!(
            calls
                .iter()
                .filter(|call| matches!(call, NetworkCall::ApplyUnencryptedPeerEndpoint { .. }))
                .count(),
            1,
            "explicit disabled mode should apply only the direct route: {calls:?}"
        );
    }

    #[tokio::test]
    async fn test_delete_node_subnet_removes_record() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.delete_node_subnet("node-a").await.unwrap();
        let record = db.get_node_subnet("node-a").await.unwrap();
        assert!(record.is_none());
    }

    /// F2-02: rootless boot must allocate the local subnet without ever
    /// reaching the peer router. Before the split, `ensure_node_subnet`
    /// always called `apply_peer_endpoint`, which crashed boot under the
    /// rootless `Bypass4NetnsPeerRouter` stub.
    #[tokio::test]
    async fn bootstrap_rootless_allocates_local_subnet_without_peer_router() {
        let db = crate::datastore::test_support::in_memory().await;

        let subnet =
            super::ensure_local_node_subnet(&db, "rootless-node-a", "10.42.0.0/16", "10.0.0.7")
                .await
                .expect("ensure_local_node_subnet must succeed without a peer router");

        assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");
        let row = db
            .get_node_subnet("rootless-node-a")
            .await
            .unwrap()
            .expect("local subnet row must exist after allocation");
        assert_eq!(row.node_ip.to_string(), "10.0.0.7");

        // Construct a peer router and prove it has zero recorded calls — the
        // local-only path must never reach apply_peer_endpoint.
        let mock_peer = MockNetworkProvider::new();
        // Sanity: the mock starts with no calls.
        assert!(
            mock_peer.calls().is_empty(),
            "mock peer router must not have been called by ensure_local_node_subnet"
        );
    }

    /// 2A-11 additive gate: a single-node cluster with no peers must not try
    /// to touch VXLAN peer state.
    #[tokio::test]
    async fn sync_peer_routes_single_node_with_no_peers_makes_no_changes() {
        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();

        let mock_peer = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .unwrap();

        assert!(
            mock_peer.calls().is_empty(),
            "single-node sync must not touch peer routes/FDB"
        );
        assert!(
            applied.is_empty(),
            "single-node sync must not record any applied peers"
        );
    }

    /// F2-02: root-mode bootstrap installs peer routes after the local subnet
    /// allocation. This pins the ordering: subnet first, then peer routes.
    #[tokio::test]
    async fn bootstrap_root_installs_peer_routes_after_local_subnet() {
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };

        let db = crate::datastore::test_support::in_memory().await;

        // Local subnet first.
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .expect("local subnet allocation must succeed");

        // Pre-existing peer with default-on encrypted dataplane metadata.
        let peer_subnet = db
            .allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .expect("peer allocation must succeed");
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
                Some("10.0.0.2".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        // Then peer routes.
        let mock_peer = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .expect("sync_peer_routes must succeed in root mode");

        let calls = mock_peer.calls();
        assert_eq!(calls.len(), 1, "root mode must apply exactly one peer");
        match &calls[0] {
            NetworkCall::ApplyWireGuardPeerEndpoint {
                node_name,
                allowed_pod_cidr,
                ..
            } => {
                assert_eq!(node_name, "node-b");
                assert_eq!(allowed_pod_cidr, &peer_subnet.subnet.to_string());
            }
            other => panic!("expected ApplyWireGuardPeerEndpoint, got {other:?}"),
        }
        assert!(applied.contains_key("node-b"));
    }

    /// Phase 2C: a rootless peer with enabled encryption dispatches to
    /// WireGuard, not HostPort/bypass4netns or VXLAN fallback.
    #[tokio::test]
    async fn sync_peer_routes_dispatches_wireguard_for_rootless_peer() {
        use crate::controllers::annotations::NodePeerMode;
        use crate::networking::types::HostPortRange;
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };

        let db = crate::datastore::test_support::in_memory().await;
        // Local node first.
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        // Rootless peer.
        db.allocate_node_subnet("rootless-b", "10.42.0.0/16", "10.0.0.9")
            .await
            .unwrap();
        db.update_node_peer_attributes(
            "rootless-b",
            NodePeerMode::Rootless,
            Some(HostPortRange {
                start: 30000,
                end: 32767,
            }),
        )
        .await
        .unwrap();
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "rootless-b".to_string(),
                DataplaneMode::Rootless,
                DataplaneEncryption::Enabled,
                Some("AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=".to_string()),
                Some("10.0.0.9".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let mock_peer = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .expect("rootless peer dispatch must succeed");
        let calls = mock_peer.calls();
        assert!(calls.iter().any(|c| matches!(
            c,
            NetworkCall::ApplyWireGuardPeerEndpoint {
                node_name,
                endpoint,
                allowed_pod_cidr
            } if node_name == "rootless-b" && endpoint == "10.0.0.9:51820" && allowed_pod_cidr == "10.42.1.0/24"
        )), "rootless peer must dispatch to WireGuard, got {calls:?}");
        assert_eq!(
            calls
                .iter()
                .filter(|c| matches!(c, NetworkCall::ApplyWireGuardPeerEndpoint { node_name, .. } if node_name == "rootless-b"))
                .count(),
            1,
            "rootless peer must dispatch exactly once to WireGuard, got {calls:?}"
        );
        let entry = applied
            .get("rootless-b")
            .expect("AppliedPeer must record the rootless apply");
        assert!(matches!(
            entry.endpoint,
            crate::networking::NodeEndpoint::WireGuard(_)
        ));
    }

    /// Phase 2C: a root peer with enabled encryption dispatches to WireGuard.
    #[tokio::test]
    async fn sync_peer_routes_dispatches_wireguard_for_root_peer() {
        use crate::networking::NodeEndpoint;
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };
        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM=".to_string()),
                Some("10.0.0.2".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let mock_peer = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .unwrap();
        let entry = applied.get("node-b").expect("root peer must be applied");
        assert!(
            matches!(entry.endpoint, NodeEndpoint::WireGuard(_)),
            "root peer must use NodeEndpoint::WireGuard, got {:?}",
            entry.endpoint
        );
    }

    /// F2-04 stale-removal gate: removal must use the SAME endpoint variant we
    /// applied. After applying a WireGuard peer, the same mock must observe a
    /// RemoveWireGuardPeerEndpoint call with the WireGuard endpoint when the peer is
    /// dropped.
    #[tokio::test]
    async fn sync_peer_routes_removes_with_matching_endpoint_variant() {
        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        db.update_node_dataplane(
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
                Some("BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ=".to_string()),
                Some("10.0.0.2".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let mock_peer = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .unwrap();
        // Drop node-b: list_peer_subnets returns []
        db.delete_node_subnet("node-b").await.unwrap();
        super::sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
            .await
            .unwrap();
        let calls = mock_peer.calls();
        let removed = calls
            .iter()
            .filter(|c| matches!(c, NetworkCall::RemoveWireGuardPeerEndpoint { node_name, .. } if node_name == "node-b"))
            .count();
        assert_eq!(removed, 1, "stale node-b must be removed exactly once");
        assert!(
            !applied.contains_key("node-b"),
            "applied map must drop the removed peer"
        );
    }

    /// F2-04 annotation projection gate: a Node with the rootless mode +
    /// hostport-range annotations must be projected onto a `node_subnets` row
    /// with mode=Rootless and the parsed range.
    #[tokio::test]
    async fn node_annotations_project_to_rootless_node_subnet() {
        use crate::controllers::annotations::NodePeerMode;
        use crate::networking::types::HostPortRange;

        let db = crate::datastore::test_support::in_memory().await;
        // Bypass the watch loop: simulate what run_peer_watch does on an
        // ADDED/MODIFIED Node event with rootless annotations.
        db.allocate_node_subnet("rootless-c", "10.42.0.0/16", "10.0.0.7")
            .await
            .unwrap();
        let node_obj = serde_json::json!({
            "metadata": {
                "annotations": {
                    "klights.io/mode": "rootless",
                    "klights.io/hostport-range": "31000-31999",
                }
            }
        });
        let (mode, range) = super::project_node_peer_attributes(&node_obj);
        assert_eq!(mode, NodePeerMode::Rootless);
        assert_eq!(
            range,
            Some(HostPortRange {
                start: 31000,
                end: 31999,
            })
        );
        db.update_node_peer_attributes("rootless-c", mode, range)
            .await
            .unwrap();

        let row = db
            .get_node_subnet("rootless-c")
            .await
            .unwrap()
            .expect("rootless-c row must exist");
        assert_eq!(row.mode, NodePeerMode::Rootless);
        assert_eq!(row.hostport_range, range);
    }

    /// Bug 2: a Ready peer that has no dataplane metadata is counted as an
    /// unreachable Ready peer, so the local node must not report Ready.
    #[tokio::test]
    async fn sync_peer_routes_counts_ready_peer_without_metadata_as_unreachable() {
        use crate::networking::test_support::MockNetworkProvider;

        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        // node-b is Ready but has NO node_dataplane row => unreachable.
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-b",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-b"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
        )
        .await
        .unwrap();

        let network = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        let outcome = super::sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .expect("sync must succeed");

        assert_eq!(outcome.desired_peers, 1);
        assert_eq!(outcome.ready_peers, 1);
        assert_eq!(
            outcome.unreachable_ready_peers, 1,
            "a Ready peer without dataplane metadata must count as unreachable"
        );

        let health = crate::networking::dataplane_health::DataplaneHealth::new_healthy();
        super::apply_peer_sync_outcome(&health, &outcome);
        assert!(
            !health.status().is_healthy(),
            "node must report NetworkUnavailable while a Ready peer is unreachable"
        );
    }

    /// Bug 2: a NotReady peer with no metadata must NOT gate our readiness —
    /// otherwise a genuinely-down node wedges the cluster NotReady forever.
    #[tokio::test]
    async fn sync_peer_routes_excludes_not_ready_peer_from_readiness() {
        use crate::networking::test_support::MockNetworkProvider;

        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        // node-b is NotReady and has no dataplane row.
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-b",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-b"},
                "status": {"conditions": [{"type": "Ready", "status": "False"}]}
            }),
        )
        .await
        .unwrap();

        let network = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        let outcome = super::sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .expect("sync must succeed");

        assert_eq!(outcome.desired_peers, 1);
        assert_eq!(outcome.ready_peers, 0);
        assert_eq!(
            outcome.unreachable_ready_peers, 0,
            "NotReady peers must be excluded from readiness gating"
        );

        let health = crate::networking::dataplane_health::DataplaneHealth::new_healthy();
        health.set_peers_pending();
        super::apply_peer_sync_outcome(&health, &outcome);
        assert!(
            health.status().is_healthy(),
            "with no reachable-Ready-peer gap the node may report Ready"
        );
    }

    /// Bug 2: a Ready peer WITH dataplane metadata is reachable, so the node
    /// reports Ready.
    #[tokio::test]
    async fn sync_peer_routes_ready_peer_with_metadata_is_connected() {
        use crate::networking::test_support::MockNetworkProvider;
        use crate::networking::wireguard::{
            DataplaneEncryption, DataplaneMode, DataplanePeerMetadata,
        };

        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-b",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-b"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
        )
        .await
        .unwrap();
        db.update_node_dataplane(
            DataplanePeerMetadata::try_new(
                "node-b".to_string(),
                DataplaneMode::Root,
                DataplaneEncryption::Enabled,
                Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
                Some("10.0.0.2".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let network = MockNetworkProvider::new();
        let mut applied: HashMap<String, super::AppliedPeer> = HashMap::new();
        let outcome = super::sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .expect("sync must succeed");

        assert_eq!(outcome.unreachable_ready_peers, 0);

        let health = crate::networking::dataplane_health::DataplaneHealth::new_healthy();
        health.set_peers_pending();
        super::apply_peer_sync_outcome(&health, &outcome);
        assert!(
            health.status().is_healthy(),
            "a Ready, reachable peer must let the node report Ready"
        );
    }

    /// F2-04 rootless gate: rootless peers are still listed for route projection.
    #[tokio::test]
    async fn list_peer_subnets_includes_rootless_peers() {
        let db = crate::datastore::test_support::in_memory().await;
        super::ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("rootless-d", "10.42.0.0/16", "10.0.0.4")
            .await
            .unwrap();
        let peers = db.list_peer_subnets("node-a").await.unwrap();
        let rootless_peer = peers
            .iter()
            .find(|p| p.node_name.as_str() == "rootless-d")
            .expect("rootless peer must appear in list_peer_subnets");
        assert_eq!(rootless_peer.mode, NodePeerMode::Root);
    }

    /// Bug 4 Option B/C: reconcile_local_readiness must NOT memo the readiness
    /// state when refresh_node_network_conditions fails to write (node not found).
    /// Without this fix, the watcher poisons its last_readiness cache with a
    /// phantom Healthy state on the initial sync, preventing future re-syncs
    /// from correcting the node's Ready condition when the node eventually
    /// appears via the watch mirror.
    #[tokio::test]
    async fn reconcile_local_readiness_does_not_memo_when_node_not_found() {
        use crate::networking::dataplane_health::DataplaneHealth;

        let db = crate::datastore::test_support::in_memory().await;
        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending(); // Start as Unavailable

        // Initial last_readiness matches the health state (Unavailable)
        let initial_status = health.status();
        let mut last_readiness = Some(initial_status.clone());

        // Simulate a successful peer sync: 0 unreachable ready peers → connected
        let outcome = super::PeerSyncOutcome {
            desired_peers: 1,
            ready_peers: 1,
            unreachable_ready_peers: 0,
        };

        // Node does NOT exist in the DB → refresh_node_network_conditions returns Ok(false)
        super::reconcile_local_readiness(
            &db,
            None, // no outbox
            "worker-node",
            Some(&health),
            &outcome,
            &mut last_readiness,
        )
        .await;

        // The health was updated to Connected (Healthy) by apply_peer_sync_outcome
        assert!(
            health.status().is_healthy(),
            "health must be updated to Healthy after successful peer sync"
        );

        // CRITICAL: last_readiness must NOT have been memo'd to Healthy.
        // It must stay at the original Unavailable value so a future re-sync retries.
        assert_eq!(
            last_readiness,
            Some(initial_status),
            "last_readiness must stay at the pre-sync Unavailable value when the node is not found"
        );
    }

    /// Bug 4 Option B/C continuation: after the node appears, a second
    /// reconcile_local_readiness call must successfully memo the readiness
    /// and update the node's conditions.
    #[tokio::test]
    async fn reconcile_local_readiness_memos_after_node_appears() {
        use crate::networking::dataplane_health::{DataplaneHealth, DataplaneHealthStatus};

        let db = crate::datastore::test_support::in_memory().await;
        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending();

        let initial_status = health.status();
        let mut last_readiness = Some(initial_status.clone());

        let outcome = super::PeerSyncOutcome {
            desired_peers: 1,
            ready_peers: 1,
            unreachable_ready_peers: 0,
        };

        // First call: node not found → should NOT memo
        super::reconcile_local_readiness(
            &db,
            None,
            "worker-node",
            Some(&health),
            &outcome,
            &mut last_readiness,
        )
        .await;

        assert_eq!(
            last_readiness,
            Some(initial_status),
            "first call must not memo when node is missing"
        );

        // Now create the node in the DB (simulates registration completing)
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-node"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "old", "lastTransitionTime": "2026-01-01T00:00:00Z"},
                        {"type": "NetworkUnavailable", "status": "True", "reason": "old", "message": "old", "lastTransitionTime": "2026-01-01T00:00:00Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        // Second call: node exists → should memo and update conditions
        super::reconcile_local_readiness(
            &db,
            None,
            "worker-node",
            Some(&health),
            &outcome,
            &mut last_readiness,
        )
        .await;

        assert_eq!(
            last_readiness,
            Some(DataplaneHealthStatus::Healthy),
            "second call must memo Healthy after the node appears"
        );

        // Verify the node was actually updated to Ready
        let node = db
            .get_resource("v1", "Node", None, "worker-node")
            .await
            .unwrap()
            .unwrap();
        let ready_cond = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["type"] == "Ready")
            .unwrap();
        assert_eq!(
            ready_cond["status"], "True",
            "node must be updated to Ready=True"
        );
        let net_cond = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["type"] == "NetworkUnavailable")
            .unwrap();
        assert_eq!(
            net_cond["status"], "False",
            "node must be updated to NetworkUnavailable=False"
        );
    }

    /// Bug 4 Option B/C: reconcile_local_readiness handles the "conditions
    /// unchanged" Ok(false) case. When health hasn't changed, the early return
    /// handles it; when conditions already match in the DB, no write is needed.
    #[tokio::test]
    async fn reconcile_local_readiness_noop_when_conditions_already_match() {
        use crate::networking::dataplane_health::DataplaneHealth;
        let db = crate::datastore::test_support::in_memory().await;
        let health = DataplaneHealth::new_healthy();
        // Health starts Healthy, no peer tracking → Healthy
        let mut last_readiness = Some(health.status()); // Some(Healthy)

        let outcome = super::PeerSyncOutcome {
            desired_peers: 0,
            ready_peers: 0,
            unreachable_ready_peers: 0,
        };

        // Create the node with Healthy conditions (matching current health)
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-node"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready"},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "route ok"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        // Call with matching conditions → Ok(false) (no change needed)
        super::reconcile_local_readiness(
            &db,
            None,
            "worker-node",
            Some(&health),
            &outcome,
            &mut last_readiness,
        )
        .await;

        // last_readiness should stay at Some(Healthy) — it was already Healthy
        // and conditions are unchanged, so no write was needed.
        // The early return at `last_readiness.as_ref() == Some(&new_status)` handles this.
        assert_eq!(
            last_readiness,
            Some(crate::networking::dataplane_health::DataplaneHealthStatus::Healthy),
            "last_readiness stays Healthy when conditions already match"
        );
    }

    #[tokio::test]
    async fn reconcile_local_readiness_memos_initial_noop_when_conditions_already_match() {
        use crate::networking::dataplane_health::DataplaneHealth;

        let db = crate::datastore::test_support::in_memory().await;
        let health = DataplaneHealth::new_healthy();
        let mut last_readiness = None;

        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "worker-node",
                    "annotations": {
                        crate::controllers::annotations::GIT_COMMIT_ANNOTATION: crate::version::GIT_COMMIT_SHORT
                    }
                },
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready"},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        super::reconcile_local_readiness(
            &db,
            None,
            "worker-node",
            Some(&health),
            &super::PeerSyncOutcome::default(),
            &mut last_readiness,
        )
        .await;

        assert_eq!(
            last_readiness,
            Some(crate::networking::dataplane_health::DataplaneHealthStatus::Healthy),
            "initial no-op reconcile must memo confirmed readiness so later Node events do not keep rechecking"
        );
    }
}
