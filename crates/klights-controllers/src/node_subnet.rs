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
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::annotations::{HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION, NodePeerMode};

use klights_network_api::parse_node_peer_mode;
use klights_types::HostPortRange;

#[cfg(test)]
const PEER_SYNC_RETRY_BASE: Duration = Duration::from_millis(20);
#[cfg(not(test))]
const PEER_SYNC_RETRY_BASE: Duration = Duration::from_millis(250);
const PEER_SYNC_RETRY_MAX_SHIFT: u32 = 5;

#[derive(Default)]
struct PeerSyncRetryState {
    attempt: u32,
    generation: u64,
    scheduled: bool,
}

impl PeerSyncRetryState {
    async fn schedule(
        &mut self,
        supervisor: &klights_supervisor::TaskSupervisor,
        sender: &tokio::sync::mpsc::Sender<u64>,
    ) -> Result<()> {
        if self.scheduled {
            return Ok(());
        }
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let shift = self.attempt.min(PEER_SYNC_RETRY_MAX_SHIFT);
        self.attempt = self.attempt.saturating_add(1);
        self.scheduled = true;
        let delay = PEER_SYNC_RETRY_BASE * (1u32 << shift);
        let sender = sender.clone();
        if let Err(error) = supervisor
            .spawn_delay(
                format!("node_peer_sync_retry:{generation}"),
                delay,
                async move {
                    let _ = sender.send(generation).await;
                },
            )
            .await
        {
            self.scheduled = false;
            return Err(error);
        }
        Ok(())
    }

    fn take(&mut self, generation: u64) -> bool {
        if !self.scheduled || self.generation != generation {
            return false;
        }
        self.scheduled = false;
        true
    }

    fn succeeded(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.attempt = 0;
        self.scheduled = false;
    }
}

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
pub trait PeerDataplaneHealth: Send + Sync {
    fn apply_peer_sync_outcome(
        &self,
        outcome: &PeerSyncOutcome,
    ) -> klights_network_api::DataplaneHealthSnapshot;
}

pub fn apply_peer_sync_outcome(
    health: &dyn PeerDataplaneHealth,
    outcome: &PeerSyncOutcome,
) -> klights_network_api::DataplaneHealthSnapshot {
    health.apply_peer_sync_outcome(outcome)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeReadinessPublishResult {
    Updated,
    Unchanged,
    Missing,
}

pub type NodeReadinessPublishFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = klights_reconcile_api::ControllerStoreResult<NodeReadinessPublishResult>,
            > + Send
            + 'a,
    >,
>;

pub trait NodeReadinessPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        node_name: &'a str,
        health: &'a klights_network_api::DataplaneHealthSnapshot,
    ) -> NodeReadinessPublishFuture<'a>;
}

/// Tracks every peer the controller has actually installed against the network
/// `PeerRouter`, keyed by node name. Stores both the persisted node subnet
/// (for change detection) and the exact `PeerRoute` variant we applied so
/// removal hits the same shape — root removal must not be issued against a
/// rootless endpoint or vice versa.
#[derive(Clone)]
pub struct AppliedPeer {
    pub subnet: PeerSubnet,
    pub endpoint: klights_network_api::PeerRoute,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSubnet {
    pub node_name: String,
    pub subnet: String,
    pub gateway_ip: std::net::Ipv4Addr,
    pub node_ip: std::net::Ipv4Addr,
    pub mode: klights_types::NodePeerMode,
    pub hostport_range: Option<HostPortRange>,
}

fn peer_subnet_from_leader(peer: klights_leader_api::NodeSubnet) -> PeerSubnet {
    PeerSubnet {
        node_name: peer.node_name().to_string(),
        subnet: peer.subnet().to_string(),
        gateway_ip: peer.gateway_ip(),
        node_ip: peer.node_ip(),
        mode: match peer.mode() {
            klights_leader_api::NetworkNodeMode::Root => klights_types::NodePeerMode::Root,
            klights_leader_api::NetworkNodeMode::Rootless => klights_types::NodePeerMode::Rootless,
        },
        hostport_range: peer.hostport_range().map(|range| HostPortRange {
            start: range.start(),
            end: range.end(),
        }),
    }
}

fn peer_route_from_leader(
    metadata: klights_leader_api::NetworkDataplane,
    peer_pod_cidr: &str,
) -> Result<klights_network_api::PeerRoute> {
    match metadata.encryption() {
        klights_leader_api::DataplaneEncryption::WireGuard => {
            use base64::Engine as _;

            let encoded_key = metadata
                .public_key()
                .context("encrypted peer metadata is missing a public key")?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded_key)
                .context("decode peer WireGuard public key")?;
            let key: [u8; 32] = decoded
                .try_into()
                .map_err(|_| anyhow::anyhow!("peer WireGuard public key must contain 32 bytes"))?;
            let port = metadata
                .port()
                .context("encrypted peer metadata is missing a listen port")?;
            let route = klights_network_api::WireGuardPeerRoute::try_new(
                metadata.node_name(),
                klights_network_api::WireGuardPeerKey::new(key),
                std::net::SocketAddr::new(metadata.endpoint(), port),
                peer_pod_cidr,
            )
            .map_err(anyhow::Error::new)?;
            Ok(klights_network_api::PeerRoute::WireGuard(route))
        }
        klights_leader_api::DataplaneEncryption::Direct => {
            let route = klights_network_api::DirectPeerRoute::try_new(
                metadata.node_name(),
                metadata.endpoint(),
                peer_pod_cidr,
            )
            .map_err(anyhow::Error::new)?;
            Ok(klights_network_api::PeerRoute::Direct(route))
        }
    }
}

/// Allocate (or retrieve) the local node's /24 subnet.
///
/// F2-02 split: this owns node-local IPAM and metadata only. Peer route install
/// lives in [`sync_peer_routes`] so callers (rootless / hybrid) that have no
/// valid `PeerRouter` for the current mode can still allocate locally.
///
/// Idempotent: re-running finds the existing allocation in SQLite.
///
pub type PeerTopologyProjectionFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = klights_reconcile_api::ControllerStoreResult<()>>
            + Send
            + 'a,
    >,
>;

/// Leader-only mutation edge that projects Node lifecycle events into the
/// canonical subnet topology. Workers receive no implementation of this port.
pub trait PeerTopologyProjection: Send + Sync {
    fn reconcile_node_event<'a>(
        &'a self,
        event: &'a klights_leader_api::ResourceEvent,
    ) -> PeerTopologyProjectionFuture<'a>;
}

#[allow(clippy::too_many_arguments)]
async fn sync_focused_peers_and_publish_readiness(
    topology: &dyn klights_leader_api::LeaderNetworkTopologyQuery,
    query: &dyn klights_leader_api::LeaderResourceQuery,
    my_node_name: &str,
    peering: &dyn klights_network_api::PeerRouter,
    applied: &mut HashMap<String, AppliedPeer>,
    readiness_publisher: &dyn NodeReadinessPublisher,
    dataplane_health: Option<&dyn PeerDataplaneHealth>,
    last_readiness: &mut Option<klights_network_api::DataplaneHealthSnapshot>,
) -> Result<()> {
    let outcome =
        sync_peer_routes_with_ports(topology, query, my_node_name, peering, applied).await?;
    reconcile_local_readiness_with_publisher(
        query,
        readiness_publisher,
        my_node_name,
        dataplane_health,
        &outcome,
        last_readiness,
    )
    .await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn run_focused_peer_watch(
    topology: std::sync::Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery>,
    watch: std::sync::Arc<dyn klights_leader_api::LeaderWatch>,
    projection: Option<std::sync::Arc<dyn PeerTopologyProjection>>,
    my_node_name: String,
    peering: std::sync::Arc<dyn klights_network_api::PeerRouter>,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    dataplane_health: Option<std::sync::Arc<dyn PeerDataplaneHealth>>,
    readiness_publisher: std::sync::Arc<dyn NodeReadinessPublisher>,
    cancel: CancellationToken,
) {
    use futures::StreamExt;

    let mut applied = HashMap::new();
    let mut last_readiness = None;
    let mut cursor = klights_leader_api::WatchResumeCursor::default();
    let mut reconnect_attempt = 0u32;
    let (retry_tx, mut retry_rx) = tokio::sync::mpsc::channel(1);
    let mut retry = PeerSyncRetryState::default();

    loop {
        match sync_focused_peers_and_publish_readiness(
            topology.as_ref(),
            query.as_ref(),
            &my_node_name,
            peering.as_ref(),
            &mut applied,
            readiness_publisher.as_ref(),
            dataplane_health.as_deref(),
            &mut last_readiness,
        )
        .await
        {
            Ok(()) => retry.succeeded(),
            Err(error) => {
                tracing::warn!(error = %error, "focused peer-route sync failed");
                if let Err(schedule_error) =
                    retry.schedule(task_supervisor.as_ref(), &retry_tx).await
                {
                    tracing::warn!(
                        error = %schedule_error,
                        "failed to schedule focused peer-route retry"
                    );
                }
            }
        }

        let request = match klights_leader_api::WatchRequest::try_new(
            "v1", "Node", None, None, None, None, None,
        )
        .and_then(|request| request.with_resume_cursor(cursor))
        {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!(error = %error, "failed to construct focused Node watch");
                break;
            }
        };
        let mut stream = match watch.watch_resources(request).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::warn!(error = %error, "failed to open focused Node watch");
                if !wait_for_peer_watch_reconnect(
                    task_supervisor.as_ref(),
                    &cancel,
                    reconnect_attempt,
                )
                .await
                {
                    break;
                }
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                continue;
            }
        };

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                Some(generation) = retry_rx.recv() => {
                    if !retry.take(generation) {
                        continue;
                    }
                    match sync_focused_peers_and_publish_readiness(
                        topology.as_ref(),
                        query.as_ref(),
                        &my_node_name,
                        peering.as_ref(),
                        &mut applied,
                        readiness_publisher.as_ref(),
                        dataplane_health.as_deref(),
                        &mut last_readiness,
                    ).await {
                        Ok(()) => retry.succeeded(),
                        Err(error) => {
                            tracing::warn!(error = %error, "focused peer-route retry failed");
                            if let Err(schedule_error) =
                                retry.schedule(task_supervisor.as_ref(), &retry_tx).await
                            {
                                tracing::warn!(
                                    error = %schedule_error,
                                    "failed to reschedule focused peer-route retry"
                                );
                            }
                        }
                    }
                }
                item = stream.next() => match item {
                    Some(Ok(event)) => {
                        if event.event_type() == klights_leader_api::WatchEventType::Error {
                            break;
                        }
                        if event.event_type() != klights_leader_api::WatchEventType::Bookmark {
                            if let Some(projection) = projection.as_ref()
                                && let Err(error) = projection.reconcile_node_event(&event).await
                            {
                                tracing::warn!(error = %error, "peer topology projection failed");
                                // Projection is part of applying this durable
                                // event. Keep the pre-event cursor and reopen
                                // through the supervised reconnect delay so
                                // the leader Node projection is replayed.
                                break;
                            }
                            match sync_focused_peers_and_publish_readiness(
                                topology.as_ref(),
                                query.as_ref(),
                                &my_node_name,
                                peering.as_ref(),
                                &mut applied,
                                readiness_publisher.as_ref(),
                                dataplane_health.as_deref(),
                                &mut last_readiness,
                            ).await {
                                Ok(()) => retry.succeeded(),
                                Err(error) => {
                                    tracing::warn!(error = %error, "focused peer-route event sync failed");
                                    if let Err(schedule_error) =
                                        retry.schedule(task_supervisor.as_ref(), &retry_tx).await
                                    {
                                        tracing::warn!(
                                            error = %schedule_error,
                                            "failed to schedule focused peer-route event retry"
                                        );
                                    }
                                }
                            }
                        }
                        if let Err(error) = cursor.advance_after_apply(&event) {
                            tracing::warn!(error = %error, "focused Node watch cursor rejected event");
                            break;
                        }
                        // Opening a transport is not progress: only a safely
                        // applied event proves the watch is healthy enough to
                        // reset exponential reconnect backoff.
                        reconnect_attempt = 0;
                    }
                    Some(Err(error)) => {
                        tracing::warn!(error = %error, "focused Node watch stream failed");
                        break;
                    }
                    None => {
                        break;
                    }
                }
            }
        }
        if !wait_for_peer_watch_reconnect(task_supervisor.as_ref(), &cancel, reconnect_attempt)
            .await
        {
            break;
        }
        reconnect_attempt = reconnect_attempt.saturating_add(1);
    }
}

async fn wait_for_peer_watch_reconnect(
    supervisor: &klights_supervisor::TaskSupervisor,
    cancel: &CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        result = supervisor.sleep(
            "focused_node_peer_watch_reconnect",
            klights_supervisor::reconnect_backoff::delay(attempt),
        ) => result.is_ok(),
    }
}

/// Reconcile peer routes using only the focused read capabilities needed by
/// networking composition. This is the bootstrap/worker-safe counterpart to
/// the legacy datastore-backed controller entry point.
pub async fn sync_peer_routes_with_ports(
    topology: &dyn klights_leader_api::LeaderNetworkTopologyQuery,
    query: &dyn klights_leader_api::LeaderResourceQuery,
    my_node_name: &str,
    network: &dyn klights_network_api::PeerRouter,
    applied: &mut HashMap<String, AppliedPeer>,
) -> Result<PeerSyncOutcome> {
    let request =
        klights_leader_api::PeerSubnetsQuery::try_new(my_node_name).map_err(anyhow::Error::new)?;
    let peers = topology
        .list_peer_subnets(request)
        .await
        .map_err(anyhow::Error::new)
        .context("list peer subnets through focused topology query")?;
    let desired: HashMap<String, PeerSubnet> = peers
        .into_vec()
        .into_iter()
        .map(peer_subnet_from_leader)
        .map(|peer| (peer.node_name.clone(), peer))
        .collect();

    let mut outcome = PeerSyncOutcome {
        desired_peers: desired.len(),
        ready_peers: 0,
        unreachable_ready_peers: 0,
    };

    for (name, peer) in &desired {
        let peer_ready = peer_node_is_ready_with_query(query, name).await;
        if peer_ready {
            outcome.ready_peers += 1;
        }

        let dataplane_request =
            klights_leader_api::NodeDataplaneQuery::try_new(name).map_err(anyhow::Error::new)?;
        let endpoint = topology
            .get_node_dataplane(dataplane_request)
            .await
            .map_err(anyhow::Error::new)?
            .into_option()
            .map(|metadata| peer_route_from_leader(metadata, &peer.subnet))
            .transpose()?;

        let Some(endpoint) = endpoint else {
            if let Some(old) = applied.get(name) {
                network
                    .remove_peer_route(&old.endpoint)
                    .await
                    .with_context(|| format!("remove peer {name} with missing metadata"))?;
                applied.remove(name);
            }
            if peer_ready {
                outcome.unreachable_ready_peers += 1;
            }
            continue;
        };

        let needs_apply = applied.get(name).is_none_or(|old| {
            let previous = &old.subnet;
            previous.subnet != peer.subnet
                || previous.gateway_ip != peer.gateway_ip
                || previous.node_ip != peer.node_ip
                || previous.mode != peer.mode
                || previous.hostport_range != peer.hostport_range
                || old.endpoint != endpoint
        });
        if !needs_apply {
            continue;
        }
        if let Some(old) = applied.get(name) {
            network
                .remove_peer_route(&old.endpoint)
                .await
                .with_context(|| format!("replace peer {name}"))?;
            applied.remove(name);
        }
        network
            .apply_peer_route(&endpoint)
            .await
            .with_context(|| format!("apply peer {name}"))?;
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
    for name in stale {
        if let Some(old) = applied.get(&name) {
            network
                .remove_peer_route(&old.endpoint)
                .await
                .with_context(|| format!("remove peer {name}"))?;
            applied.remove(&name);
        }
    }
    Ok(outcome)
}

async fn peer_node_is_ready_with_query(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_name: &str,
) -> bool {
    let request = match klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    ) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(node = %node_name, error = %error, "invalid peer Node readiness query");
            return false;
        }
    };
    match query.get_resource(request).await {
        Ok(Some(node)) => node_ready_condition_is_true(&node.data),
        Ok(None) => false,
        Err(error) => {
            tracing::warn!(
                node = %node_name,
                error = %error,
                "failed to read peer Node readiness through focused query"
            );
            false
        }
    }
}

/// Update local dataplane health from a peer-sync outcome and, if the combined
/// readiness changed, re-publish the node's `Ready`/`NetworkUnavailable`
/// conditions. No-op when health tracking is disabled (single-node test paths).
async fn reconcile_local_readiness_with_publisher(
    _query: &dyn klights_leader_api::LeaderResourceQuery,
    readiness_publisher: &dyn NodeReadinessPublisher,
    my_node_name: &str,
    dataplane_health: Option<&dyn PeerDataplaneHealth>,
    outcome: &PeerSyncOutcome,
    last_readiness: &mut Option<klights_network_api::DataplaneHealthSnapshot>,
) {
    let Some(health) = dataplane_health else {
        return;
    };
    let new_status = health.apply_peer_sync_outcome(outcome);
    if last_readiness.as_ref() == Some(&new_status) {
        return;
    }
    match readiness_publisher.publish(my_node_name, &new_status).await {
        Ok(NodeReadinessPublishResult::Updated) => {
            tracing::info!(
                node = %my_node_name,
                ready = new_status.is_healthy(),
                reason = new_status.reason().unwrap_or("Ready"),
                "node_subnet: dataplane readiness updated"
            );
            *last_readiness = Some(new_status);
        }
        Ok(NodeReadinessPublishResult::Unchanged) => {
            *last_readiness = Some(new_status);
            tracing::debug!(
                node = %my_node_name,
                "node_subnet: readiness refresh skipped (conditions unchanged)"
            );
        }
        Ok(NodeReadinessPublishResult::Missing) => {
            tracing::debug!(
                node = %my_node_name,
                "node_subnet: readiness refresh skipped (node not found)"
            );
        }
        Err(error) => {
            tracing::warn!("node_subnet: failed to publish node network conditions: {error:#}");
        }
    }
}

/// Read a peer Node's `Ready` condition. Missing node or non-True status => not
/// ready (and therefore excluded from readiness gating).
///
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

pub fn node_dataplane_ip(node: &serde_json::Value) -> Option<String> {
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
pub fn project_node_peer_attributes(
    node: &serde_json::Value,
) -> (NodePeerMode, Option<HostPortRange>) {
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
