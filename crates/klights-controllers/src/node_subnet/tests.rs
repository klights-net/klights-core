use super::*;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    DataplaneEncryption, LeaderNetworkTopologyQuery, LeaderResourceQuery, LeaderWatch,
    NetworkDataplane, NetworkNodeMode, NetworkTopologyFuture, NodeDataplaneQuery,
    NodeDataplaneResult, NodeSubnet, NodeSubnetQuery, NodeSubnetResult, PeerSubnetsQuery,
    PeerSubnetsResult, ResourceEvent, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryFuture, WatchEventType, WatchRequest, WatchResumeCursor,
};
use klights_network_api::{
    DataplaneHealthSnapshot, PeerRoute, PeerRouter, PeerRouterError, PeerRouterFuture,
};
use klights_reconcile_api::ControllerStoreError;
use serde_json::json;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn subnet(name: &str, index: u8, mode: NetworkNodeMode) -> NodeSubnet {
    let hostport_range = (mode == NetworkNodeMode::Rootless)
        .then(|| klights_leader_api::HostPortRange::try_new(30_000, 32_767).unwrap());
    let base = Ipv4Addr::new(10, 42, index, 0);
    NodeSubnet::try_new(
        name,
        format!("10.42.{index}.0/24"),
        u32::from(base),
        base,
        Ipv4Addr::new(10, 0, 0, index.saturating_add(1)),
        mode,
        hostport_range,
    )
    .unwrap()
}

fn wireguard(name: &str, mode: NetworkNodeMode, octet: u8) -> NetworkDataplane {
    NetworkDataplane::try_new(
        name,
        mode,
        DataplaneEncryption::WireGuard,
        Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="),
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, octet)),
        Some(51_820),
    )
    .unwrap()
}

fn direct(name: &str, octet: u8) -> NetworkDataplane {
    NetworkDataplane::try_new(
        name,
        NetworkNodeMode::Root,
        DataplaneEncryption::Direct,
        None,
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, octet)),
        None,
    )
    .unwrap()
}

#[derive(Default)]
struct FakePorts {
    peers: Mutex<Vec<NodeSubnet>>,
    dataplanes: Mutex<HashMap<String, NetworkDataplane>>,
    nodes: Mutex<HashMap<String, Resource>>,
    trace: Mutex<Vec<&'static str>>,
}

impl FakePorts {
    fn with_peer(peer: NodeSubnet, dataplane: Option<NetworkDataplane>, ready: bool) -> Self {
        let name = peer.node_name().to_string();
        let mut dataplanes = HashMap::new();
        if let Some(dataplane) = dataplane {
            dataplanes.insert(name.clone(), dataplane);
        }
        let node = Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name, "uid": format!("uid-{name}"), "resourceVersion": "1"},
            "status": {"conditions": [{"type": "Ready", "status": if ready { "True" } else { "False" }}]}
        })))
        .unwrap();
        Self {
            peers: Mutex::new(vec![peer]),
            dataplanes: Mutex::new(dataplanes),
            nodes: Mutex::new(HashMap::from([(name, node)])),
            trace: Mutex::new(Vec::new()),
        }
    }
}

impl LeaderNetworkTopologyQuery for FakePorts {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        let row = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .find(|peer| peer.node_name() == request.node_name())
            .cloned();
        Box::pin(
            async move { NodeSubnetResult::try_from_wire(request.node_name(), row.is_some(), row) },
        )
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        self.trace.lock().unwrap().push("list");
        let peers = self.peers.lock().unwrap().clone();
        Box::pin(async move { PeerSubnetsResult::try_new(request.node_name(), peers) })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        self.trace.lock().unwrap().push("dataplane");
        let row = self
            .dataplanes
            .lock()
            .unwrap()
            .get(request.node_name())
            .cloned();
        Box::pin(async move {
            NodeDataplaneResult::try_from_wire(request.node_name(), row.is_some(), row)
        })
    }
}

impl LeaderResourceQuery for FakePorts {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        let row = self.nodes.lock().unwrap().get(&request.key().name).cloned();
        Box::pin(async move { Ok(row) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RouteCall {
    Apply(PeerRoute),
    Remove(PeerRoute),
}

#[derive(Default)]
struct RecordingRouter {
    calls: Mutex<Vec<RouteCall>>,
    fail_next_remove: AtomicBool,
    trace: Option<Arc<Mutex<Vec<&'static str>>>>,
}

impl RecordingRouter {
    fn calls(&self) -> Vec<RouteCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl PeerRouter for RecordingRouter {
    fn apply_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        Box::pin(async move {
            if let Some(trace) = &self.trace {
                trace.lock().unwrap().push("apply");
            }
            self.calls
                .lock()
                .unwrap()
                .push(RouteCall::Apply(route.clone()));
            Ok(())
        })
    }

    fn remove_peer_route<'a>(&'a self, route: &'a PeerRoute) -> PeerRouterFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(RouteCall::Remove(route.clone()));
            if self.fail_next_remove.swap(false, Ordering::SeqCst) {
                Err(PeerRouterError::remove("injected removal failure"))
            } else {
                Ok(())
            }
        })
    }
}

#[test]
fn test_node_subnet_caller_takes_only_peer_router() {
    fn controller_call(_pr: &dyn PeerRouter) {}
    let router = RecordingRouter::default();
    controller_call(&router);
}

#[derive(Default)]
struct NoopReadinessPublisher;

impl NodeReadinessPublisher for NoopReadinessPublisher {
    fn publish<'a>(
        &'a self,
        _node_name: &'a str,
        _health: &'a DataplaneHealthSnapshot,
    ) -> NodeReadinessPublishFuture<'a> {
        Box::pin(async { Ok(NodeReadinessPublishResult::Unchanged) })
    }
}

struct ReplayUntilAppliedWatch {
    event: ResourceEvent,
    requests: Mutex<Vec<WatchResumeCursor>>,
}

impl LeaderWatch for ReplayUntilAppliedWatch {
    fn watch_resources(&self, request: WatchRequest) -> klights_leader_api::LeaderWatchFuture<'_> {
        Box::pin(async move {
            let cursor = WatchResumeCursor::try_new(
                request.start_resource_version(),
                request.start_watch_replay_position(),
            )?;
            self.requests.lock().unwrap().push(cursor);
            let events = if cursor == WatchResumeCursor::default() {
                vec![Ok(self.event.clone())]
            } else {
                Vec::new()
            };
            Ok(klights_leader_api::WatchStream::positioned(
                Box::pin(futures::stream::iter(events)),
                cursor,
            ))
        })
    }
}

#[derive(Default)]
struct ImmediatelyFailingWatch {
    opens: AtomicUsize,
    opened: tokio::sync::Notify,
}

impl ImmediatelyFailingWatch {
    async fn wait_for_opens(&self, expected: usize) {
        loop {
            let opened = self.opened.notified();
            if self.opens.load(Ordering::SeqCst) >= expected {
                return;
            }
            opened.await;
        }
    }
}

impl LeaderWatch for ImmediatelyFailingWatch {
    fn watch_resources(&self, _request: WatchRequest) -> klights_leader_api::LeaderWatchFuture<'_> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.opened.notify_waiters();
        Box::pin(async {
            Ok(klights_leader_api::WatchStream::positioned(
                Box::pin(futures::stream::once(async {
                    Err(klights_leader_api::LeaderWatchError::ReplayExpired {
                        accepted_resource_version: 0,
                    })
                })),
                WatchResumeCursor::default(),
            ))
        })
    }
}

struct FailProjectionOnce {
    calls: AtomicUsize,
    replayed: tokio::sync::Notify,
}

impl PeerTopologyProjection for FailProjectionOnce {
    fn reconcile_node_event<'a>(
        &'a self,
        _event: &'a ResourceEvent,
    ) -> PeerTopologyProjectionFuture<'a> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(ControllerStoreError::unavailable(
                    "injected projection failure",
                ));
            }
            self.replayed.notify_waiters();
            Ok(())
        })
    }
}

#[tokio::test]
async fn focused_watch_backs_off_after_stream_fails_without_progress() {
    let watch = Arc::new(ImmediatelyFailingWatch::default());
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cancel = CancellationToken::new();
    let ports = Arc::new(FakePorts::default());
    let handle = supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Network,
            "focused_watch_stream_failure_backoff_test",
            run_focused_peer_watch(
                ports.clone(),
                ports,
                watch.clone(),
                None,
                "node-a".to_string(),
                Arc::new(RecordingRouter::default()),
                supervisor.clone(),
                None,
                Arc::new(NoopReadinessPublisher),
                cancel.clone(),
            ),
        )
        .await
        .unwrap();

    watch.wait_for_opens(1).await;
    watch.wait_for_opens(2).await;
    let second_open = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), watch.wait_for_opens(3))
        .await
        .expect("the third watch open must occur after the bounded reconnect delay");
    assert!(
        second_open.elapsed() >= Duration::from_millis(900),
        "the second consecutive failure must use the one-second backoff"
    );
    cancel.cancel();
    handle.join().await.unwrap();
    assert_eq!(
        supervisor
            .shutdown(Duration::from_secs(1))
            .await
            .remaining_active,
        0
    );
}

#[tokio::test]
async fn focused_watch_replays_event_when_topology_projection_fails() {
    let position = WatchReplayPosition {
        resource_version: 41,
        event_id: 73,
        resource_version_filter_through_event_id: 0,
    };
    let event = ResourceEvent::try_new(
        WatchEventType::Added,
        Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "node-b", "uid": "uid-node-b", "resourceVersion": "41"}
        })))
        .unwrap(),
        Some(position),
    )
    .unwrap();
    let watch = Arc::new(ReplayUntilAppliedWatch {
        event,
        requests: Mutex::new(Vec::new()),
    });
    let projection = Arc::new(FailProjectionOnce {
        calls: AtomicUsize::new(0),
        replayed: tokio::sync::Notify::new(),
    });
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cancel = CancellationToken::new();
    let ports = Arc::new(FakePorts::default());
    let handle = supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Network,
            "focused_projection_replay_test",
            run_focused_peer_watch(
                ports.clone(),
                ports,
                watch.clone(),
                Some(projection.clone()),
                "node-a".to_string(),
                Arc::new(RecordingRouter::default()),
                supervisor.clone(),
                None,
                Arc::new(NoopReadinessPublisher),
                cancel.clone(),
            ),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), projection.replayed.notified())
        .await
        .expect("the failed event must be replayed");
    cancel.cancel();
    handle.join().await.unwrap();
    assert_eq!(
        supervisor
            .shutdown(Duration::from_secs(1))
            .await
            .remaining_active,
        0
    );
    assert_eq!(projection.calls.load(Ordering::SeqCst), 2);
    let requests = watch.requests.lock().unwrap();
    assert!(requests.len() >= 2);
    assert_eq!(requests[1], WatchResumeCursor::default());
}

#[test]
fn node_dataplane_ip_prefers_external_ip_for_peer_routing() {
    let node = json!({"status": {"addresses": [
        {"type": "InternalIP", "address": "10.0.0.7"},
        {"type": "ExternalIP", "address": "203.0.113.77"}
    ]}});
    assert_eq!(node_dataplane_ip(&node).as_deref(), Some("203.0.113.77"));
}

#[test]
fn node_dataplane_ip_falls_back_to_internal_ip_when_external_missing() {
    let node = json!({"status": {"addresses": [
        {"type": "InternalIP", "address": "10.0.0.7"}
    ]}});
    assert_eq!(node_dataplane_ip(&node).as_deref(), Some("10.0.0.7"));
}

async fn sync_one(
    mode: NetworkNodeMode,
    dataplane: Option<NetworkDataplane>,
    ready: bool,
) -> (
    FakePorts,
    RecordingRouter,
    HashMap<String, AppliedPeer>,
    PeerSyncOutcome,
) {
    let ports = FakePorts::with_peer(subnet("node-b", 1, mode), dataplane, ready);
    let router = RecordingRouter::default();
    let mut applied = HashMap::new();
    let outcome = sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    (ports, router, applied, outcome)
}

#[tokio::test]
async fn sync_peer_routes_uses_wireguard_by_default_for_enabled_peer() {
    let (_, router, applied, _) = sync_one(
        NetworkNodeMode::Root,
        Some(wireguard("node-b", NetworkNodeMode::Root, 2)),
        true,
    )
    .await;
    assert!(
        matches!(router.calls().as_slice(), [RouteCall::Apply(PeerRoute::WireGuard(route))] if route.node_name() == "node-b" && route.endpoint().to_string() == "10.0.0.2:51820" && route.allowed_pod_cidr().to_string() == "10.42.1.0/24")
    );
    assert!(applied.contains_key("node-b"));
}

#[tokio::test]
async fn sync_peer_routes_disabled_encryption_uses_typed_unencrypted_direct_route() {
    let (_, router, _, _) = sync_one(NetworkNodeMode::Root, Some(direct("node-b", 2)), true).await;
    assert!(
        matches!(router.calls().as_slice(), [RouteCall::Apply(PeerRoute::Direct(route))] if route.node_name() == "node-b" && route.gateway() == Ipv4Addr::new(10, 0, 0, 2))
    );
}

#[tokio::test]
async fn sync_peer_routes_single_node_with_no_peers_makes_no_changes() {
    let ports = FakePorts::default();
    let router = RecordingRouter::default();
    let mut applied = HashMap::new();
    let outcome = sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    assert_eq!(outcome, PeerSyncOutcome::default());
    assert!(router.calls().is_empty());
    assert!(applied.is_empty());
}

#[tokio::test]
async fn bootstrap_root_installs_peer_routes_after_local_subnet() {
    let ports = FakePorts::with_peer(
        subnet("node-b", 1, NetworkNodeMode::Root),
        Some(wireguard("node-b", NetworkNodeMode::Root, 2)),
        true,
    );
    let trace = Arc::new(Mutex::new(Vec::new()));
    let router = RecordingRouter {
        trace: Some(trace.clone()),
        ..RecordingRouter::default()
    };
    ports.trace.lock().unwrap().clear();
    let mut applied = HashMap::new();
    sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    let mut combined = ports.trace.lock().unwrap().clone();
    combined.extend(trace.lock().unwrap().iter().copied());
    assert_eq!(combined, ["list", "dataplane", "apply"]);
}

#[tokio::test]
async fn sync_peer_routes_dispatches_wireguard_for_rootless_peer() {
    let (_, router, applied, _) = sync_one(
        NetworkNodeMode::Rootless,
        Some(wireguard("node-b", NetworkNodeMode::Rootless, 9)),
        true,
    )
    .await;
    assert!(
        matches!(router.calls().as_slice(), [RouteCall::Apply(PeerRoute::WireGuard(route))] if route.endpoint().to_string() == "10.0.0.9:51820")
    );
    assert!(matches!(
        applied["node-b"].endpoint,
        PeerRoute::WireGuard(_)
    ));
}

#[tokio::test]
async fn sync_peer_routes_dispatches_wireguard_for_root_peer() {
    let (_, _, applied, _) = sync_one(
        NetworkNodeMode::Root,
        Some(wireguard("node-b", NetworkNodeMode::Root, 2)),
        true,
    )
    .await;
    assert!(matches!(
        applied["node-b"].endpoint,
        PeerRoute::WireGuard(_)
    ));
}

async fn applied_peer_fixture() -> (FakePorts, RecordingRouter, HashMap<String, AppliedPeer>) {
    let (ports, router, applied, _) = sync_one(
        NetworkNodeMode::Root,
        Some(wireguard("node-b", NetworkNodeMode::Root, 2)),
        true,
    )
    .await;
    router.calls.lock().unwrap().clear();
    (ports, router, applied)
}

#[tokio::test]
async fn sync_peer_routes_removes_with_matching_endpoint_variant() {
    let (ports, router, mut applied) = applied_peer_fixture().await;
    ports.peers.lock().unwrap().clear();
    sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    assert!(
        matches!(router.calls().as_slice(), [RouteCall::Remove(PeerRoute::WireGuard(route))] if route.node_name() == "node-b")
    );
    assert!(!applied.contains_key("node-b"));
}

#[tokio::test]
async fn sync_peer_routes_removes_exact_applied_route_when_metadata_disappears() {
    let (ports, router, mut applied) = applied_peer_fixture().await;
    let expected = applied["node-b"].endpoint.clone();
    ports.dataplanes.lock().unwrap().clear();
    sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    assert_eq!(router.calls(), vec![RouteCall::Remove(expected)]);
}

#[tokio::test]
async fn sync_peer_routes_retries_failed_missing_metadata_removal() {
    let (ports, router, mut applied) = applied_peer_fixture().await;
    ports.dataplanes.lock().unwrap().clear();
    router.fail_next_remove.store(true, Ordering::SeqCst);
    assert!(
        sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
            .await
            .is_err()
    );
    assert!(applied.contains_key("node-b"));
    sync_peer_routes_with_ports(&ports, &ports, "node-a", &router, &mut applied)
        .await
        .unwrap();
    assert!(!applied.contains_key("node-b"));
    assert_eq!(router.calls().len(), 2);
}

#[tokio::test]
async fn node_annotations_project_to_rootless_node_subnet() {
    let node = json!({"metadata": {"annotations": {
        "klights.io/mode": "rootless",
        "klights.io/hostport-range": "31000-31999"
    }}});
    let (mode, range) = project_node_peer_attributes(&node);
    assert_eq!(mode, NodePeerMode::Rootless);
    assert_eq!(
        range,
        Some(HostPortRange {
            start: 31_000,
            end: 31_999
        })
    );
}

struct MemoryHealth(Mutex<DataplaneHealthSnapshot>);

impl Default for MemoryHealth {
    fn default() -> Self {
        Self(Mutex::new(DataplaneHealthSnapshot::healthy()))
    }
}

impl PeerDataplaneHealth for MemoryHealth {
    fn apply_peer_sync_outcome(&self, outcome: &PeerSyncOutcome) -> DataplaneHealthSnapshot {
        let next = if outcome.unreachable_ready_peers == 0 {
            DataplaneHealthSnapshot::healthy()
        } else {
            DataplaneHealthSnapshot::unavailable("peer unreachable")
        };
        *self.0.lock().unwrap() = next.clone();
        next
    }
}

#[tokio::test]
async fn sync_peer_routes_counts_ready_peer_without_metadata_as_unreachable() {
    let (_, _, _, outcome) = sync_one(NetworkNodeMode::Root, None, true).await;
    assert_eq!(
        outcome,
        PeerSyncOutcome {
            desired_peers: 1,
            ready_peers: 1,
            unreachable_ready_peers: 1
        }
    );
    assert!(
        !MemoryHealth::default()
            .apply_peer_sync_outcome(&outcome)
            .is_healthy()
    );
}

#[tokio::test]
async fn sync_peer_routes_excludes_not_ready_peer_from_readiness() {
    let (_, _, _, outcome) = sync_one(NetworkNodeMode::Root, None, false).await;
    assert_eq!(
        outcome,
        PeerSyncOutcome {
            desired_peers: 1,
            ready_peers: 0,
            unreachable_ready_peers: 0
        }
    );
    assert!(
        MemoryHealth::default()
            .apply_peer_sync_outcome(&outcome)
            .is_healthy()
    );
}

#[tokio::test]
async fn sync_peer_routes_ready_peer_with_metadata_is_connected() {
    let (_, _, _, outcome) = sync_one(
        NetworkNodeMode::Root,
        Some(wireguard("node-b", NetworkNodeMode::Root, 2)),
        true,
    )
    .await;
    assert_eq!(outcome.unreachable_ready_peers, 0);
    assert!(
        MemoryHealth::default()
            .apply_peer_sync_outcome(&outcome)
            .is_healthy()
    );
}

struct RecordingPublisher {
    results: Mutex<Vec<NodeReadinessPublishResult>>,
    calls: AtomicUsize,
}

impl RecordingPublisher {
    fn new(results: impl IntoIterator<Item = NodeReadinessPublishResult>) -> Self {
        let mut results: Vec<_> = results.into_iter().collect();
        results.reverse();
        Self {
            results: Mutex::new(results),
            calls: AtomicUsize::new(0),
        }
    }
}

impl NodeReadinessPublisher for RecordingPublisher {
    fn publish<'a>(
        &'a self,
        _node_name: &'a str,
        _health: &'a DataplaneHealthSnapshot,
    ) -> NodeReadinessPublishFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = self.results.lock().unwrap().pop().unwrap();
        Box::pin(async move { Ok(result) })
    }
}

fn healthy_outcome() -> PeerSyncOutcome {
    PeerSyncOutcome {
        desired_peers: 1,
        ready_peers: 1,
        unreachable_ready_peers: 0,
    }
}

#[tokio::test]
async fn reconcile_local_readiness_does_not_memo_when_node_not_found() {
    let query = FakePorts::default();
    let publisher = RecordingPublisher::new([NodeReadinessPublishResult::Missing]);
    let health = MemoryHealth::default();
    let mut last = Some(DataplaneHealthSnapshot::unavailable("initial"));
    reconcile_local_readiness_with_publisher(
        &query,
        &publisher,
        "worker",
        Some(&health),
        &healthy_outcome(),
        &mut last,
    )
    .await;
    assert_eq!(last, Some(DataplaneHealthSnapshot::unavailable("initial")));
}

#[tokio::test]
async fn reconcile_local_readiness_memos_after_node_appears() {
    let query = FakePorts::default();
    let publisher = RecordingPublisher::new([
        NodeReadinessPublishResult::Missing,
        NodeReadinessPublishResult::Updated,
    ]);
    let health = MemoryHealth::default();
    let mut last = Some(DataplaneHealthSnapshot::unavailable("initial"));
    reconcile_local_readiness_with_publisher(
        &query,
        &publisher,
        "worker",
        Some(&health),
        &healthy_outcome(),
        &mut last,
    )
    .await;
    reconcile_local_readiness_with_publisher(
        &query,
        &publisher,
        "worker",
        Some(&health),
        &healthy_outcome(),
        &mut last,
    )
    .await;
    assert_eq!(last, Some(DataplaneHealthSnapshot::healthy()));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reconcile_local_readiness_noop_when_conditions_already_match() {
    let query = FakePorts::default();
    let publisher = RecordingPublisher::new([]);
    let health = MemoryHealth::default();
    let mut last = Some(DataplaneHealthSnapshot::healthy());
    reconcile_local_readiness_with_publisher(
        &query,
        &publisher,
        "worker",
        Some(&health),
        &healthy_outcome(),
        &mut last,
    )
    .await;
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reconcile_local_readiness_memos_initial_noop_when_conditions_already_match() {
    let query = FakePorts::default();
    let publisher = RecordingPublisher::new([NodeReadinessPublishResult::Unchanged]);
    let health = MemoryHealth::default();
    let mut last = None;
    reconcile_local_readiness_with_publisher(
        &query,
        &publisher,
        "worker",
        Some(&health),
        &healthy_outcome(),
        &mut last,
    )
    .await;
    assert_eq!(last, Some(DataplaneHealthSnapshot::healthy()));
    assert_eq!(publisher.calls.load(Ordering::SeqCst), 1);
}
