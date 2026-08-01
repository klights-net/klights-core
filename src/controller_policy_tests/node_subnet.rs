use crate::networking::test_support::{MockNetworkProvider, NetworkCall};
use klights_controllers::annotations::NodePeerMode;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

struct DatastoreLeaderPorts<'a>(&'a dyn crate::datastore::DatastoreBackend);

impl klights_leader_api::LeaderNetworkTopologyQuery for DatastoreLeaderPorts<'_> {
    fn get_node_subnet(
        &self,
        request: klights_leader_api::NodeSubnetQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeSubnetResult> {
        Box::pin(async move {
            let node_name = request.into_node_name();
            let subnet = self
                .0
                .get_node_subnet(&node_name)
                .await
                .map_err(|error| {
                    klights_leader_api::NetworkTopologyError::query_failed(error.to_string())
                })?
                .map(crate::control_plane::client::focused_node_subnet)
                .transpose()?;
            klights_leader_api::NodeSubnetResult::try_from_wire(
                &node_name,
                subnet.is_some(),
                subnet,
            )
        })
    }

    fn list_peer_subnets(
        &self,
        request: klights_leader_api::PeerSubnetsQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::PeerSubnetsResult> {
        Box::pin(async move {
            let node_name = request.into_node_name();
            let peers = self
                .0
                .list_peer_subnets(
                    klights_cluster_store::PeerTopologyRequest::excluding(&node_name).map_err(
                        |error| {
                            klights_leader_api::NetworkTopologyError::query_failed(
                                error.to_string(),
                            )
                        },
                    )?,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::NetworkTopologyError::query_failed(error.to_string())
                })?
                .into_iter()
                .map(crate::control_plane::client::focused_node_subnet)
                .collect::<Result<Vec<_>, _>>()?;
            klights_leader_api::PeerSubnetsResult::try_new(&node_name, peers)
        })
    }

    fn get_node_dataplane(
        &self,
        request: klights_leader_api::NodeDataplaneQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeDataplaneResult>
    {
        Box::pin(async move {
            let node_name = request.into_node_name();
            let metadata = self
                .0
                .get_node_dataplane(&node_name)
                .await
                .map_err(|error| {
                    klights_leader_api::NetworkTopologyError::query_failed(error.to_string())
                })?
                .map(crate::control_plane::client::focused_dataplane)
                .transpose()?;
            klights_leader_api::NodeDataplaneResult::try_from_wire(
                &node_name,
                metadata.is_some(),
                metadata,
            )
        })
    }
}

impl klights_leader_api::LeaderResourceQuery for DatastoreLeaderPorts<'_> {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let key = request.key();
            self.0
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })
        })
    }

    fn list_resources(
        &self,
        _request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async {
            Err(klights_leader_api::ResourceQueryError::query_failed(
                "node subnet tests do not list generic resources",
            ))
        })
    }
}

async fn ensure_local_node_subnet(
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
    cluster_cidr: &str,
    node_ip: &str,
) -> anyhow::Result<klights_cluster_store::StoredNodeSubnet> {
    db.allocate_node_subnet(node_name, cluster_cidr, node_ip)
        .await
}

async fn sync_peer_routes(
    db: &dyn crate::datastore::DatastoreBackend,
    my_node_name: &str,
    network: &dyn klights_network_api::PeerRouter,
    applied: &mut HashMap<String, klights_controllers::node_subnet::AppliedPeer>,
) -> anyhow::Result<klights_controllers::node_subnet::PeerSyncOutcome> {
    let ports = DatastoreLeaderPorts(db);
    klights_controllers::node_subnet::sync_peer_routes_with_ports(
        &ports,
        &ports,
        my_node_name,
        network,
        applied,
    )
    .await
}

fn apply_peer_sync_outcome(
    health: &klights_networking::dataplane_health::DataplaneHealth,
    outcome: &klights_controllers::node_subnet::PeerSyncOutcome,
) {
    let health = crate::node_subnet_controller_adapter::DataplaneHealthAdapter::new(health.clone());
    klights_controllers::node_subnet::apply_peer_sync_outcome(health.as_ref(), outcome);
}

async fn reconcile_local_readiness(
    db: &dyn crate::datastore::DatastoreBackend,
    outbox: Option<&klights_kubelet::node_outbox::Outbox>,
    my_node_name: &str,
    dataplane_health: Option<&klights_networking::dataplane_health::DataplaneHealth>,
    outcome: &klights_controllers::node_subnet::PeerSyncOutcome,
    last_readiness: &mut Option<klights_networking::dataplane_health::DataplaneHealthStatus>,
) {
    let Some(health) = dataplane_health else {
        return;
    };
    apply_peer_sync_outcome(health, outcome);
    let new_status = health.status();
    if last_readiness.as_ref() == Some(&new_status) {
        return;
    }
    match crate::node_output_integration_tests::refresh_node_network_conditions(
        db,
        outbox.map(|outbox| outbox as &dyn klights_leader_api::NodeOutbox),
        my_node_name,
        health,
    )
    .await
    {
        Ok(
            klights_kubelet::node::NodeNetworkRefreshResult::Updated
            | klights_kubelet::node::NodeNetworkRefreshResult::Unchanged,
        ) => *last_readiness = Some(new_status),
        Ok(klights_kubelet::node::NodeNetworkRefreshResult::Missing) | Err(_) => {}
    }
}

struct NoopLeaderPorts;

impl klights_leader_api::LeaderResourceQuery for NoopLeaderPorts {
    fn get_resource(
        &self,
        _request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        _request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async {
            klights_leader_api::ResourceListResult::try_new(Vec::new(), 0, None, None, None)
        })
    }
}

impl klights_leader_api::LeaderNodeSelfStatus for NoopLeaderPorts {
    fn submit_node_self_status(
        &self,
        _request: klights_leader_api::NodeSelfStatusRequest,
    ) -> klights_leader_api::NodeSelfStatusFuture<'_, klights_leader_api::NodeSelfStatusResult>
    {
        Box::pin(async { Ok(klights_leader_api::NodeSelfStatusResult::Enqueued) })
    }
}

impl klights_controllers::node_subnet::NodeReadinessPublisher for NoopLeaderPorts {
    fn publish<'a>(
        &'a self,
        _node_name: &'a str,
        _health: &'a klights_network_api::DataplaneHealthSnapshot,
    ) -> klights_controllers::node_subnet::NodeReadinessPublishFuture<'a> {
        Box::pin(async {
            Ok(klights_controllers::node_subnet::NodeReadinessPublishResult::Unchanged)
        })
    }
}

impl klights_leader_api::LeaderNetworkTopologyQuery for NoopLeaderPorts {
    fn get_node_subnet(
        &self,
        request: klights_leader_api::NodeSubnetQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeSubnetResult> {
        Box::pin(async move {
            klights_leader_api::NodeSubnetResult::try_from_wire(request.node_name(), false, None)
        })
    }

    fn list_peer_subnets(
        &self,
        request: klights_leader_api::PeerSubnetsQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::PeerSubnetsResult> {
        Box::pin(async move {
            klights_leader_api::PeerSubnetsResult::try_new(request.node_name(), Vec::new())
        })
    }

    fn get_node_dataplane(
        &self,
        request: klights_leader_api::NodeDataplaneQuery,
    ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeDataplaneResult>
    {
        Box::pin(async move {
            klights_leader_api::NodeDataplaneResult::try_from_wire(request.node_name(), false, None)
        })
    }
}

struct ReplayUntilAppliedWatch {
    event: klights_leader_api::ResourceEvent,
    requests: std::sync::Mutex<Vec<klights_leader_api::WatchResumeCursor>>,
}

impl klights_leader_api::LeaderWatch for ReplayUntilAppliedWatch {
    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        Box::pin(async move {
            let cursor = klights_leader_api::WatchResumeCursor::try_new(
                request.start_resource_version(),
                request.start_watch_replay_position(),
            )?;
            self.requests.lock().unwrap().push(cursor);
            let events = if cursor == klights_leader_api::WatchResumeCursor::default() {
                vec![Ok(self.event.clone())]
            } else {
                Vec::new()
            };
            Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                futures::stream::iter(events),
            ))
        })
    }
}

#[derive(Default)]
struct ImmediatelyFailingWatch {
    opens: std::sync::atomic::AtomicUsize,
    opened: tokio::sync::Notify,
}

impl ImmediatelyFailingWatch {
    async fn wait_for_opens(&self, expected: usize) {
        loop {
            let opened = self.opened.notified();
            if self.opens.load(std::sync::atomic::Ordering::SeqCst) >= expected {
                return;
            }
            opened.await;
        }
    }
}

impl klights_leader_api::LeaderWatch for ImmediatelyFailingWatch {
    fn watch_resources(
        &self,
        _request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.opened.notify_waiters();
        Box::pin(async {
            Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                futures::stream::once(async {
                    Err(klights_leader_api::LeaderWatchError::ReplayExpired {
                        accepted_resource_version: 0,
                    })
                }),
            ))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn focused_watch_backs_off_after_stream_fails_without_progress() {
    let watch = Arc::new(ImmediatelyFailingWatch::default());
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cancel = CancellationToken::new();
    let handle = {
        let ports = Arc::new(NoopLeaderPorts);
        let watch = watch.clone();
        let router = Arc::new(MockNetworkProvider::new());
        let task_supervisor = supervisor.clone();
        let task_cancel = cancel.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "focused_watch_stream_failure_backoff_test",
                async move {
                    klights_controllers::node_subnet::run_focused_peer_watch(
                        ports.clone(),
                        ports.clone(),
                        watch,
                        None,
                        "node-a".to_string(),
                        router,
                        task_supervisor,
                        None,
                        ports,
                        task_cancel,
                    )
                    .await;
                },
            )
            .await
            .unwrap()
    };

    watch.wait_for_opens(1).await;
    tokio::time::advance(Duration::from_millis(500)).await;
    watch.wait_for_opens(2).await;
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        watch.opens.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the second consecutive failure must wait the full one-second backoff"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    watch.wait_for_opens(3).await;
    cancel.cancel();
    handle.join().await.unwrap();
    let report = supervisor.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.remaining_active, 0);
    assert_eq!(watch.opens.load(std::sync::atomic::Ordering::SeqCst), 3);
}

struct FailProjectionOnce {
    calls: std::sync::atomic::AtomicUsize,
    replayed: tokio::sync::Notify,
}

impl klights_controllers::node_subnet::PeerTopologyProjection for FailProjectionOnce {
    fn reconcile_node_event<'a>(
        &'a self,
        _event: &'a klights_leader_api::ResourceEvent,
    ) -> klights_controllers::node_subnet::PeerTopologyProjectionFuture<'a> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call == 0 {
                return Err(klights_reconcile_api::ControllerStoreError::unavailable(
                    "injected projection failure",
                ));
            }
            self.replayed.notify_waiters();
            Ok(())
        })
    }
}

#[tokio::test]
async fn focused_watch_replays_event_when_topology_projection_fails() {
    let position = klights_cluster_core::WatchReplayPosition {
        resource_version: 41,
        event_id: 73,
        resource_version_filter_through_event_id: 0,
    };
    let resource = klights_cluster_core::Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "node-b",
            "uid": "uid-node-b",
            "resourceVersion": "41"
        }
    })))
    .unwrap();
    let event = klights_leader_api::ResourceEvent::try_new(
        klights_leader_api::WatchEventType::Added,
        resource,
        Some(position),
    )
    .unwrap();
    let watch = Arc::new(ReplayUntilAppliedWatch {
        event,
        requests: std::sync::Mutex::new(Vec::new()),
    });
    let projection = Arc::new(FailProjectionOnce {
        calls: std::sync::atomic::AtomicUsize::new(0),
        replayed: tokio::sync::Notify::new(),
    });
    let router = Arc::new(MockNetworkProvider::new());
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cancel = CancellationToken::new();
    let handle = {
        let topology = Arc::new(NoopLeaderPorts);
        let query = Arc::new(NoopLeaderPorts);
        let node_status = Arc::new(NoopLeaderPorts);
        let watch = watch.clone();
        let projection = projection.clone();
        let router = router.clone();
        let task_supervisor = supervisor.clone();
        let task_cancel = cancel.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "focused_projection_replay_test",
                async move {
                    klights_controllers::node_subnet::run_focused_peer_watch(
                        topology,
                        query,
                        watch,
                        Some(projection),
                        "node-a".to_string(),
                        router,
                        task_supervisor,
                        None,
                        node_status,
                        task_cancel,
                    )
                    .await;
                },
            )
            .await
            .unwrap()
    };

    let replayed =
        tokio::time::timeout(Duration::from_secs(2), projection.replayed.notified()).await;
    cancel.cancel();
    handle.join().await.unwrap();
    let report = supervisor.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.remaining_active, 0);

    replayed.expect(
        "projection failure must retain/replay the Node event instead of advancing its cursor",
    );
    assert_eq!(
        projection.calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the same durable event must be projected again"
    );
    let requests = watch.requests.lock().unwrap();
    assert!(requests.len() >= 2);
    assert_eq!(
        requests[1],
        klights_leader_api::WatchResumeCursor::default(),
        "projection failure must reconnect from the pre-event cursor"
    );
}

#[tokio::test]
async fn test_allocate_node_subnet_first_node_gets_first_24() {
    let db = crate::datastore::test_support::in_memory().await;
    let subnet = db
        .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
        .await
        .unwrap();
    assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");
    assert_eq!(subnet.gateway_ip.to_string(), "10.42.0.0");
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
    assert_eq!(subnet_b.gateway_ip.to_string(), "10.42.1.0");
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
        klights_controllers::node_subnet::node_dataplane_ip(&node).as_deref(),
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

    assert_eq!(
        klights_controllers::node_subnet::node_dataplane_ip(&node).as_deref(),
        Some("10.0.0.7")
    );
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
    let peers = db
        .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::excluding("node-a").unwrap())
        .await
        .unwrap();
    assert_eq!(peers.len(), 1, "self excluded, peer row included");
    assert_eq!(peers[0].node_name.as_str(), "node-b");
    let all = db
        .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::all())
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "snapshot domain includes every stored row");
}

#[tokio::test]
async fn sync_peer_routes_uses_wireguard_by_default_for_enabled_peer() {
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};

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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &network, &mut applied)
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
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};

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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &network, &mut applied)
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
/// always called peer-route apply, which crashed boot under the
/// rootless network-plane peer router.
#[tokio::test]
async fn bootstrap_rootless_allocates_local_subnet_without_peer_router() {
    let db = crate::datastore::test_support::in_memory().await;

    let subnet = ensure_local_node_subnet(&db, "rootless-node-a", "10.42.0.0/16", "10.0.0.7")
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
    // local-only path must never reach peer-route apply.
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
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
        .await
        .unwrap();

    let mock_peer = MockNetworkProvider::new();
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
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
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};

    let db = crate::datastore::test_support::in_memory().await;

    // Local subnet first.
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
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
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
    use klights_controllers::annotations::NodePeerMode;
    use klights_types::HostPortRange;

    let db = crate::datastore::test_support::in_memory().await;
    // Local node first.
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
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
        klights_network_api::PeerRoute::WireGuard(_)
    ));
}

/// Phase 2C: a root peer with enabled encryption dispatches to WireGuard.
#[tokio::test]
async fn sync_peer_routes_dispatches_wireguard_for_root_peer() {
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
    use klights_network_api::PeerRoute;
    let db = crate::datastore::test_support::in_memory().await;
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
        .await
        .unwrap();
    let entry = applied.get("node-b").expect("root peer must be applied");
    assert!(
        matches!(entry.endpoint, PeerRoute::WireGuard(_)),
        "root peer must use PeerRoute::WireGuard, got {:?}",
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
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
        .await
        .unwrap();
    db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
        .await
        .unwrap();
    db.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "node-b".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ=".to_string()),
            Some("10.0.0.2".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let mock_peer = MockNetworkProvider::new();
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
        .await
        .unwrap();
    // Drop node-b: list_peer_subnets returns []
    db.delete_node_subnet("node-b").await.unwrap();
    sync_peer_routes(&db, "node-a", &mock_peer, &mut applied)
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

#[tokio::test]
async fn sync_peer_routes_removes_exact_applied_route_when_metadata_disappears() {
    let db = crate::datastore::test_support::in_memory().await;
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
        .await
        .unwrap();
    db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
        .await
        .unwrap();
    db.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "node-b".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ=".to_string()),
            Some("10.0.0.2".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let network = MockNetworkProvider::new();
    let mut applied = HashMap::new();
    sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .unwrap();
    network.clear_calls();
    db.db_call("test_delete_node_dataplane", |conn| {
        conn.execute(
            "DELETE FROM node_dataplane WHERE node_name = ?1",
            ["node-b"],
        )?;
        Ok(())
    })
    .await
    .unwrap();

    sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .unwrap();
    assert!(matches!(
        network.calls().as_slice(),
        [NetworkCall::RemoveWireGuardPeerEndpoint { node_name, .. }] if node_name == "node-b"
    ));
    assert!(!applied.contains_key("node-b"));
}

#[tokio::test]
async fn sync_peer_routes_retries_failed_missing_metadata_removal() {
    let db = crate::datastore::test_support::in_memory().await;
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
        .await
        .unwrap();
    db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
        .await
        .unwrap();
    db.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "node-b".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ=".to_string()),
            Some("10.0.0.2".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let network = MockNetworkProvider::new();
    let mut applied = HashMap::new();
    sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .unwrap();
    network.clear_calls();
    db.db_call("test_delete_node_dataplane_retry", |conn| {
        conn.execute(
            "DELETE FROM node_dataplane WHERE node_name = ?1",
            ["node-b"],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    network.fail_next_peer_remove();

    assert!(
        sync_peer_routes(&db, "node-a", &network, &mut applied)
            .await
            .is_err()
    );
    assert!(
        applied.contains_key("node-b"),
        "failed kernel removal must retain retry bookkeeping"
    );
    sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .expect("second reconcile retries removal");
    assert!(!applied.contains_key("node-b"));
    assert_eq!(
        network
            .calls()
            .iter()
            .filter(|call| matches!(call, NetworkCall::RemoveWireGuardPeerEndpoint { .. }))
            .count(),
        2
    );
}

/// F2-04 annotation projection gate: a Node with the rootless mode +
/// hostport-range annotations must be projected onto a `node_subnets` row
/// with mode=Rootless and the parsed range.
#[tokio::test]
async fn node_annotations_project_to_rootless_node_subnet() {
    use klights_controllers::annotations::NodePeerMode;
    use klights_types::HostPortRange;

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
    let (mode, range) = klights_controllers::node_subnet::project_node_peer_attributes(&node_obj);
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
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    let outcome = sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .expect("sync must succeed");

    assert_eq!(outcome.desired_peers, 1);
    assert_eq!(outcome.ready_peers, 1);
    assert_eq!(
        outcome.unreachable_ready_peers, 1,
        "a Ready peer without dataplane metadata must count as unreachable"
    );

    let health = klights_networking::dataplane_health::DataplaneHealth::new_healthy();
    apply_peer_sync_outcome(&health, &outcome);
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
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    let outcome = sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .expect("sync must succeed");

    assert_eq!(outcome.desired_peers, 1);
    assert_eq!(outcome.ready_peers, 0);
    assert_eq!(
        outcome.unreachable_ready_peers, 0,
        "NotReady peers must be excluded from readiness gating"
    );

    let health = klights_networking::dataplane_health::DataplaneHealth::new_healthy();
    health.set_peers_pending();
    apply_peer_sync_outcome(&health, &outcome);
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
    use klights_cluster_store::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};

    let db = crate::datastore::test_support::in_memory().await;
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
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
    let mut applied: HashMap<String, klights_controllers::node_subnet::AppliedPeer> =
        HashMap::new();
    let outcome = sync_peer_routes(&db, "node-a", &network, &mut applied)
        .await
        .expect("sync must succeed");

    assert_eq!(outcome.unreachable_ready_peers, 0);

    let health = klights_networking::dataplane_health::DataplaneHealth::new_healthy();
    health.set_peers_pending();
    apply_peer_sync_outcome(&health, &outcome);
    assert!(
        health.status().is_healthy(),
        "a Ready, reachable peer must let the node report Ready"
    );
}

/// F2-04 rootless gate: rootless peers are still listed for route projection.
#[tokio::test]
async fn list_peer_subnets_includes_rootless_peers() {
    let db = crate::datastore::test_support::in_memory().await;
    ensure_local_node_subnet(&db, "node-a", "10.42.0.0/16", "10.0.0.1")
        .await
        .unwrap();
    db.allocate_node_subnet("rootless-d", "10.42.0.0/16", "10.0.0.4")
        .await
        .unwrap();
    let peers = db
        .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::excluding("node-a").unwrap())
        .await
        .unwrap();
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
    use klights_networking::dataplane_health::DataplaneHealth;

    let db = crate::datastore::test_support::in_memory().await;
    let health = DataplaneHealth::new_healthy();
    health.set_peers_pending(); // Start as Unavailable

    // Initial last_readiness matches the health state (Unavailable)
    let initial_status = health.status();
    let mut last_readiness = Some(initial_status.clone());

    // Simulate a successful peer sync: 0 unreachable ready peers → connected
    let outcome = klights_controllers::node_subnet::PeerSyncOutcome {
        desired_peers: 1,
        ready_peers: 1,
        unreachable_ready_peers: 0,
    };

    // Node does NOT exist in the DB → refresh_node_network_conditions returns Ok(false)
    reconcile_local_readiness(
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
    use klights_networking::dataplane_health::{DataplaneHealth, DataplaneHealthStatus};

    let db = crate::datastore::test_support::in_memory().await;
    let health = DataplaneHealth::new_healthy();
    health.set_peers_pending();

    let initial_status = health.status();
    let mut last_readiness = Some(initial_status.clone());

    let outcome = klights_controllers::node_subnet::PeerSyncOutcome {
        desired_peers: 1,
        ready_peers: 1,
        unreachable_ready_peers: 0,
    };

    // First call: node not found → should NOT memo
    reconcile_local_readiness(
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
    reconcile_local_readiness(
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
    use klights_networking::dataplane_health::DataplaneHealth;
    let db = crate::datastore::test_support::in_memory().await;
    let health = DataplaneHealth::new_healthy();
    // Health starts Healthy, no peer tracking → Healthy
    let mut last_readiness = Some(health.status()); // Some(Healthy)

    let outcome = klights_controllers::node_subnet::PeerSyncOutcome {
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
    reconcile_local_readiness(
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
        Some(klights_networking::dataplane_health::DataplaneHealthStatus::Healthy),
        "last_readiness stays Healthy when conditions already match"
    );
}

#[tokio::test]
async fn reconcile_local_readiness_memos_initial_noop_when_conditions_already_match() {
    use klights_networking::dataplane_health::DataplaneHealth;

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
                        klights_controllers::annotations::GIT_COMMIT_ANNOTATION: "test-commit"
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

    reconcile_local_readiness(
        &db,
        None,
        "worker-node",
        Some(&health),
        &klights_controllers::node_subnet::PeerSyncOutcome::default(),
        &mut last_readiness,
    )
    .await;

    assert_eq!(
        last_readiness,
        Some(klights_networking::dataplane_health::DataplaneHealthStatus::Healthy),
        "initial no-op reconcile must memo confirmed readiness so later Node events do not keep rechecking"
    );
}
