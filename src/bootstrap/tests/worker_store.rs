use crate::datastore::Resource;
use async_trait::async_trait;
use klights_cluster_core::WatchReplayPosition;
use klights_kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use klights_kubelet::pod_lifecycle_router::{
    PodLifecycleDiagnostics, PodLifecycleRouteBackend, PodLifecycleRouteError,
    PodLifecycleRouteMode, PodLifecycleRouter,
};
use klights_kubelet::worker_store::reflector::ReflectorState;
use klights_kubelet::worker_store::{
    WorkerListPage, WorkerStoreAdapter, WorkerStorePorts, WorkerWatchBus,
};
use klights_leader_api::ResourceListRequest;
use klights_leader_api::{
    CacheReadinessFuture, CacheReadinessRequest, LeaderCacheReadiness, LeaderResourceQuery,
    LeaderWatch, LeaderWatchError, LeaderWatchFuture, ResourceEvent, ResourceGetRequest,
    ResourceListResult, ResourceQueryConsistency, ResourceQueryFuture, WatchEventType,
    WatchRequest, WatchStream,
};
use klights_node_store::OutboxClaimRequest;
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use klights_watch::{EventType, WatchEvent, WatchTarget, WatchTopic};
use serde_json::Value;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn worker_pod_watch_request() -> WatchRequest {
    WatchRequest::try_new(
        "v1",
        "Pod",
        None,
        None,
        Some("spec.nodeName=worker-a".to_string()),
        None,
        None,
    )
    .expect("valid worker Pod watch")
}

fn worker_store_from_local(
    db: crate::datastore::DatastoreHandle,
    passive_reads: &crate::datastore::selector::PassiveReadPorts,
    node_name: &str,
) -> WorkerStoreAdapter {
    let authority =
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
    let proposal =
        Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()));
    let network = Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
            db.clone(),
            proposal.clone(),
            authority.clone(),
        ),
    );
    let cleanup = Arc::new(
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup::new(
            db.clone(),
            proposal,
            authority.clone(),
        ),
    );
    WorkerStoreAdapter::from_focused_ports(
        WorkerStorePorts {
            resource_query: crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
                db.clone(),
                authority.clone(),
            ),
            leader_watch: Arc::new(
                crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                    passive_reads,
                    db,
                ),
            ),
            subnet_allocation: network.clone(),
            network_topology: network,
            cleanup_intents: cleanup,
            watch_events: Arc::new(WorkerWatchBus::new()),
        },
        node_name.to_string(),
    )
}

async fn worker_replay_since_checked_bounded(
    adapter: &WorkerStoreAdapter,
    targets: &[WatchTarget],
    resource_version: i64,
    limit: NonZeroUsize,
) -> anyhow::Result<crate::datastore::WatchReplayRead> {
    let read = adapter.list_watch_events_after_position_checked_bounded(
        targets,
        WatchReplayPosition::from_resource_version(resource_version),
        limit,
    );
    Ok(match read {
        klights_watch::PositionedWatchReplayRead::Expired => {
            crate::datastore::WatchReplayRead::Expired
        }
        klights_watch::PositionedWatchReplayRead::Events(replay) => {
            crate::datastore::WatchReplayRead::Events(
                replay
                    .events
                    .into_iter()
                    .map(|positioned| crate::datastore::CatchUpResource {
                        resource: positioned.event.resource,
                        event_type: std::borrow::Cow::Owned(positioned.event.event_type),
                    })
                    .collect(),
            )
        }
    })
}

fn worker_replay_since(
    adapter: &WorkerStoreAdapter,
    targets: &[WatchTarget],
    resource_version: i64,
) -> Vec<crate::datastore::CatchUpResource> {
    adapter
        .list_watch_events_since(targets, resource_version)
        .into_iter()
        .map(|event| crate::datastore::CatchUpResource {
            resource: event.resource,
            event_type: std::borrow::Cow::Owned(event.event_type),
        })
        .collect()
}

#[tokio::test]
async fn network_metadata_surfaces_forward_through_focused_leader_ports() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let stored_dataplane = klights_cluster_store::DataplanePeerMetadata::try_new(
        "worker-b".to_string(),
        klights_cluster_store::DataplaneMode::Root,
        klights_cluster_store::DataplaneEncryption::Disabled,
        None,
        Some("192.0.2.11".to_string()),
        None,
    )
    .expect("valid direct-route dataplane metadata");
    cluster_db
        .update_node_dataplane(stored_dataplane.clone())
        .await
        .expect("seed leader dataplane metadata");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-focused-network-forwarding-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");

    let worker_a = adapter
        .allocate_node_subnet("worker-a", "10.77.0.0/16", "192.0.2.10")
        .await
        .expect("allocate through focused network metadata surface");
    let worker_b = adapter
        .allocate_node_subnet("worker-b", "10.77.0.0/16", "192.0.2.11")
        .await
        .expect("allocate through focused datastore surface");

    assert_eq!(
        adapter
            .get_node_subnet("worker-a")
            .await
            .expect("query focused surface"),
        Some(worker_a.clone())
    );
    assert_eq!(
        adapter
            .get_node_subnet("worker-b")
            .await
            .expect("query focused surface"),
        Some(worker_b.clone())
    );
    assert_eq!(
        adapter
            .list_peer_subnets("worker-a")
            .await
            .expect("list focused peers"),
        vec![worker_b]
    );
    assert_eq!(
        adapter
            .list_peer_subnets("worker-b")
            .await
            .expect("list focused peers"),
        vec![worker_a]
    );
    assert_eq!(
        adapter
            .get_node_dataplane("worker-b")
            .await
            .expect("query focused dataplane"),
        Some(
            klights_leader_api::NetworkDataplane::try_new(
                "worker-b",
                klights_leader_api::NetworkNodeMode::Root,
                klights_leader_api::DataplaneEncryption::Direct,
                None,
                "192.0.2.11".parse().expect("valid endpoint"),
                None,
            )
            .expect("valid focused dataplane metadata"),
        )
    );
}

struct FailingPodLifecycleBackend {
    remaining_failures: AtomicUsize,
    route_attempts: AtomicUsize,
}

impl FailingPodLifecycleBackend {
    fn new(failures: usize) -> Self {
        Self {
            remaining_failures: AtomicUsize::new(failures),
            route_attempts: AtomicUsize::new(0),
        }
    }

    fn route_attempts(&self) -> usize {
        self.route_attempts.load(Ordering::Acquire)
    }
}

fn configure_successful_pod_router(adapter: &WorkerStoreAdapter) {
    adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(Arc::new(
        FailingPodLifecycleBackend::new(0),
    ))));
}

#[async_trait]
impl PodLifecycleRouteBackend for FailingPodLifecycleBackend {
    async fn route(
        &self,
        _message: LifecycleMessage,
    ) -> std::result::Result<(), PodLifecycleRouteError> {
        self.route_attempts.fetch_add(1, Ordering::AcqRel);
        if self
            .remaining_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            Err(PodLifecycleRouteError::SendError(
                "injected worker mirror route failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn try_route_nonblocking(&self, _message: LifecycleMessage) {}

    fn mode(&self) -> PodLifecycleRouteMode {
        PodLifecycleRouteMode::Actor
    }

    async fn remove_pod_state(&self, _key: &PodLifecycleKey) -> bool {
        false
    }

    async fn diagnostics(&self) -> PodLifecycleDiagnostics {
        PodLifecycleDiagnostics {
            mode: PodLifecycleRouteMode::Actor,
            actor_states: Vec::new(),
            recent_trace: Vec::new(),
            active_pod_count: 0,
        }
    }

    async fn active_pod_count(&self) -> usize {
        0
    }

    async fn in_flight_start_keys(&self) -> Vec<PodLifecycleKey> {
        Vec::new()
    }
}

#[tokio::test]
async fn failed_local_pod_route_is_not_published_by_worker_mirror() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-route-apply-gate-test",
    )
    .await
    .expect("open node-local");
    let adapter = WorkerStoreAdapter::new(Arc::new(HandoffLeaderApi), "worker-a".to_string());
    let backend = Arc::new(FailingPodLifecycleBackend::new(1));
    adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
        backend.clone(),
    )));
    let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

    let result = adapter
        .publish_watch_from_mirror(WatchEvent::added(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "must-replay",
                "uid": "uid-must-replay",
                "resourceVersion": "42"
            },
            "spec": {"nodeName": "worker-a"}
        })))
        .await;

    assert!(
        result.is_err(),
        "the lifecycle routing failure must propagate"
    );
    assert!(
        watch.try_recv().is_err(),
        "a Pod event whose lifecycle route failed must not be locally published"
    );
    assert_eq!(
        adapter.current_resource_version().await,
        0,
        "a failed route must not advance worker mirror state"
    );
    assert_eq!(backend.route_attempts(), 1);
}

#[tokio::test]
async fn failed_snapshot_pod_route_retries_without_committing_reflector_or_membership() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    cluster_db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "snapshot-replay",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "snapshot-replay",
                    "uid": "uid-snapshot-replay"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "busybox"}]
                }
            }),
        )
        .await
        .expect("create snapshot Pod");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-snapshot-apply-gate-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");
    let backend = Arc::new(FailingPodLifecycleBackend::new(1));
    adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
        backend.clone(),
    )));
    let req = worker_pod_watch_request();
    let mut state = ReflectorState::default();
    let mut membership = adapter.transition_projector_for_test(&req).unwrap();
    let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

    let first = adapter
        .reconcile_watch_snapshot_for_test(&req, &mut state, membership.as_mut())
        .await;
    assert!(
        first.is_err(),
        "the initial snapshot route failure must propagate"
    );
    assert!(state.is_empty(), "reflector state must remain uncommitted");
    assert_eq!(adapter.current_resource_version().await, 0);
    assert!(
        watch.try_recv().is_err(),
        "failed snapshot must not publish"
    );

    adapter
        .reconcile_watch_snapshot_for_test(&req, &mut state, membership.as_mut())
        .await
        .expect("the same snapshot must replay after the route recovers");
    let replayed = watch.try_recv().expect("replayed snapshot event");
    assert_eq!(replayed.event_type, EventType::Added);
    assert_eq!(
        replayed
            .object
            .pointer("/metadata/name")
            .and_then(Value::as_str),
        Some("snapshot-replay")
    );
    assert_eq!(state.len(), 1);
    assert_eq!(
        backend.route_attempts(),
        2,
        "the failed initial-list event must be routed again on snapshot retry"
    );
}

#[derive(Default)]
struct HandoffLeaderApi;

impl LeaderResourceQuery for HandoffLeaderApi {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let consistency = request.consistency();
            let key = request.into_key();
            if key.api_version == "v1" && key.kind == "Namespace" && key.name == "fresh-events" {
                return Ok(
                    (consistency == ResourceQueryConsistency::LeaderFresh).then(|| Resource {
                        id: 2,
                        api_version: "v1".to_string(),
                        kind: "Namespace".to_string(),
                        namespace: None,
                        name: "fresh-events".to_string(),
                        uid: "uid-fresh-events".to_string(),
                        resource_version: 13,
                        data: Arc::new(serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": {
                                "name": "fresh-events",
                                "uid": "uid-fresh-events",
                                "resourceVersion": "13"
                            },
                            "status": {"phase": "Active"}
                        })),
                    }),
                );
            }
            if key.api_version == "v1" && key.kind == "Pod" && key.name == "cached-deleted" {
                if consistency == ResourceQueryConsistency::LeaderFresh {
                    return Ok(None);
                }
                return Ok(Some(Resource {
                    id: 1,
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "cached-deleted".to_string(),
                    uid: "uid-cached".to_string(),
                    resource_version: 12,
                    data: Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "cached-deleted",
                            "uid": "uid-cached",
                            "resourceVersion": "12"
                        },
                        "spec": {
                            "nodeName": "worker-a",
                            "containers": [{"name": "app", "image": "nginx"}]
                        }
                    })),
                }));
            }
            unreachable!("handoff test does not use get_resource for {key:?}")
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let resource_version = if request.api_version() == "v1" && request.kind() == "Pod" {
                assert_eq!(request.field_selector(), Some("spec.nodeName=worker-a"));
                41
            } else {
                0
            };
            ResourceListResult::try_new(
                Vec::new(),
                resource_version,
                (resource_version > 0).then_some(WatchReplayPosition {
                    resource_version,
                    event_id: 91,
                    resource_version_filter_through_event_id: 0,
                }),
                None,
                None,
            )
        })
    }
}

impl LeaderWatch for HandoffLeaderApi {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        Box::pin(async move {
            if req.api_version() == "v1" && req.kind() == "Pod" {
                assert_eq!(req.start_resource_version(), Some(41));
                assert_eq!(
                    req.start_watch_replay_position(),
                    Some(WatchReplayPosition {
                        resource_version: 41,
                        event_id: 91,
                        resource_version_filter_through_event_id: 0,
                    })
                );
                let resource = Resource::try_from_data(Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "bound-during-handoff",
                        "uid": "uid-handoff",
                        "resourceVersion": "42"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    },
                    "status": {"phase": "Pending"}
                })))
                .expect("valid handoff Pod");
                let event = ResourceEvent::try_new(
                    WatchEventType::Modified,
                    resource,
                    Some(WatchReplayPosition {
                        resource_version: 42,
                        event_id: 92,
                        resource_version_filter_through_event_id: 0,
                    }),
                )
                .expect("valid positioned event");
                return Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::once(async move { Ok(event) }),
                ));
            }
            Ok(WatchStream::unpositioned_test_stream(
                futures::stream::pending(),
            ))
        })
    }
}

impl LeaderCacheReadiness for HandoffLeaderApi {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

crate::bootstrap::leader_test_support::impl_unavailable_leader_pod_effects!(HandoffLeaderApi);

#[tokio::test]
async fn failed_pod_route_reconnects_and_replays_from_prior_exact_position() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-route-replay-position-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(
        Arc::new(HandoffLeaderApi),
        "worker-a".to_string(),
    ));
    let backend = Arc::new(FailingPodLifecycleBackend::new(1));
    adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
        backend.clone(),
    )));
    let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));
    let cancel = tokio_util::sync::CancellationToken::new();
    let driver_adapter = adapter.clone();
    let driver_supervisor = supervisor.clone();
    let driver_cancel = cancel.clone();
    let handle = supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Network,
            "worker_store_route_replay_position_test",
            async move {
                driver_adapter
                    .run_watch_mirror_for_test(
                        worker_pod_watch_request(),
                        driver_supervisor,
                        driver_cancel,
                    )
                    .await;
            },
        )
        .await
        .expect("spawn mirror driver");

    let replayed = tokio::time::timeout(std::time::Duration::from_secs(3), watch.recv())
        .await
        .expect("failed route should reconnect and replay")
        .expect("worker Pod watch remains open");
    assert_eq!(
        replayed
            .object
            .pointer("/metadata/name")
            .and_then(Value::as_str),
        Some("bound-during-handoff")
    );
    assert!(
        backend.route_attempts() >= 2,
        "the event must be routed once unsuccessfully, then replayed and routed again"
    );

    cancel.cancel();
    let _ = handle.join().await;
}

#[derive(Clone, Copy)]
enum OpenExpiryMode {
    TypedOnce,
    TypedAlways,
    UnmarkedOnce,
}

struct OpenExpiredThenRelistLeaderApi {
    list_count: AtomicUsize,
    watch_count: AtomicUsize,
    watch_attempted: tokio::sync::Notify,
    expiry_mode: OpenExpiryMode,
}

impl OpenExpiredThenRelistLeaderApi {
    fn typed_expiry() -> Self {
        Self {
            list_count: AtomicUsize::new(0),
            watch_count: AtomicUsize::new(0),
            watch_attempted: tokio::sync::Notify::new(),
            expiry_mode: OpenExpiryMode::TypedOnce,
        }
    }

    fn repeated_typed_expiry() -> Self {
        Self {
            list_count: AtomicUsize::new(0),
            watch_count: AtomicUsize::new(0),
            watch_attempted: tokio::sync::Notify::new(),
            expiry_mode: OpenExpiryMode::TypedAlways,
        }
    }

    fn unmarked_out_of_range() -> Self {
        Self {
            list_count: AtomicUsize::new(0),
            watch_count: AtomicUsize::new(0),
            watch_attempted: tokio::sync::Notify::new(),
            expiry_mode: OpenExpiryMode::UnmarkedOnce,
        }
    }

    async fn wait_for_watch_attempts(&self, expected: usize) {
        while self.watch_count.load(Ordering::SeqCst) < expected {
            self.watch_attempted.notified().await;
        }
    }
}

impl LeaderResourceQuery for OpenExpiredThenRelistLeaderApi {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        HandoffLeaderApi.get_resource(request)
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            if request.api_version() != "v1" || request.kind() != "Pod" {
                return ResourceListResult::try_new(Vec::new(), 0, None, None, None);
            }
            assert_eq!(request.field_selector(), Some("spec.nodeName=worker-a"));
            let attempt = self.list_count.fetch_add(1, Ordering::SeqCst);
            let (name, uid, resource_version) = if attempt == 0 {
                ("removed-before-relist", "uid-removed", 41)
            } else {
                ("scheduled-after-relist", "uid-after-relist", 52)
            };
            let items = vec![Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: name.to_string(),
                uid: uid.to_string(),
                resource_version,
                data: Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": name,
                        "uid": uid,
                        "resourceVersion": resource_version.to_string()
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "busybox"}]
                    },
                    "status": {"phase": "Pending"}
                })),
            }];
            ResourceListResult::try_new(items, if attempt == 0 { 41 } else { 52 }, None, None, None)
        })
    }
}

impl LeaderWatch for OpenExpiredThenRelistLeaderApi {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        Box::pin(async move {
            if req.api_version() != "v1" || req.kind() != "Pod" {
                return Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::pending(),
                ));
            }
            assert_eq!(req.field_selector(), Some("spec.nodeName=worker-a"));
            let attempt = self.watch_count.fetch_add(1, Ordering::SeqCst);
            self.watch_attempted.notify_waiters();
            let should_expire =
                attempt == 0 || matches!(self.expiry_mode, OpenExpiryMode::TypedAlways);
            if should_expire {
                let expected_rv = if attempt == 0 { 41 } else { 52 };
                assert_eq!(req.start_resource_version(), Some(expected_rv));
                return Err(match self.expiry_mode {
                    OpenExpiryMode::TypedOnce | OpenExpiryMode::TypedAlways => {
                        LeaderWatchError::ReplayExpired {
                            accepted_resource_version: expected_rv,
                        }
                    }
                    OpenExpiryMode::UnmarkedOnce => {
                        LeaderWatchError::transport("message exceeds configured maximum size")
                    }
                });
            }
            assert_eq!(
                req.start_resource_version(),
                Some(match self.expiry_mode {
                    OpenExpiryMode::TypedOnce | OpenExpiryMode::TypedAlways => 52,
                    OpenExpiryMode::UnmarkedOnce => 41,
                })
            );
            Ok(WatchStream::unpositioned_test_stream(
                futures::stream::pending(),
            ))
        })
    }
}

impl LeaderCacheReadiness for OpenExpiredThenRelistLeaderApi {
    fn wait_cache_ready(&self, scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        HandoffLeaderApi.wait_cache_ready(scope)
    }
}

crate::bootstrap::leader_test_support::impl_unavailable_leader_pod_effects!(
    OpenExpiredThenRelistLeaderApi
);

#[tokio::test]
async fn worker_pod_get_uses_worker_cache_not_fresh_leader_state() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-pod-get-fresh-test",
    )
    .await
    .expect("open node-local");
    let adapter = WorkerStoreAdapter::new(Arc::new(HandoffLeaderApi), "worker-a".to_string());

    let pod = adapter
        .get_resource("v1", "Pod", Some("default"), "cached-deleted")
        .await
        .expect("fresh pod get should succeed");

    assert_eq!(
        pod.as_ref().map(|resource| resource.uid.as_str()),
        Some("uid-cached"),
        "worker pod get must read the worker cache and avoid a fresh leader unary read"
    );
}

#[tokio::test]
async fn worker_store_pod_events_use_fresh_namespace_state_before_outbox_enqueue() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-event-namespace-fresh-test",
    )
    .await
    .expect("open node-local");
    let adapter = WorkerStoreAdapter::new(Arc::new(HandoffLeaderApi), "worker-a".to_string());
    let outbox =
        crate::bootstrap::composition_tests::support::outbox_from_node_db(node_local.clone());
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "fresh-events",
            "name": "sysctl-pod",
            "uid": "uid-sysctl-pod"
        },
        "spec": {
            "nodeName": "worker-a",
            "containers": [{"name": "test-container", "image": "busybox"}]
        }
    });

    let query = crate::bootstrap::composition_adapters::pod_event_adapter::LeaderPodEventQuery::new(
        adapter.resource_query_for_test(),
    );
    klights_kubelet::pod_events::emit_worker_pod_event(
        &query,
        &outbox,
        klights_kubelet::pod_events::PodEventRecord {
            pod: &pod,
            reason: "Started",
            message: "Started container test-container",
            event_type: "Normal",
            reporting_component: "klights-kubelet",
            reporting_instance: "worker-a",
            operation_now: klights_supervisor::SystemWallClock::now_utc(),
        },
    )
    .await
    .expect("worker-store event emission should enqueue event");

    let row = node_local
        .outbox_dispatcher()
        .claim_next_due_outbox(
            OutboxClaimRequest::try_new(i64::MAX / 2, 1_000, "event-test")
                .expect("valid outbox claim request"),
        )
        .await
        .expect("claim outbox")
        .expect("event outbox row should be enqueued");
    assert_eq!(row.operation(), "EventCreate");
    assert_eq!(
        row.subject().resource().namespace.as_deref(),
        Some("fresh-events")
    );
    assert_eq!(row.subject().resource().kind, "Event");
}

#[tokio::test]
async fn worker_pod_lists_are_constrained_to_local_node() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-pod-list-local-node-test",
    )
    .await
    .expect("open node-local");
    let adapter = WorkerStoreAdapter::new(Arc::new(HandoffLeaderApi), "worker-a".to_string());

    let list = adapter
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            None,
            None,
            WorkerListPage::unbounded(),
        )
        .await
        .expect("list local pods");

    assert_eq!(list.resource_version, 41);
}

#[tokio::test]
async fn worker_list_page_preserves_continuation_metadata() {
    // Regression: list_resources_page used to pass limit/continue_token to
    // the leader *and* re-apply ListPageRequest locally. The leader-side
    // pagination already truncated the page, so the local re-apply saw a
    // list no longer than the limit and cleared the leader-provided
    // continue_token / remaining_item_count — workers' LIST silently dropped
    // the rest of the collection. Pagination must be applied exactly once.
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    for name in ["cm-a", "cm-b", "cm-c"] {
        cluster_db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": name}
                }),
            )
            .await
            .expect("create configmap");
    }
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-pagination-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");

    let first = adapter
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            None,
            None,
            WorkerListPage {
                limit: Some(2),
                continue_token: None,
            },
        )
        .await
        .expect("list first page");
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["cm-a", "cm-b"]
    );
    assert_eq!(
        first.continue_token.as_deref(),
        Some("cm-b"),
        "first page must expose a continue token for the remaining item"
    );
    assert_eq!(first.remaining_item_count, Some(1));

    let second = adapter
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            None,
            None,
            WorkerListPage {
                limit: Some(2),
                continue_token: first.continue_token.clone(),
            },
        )
        .await
        .expect("list second page");
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["cm-c"]
    );
    assert!(
        second.continue_token.is_none(),
        "final page must not advertise a continue token"
    );
}

#[tokio::test]
async fn worker_watch_replay_respects_resume_resource_version() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    for name in ["cm-a", "cm-b", "cm-c"] {
        cluster_db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": name}
                }),
            )
            .await
            .expect("create configmap");
    }
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-watch-resume-rv-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");
    for (index, name) in ["cm-a", "cm-b", "cm-c"].into_iter().enumerate() {
        adapter.publish_watch_for_test(WatchEvent::added(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": format!("uid-{name}"),
                "resourceVersion": (index + 1).to_string()
            }
        })));
    }
    let targets = [WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "default",
    )];
    let limit = std::num::NonZeroUsize::new(3).expect("non-zero limit");

    let first = worker_replay_since_checked_bounded(&adapter, &targets, 0, limit)
        .await
        .expect("initial watch replay");
    let crate::datastore::WatchReplayRead::Events(first_events) = first else {
        panic!("worker adapter replay should not expire");
    };
    assert_eq!(first_events.len(), 3);
    let max_rv = first_events
        .iter()
        .map(|event| event.resource.resource_version)
        .max()
        .expect("initial replay should have a max rv");

    let second = worker_replay_since_checked_bounded(&adapter, &targets, max_rv, limit)
        .await
        .expect("resumed watch replay");
    let crate::datastore::WatchReplayRead::Events(second_events) = second else {
        panic!("worker adapter replay should not expire");
    };
    assert!(
        second_events.is_empty(),
        "resumed worker replay must not return resources at or below the resume RV"
    );
}

#[tokio::test]
async fn worker_scalar_watch_replay_never_synthesizes_events_from_live_list_state() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    cluster_db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "not-durable-worker-history",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "not-durable-worker-history"
                }
            }),
        )
        .await
        .expect("create configmap");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-no-scalar-snapshot-replay-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");

    let replay = worker_replay_since(
        &adapter,
        &[WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )],
        0,
    );

    assert!(
        replay.is_empty(),
        "worker scalar replay must expose only its local durable mirror history; live LIST synthesis is a second establishment algorithm"
    );
}

#[tokio::test]
async fn worker_watch_replay_preserves_mirrored_delete_events() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-watch-delete-replay-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");

    let pending = crate::datastore::create_staged_post_commit(
        "v1",
        "ConfigMap",
        Some("default"),
        "deleted-config",
        42,
        "DELETED",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "deleted-config",
                "resourceVersion": "41"
            },
            "data": {"data-1": "value-1"}
        }),
    );
    adapter.publish_watch_for_test(
        crate::datastore::staged_test_event(&pending).expect("staged test watch event"),
    );

    let replay = worker_replay_since_checked_bounded(
        &adapter,
        &[WatchTarget::namespaced("v1", "ConfigMap")],
        0,
        NonZeroUsize::new(8).expect("non-zero limit"),
    )
    .await
    .expect("watch replay should succeed");

    let crate::datastore::WatchReplayRead::Events(events) = replay else {
        panic!("worker adapter replay should not expire");
    };
    assert!(
        events.iter().any(|event| {
            event.event_type.as_ref() == "DELETED"
                && event.resource.kind == "ConfigMap"
                && event.resource.name == "deleted-config"
                && event.resource.resource_version == 42
        }),
        "worker watch replay must preserve mirrored DELETED events because deleted resources are absent from snapshot replay"
    );
}

#[tokio::test]
async fn worker_watch_replay_marks_resumed_bound_pod_snapshot_changes_modified() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = cluster_db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "deadline-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "deadline-pod",
                    "uid": "uid-deadline"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{
                        "name": "pause",
                        "image": "registry.k8s.io/pause:3.10"
                    }]
                }
            }),
        )
        .await
        .expect("create pod");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-watch-resume-pod-modified-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");
    let mut created_event = (*created.data).clone();
    created_event["metadata"]["resourceVersion"] =
        serde_json::json!(created.resource_version.to_string());
    adapter.publish_watch_for_test(WatchEvent::added(created_event));
    let targets = [WatchTarget::namespaced_in_namespace("v1", "Pod", "default")];
    let limit = std::num::NonZeroUsize::new(4).expect("non-zero limit");

    let first = worker_replay_since_checked_bounded(&adapter, &targets, 0, limit)
        .await
        .expect("initial watch replay");
    let crate::datastore::WatchReplayRead::Events(first_events) = first else {
        panic!("worker adapter replay should not expire");
    };
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].event_type.as_ref(), "ADDED");

    let updated = cluster_db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "deadline-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "deadline-pod",
                    "uid": "uid-deadline"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "activeDeadlineSeconds": 1,
                    "containers": [{
                        "name": "pause",
                        "image": "registry.k8s.io/pause:3.10"
                    }]
                }
            }),
            created.resource_version,
        )
        .await
        .expect("update pod");
    let mut updated_event = (*updated.data).clone();
    updated_event["metadata"]["resourceVersion"] =
        serde_json::json!(updated.resource_version.to_string());
    adapter.publish_watch_for_test(WatchEvent::modified(updated_event));

    let resumed =
        worker_replay_since_checked_bounded(&adapter, &targets, created.resource_version, limit)
            .await
            .expect("resumed watch replay");
    let crate::datastore::WatchReplayRead::Events(resumed_events) = resumed else {
        panic!("worker adapter replay should not expire");
    };
    assert_eq!(resumed_events.len(), 1);
    assert_eq!(
        resumed_events[0].event_type.as_ref(),
        "MODIFIED",
        "worker snapshot replay after a resume RV must preserve update semantics"
    );
    assert_eq!(
        resumed_events[0]
            .resource
            .data
            .pointer("/spec/activeDeadlineSeconds")
            .and_then(|value| value.as_i64()),
        Some(1)
    );
}

#[tokio::test]
async fn reads_cluster_objects_through_worker_cache_and_runtime_rows_from_node_local() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    cluster_db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                }
            }),
        )
        .await
        .expect("create cluster pod");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-test",
    )
    .await
    .expect("open node-local");
    let adapter = worker_store_from_local(cluster_db_handle, &passive_reads, "worker-a");

    let pod = adapter
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod through leader api")
        .expect("pod exists");
    assert_eq!(pod.uid, "uid-1");

    node_local
        .pod_runtime()
        .record_owned_sandbox(
            klights_node_store::OwnedPodSandbox::try_new(
                klights_types::PodIdentity::new("default", "web", "uid-1"),
                "worker-a",
                "sandbox-1",
                0,
            )
            .unwrap(),
        )
        .await
        .expect("record sandbox in node-local store");
    assert_eq!(
        node_local
            .pod_runtime()
            .get_pod_runtime(klights_node_store::RuntimePodUid::try_new("uid-1").unwrap())
            .await
            .expect("read worker sandbox")
            .and_then(|row| row.sandbox_id().map(str::to_string)),
        Some("sandbox-1".to_string())
    );
}

#[tokio::test]
async fn watch_mirror_publishes_existing_node_pods_on_startup() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    cluster_db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "already-bound",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "already-bound",
                    "uid": "uid-bound"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create cluster pod");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-watch-bootstrap-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(worker_store_from_local(
        cluster_db_handle,
        &passive_reads,
        "worker-a",
    ));
    configure_successful_pod_router(&adapter);
    let mut watch_rx = adapter.watch_topic(klights_watch::WatchTopic::new("v1", "Pod"));
    let cancel = tokio_util::sync::CancellationToken::new();

    let handles = adapter
        .start_watch_mirrors(supervisor.clone(), cancel.clone())
        .await
        .expect("start watch mirrors");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
        .await
        .expect("existing node pod should be replayed into worker watch")
        .expect("watch channel should remain open");
    cancel.cancel();
    for handle in handles {
        let _ = handle.join().await;
    }

    assert_eq!(event.event_type, klights_watch::EventType::Added);
    assert_eq!(
        event
            .object
            .pointer("/metadata/name")
            .and_then(|v| v.as_str()),
        Some("already-bound")
    );
    assert_eq!(
        event
            .object
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("worker-a")
    );
}

#[tokio::test]
async fn watch_mirror_publishes_namespace_events_on_startup() {
    let cluster_db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    cluster_db
        .create_namespace(
            "terminating-ns",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-ns",
                    "uid": "ns-uid",
                    "deletionTimestamp": "2026-05-18T20:06:06Z"
                },
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .expect("create terminating namespace");
    let passive_reads = crate::datastore::selector::sqlite_passive_read_ports(&cluster_db);
    let cluster_db_handle: crate::datastore::DatastoreHandle = Arc::new(cluster_db.clone());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-namespace-watch-bootstrap-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(worker_store_from_local(
        cluster_db_handle,
        &passive_reads,
        "worker-a",
    ));
    let mut watch_rx = adapter.watch_topic(klights_watch::WatchTopic::new("v1", "Namespace"));
    let cancel = tokio_util::sync::CancellationToken::new();

    let handles = adapter
        .start_watch_mirrors(supervisor.clone(), cancel.clone())
        .await
        .expect("start watch mirrors");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let event = watch_rx
                .recv()
                .await
                .expect("watch channel should remain open");
            if event.object.get("kind").and_then(|value| value.as_str()) == Some("Namespace") {
                break event;
            }
        }
    })
    .await
    .expect("terminating namespace should be replayed into worker watch");
    cancel.cancel();
    for handle in handles {
        let _ = handle.join().await;
    }

    assert_eq!(event.event_type, klights_watch::EventType::Added);
    assert_eq!(
        event
            .object
            .pointer("/metadata/name")
            .and_then(|value| value.as_str()),
        Some("terminating-ns")
    );
    assert_eq!(
        event
            .object
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some("2026-05-18T20:06:06Z")
    );
}

#[tokio::test]
async fn watch_mirror_relists_after_open_time_replay_window_expiration() {
    let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::typed_expiry());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-watch-open-expired-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(
        cluster_api.clone(),
        "worker-a".to_string(),
    ));
    configure_successful_pod_router(&adapter);
    let mut watch_rx = adapter.watch_topic(klights_watch::WatchTopic::new("v1", "Pod"));
    let cancel = tokio_util::sync::CancellationToken::new();

    let handles = adapter
        .start_watch_mirrors(supervisor.clone(), cancel.clone())
        .await
        .expect("start watch mirrors");

    let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut events = Vec::new();
        while events.len() < 3 {
            events.push(
                watch_rx
                    .recv()
                    .await
                    .expect("watch channel should remain open"),
            );
        }
        events
    })
    .await
    .expect("mirror should publish initial and authoritative replacement events");
    cancel.cancel();
    for handle in handles {
        let _ = handle.join().await;
    }

    assert_eq!(
        events
            .iter()
            .map(|event| (
                event.event_type,
                event
                    .object
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![
            (EventType::Added, "removed-before-relist"),
            (EventType::Deleted, "removed-before-relist"),
            (EventType::Added, "scheduled-after-relist"),
        ],
        "expired replay relist must remove objects absent from the authoritative snapshot"
    );
    assert!(
        cluster_api.list_count.load(Ordering::SeqCst) >= 2,
        "open-time typed replay expiry must force a fresh LIST"
    );
}

#[tokio::test]
async fn watch_mirror_unmarked_out_of_range_reconnects_without_relist() {
    let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::unmarked_out_of_range());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-watch-unmarked-out-of-range-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(
        cluster_api.clone(),
        "worker-a".to_string(),
    ));
    configure_successful_pod_router(&adapter);
    let cancel = tokio_util::sync::CancellationToken::new();
    let driver_adapter = adapter.clone();
    let driver_supervisor = supervisor.clone();
    let driver_cancel = cancel.clone();
    let handle = supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Network,
            "worker_store_watch_unmarked_out_of_range_test",
            async move {
                driver_adapter
                    .run_watch_mirror_for_test(
                        worker_pod_watch_request(),
                        driver_supervisor,
                        driver_cancel,
                    )
                    .await;
            },
        )
        .await
        .expect("spawn mirror driver");

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cluster_api.wait_for_watch_attempts(2),
    )
    .await
    .expect("unmarked OutOfRange should reconnect without requiring a relist");
    cancel.cancel();
    let _ = handle.join().await;

    assert_eq!(
        cluster_api.list_count.load(Ordering::SeqCst),
        1,
        "unmarked OutOfRange must keep the safe resume position and avoid authoritative LIST"
    );
}

#[tokio::test]
async fn watch_mirror_repeated_expiry_backs_off_before_next_relist() {
    let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::repeated_typed_expiry());
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-watch-repeated-expiry-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(
        cluster_api.clone(),
        "worker-a".to_string(),
    ));
    configure_successful_pod_router(&adapter);
    let cancel = tokio_util::sync::CancellationToken::new();
    let driver_adapter = adapter.clone();
    let driver_supervisor = supervisor.clone();
    let driver_cancel = cancel.clone();
    let handle = supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Network,
            "worker_store_watch_repeated_expiry_test",
            async move {
                driver_adapter
                    .run_watch_mirror_for_test(
                        worker_pod_watch_request(),
                        driver_supervisor,
                        driver_cancel,
                    )
                    .await;
            },
        )
        .await
        .expect("spawn mirror driver");

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        cluster_api.wait_for_watch_attempts(2),
    )
    .await
    .expect("second typed expiry should be observed");
    assert_eq!(
        cluster_api.list_count.load(Ordering::SeqCst),
        2,
        "first typed expiry should get exactly one immediate relist"
    );

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    cancel.cancel();
    let _ = handle.join().await;

    assert_eq!(
        cluster_api.list_count.load(Ordering::SeqCst),
        2,
        "second consecutive typed expiry must back off instead of immediately relisting again"
    );
}

#[tokio::test]
async fn worker_store_requeues_node_local_pod_workqueue_failures() {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        "sqlite:worker-store-workqueue-retry-test",
    )
    .await
    .expect("open node-local");
    let pod = klights_types::PodIdentity::new("default", "stuck", "uid-stuck");
    let workqueue = node_local.pod_workqueue();
    workqueue
        .enqueue_work(
            klights_node_store::PodWorkqueueEnqueue::try_new(
                klights_node_store::PodWorkIdentity::try_pod(pod.clone())
                    .expect("valid Pod work identity"),
                serde_json::to_vec(&serde_json::json!({"source": "test"}))
                    .expect("encode work payload"),
                3,
                0,
                None,
            )
            .expect("valid workqueue enqueue"),
        )
        .await
        .expect("enqueue workqueue row");
    let claimed = workqueue
        .claim_due_work_with_lease(
            klights_node_store::PodWorkqueueClaimRequest::try_new(i64::MAX - 1, 1)
                .expect("valid workqueue claim"),
        )
        .await
        .expect("claim workqueue row")
        .expect("workqueue row exists");

    let entry = claimed.entry();
    workqueue
        .enqueue_work(
            klights_node_store::PodWorkqueueEnqueue::try_new(
                entry.identity().clone(),
                entry.payload().to_vec(),
                entry.attempt_count().saturating_add(1),
                0,
                Some("missed delete".to_string()),
            )
            .expect("valid retry enqueue"),
        )
        .await
        .expect("record worker-local failure");

    let retried = workqueue
        .claim_due_work_with_lease(
            klights_node_store::PodWorkqueueClaimRequest::try_new(i64::MAX - 1, 1)
                .expect("valid retry claim"),
        )
        .await
        .expect("claim retried workqueue row")
        .expect("failure must requeue worker-local pod delete work");
    let entry = retried.entry();
    assert_eq!(entry.kind(), klights_node_store::PodWorkqueueKind::Pod);
    let pod = entry.identity().as_pod().expect("retry remains Pod work");
    assert_eq!(pod.namespace, "default");
    assert_eq!(pod.name, "stuck");
    assert_eq!(pod.uid, "uid-stuck");
    assert_eq!(entry.attempt_count(), 4);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(entry.payload()).expect("decode retry payload"),
        serde_json::json!({"source": "test"})
    );
}

#[tokio::test]
async fn worker_store_routes_local_pod_watch_to_lifecycle_actor() {
    struct LocalPodLeaderApi;

    impl LocalPodLeaderApi {
        fn event(event_type: WatchEventType, data: Value) -> ResourceEvent {
            ResourceEvent::try_new(
                event_type,
                Resource::try_from_data(Arc::new(data)).expect("valid test Pod"),
                None,
            )
            .expect("valid test watch event")
        }
    }

    impl LeaderResourceQuery for LocalPodLeaderApi {
        fn get_resource(
            &self,
            request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            Box::pin(async move {
                unreachable!(
                    "local pod watch test does not use get_resource for {:?}",
                    request.key()
                )
            })
        }

        fn list_resources(
            &self,
            request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            Box::pin(async move {
                ResourceListResult::try_new(
                    Vec::new(),
                    if request.api_version() == "v1" && request.kind() == "Pod" {
                        41
                    } else {
                        0
                    },
                    None,
                    None,
                    None,
                )
            })
        }
    }

    impl LeaderWatch for LocalPodLeaderApi {
        fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
            Box::pin(async move {
                if req.api_version() == "v1" && req.kind() == "Pod" {
                    if req.start_resource_version() != Some(41) {
                        return Ok(WatchStream::unpositioned_test_stream(
                            futures::stream::pending(),
                        ));
                    }
                    let events = vec![
                        Self::event(
                            WatchEventType::Added,
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "startable",
                                    "uid": "uid-startable",
                                    "resourceVersion": "42"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Pending"}
                            }),
                        ),
                        Self::event(
                            WatchEventType::Modified,
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "terminating",
                                    "uid": "uid-terminating",
                                    "resourceVersion": "43",
                                    "deletionTimestamp": "2026-06-21T02:07:04Z"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Succeeded"}
                            }),
                        ),
                        Self::event(
                            WatchEventType::Added,
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "moving-away",
                                    "uid": "uid-moving-away",
                                    "resourceVersion": "44"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Running"}
                            }),
                        ),
                        Self::event(
                            WatchEventType::Modified,
                            serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "moving-away",
                                    "uid": "uid-moving-away",
                                    "resourceVersion": "45"
                                },
                                "spec": {
                                    "nodeName": "worker-b",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Running"}
                            }),
                        ),
                    ];
                    return Ok(WatchStream::unpositioned_test_stream(
                        futures::stream::iter(events.into_iter().map(Ok)),
                    ));
                }
                Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::pending(),
                ))
            })
        }
    }

    impl LeaderCacheReadiness for LocalPodLeaderApi {
        fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    crate::bootstrap::leader_test_support::impl_unavailable_leader_pod_effects!(LocalPodLeaderApi);

    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-terminating-pod-watch-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(
        Arc::new(LocalPodLeaderApi),
        "worker-a".to_string(),
    ));
    let executor = klights_kubelet::pod_lifecycle_router::executor::RecordingExecutor::new();
    let registry = Arc::new(
            klights_kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(
                    executor.clone()
                        as Arc<
                            dyn klights_kubelet::pod_lifecycle_router::executor::PodWorkExecutor,
                        >,
                )),
            ),
        );
    let router = Arc::new(
        klights_kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
            registry,
            executor.clone()
                as Arc<dyn klights_kubelet::pod_lifecycle_router::executor::PodWorkExecutor>,
        ),
    );
    adapter.set_pod_lifecycle_router(router);

    let mut pod_watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

    let cancel = tokio_util::sync::CancellationToken::new();
    let handles = adapter
        .start_watch_mirrors(supervisor, cancel.clone())
        .await
        .expect("start watch mirrors");

    let moving_types = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let mut types = Vec::new();
        while types.len() < 2 {
            let event = pod_watch.recv().await.expect("Pod watch remains open");
            if event
                .object
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                == Some("moving-away")
            {
                types.push(event.event_type);
            }
        }
        types
    })
    .await
    .expect("nodeName leave transition should be mirrored");
    assert_eq!(
        moving_types,
        vec![EventType::Added, EventType::Deleted],
        "a Pod leaving spec.nodeName=worker-a must synthesize Deleted on the worker mirror"
    );

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    let mut observed = Vec::new();
    loop {
        observed.extend(executor.take_actions());
        let start_seen = observed.iter().any(|action| {
            matches!(
                action,
                klights_kubelet::pod_lifecycle_core::action::PodAction::StartPod {
                    key, ..
                }
                | klights_kubelet::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                    key,
                    ..
                } if key.name == "startable" && key.uid == "uid-startable"
            )
        });
        let stop_seen = observed.iter().any(|action| {
            matches!(
                action,
                klights_kubelet::pod_lifecycle_core::action::PodAction::StopPod {
                    key, ..
                } if key.name == "terminating" && key.uid == "uid-terminating"
            )
        });
        if start_seen && stop_seen {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "local Pod watch events must wake lifecycle actors; observed actions: {observed:?}"
            );
        }
        tokio::task::yield_now().await;
    }
    cancel.cancel();
    for handle in handles {
        let _ = handle.join().await;
    }
}

#[tokio::test]
async fn watch_mirror_replays_pods_bound_between_initial_list_and_watch() {
    let cluster_api = Arc::new(HandoffLeaderApi);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let _node_local = crate::bootstrap::node_store::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor.clone(),
        "sqlite:worker-store-watch-handoff-test",
    )
    .await
    .expect("open node-local");
    let adapter = Arc::new(WorkerStoreAdapter::new(cluster_api, "worker-a".to_string()));
    configure_successful_pod_router(&adapter);
    let mut watch_rx = adapter.watch_topic(klights_watch::WatchTopic::new("v1", "Pod"));
    let cancel = tokio_util::sync::CancellationToken::new();

    let handles = adapter
        .start_watch_mirrors(supervisor.clone(), cancel.clone())
        .await
        .expect("start watch mirrors");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
        .await
        .expect("pod bound after the initial list should be replayed from list RV")
        .expect("watch channel should remain open");
    cancel.cancel();
    for handle in handles {
        let _ = handle.join().await;
    }

    assert_eq!(
        event.event_type,
        klights_watch::EventType::Added,
        "a Pod entering the worker's nodeName selector after LIST must be ADDED"
    );
    assert_eq!(
        event
            .object
            .pointer("/metadata/name")
            .and_then(|v| v.as_str()),
        Some("bound-during-handoff")
    );
    assert_eq!(
        event
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("42")
    );
}
