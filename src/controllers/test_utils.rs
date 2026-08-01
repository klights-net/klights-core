//! Shared test utilities for controller wrapper tests.

use std::sync::Arc;

struct TestGcNonPodFinalizationPort;

impl klights_reconcile_api::GcNonPodFinalizationPort for TestGcNonPodFinalizationPort {
    fn finalize_non_pod(
        &self,
        _request: klights_reconcile_api::GcNonPodFinalizationRequest,
    ) -> klights_reconcile_api::GcNonPodFinalizationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone) })
    }
}

pub fn non_pod_finalization_port_for_test()
-> &'static dyn klights_reconcile_api::GcNonPodFinalizationPort {
    static PORT: TestGcNonPodFinalizationPort = TestGcNonPodFinalizationPort;
    &PORT
}

/// Store a resource in the DB and return it with resourceVersion injected,
/// matching how the API server passes resources to reconcile.
pub async fn store_and_prepare(
    db: &crate::datastore::sqlite::Datastore,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: serde_json::Value,
) -> serde_json::Value {
    let created = db
        .create_resource(
            api_version,
            kind,
            namespace.map(String::from).as_deref(),
            name,
            data,
        )
        .await
        .unwrap();
    crate::api::inject_resource_version(created.data, created.resource_version)
}

/// Build a `PodRepository` over the supplied in-memory `Datastore` for use
/// in controller unit tests that exercise `reconcile_deployment` /
/// `reconcile_replicaset` without going through the full dispatcher.
///
/// Mirrors the wiring in `api::test_support::build_test_app_state` —
/// returns the same kind of repository the production dispatcher would
/// hand to these controllers.
pub fn pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(crate::side_effects::SideEffectRegistry::new());
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle,
        supervisor,
        side_effects,
        metrics,
    ))
}

pub(crate) async fn pod_repository_with_node_local_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> (
    Arc<crate::kubelet::pod_repository::PodRepository>,
    Arc<crate::datastore::node_local::NodeLocalStores>,
) {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::kubelet::pod_repository::test_node_local_store(supervisor.clone()).await;
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    let repository = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db_handle,
            pod_workqueue_store: Some(node_local.clone()),
            supervisor,
            side_effects: Arc::new(crate::side_effects::SideEffectRegistry::new()),
            metrics: crate::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::kubelet::pod_repository::test_pod_network_cache(
                node_local.clone(),
            ),
            assignment_waiter: crate::kubelet::pod_repository::test_assignment_bus(),
            scheduling_mode: crate::kubelet::pod_repository::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            scheduler_bind_gate: None,
        },
    )
    .repository;
    (Arc::new(repository), node_local)
}

/// Build the leader/deferred `PodRepository` shape used by multinode controller
/// wiring, where metadata writes go through the outbox before the local store
/// observes them.
pub async fn deferred_outbox_pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(crate::side_effects::SideEffectRegistry::new());
    let outbox = Arc::new(crate::outbox_test_support::test_outbox().await);
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    Arc::new(
        crate::kubelet::pod_repository::PodRepository::new_with_scheduling_mode_and_outbox(
            db_handle,
            supervisor,
            side_effects,
            metrics,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            Some(outbox),
        ),
    )
}
