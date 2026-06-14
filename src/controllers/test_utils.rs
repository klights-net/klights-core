//! Shared test utilities for controller wrapper tests.

use std::sync::Arc;

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
    let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
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

/// Build the leader/deferred `PodRepository` shape used by multinode controller
/// wiring, where metadata writes go through the outbox before the local store
/// observes them.
pub async fn deferred_outbox_pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(crate::side_effects::SideEffectRegistry::new());
    let outbox = Arc::new(crate::kubelet::outbox::Outbox::test_outbox().await);
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    Arc::new(
        crate::kubelet::pod_repository::PodRepository::new_with_scheduling_mode_and_outbox(
            db_handle,
            supervisor,
            side_effects,
            metrics,
            crate::kubelet::pod_repository::api::PodSchedulingMode::DeferredMultiNodeLeader,
            Some(outbox),
        ),
    )
}
