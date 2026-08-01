use serde_json::json;

fn coordination() -> &'static klights_controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<klights_controllers::ControllerCoordination> =
        std::sync::LazyLock::new(klights_controllers::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_replicaset<T>(
    db: &T,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    pod_writer: &dyn crate::kubelet::pod_repository::PodObjectWriter,
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    replicaset: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()>
where
    T: crate::datastore::DatastoreBackend + Clone + 'static,
{
    let non_pod_finalization =
        crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(std::sync::Arc::new(db.clone()));
    let store = crate::controller_runtime_adapter::RootControllerLeaderPort::new(
        std::sync::Arc::new(db.clone()),
    );
    super::reconcile_replicaset(
        &store,
        pod_reader,
        pod_writer,
        crate::controllers::test_utils::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        &non_pod_finalization,
        replicaset,
        crate::controllers::test_reconcile_context(coordination(), node_name),
    )
    .await
}

mod adoption_and_ownerref_tests;
mod reconcile_scale_tests;
