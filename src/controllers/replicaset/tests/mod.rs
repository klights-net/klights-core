use super::*;

fn coordination() -> &'static crate::controllers::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<crate::controllers::ControllerCoordination> =
        std::sync::LazyLock::new(crate::controllers::ControllerCoordination::new);
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
    super::reconcile_replicaset(
        db,
        pod_reader,
        pod_writer,
        pod_delete_sink,
        &non_pod_finalization,
        coordination(),
        replicaset,
        node_name,
    )
    .await
}

mod adoption_and_ownerref_tests;
mod reconcile_scale_tests;
