use serde_json::json;

fn coordination() -> &'static crate::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<crate::ControllerCoordination> =
        std::sync::LazyLock::new(crate::ControllerCoordination::new);
    &COORDINATION
}

async fn reconcile_replicaset(
    db: &crate::test_support::TestStore,
    pod_reader: &(impl klights_pod_api::PodQuery + ?Sized),
    pod_writer: &(impl crate::replicaset::ReplicaSetPodMutation + ?Sized),
    pod_delete_sink: &dyn klights_reconcile_api::GcPodDeleteSink,
    replicaset: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()> {
    super::reconcile_replicaset(
        db,
        pod_reader,
        pod_writer,
        crate::test_support::deterministic_controller_identity().as_ref(),
        pod_delete_sink,
        db,
        replicaset,
        crate::test_support::test_reconcile_context(coordination(), node_name),
    )
    .await
}

mod adoption_and_ownerref_tests;
mod reconcile_scale_tests;
