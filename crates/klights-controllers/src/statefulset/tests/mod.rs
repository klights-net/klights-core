use super::*;
use klights_reconcile_api::compute_statefulset_update_revision;
use serde_json::json;

async fn is_pod_ready(
    pod_query: &(impl klights_pod_api::PodQuery + ?Sized),
    namespace: &str,
    pod_name: &str,
) -> anyhow::Result<bool> {
    let request = klights_pod_api::PodGetRequest::try_by_name(namespace, pod_name)?;
    Ok(pod_query
        .get_pod(request)
        .await?
        .is_some_and(|pod| crate::common::controller_common().is_pod_ready(&pod.data)))
}

/// Private focused-port harness for controller-owned policy regressions.
async fn reconcile_statefulset_test(
    db: &crate::test_support::TestStore,
    statefulset: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()> {
    super::reconcile_statefulset(
        db,
        db,
        db,
        crate::test_support::deterministic_controller_identity().as_ref(),
        db,
        db,
        statefulset,
        crate::test_support::test_reconcile_context(
            &crate::ControllerCoordination::new(),
            node_name,
        ),
    )
    .await
}

mod deletion_and_status_tests;
mod ordinal_and_revision_tests;
mod reconcile_core_tests;
mod update_strategy_tests;
