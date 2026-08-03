use super::*;
use klights_reconcile_api::compute_statefulset_update_revision;
use serde_json::json;

async fn is_pod_ready(
    pod_query: &(impl klights_pod_api::PodQuery + ?Sized),
    namespace: &str,
    pod_name: &str,
) -> anyhow::Result<bool> {
    let request = klights_pod_api::PodGetRequest::try_by_name(namespace, pod_name)?;
    Ok(pod_query.get_pod(request).await?.is_some_and(|pod| {
        klights_controllers::common::controller_common().is_pod_ready(&pod.data)
    }))
}

/// Test-only shim that mirrors the public `reconcile_statefulset` signature
/// before the Task 18 migration. Builds a `PodRepository` over the supplied
/// in-memory `Datastore` so existing tests don't need to plumb the trait
/// objects themselves.
async fn reconcile_statefulset_test(
    db: &crate::datastore::sqlite::Datastore,
    statefulset: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()> {
    let repo = crate::controller_test_support::pod_repository_for_test(db);
    let store = crate::controller_test_support::controller_store_for_test(db);
    super::reconcile_statefulset(
        &store,
        repo.as_ref(),
        repo.as_ref(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        repo.as_ref(),
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        statefulset,
        crate::controller_test_support::test_reconcile_context(
            &klights_controllers::ControllerCoordination::new(),
            node_name,
        ),
    )
    .await
}

mod deletion_and_status_tests;
mod ordinal_and_revision_tests;
mod reconcile_core_tests;
mod update_strategy_tests;
