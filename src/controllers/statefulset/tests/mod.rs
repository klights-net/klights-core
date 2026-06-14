use super::*;

/// Test-only shim that mirrors the public `reconcile_statefulset` signature
/// before the Task 18 migration. Builds a `PodRepository` over the supplied
/// in-memory `Datastore` so existing tests don't need to plumb the trait
/// objects themselves.
async fn reconcile_statefulset_test(
    db: &crate::datastore::sqlite::Datastore,
    statefulset: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    super::reconcile_statefulset(
        db,
        repo.as_ref(),
        repo.as_ref(),
        repo.as_ref(),
        statefulset,
        node_name,
    )
    .await
}

mod deletion_and_status_tests;
mod ordinal_and_revision_tests;
mod reconcile_core_tests;
mod update_strategy_tests;
