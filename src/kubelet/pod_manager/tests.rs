use super::*;

mod tests_misc;
mod tests_mounts_and_create;
mod tests_phase_and_recovery;

/// Build a `PodRepository` Arc directly from an existing `Datastore` for the
/// pod_manager test files that previously called migrated helpers
/// (`apply_pod_phase_update`, `mark_pod_failed`, `mark_pod_start_pending_for_retry`)
/// with only a raw datastore.
pub(super) fn fixture_pod_repository(
    db: &crate::datastore::sqlite::Datastore,
) -> std::sync::Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        None,
        Some(supervisor.clone()),
        Some(db_handle.clone()),
    ));
    let repo = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle,
        supervisor,
        side_effects.clone(),
        metrics,
    ));
    side_effects.set_pod_repository(repo.clone());
    repo
}
