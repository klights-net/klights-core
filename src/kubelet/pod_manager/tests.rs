use super::*;

mod tests_misc;
mod tests_mounts_and_create;
mod tests_phase_and_recovery;

#[cfg(test)]
pub(crate) struct PodManagerTestPorts {
    pub(crate) pod_query: std::sync::Arc<dyn klights_pod_api::PodQuery>,
    pub(crate) pod_status_writer:
        std::sync::Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    pub(crate) pod_workqueue:
        std::sync::Arc<klights_kubelet::pod_repository::workqueue::PodWorkqueue>,
    pub(crate) mutation_reconcile:
        std::sync::Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
}

pub(super) fn kubelet_runtime_paths_for_test(
    namespace: &str,
) -> klights_kubelet::runtime_paths::KubeletRuntimePaths {
    klights_kubelet::runtime_paths::KubeletRuntimePaths::new(crate::paths::test_data_root_path(
        namespace,
    ))
    .expect("kubelet test runtime path must be absolute")
}

pub(super) fn fixture_pod_repository(
    db: &crate::datastore::sqlite::Datastore,
) -> PodManagerTestPorts {
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let side_effects = std::sync::Arc::new(crate::bootstrap::side_effects::default_registry(
        klights_controllers::side_effects::SideEffectMetrics::new(),
        None,
        None,
        Some(std::sync::Arc::new(db.clone())),
        crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
    ));
    let (
        pod_query,
        _pod_snapshot,
        _pod_update,
        pod_status_writer,
        pod_workqueue,
        _pod_network_assignment,
        _pod_host_ip,
        _background,
        _deletion_finalizer,
        _dirty_counter,
        mutation_reconcile,
        gc_delete,
        _eviction_admission,
        _namespace_bootstrap,
        _namespace_termination_queue,
        _pod_api,
        _pod_subresource,
        _pod_scheduling,
        _watch_source,
        _bound_finalization,
        _deferred_runtime,
        _test_api,
        _test_subresource,
    ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
        crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
            db: std::sync::Arc::new(db.clone()),
            pod_workqueue_store: None,
            supervisor,
            side_effects: side_effects.clone(),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::bootstrap::pod_repository_composition::empty_test_pod_network_cache(),
            assignment_waiter: crate::bootstrap::pod_repository_composition::test_assignment_bus(),
            scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            remote_delivery_required: false,
            controller_identity:
                crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            scheduler_bind_gate: None,
        },
        None,
    );
    side_effects.set_pod_ports(pod_query.clone(), gc_delete);
    PodManagerTestPorts {
        pod_query,
        pod_status_writer,
        pod_workqueue,
        mutation_reconcile,
    }
}

/// Canonical builder for the small set of pod_manager tests that must
/// observe controller-dispatcher enqueue side effects directly (bypassing
/// `crate::bootstrap::side_effects::default_registry`'s broader wiring, which
/// would pull in unrelated side-effect ports). This is the single place in
/// the pod_manager test tree that names the concrete root repository type
/// for a dispatcher-bound fixture; callers receive only the `Arc` handle and
/// never construct it inline themselves.
pub(super) fn fixture_pod_repository_with_dispatcher<T>(
    db_handle: crate::datastore::DatastoreHandle,
    dispatcher: std::sync::Arc<T>,
) -> PodManagerTestPorts
where
    T: klights_reconcile_api::ControllerReconcileSink
        + klights_reconcile_api::ServiceReconcileSink
        + 'static,
{
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let side_effects =
        std::sync::Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
    side_effects.set_controller_dispatcher(dispatcher);
    let (
        pod_query,
        _pod_snapshot,
        _pod_update,
        pod_status_writer,
        pod_workqueue,
        _pod_network_assignment,
        _pod_host_ip,
        _background,
        _deletion_finalizer,
        _dirty_counter,
        mutation_reconcile,
        _gc_delete,
        _eviction_admission,
        _namespace_bootstrap,
        _namespace_termination_queue,
        _pod_api,
        _pod_subresource,
        _pod_scheduling,
        _watch_source,
        _bound_finalization,
        _deferred_runtime,
        _test_api,
        _test_subresource,
    ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
        crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
            db: db_handle,
            pod_workqueue_store: None,
            supervisor,
            side_effects,
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::bootstrap::pod_repository_composition::empty_test_pod_network_cache(),
            assignment_waiter: crate::bootstrap::pod_repository_composition::test_assignment_bus(),
            scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            remote_delivery_required: false,
            controller_identity:
                crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            scheduler_bind_gate: None,
        },
        None,
    );
    PodManagerTestPorts {
        pod_query,
        pod_status_writer,
        pod_workqueue,
        mutation_reconcile,
    }
}

pub(super) fn pod_query_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> std::sync::Arc<dyn klights_pod_api::PodQuery> {
    crate::bootstrap::pod_repository_composition::pod_query_for_test(db)
}
