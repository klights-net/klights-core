pub use klights_kubelet::pod_deletion_finalizer::{
    PodDeletionFinalizer, RealPodDeletionFinalizerDependencies, compose_real_pod_deletion_finalizer,
};

#[cfg(not(test))]
pub use klights_kubelet::pod_deletion_finalizer::RealPodDeletionFinalizer;

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
struct TestNamespaceTerminationSink;

#[cfg(test)]
impl klights_reconcile_api::NamespaceTerminationSink for TestNamespaceTerminationSink {
    fn reconcile_namespace_termination(
        &self,
        _request: klights_reconcile_api::NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::NamespaceTerminationOutcome::Finalized) })
    }
}

#[cfg(test)]
struct TestPodGcReconcileSink;

#[cfg(test)]
impl klights_reconcile_api::PodGcReconcileSink for TestPodGcReconcileSink {
    fn reconcile_owner_references<'a>(
        &'a self,
        _pod: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn cascade_delete_dependents<'a>(
        &'a self,
        _owner: klights_types::PodIdentity,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn finalize_foreground_owners<'a>(
        &'a self,
        _deleted_dependent: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
struct TestPodPdbReconcileSink;

#[cfg(test)]
impl klights_reconcile_api::PodPdbReconcileSink for TestPodPdbReconcileSink {
    fn reconcile_namespace_pdbs(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Root-only test fixture retaining the concrete store so integration tests
/// can seed and inspect rows while production policy remains crate-owned.
#[cfg(test)]
pub(crate) struct RealPodDeletionFinalizer {
    inner: Arc<dyn PodDeletionFinalizer>,
    pub(crate) store: Arc<crate::kubelet::pod_repository::store::PodStore>,
}

#[cfg(test)]
impl RealPodDeletionFinalizer {
    pub(crate) fn new(
        store: Arc<crate::kubelet::pod_repository::store::PodStore>,
        gc_pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
        cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
        side_effects: Arc<klights_controllers::side_effects::SideEffectRegistry>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        let bound_pod_finalization =
            crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::new_for_root(
                store.clone(),
                cluster_api.clone(),
                outbox.clone(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            );
        let mutation_reconcile = Arc::new(
            crate::bootstrap::controller_adapters::pod_reconcile_adapter::PodReconcileAdapter::new(
                store.db().clone(),
                side_effects.controller_dispatcher_slot(),
                metrics.clone(),
                side_effects,
                store.clone(),
                crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            ),
        );
        let inner = compose_real_pod_deletion_finalizer(RealPodDeletionFinalizerDependencies {
            pod_query: store.clone(),
            gc_pod_delete_sink,
            gc_reconcile: Arc::new(TestPodGcReconcileSink),
            pdb_reconcile: Arc::new(TestPodPdbReconcileSink),
            namespace_termination: Arc::new(TestNamespaceTerminationSink),
            cluster_api,
            outbox: outbox.map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>),
            bound_pod_finalization,
            mutation_reconcile,
            metrics,
            supervisor,
        });
        Self { inner, store }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl PodDeletionFinalizer for RealPodDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult> {
        self.inner.finalize_after_actor_cleanup(key).await
    }
}
