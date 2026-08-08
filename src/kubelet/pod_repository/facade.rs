//! PodRepository facade types -- build parts and the isolated service
//! traits extracted from the monolithic repository.

use std::sync::Arc;

use klights_kubelet::pod_repository::background::PodRepositoryBackground;

/// Focused capability for binding a Pod lifecycle router to whichever
/// repository owns workqueue dispatch. Composition-root callers use this
/// narrow trait object instead of holding the full `PodRepository`
/// aggregate just to reach `set_pod_lifecycle_router_for_node`.
pub(crate) trait PodLifecycleRouterBinding: Send + Sync {
    fn bind_pod_lifecycle_router(
        &self,
        router: Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        local_node_name: String,
    );
}

impl PodLifecycleRouterBinding for super::PodRepository {
    fn bind_pod_lifecycle_router(
        &self,
        router: Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        local_node_name: String,
    ) {
        self.set_pod_lifecycle_router_for_node(router, local_node_name);
    }
}

/// Returned by `PodRepository::build_parts`. Separates the repository from
/// services that require explicit startup so construction is side-effect-free.
pub struct PodRepositoryParts {
    pub repository: super::PodRepository,
    pub background: PodRepositoryBackground,
    deletion_finalizer_dependencies: super::PodDeletionFinalizerDependencies,
}

impl PodRepositoryParts {
    pub(super) fn new(
        repository: super::PodRepository,
        background: PodRepositoryBackground,
        deletion_finalizer_dependencies: super::PodDeletionFinalizerDependencies,
    ) -> Self {
        Self {
            repository,
            background,
            deletion_finalizer_dependencies,
        }
    }

    pub(crate) fn into_pod_subsystem_parts(
        self,
    ) -> (
        super::PodRepository,
        PodRepositoryBackground,
        std::sync::Arc<dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer>,
    ) {
        let finalizer = super::compose_pod_deletion_finalizer(self.deletion_finalizer_dependencies);
        (self.repository, self.background, finalizer)
    }
}
