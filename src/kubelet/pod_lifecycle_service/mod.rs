//! `PodLifecycleService` — facade around `PodLifecycleRouter`.
//!
//! This wrapper provides the public API surface for pod lifecycle routing
//! without exposing the router's internal backend selection. It is the
//! single entry point that bootstrap, API handlers, and debug endpoints
//! use to interact with pod lifecycles.

use std::sync::Arc;

use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor;
use crate::kubelet::pod_lifecycle_router::{
    PodLifecycleDiagnostics, PodLifecycleRouteError, PodLifecycleRouteMode, PodLifecycleRouter,
};

/// Facade around `PodLifecycleRouter` that provides the public API surface
/// for pod lifecycle routing.
pub struct PodLifecycleService {
    router: Arc<PodLifecycleRouter>,
}

impl PodLifecycleService {
    pub fn new(router: Arc<PodLifecycleRouter>) -> Self {
        Self { router }
    }

    /// Route a lifecycle message to the selected backend.
    pub async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
        self.router.route(message).await
    }

    /// Fire-and-forget nonblocking route.
    pub fn try_route_nonblocking(&self, message: LifecycleMessage) {
        self.router.try_route_nonblocking(message);
    }

    /// Remove per-pod state for a deleted pod.
    pub async fn remove_pod_state(&self, key: &PodLifecycleKey) -> bool {
        self.router.remove_pod_state(key).await
    }

    /// Replace the work executor at runtime.
    pub fn set_work_executor(&self, executor: Arc<dyn PodWorkExecutor>) {
        self.router.set_work_executor(executor);
    }

    /// Diagnostics for the debug endpoint.
    pub async fn diagnostics(&self) -> PodLifecycleDiagnostics {
        self.router.diagnostics().await
    }

    /// Active pod lifecycle actor count.
    pub async fn active_pod_count(&self) -> usize {
        self.router.active_pod_count().await
    }

    /// Selected mode for diagnostics.
    pub fn mode(&self) -> PodLifecycleRouteMode {
        self.router.mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
    use crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
    use crate::kubelet::pod_lifecycle_router::executor::NoopExecutor;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn test_supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn test_registry() -> Arc<PodLifecycleRegistry> {
        let holder = Arc::new(std::sync::Mutex::new(
            Arc::new(NoopExecutor) as Arc<dyn PodWorkExecutor>
        ));
        Arc::new(PodLifecycleRegistry::new(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            holder,
        ))
    }

    #[tokio::test]
    async fn pod_lifecycle_service_wraps_router_without_changing_mode() {
        let registry = test_registry();
        let router = Arc::new(PodLifecycleRouter::new_actor(registry.clone()));
        let service = PodLifecycleService::new(router.clone());

        // Mode must match between service and router.
        assert_eq!(service.mode(), PodLifecycleRouteMode::Actor);
        assert_eq!(service.mode(), router.mode());

        // Route a message through the service facade.
        let key = PodLifecycleKey::new("default", "svc-pod", "uid-svc");
        service
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await
            .unwrap();

        // Active pod count must reflect the routed message.
        assert_eq!(service.active_pod_count().await, 1);

        // Diagnostics must show Actor mode and the routed pod.
        let diag = service.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Actor);
        assert_eq!(diag.active_pod_count, 1);

        // remove_pod_state must work through the service.
        assert!(service.remove_pod_state(&key).await);
        assert_eq!(service.active_pod_count().await, 0);
    }
}
