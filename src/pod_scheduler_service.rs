//! Focused controller-facing scheduling capability.

use std::sync::Arc;

use klights_pod_api::{PodRepositoryError, PodScheduling, PodSchedulingFuture};

pub(crate) struct PodSchedulerService {
    orchestration: Arc<crate::pod_native_orchestration::PodNativeOrchestration>,
}

impl PodSchedulerService {
    pub(crate) fn new(
        orchestration: Arc<crate::pod_native_orchestration::PodNativeOrchestration>,
    ) -> Arc<Self> {
        Arc::new(Self { orchestration })
    }
}

fn scheduling_error(error: crate::api::AppError) -> PodRepositoryError {
    PodRepositoryError::unavailable(format!("Pod scheduling failed: {error:?}"))
}

impl PodScheduling for PodSchedulerService {
    fn schedule_all_unbound_pods(&self) -> PodSchedulingFuture<'_, ()> {
        Box::pin(async move {
            self.orchestration
                .schedule_all_unbound_pods()
                .await
                .map_err(scheduling_error)
        })
    }

    fn schedule_pending_pod(
        &self,
        namespace: String,
        name: String,
    ) -> PodSchedulingFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.orchestration
                .schedule_pending_pod(&namespace, &name)
                .await
                .map_err(scheduling_error)
        })
    }
}
