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

fn scheduling_error(
    error: crate::api::AppError,
    namespace: &str,
    name: &str,
) -> PodRepositoryError {
    crate::pod_native_orchestration::map_api_error_to_pod_repository(error, namespace, name)
}

impl PodScheduling for PodSchedulerService {
    fn schedule_all_unbound_pods(&self) -> PodSchedulingFuture<'_, ()> {
        Box::pin(async move {
            self.orchestration
                .schedule_all_unbound_pods()
                .await
                .map_err(|error| scheduling_error(error, "", ""))
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
                .map_err(|error| scheduling_error(error, &namespace, &name))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::scheduling_error;
    use crate::api::AppError;
    use klights_pod_api::PodRepositoryError;

    #[test]
    fn scheduling_boundary_preserves_kubernetes_error_categories() {
        let cases = [
            (AppError::NotFound("missing".to_string()), "NotFound"),
            (AppError::Conflict("stale".to_string()), "Conflict"),
            (AppError::Forbidden("denied".to_string()), "Forbidden"),
        ];

        for (error, expected) in cases {
            let mapped = scheduling_error(error, "default", "pod-a");
            let actual = match mapped {
                PodRepositoryError::NotFound { .. } => "NotFound",
                PodRepositoryError::Conflict { .. } => "Conflict",
                PodRepositoryError::Forbidden { .. } => "Forbidden",
                PodRepositoryError::Unavailable { .. } => "Unavailable",
                other => panic!("unexpected scheduling error category: {other:?}"),
            };
            assert_eq!(actual, expected);
        }
    }
}
