//! `Controller` impl for `HorizontalPodAutoscaler`. Registered in `ControllerDispatcher`.

use crate::controller::{Context, Controller};
use crate::controllers::hpa as hpa_core;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

pub struct HpaController;

#[async_trait]
impl Controller for HpaController {
    fn name(&self) -> &'static str {
        "horizontalpodautoscaler"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> Result<()> {
        let pod_repository = ctx.pod_repository().ok_or_else(|| {
            anyhow::anyhow!(
                "horizontalpodautoscaler requires pod_repository in Context — wire it via \
                 ControllerDispatcher::set_pod_repository or Context::with_pod_repository"
            )
        })?;
        let fallback_metrics;
        let metrics_provider = match ctx.metrics_provider() {
            Some(provider) => provider.as_ref(),
            None => {
                fallback_metrics = crate::metrics::FallbackOnlyMetricsProvider;
                &fallback_metrics as &dyn crate::metrics::MetricsProvider
            }
        };
        hpa_core::reconcile_hpa_with_metrics(
            ctx.db_handle().as_ref(),
            pod_repository.as_ref(),
            &resource,
            ctx.node_name(),
            metrics_provider,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hpa_controller_name() {
        assert_eq!(HpaController.name(), "horizontalpodautoscaler");
    }
}
