//! Side effect to recount ResourceQuota after namespaced resource mutations.

use super::{PodRepositorySlot, SideEffect};
use crate::controllers::resource_quota;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Recounts ResourceQuota status.used after any namespaced resource mutation.
///
/// Holds a [`PodRepositorySlot`] so the late-bound `PodRepository` is
/// resolved at `apply` time — pod-scoped quota counts (cpu/memory sums,
/// scoped pod counts) go through `PodReader::list_pods`.
pub struct ResourceQuotaEffect {
    pod_repository: PodRepositorySlot,
}

#[async_trait]
impl SideEffect for ResourceQuotaEffect {
    fn name(&self) -> &'static str {
        "resource_quota_recount"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if namespace.is_empty() {
            return Ok(());
        }

        let kind = resource
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let name = resource
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let Some(pod_repository) = self.pod_repository.get() else {
            tracing::debug!(
                "ResourceQuotaEffect skipped for {}: PodRepository not yet bound",
                namespace
            );
            return Ok(());
        };

        tracing::info!(
            kind = %kind,
            name = %name,
            namespace = %namespace,
            "ResourceQuotaEffect firing"
        );

        resource_quota::reconcile_resource_quotas_for_namespace(
            db,
            pod_repository.as_ref(),
            namespace,
        )
        .await
    }
}

/// Create a ResourceQuotaEffect instance backed by the supplied late-bound
/// `PodRepository` slot.
pub fn resource_quota_recount(pod_repository: PodRepositorySlot) -> Arc<dyn SideEffect> {
    Arc::new(ResourceQuotaEffect { pod_repository })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resource_quota_recount_name() {
        let effect = resource_quota_recount(PodRepositorySlot::new());
        assert_eq!(effect.name(), "resource_quota_recount");
    }
}
