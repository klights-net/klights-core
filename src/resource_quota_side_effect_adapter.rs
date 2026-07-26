use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::side_effects::resource_quota::{
    ResourceQuotaSideEffectPort, apply_resource_quota_event,
};
use crate::side_effects::{PodSideEffectPortsSlot, SideEffect};
use klights_pod_api::PodQuery;

/// Recounts ResourceQuota status.used after any namespaced resource mutation.
///
/// The late-bound repository is resolved for every event so construction
/// order remains independent and pod-scoped counts always use `PodReader`.
struct ResourceQuotaEffect {
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundResourceQuotaPort<'a> {
    db: &'a dyn DatastoreBackend,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl ResourceQuotaSideEffectPort for BoundResourceQuotaPort<'_> {
    async fn recount_namespace(&self, namespace: &str) -> Result<()> {
        crate::resource_quota_controller_adapter::reconcile_resource_quotas_for_namespace(
            self.db,
            self.pod_query,
            namespace,
        )
        .await
    }
}

#[async_trait]
impl SideEffect for ResourceQuotaEffect {
    fn name(&self) -> &'static str {
        "resource_quota_recount"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let Some(pod_query) = self.pod_repository.query() else {
            let namespace = resource
                .pointer("/metadata/namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !namespace.is_empty() {
                tracing::debug!(
                    "ResourceQuotaEffect skipped for {}: PodRepository not yet bound",
                    namespace
                );
            }
            return Ok(());
        };
        apply_resource_quota_event(
            resource,
            &BoundResourceQuotaPort {
                db: self.db.as_ref(),
                pod_query: pod_query.as_ref(),
            },
        )
        .await
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(ResourceQuotaEffect { db, pod_repository })
}
