use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use crate::side_effects::job::{JobSideEffectStore, job_reconcile_keys_for_pod};
use crate::side_effects::{ControllerDispatcherSlot, SideEffect};

/// Reconciles namespace Jobs after Pod create/update/delete events.
///
/// Job ownership is driven by Pod state as well as Job spec. A direct Pod
/// update can orphan or relabel a Pod, so the Job controller must run from the
/// Pod mutation path instead of waiting for another Job update.
struct JobReconcileEffect {
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for JobReconcileEffect {
    fn name(&self) -> &'static str {
        "job_reconcile"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if namespace.is_empty() {
            return Ok(());
        }

        let Some(dispatcher) = self.controller_dispatcher.get() else {
            tracing::debug!(
                "JobReconcileEffect skipped for {}: controller dispatcher not yet bound",
                namespace
            );
            return Ok(());
        };

        dispatcher
            .enqueue_reconcile_batch(
                job_reconcile_keys_for_pod(resource, self.db.as_ref(), namespace).await?,
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<T> JobSideEffectStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources("batch/v1", "Job", Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(JobReconcileEffect {
        db,
        controller_dispatcher,
    })
}
