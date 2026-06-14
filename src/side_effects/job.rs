//! Side effect to reconcile Jobs after Pod mutations.

use super::{ControllerDispatcherSlot, PodRepositorySlot, SideEffect};
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Reconciles namespace Jobs after Pod create/update/delete events.
///
/// Job ownership is driven by Pod state as well as Job spec. A direct Pod
/// update can orphan or relabel a Pod, so the Job controller must run from the
/// Pod mutation path instead of waiting for another Job update.
pub struct JobReconcileEffect {
    _pod_repository: PodRepositorySlot,
    controller_dispatcher: ControllerDispatcherSlot,
}

#[async_trait]
impl SideEffect for JobReconcileEffect {
    fn name(&self) -> &'static str {
        "job_reconcile"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
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

        let keys = job_reconcile_keys_for_pod(resource, db, namespace).await?;
        for key in keys {
            dispatcher.enqueue_reconcile_key(key).await;
        }

        Ok(())
    }
}

pub async fn job_reconcile_keys_for_pod(
    pod: &Value,
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<Vec<crate::controllers::workqueue::ReconcileKey>> {
    let mut keys = Vec::new();
    if let Some(owner_refs) = pod
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
    {
        for owner in owner_refs {
            let is_job = owner
                .get("kind")
                .and_then(|v| v.as_str())
                .is_some_and(|kind| kind == "Job")
                && owner
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .map(|api_version| api_version == "batch/v1")
                    .unwrap_or(true);
            if is_job && let Some(name) = owner.get("name").and_then(|v| v.as_str()) {
                keys.push(crate::controllers::workqueue::ReconcileKey::namespaced(
                    "batch/v1", "Job", namespace, name,
                ));
            }
        }
        if !keys.is_empty() {
            return Ok(keys);
        }
    }

    let pod_labels = pod
        .pointer("/metadata/labels")
        .and_then(|labels| labels.as_object());
    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for job in jobs.items {
        let selector_matches = job_selector_for_pod_side_effect(&job.data)
            .map(|selector| selector.matches_labels(pod_labels))
            .unwrap_or(false);
        if selector_matches {
            keys.push(crate::controllers::workqueue::ReconcileKey::namespaced(
                "batch/v1", "Job", namespace, &job.name,
            ));
        }
    }
    Ok(keys)
}

fn job_selector_for_pod_side_effect(job: &Value) -> Option<crate::label_selector::LabelSelector> {
    let selector = if let Some(selector) = job.pointer("/spec/selector") {
        selector.clone()
    } else {
        let labels = job
            .pointer("/spec/template/metadata/labels")
            .and_then(|v| v.as_object())?;
        if labels.is_empty() {
            return None;
        }
        serde_json::json!({ "matchLabels": labels })
    };
    crate::label_selector::LabelSelector::from_k8s_selector(&selector).ok()
}

/// Create a JobReconcileEffect instance backed by the supplied late-bound
/// `PodRepository` slot.
pub fn job_reconcile(
    pod_repository: PodRepositorySlot,
    controller_dispatcher: ControllerDispatcherSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(JobReconcileEffect {
        _pod_repository: pod_repository,
        controller_dispatcher,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_job_reconcile_name() {
        let effect = job_reconcile(PodRepositorySlot::new(), ControllerDispatcherSlot::new());
        assert_eq!(effect.name(), "job_reconcile");
    }
}
