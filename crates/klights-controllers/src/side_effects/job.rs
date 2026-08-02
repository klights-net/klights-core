//! Side effect to reconcile Jobs after Pod mutations.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;

#[async_trait]
pub trait JobSideEffectStore: Send + Sync {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>>;
}

pub async fn job_reconcile_keys_for_pod<Store: JobSideEffectStore + ?Sized>(
    pod: &Value,
    store: &Store,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
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
                keys.push(ReconcileKey::namespaced("batch/v1", "Job", namespace, name));
            }
        }
        if !keys.is_empty() {
            return Ok(keys);
        }
    }

    let pod_labels = pod
        .pointer("/metadata/labels")
        .and_then(|labels| labels.as_object());
    for job in store.list_jobs(namespace).await? {
        let selector_matches = job_selector_for_pod_side_effect(&job.data)
            .map(|selector| selector.matches_labels(pod_labels))
            .unwrap_or(false);
        if selector_matches {
            keys.push(ReconcileKey::namespaced(
                "batch/v1", "Job", namespace, &job.name,
            ));
        }
    }
    Ok(keys)
}

fn job_selector_for_pod_side_effect(job: &Value) -> Option<klights_types::LabelSelector> {
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
    klights_types::LabelSelector::from_k8s_selector(&selector).ok()
}
