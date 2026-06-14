use crate::kubelet::pod_repository::PodReader;
use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Compute a deterministic pod-template-hash from the pod template spec.
/// K8s uses this as the RS name suffix, and adds it to RS labels, RS selector,
/// and RS template labels. We use SHA256 truncated to 10 hex chars (same approach
/// as DaemonSet controller).
pub fn compute_pod_template_hash(template: &Value) -> String {
    let template_str = serde_json::to_string(template).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(template_str.as_bytes());
    let result = hasher.finalize();
    let hex_string = result
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    hex_string[..10].to_string()
}

/// Compare pod templates ignoring the injected `pod-template-hash` label.
/// K8s injects this label into RS template labels after creation, so we must
/// strip it before comparing against the deployment's original template.
pub fn templates_match(template1: &Value, template2: &Value) -> bool {
    let strip_hash = |t: &Value| -> Value {
        let mut t = t.clone();
        if let Some(labels) = t
            .pointer_mut("/metadata/labels")
            .and_then(|l| l.as_object_mut())
        {
            labels.remove("pod-template-hash");
        }
        t
    };
    strip_hash(template1) == strip_hash(template2)
}

pub(super) fn labels_match_selector(
    selector: &Value,
    labels: &serde_json::Map<String, Value>,
) -> bool {
    // Route through the canonical LabelSelector helper so Deployment shares
    // matchExpressions semantics with every other workload controller.
    // A malformed selector (unknown operator, etc.) is treated as
    // match-none so a broken Deployment cannot accidentally adopt every
    // pod in the namespace.
    match crate::label_selector::LabelSelector::from_k8s_selector(selector) {
        Ok(s) => s.matches_labels(Some(labels)),
        Err(_) => false,
    }
}

pub(super) fn get_max_surge(spec: &Value, desired_replicas: i64) -> i64 {
    let strategy = spec.get("strategy");
    if let Some(rolling_update) = strategy.and_then(|s| s.get("rollingUpdate"))
        && let Some(max_surge) = rolling_update.get("maxSurge")
    {
        // Handle both integer and percentage (e.g., "25%")
        if let Some(n) = max_surge.as_i64() {
            return n;
        } else if let Some(s) = max_surge.as_str()
            && let Some(percent_str) = s.strip_suffix('%')
            && let Ok(percent) = percent_str.parse::<i64>()
        {
            return (desired_replicas * percent + 99) / 100; // ceiling division
        }
    }
    // Default: 25% of desired replicas (ceiling)
    (desired_replicas * 25 + 99) / 100
}

pub(super) fn get_max_unavailable(spec: &Value, desired_replicas: i64) -> i64 {
    let strategy = spec.get("strategy");
    if let Some(rolling_update) = strategy.and_then(|s| s.get("rollingUpdate"))
        && let Some(max_unavailable) = rolling_update.get("maxUnavailable")
    {
        // Handle both integer and percentage
        if let Some(n) = max_unavailable.as_i64() {
            return n;
        } else if let Some(s) = max_unavailable.as_str()
            && let Some(percent_str) = s.strip_suffix('%')
            && let Ok(percent) = percent_str.parse::<i64>()
        {
            return (desired_replicas * percent) / 100; // floor division
        }
    }
    // Default: 25% of desired replicas (floor)
    (desired_replicas * 25) / 100
}

pub(super) fn get_next_revision(owned_rs_list: &[crate::datastore::Resource]) -> i64 {
    let mut max_revision = 0i64;
    for rs in owned_rs_list {
        if let Some(annotations) = rs
            .data
            .get("metadata")
            .and_then(|m: &Value| m.get("annotations"))
            .and_then(|a: &Value| a.as_object())
            && let Some(rev_value) = annotations.get("deployment.kubernetes.io/revision")
            && let Some(rev_str) = rev_value.as_str()
            && let Ok(rev) = rev_str.parse::<i64>()
        {
            max_revision = max_revision.max(rev);
        }
    }
    max_revision + 1
}

/// Count pods owned by deployment (via its ReplicaSets)
/// Returns (total_pods, ready_pods, updated_pods, available_pods)
pub(super) async fn count_deployment_pods(
    pod_reader: &dyn PodReader,
    namespace: &str,
    owned_rs_list: &[crate::datastore::Resource],
    current_template: &Value,
) -> Result<(i64, i64, i64, i64)> {
    let common = crate::controllers::common::controller_common();
    let mut seen_pod_uids = std::collections::HashSet::new();
    let mut total_pods = 0i64;
    let mut ready_pods = 0i64;
    let mut updated_pods = 0i64;
    let mut available_pods = 0i64;

    for rs in owned_rs_list {
        let rs_uid = rs
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .ok_or_else(|| anyhow::anyhow!("RS missing uid"))?;

        let rs_template = rs.data.get("spec").and_then(|s| s.get("template"));

        // Determine if this RS matches the current template (ignoring pod-template-hash)
        let matches_current = rs_template
            .map(|t| templates_match(t, current_template))
            .unwrap_or(false);

        // Primary path: owner-reference fetch must match the RS UID at any
        // ownerReferences position.
        let mut owned_pods = pod_reader.list_pods_by_owner_uid(namespace, rs_uid).await?;
        // Fallback for conformance: if owner refs are temporarily absent on a
        // pod, count by RS selector labels so Deployment availability does not
        // collapse to zero during rollout math.
        if owned_pods.is_empty()
            && let Some(selector_obj) = rs.data.get("spec").and_then(|s| s.get("selector"))
        {
            let all_ns_pods = pod_reader
                .list_pods(Some(namespace), None, None, None, None)
                .await?;
            owned_pods = all_ns_pods
                .items
                .into_iter()
                .filter(|pod| {
                    pod.data
                        .pointer("/metadata/labels")
                        .and_then(|v| v.as_object())
                        .is_some_and(|labels| labels_match_selector(selector_obj, labels))
                })
                .collect();
        }

        for pod_resource in owned_pods {
            if !pod_is_active(&pod_resource.data) {
                continue;
            }
            let pod_uid = pod_resource
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !pod_uid.is_empty() && !seen_pod_uids.insert(pod_uid.to_string()) {
                continue;
            }
            total_pods += 1;
            if common.is_pod_ready(&pod_resource.data) {
                ready_pods += 1;
                available_pods += 1;
            }
            if matches_current {
                updated_pods += 1;
            }
        }
    }

    Ok((total_pods, ready_pods, updated_pods, available_pods))
}

fn pod_is_active(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp").is_none()
        && !matches!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Succeeded" | "Failed")
        )
}
