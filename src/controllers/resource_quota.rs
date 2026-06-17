//! ResourceQuota controller — updates status.used counts by counting live resources.
//!
//! K8s conformance tests create a ResourceQuota and then create/delete resources,
//! expecting status.used to reflect the current count. This reconciler scans all
//! ResourceQuotas in a namespace and updates their status.used fields.

use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodReader;
use anyhow::Result;
use serde_json::{Value, json};

/// Map from K8s quota resource name to (apiVersion, kind) for counting.
/// Only covers the resources tracked in spec.hard that we actually serve.
fn quota_resource_to_kind(resource_name: &str) -> Option<(&'static str, &'static str)> {
    match resource_name {
        "pods" => Some(("v1", "Pod")),
        "secrets" => Some(("v1", "Secret")),
        "configmaps" => Some(("v1", "ConfigMap")),
        "persistentvolumeclaims" => Some(("v1", "PersistentVolumeClaim")),
        "services" => Some(("v1", "Service")),
        "replicationcontrollers" => Some(("v1", "ReplicationController")),
        "resourcequotas" => Some(("v1", "ResourceQuota")),
        "endpoints" => Some(("v1", "Endpoints")),
        "serviceaccounts" => Some(("v1", "ServiceAccount")),
        _ => None,
    }
}

/// Map from plural resource name to kind, for `count/` prefix quota key parsing.
fn plural_to_kind(plural: &str) -> Option<&'static str> {
    match plural {
        "pods" => Some("Pod"),
        "secrets" => Some("Secret"),
        "configmaps" => Some("ConfigMap"),
        "persistentvolumeclaims" => Some("PersistentVolumeClaim"),
        "services" => Some("Service"),
        "replicationcontrollers" => Some("ReplicationController"),
        "resourcequotas" => Some("ResourceQuota"),
        "endpoints" => Some("Endpoints"),
        "serviceaccounts" => Some("ServiceAccount"),
        "namespaces" => Some("Namespace"),
        "nodes" => Some("Node"),
        "deployments" => Some("Deployment"),
        "replicasets" => Some("ReplicaSet"),
        "statefulsets" => Some("StatefulSet"),
        "daemonsets" => Some("DaemonSet"),
        "jobs" => Some("Job"),
        "cronjobs" => Some("CronJob"),
        "ingresses" => Some("Ingress"),
        "networkpolicies" => Some("NetworkPolicy"),
        "horizontalpodautoscalers" => Some("HorizontalPodAutoscaler"),
        "poddisruptionbudgets" => Some("PodDisruptionBudget"),
        "persistentvolumes" => Some("PersistentVolume"),
        "storageclasses" => Some("StorageClass"),
        "clusterroles" => Some("ClusterRole"),
        "clusterrolebindings" => Some("ClusterRoleBinding"),
        "roles" => Some("Role"),
        "rolebindings" => Some("RoleBinding"),
        "customresourcedefinitions" => Some("CustomResourceDefinition"),
        _ => None,
    }
}

/// Parse a `count/<plural>.<group>` or `count/<plural>` quota key.
/// Returns (api_version, kind) as owned Strings if parseable.
fn parse_count_quota_key(resource_name: &str) -> Option<(String, String)> {
    let plural_and_group = resource_name.strip_prefix("count/")?;

    // Split on last '.' to separate plural from group
    // e.g., "replicasets.apps" → plural="replicasets", group="apps"
    // e.g., "configmaps" → plural="configmaps", group="" (core)
    let (plural, group) = if let Some(dot_pos) = plural_and_group.rfind('.') {
        let (p, g) = plural_and_group.split_at(dot_pos);
        (p, &g[1..]) // skip the dot
    } else {
        (plural_and_group, "")
    };

    let kind = plural_to_kind(plural)?;

    // Determine apiVersion
    let api_version = if group.is_empty() {
        "v1".to_string()
    } else {
        format!("{}/v1", group)
    };

    Some((api_version, kind.to_string()))
}

/// Count Service resources that match a specific type filter.
async fn count_services_by_type(db: &dyn DatastoreBackend, namespace: &str, svc_type: &str) -> i64 {
    db.list_resources(
        "v1",
        "Service",
        Some(namespace),
        crate::datastore::ResourceListQuery::all(),
    )
    .await
    .map(|list| {
        list.items
            .iter()
            .filter(|s| s.data.pointer("/spec/type").and_then(|t| t.as_str()) == Some(svc_type))
            .count() as i64
    })
    .unwrap_or(0)
}

/// Count live (non-deleted) resources of a given kind in a namespace.
async fn count_resources(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    namespace: &str,
) -> i64 {
    db.list_resources(
        api_version,
        kind,
        Some(namespace),
        crate::datastore::ResourceListQuery::all(),
    )
    .await
    .map(|list| list.items.len() as i64)
    .unwrap_or(0)
}

/// Check if a pod has `deletionTimestamp` set (terminating).
/// The ResourceQuota controller excludes these pods from counting.
pub fn pod_has_deletion_timestamp(pod: &serde_json::Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty())
}

/// Check if a pod is "best-effort" (no resource requests or limits on any container).
pub fn pod_is_best_effort(pod: &serde_json::Value) -> bool {
    let containers = pod
        .pointer("/spec/containers")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    let init_containers = pod
        .pointer("/spec/initContainers")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();

    for container in containers.iter().chain(init_containers.iter()) {
        let has_requests = container
            .pointer("/resources/requests")
            .and_then(|r| r.as_object())
            .is_some_and(|m| !m.is_empty());
        let has_limits = container
            .pointer("/resources/limits")
            .and_then(|l| l.as_object())
            .is_some_and(|m| !m.is_empty());
        if has_requests || has_limits {
            return false;
        }
    }
    true
}

/// Check if a pod is "terminating" for ResourceQuota scope matching.
/// K8s defines `Terminating` scope based on `spec.activeDeadlineSeconds`.
pub fn pod_is_terminating(pod: &serde_json::Value) -> bool {
    pod.pointer("/spec/activeDeadlineSeconds")
        .and_then(|v| v.as_i64())
        .is_some()
}

/// Check whether a pod matches all configured ResourceQuota scopes.
pub fn pod_matches_scopes(pod: &serde_json::Value, scopes: &[&str]) -> bool {
    scopes.iter().all(|&scope| match scope {
        "BestEffort" => pod_is_best_effort(pod),
        "NotBestEffort" => !pod_is_best_effort(pod),
        "Terminating" => pod_is_terminating(pod),
        "NotTerminating" => !pod_is_terminating(pod),
        "PriorityClass" => pod_priority_class_name(pod).is_some(),
        "CrossNamespacePodAffinity" => pod_has_cross_namespace_pod_affinity(pod),
        _ => true,
    })
}

fn pod_priority_class_name(pod: &serde_json::Value) -> Option<&str> {
    pod.pointer("/spec/priorityClassName")
        .and_then(|v| v.as_str())
        .filter(|name| !name.is_empty())
}

fn pod_has_cross_namespace_pod_affinity(pod: &serde_json::Value) -> bool {
    let affinity = pod.pointer("/spec/affinity");
    for terms_pointer in [
        "/podAffinity/requiredDuringSchedulingIgnoredDuringExecution",
        "/podAntiAffinity/requiredDuringSchedulingIgnoredDuringExecution",
    ] {
        if affinity
            .and_then(|affinity| affinity.pointer(terms_pointer))
            .and_then(|terms| terms.as_array())
            .is_some_and(|terms| terms.iter().any(pod_affinity_term_is_cross_namespace))
        {
            return true;
        }
    }

    for terms_pointer in [
        "/podAffinity/preferredDuringSchedulingIgnoredDuringExecution",
        "/podAntiAffinity/preferredDuringSchedulingIgnoredDuringExecution",
    ] {
        if affinity
            .and_then(|affinity| affinity.pointer(terms_pointer))
            .and_then(|terms| terms.as_array())
            .is_some_and(|terms| {
                terms.iter().any(|weighted| {
                    weighted
                        .get("podAffinityTerm")
                        .is_some_and(pod_affinity_term_is_cross_namespace)
                })
            })
        {
            return true;
        }
    }

    false
}

fn pod_affinity_term_is_cross_namespace(term: &serde_json::Value) -> bool {
    term.get("namespaces")
        .and_then(|namespaces| namespaces.as_array())
        .is_some_and(|namespaces| !namespaces.is_empty())
        || term.get("namespaceSelector").is_some()
}

fn pod_quota_scope_value<'a>(pod: &'a serde_json::Value, scope_name: &str) -> Option<&'a str> {
    match scope_name {
        "BestEffort" if pod_is_best_effort(pod) => Some("BestEffort"),
        "NotBestEffort" if !pod_is_best_effort(pod) => Some("NotBestEffort"),
        "Terminating" if pod_is_terminating(pod) => Some("Terminating"),
        "NotTerminating" if !pod_is_terminating(pod) => Some("NotTerminating"),
        "PriorityClass" => pod_priority_class_name(pod),
        "CrossNamespacePodAffinity" if pod_has_cross_namespace_pod_affinity(pod) => {
            Some("CrossNamespacePodAffinity")
        }
        _ => None,
    }
}

fn pod_matches_scope_selector_expression(
    pod: &serde_json::Value,
    expr: &serde_json::Value,
) -> bool {
    let Some(scope_name) = expr.get("scopeName").and_then(|v| v.as_str()) else {
        return true;
    };
    let operator = expr
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("In");
    let value = pod_quota_scope_value(pod, scope_name);
    let values: Vec<&str> = expr
        .get("values")
        .and_then(|v| v.as_array())
        .map(|values| values.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    match operator {
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "In" => value.is_some_and(|value| values.contains(&value)),
        "NotIn" => value.is_none_or(|value| !values.contains(&value)),
        _ => true,
    }
}

fn pod_matches_scope_selector(
    pod: &serde_json::Value,
    scope_selector: Option<&serde_json::Value>,
) -> bool {
    scope_selector
        .and_then(|selector| selector.get("matchExpressions"))
        .and_then(|expressions| expressions.as_array())
        .map(|expressions| {
            expressions
                .iter()
                .all(|expr| pod_matches_scope_selector_expression(pod, expr))
        })
        .unwrap_or(true)
}

pub fn pod_matches_resource_quota_scopes(
    pod: &serde_json::Value,
    resource_quota: &serde_json::Value,
) -> bool {
    let scopes: Vec<&str> = resource_quota
        .pointer("/spec/scopes")
        .and_then(|s| s.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    pod_matches_scopes(pod, &scopes)
        && pod_matches_scope_selector(pod, resource_quota.pointer("/spec/scopeSelector"))
}

fn is_pod_scope_name(scope_name: &str) -> bool {
    matches!(
        scope_name,
        "BestEffort"
            | "NotBestEffort"
            | "Terminating"
            | "NotTerminating"
            | "PriorityClass"
            | "CrossNamespacePodAffinity"
    )
}

pub fn resource_quota_has_pod_scope_constraints(resource_quota: &serde_json::Value) -> bool {
    let has_scope = resource_quota
        .pointer("/spec/scopes")
        .and_then(|scopes| scopes.as_array())
        .is_some_and(|scopes| {
            scopes
                .iter()
                .filter_map(|scope| scope.as_str())
                .any(is_pod_scope_name)
        });
    let has_scope_selector = resource_quota
        .pointer("/spec/scopeSelector/matchExpressions")
        .and_then(|expressions| expressions.as_array())
        .is_some_and(|expressions| {
            expressions
                .iter()
                .filter_map(|expr| expr.get("scopeName").and_then(|scope| scope.as_str()))
                .any(is_pod_scope_name)
        });
    has_scope || has_scope_selector
}

fn parse_cpu_milli(q: &str) -> Option<i64> {
    if let Some(stripped) = q.strip_suffix('m') {
        return stripped.parse::<i64>().ok();
    }
    if let Ok(whole) = q.parse::<i64>() {
        return Some(whole * 1000);
    }
    let as_float = q.parse::<f64>().ok()?;
    Some((as_float * 1000.0).round() as i64)
}

fn format_cpu_milli(milli: i64) -> String {
    if milli % 1000 == 0 {
        (milli / 1000).to_string()
    } else {
        format!("{milli}m")
    }
}

fn parse_memory_bytes(q: &str) -> Option<i64> {
    let units: [(&str, i64); 10] = [
        ("Ki", 1024_i64),
        ("Mi", 1024_i64.pow(2)),
        ("Gi", 1024_i64.pow(3)),
        ("Ti", 1024_i64.pow(4)),
        ("Pi", 1024_i64.pow(5)),
        ("Ei", 1024_i64.pow(6)),
        ("K", 1000_i64),
        ("M", 1000_i64.pow(2)),
        ("G", 1000_i64.pow(3)),
        ("T", 1000_i64.pow(4)),
    ];
    for (suffix, mult) in units {
        if let Some(stripped) = q.strip_suffix(suffix) {
            let value = stripped.parse::<f64>().ok()?;
            return Some((value * mult as f64).round() as i64);
        }
    }
    q.parse::<i64>().ok()
}

fn format_memory_bytes(bytes: i64) -> String {
    for (suffix, mult) in [
        ("Ei", 1024_i64.pow(6)),
        ("Pi", 1024_i64.pow(5)),
        ("Ti", 1024_i64.pow(4)),
        ("Gi", 1024_i64.pow(3)),
        ("Mi", 1024_i64.pow(2)),
        ("Ki", 1024_i64),
    ] {
        if bytes % mult == 0 && bytes >= mult {
            return format!("{}{}", bytes / mult, suffix);
        }
    }
    bytes.to_string()
}

fn is_binary_quantity_resource(resource_key: &str) -> bool {
    resource_key == "memory"
        || resource_key == "ephemeral-storage"
        || resource_key.contains("storage")
        || resource_key.starts_with("hugepages-")
}

fn parse_decimal_si_quantity(q: &str) -> Option<i64> {
    let units: [(&str, f64); 7] = [
        ("E", 1_000_000_000_000_000_000_f64),
        ("P", 1_000_000_000_000_000_f64),
        ("T", 1_000_000_000_000_f64),
        ("G", 1_000_000_000_f64),
        ("M", 1_000_000_f64),
        ("k", 1_000_f64),
        ("m", 0.001_f64),
    ];
    for (suffix, mult) in units {
        if let Some(stripped) = q.strip_suffix(suffix) {
            let value = stripped.parse::<f64>().ok()?;
            if !value.is_finite() {
                return None;
            }
            return Some((value * mult).ceil() as i64);
        }
    }
    q.parse::<i64>().ok()
}

pub fn parse_resource_quantity(resource_key: &str, quantity: &str) -> Option<i64> {
    if resource_key == "cpu" {
        parse_cpu_milli(quantity)
    } else if is_binary_quantity_resource(resource_key) {
        parse_memory_bytes(quantity)
    } else {
        parse_decimal_si_quantity(quantity)
    }
}

pub fn format_resource_quantity(resource_key: &str, value: i64) -> String {
    if resource_key == "cpu" {
        format_cpu_milli(value)
    } else if is_binary_quantity_resource(resource_key) {
        format_memory_bytes(value)
    } else {
        value.to_string()
    }
}

pub fn calculate_pod_effective_resource_for_key(
    pod: &Value,
    bucket: &str,
    resource_key: &str,
) -> i64 {
    let mut regular_sum = 0_i64;
    let mut init_max = 0_i64;

    if let Some(containers) = pod.pointer("/spec/containers").and_then(|v| v.as_array()) {
        for c in containers {
            let quantity = c
                .get("resources")
                .and_then(|r| r.get(bucket))
                .and_then(|m| m.get(resource_key))
                .and_then(|v| v.as_str())
                .and_then(|q| parse_resource_quantity(resource_key, q))
                .unwrap_or(0);
            regular_sum += quantity;
        }
    }

    if let Some(init_containers) = pod
        .pointer("/spec/initContainers")
        .and_then(|v| v.as_array())
    {
        for c in init_containers {
            let quantity = c
                .get("resources")
                .and_then(|r| r.get(bucket))
                .and_then(|m| m.get(resource_key))
                .and_then(|v| v.as_str())
                .and_then(|q| parse_resource_quantity(resource_key, q))
                .unwrap_or(0);
            init_max = init_max.max(quantity);
        }
    }

    regular_sum.max(init_max)
}

async fn sum_pod_resource_quota_key(
    pod_reader: &dyn PodReader,
    namespace: &str,
    resource_quota: &Value,
    quota_key: &str,
) -> Option<String> {
    let (bucket, resource_key) = if let Some(suffix) = quota_key.strip_prefix("requests.") {
        ("requests", suffix)
    } else if let Some(suffix) = quota_key.strip_prefix("limits.") {
        ("limits", suffix)
    } else if quota_key == "cpu" {
        ("requests", "cpu")
    } else if quota_key == "memory" {
        ("requests", "memory")
    } else if quota_key == "ephemeral-storage" {
        ("requests", "ephemeral-storage")
    } else {
        return None;
    };

    let pods = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await
        .ok()?
        .items;

    let mut total = 0_i64;
    for pod in pods {
        // Exclude terminating pods (deletionTimestamp set) from quota usage.
        // In K8s, the quota controller does not count pods being deleted.
        if pod_has_deletion_timestamp(&pod.data) {
            continue;
        }
        if !pod_matches_resource_quota_scopes(&pod.data, resource_quota) {
            continue;
        }
        total += calculate_pod_effective_resource_for_key(&pod.data, bucket, resource_key);
    }

    Some(format_resource_quantity(resource_key, total))
}

/// Count pods that match the given scope selector, or all pods if scopes is empty.
async fn count_pods_with_scope(
    pod_reader: &dyn PodReader,
    namespace: &str,
    resource_quota: &Value,
) -> i64 {
    let pods = match pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await
    {
        Ok(list) => list.items,
        Err(_) => return 0,
    };

    pods.iter()
        .filter(|pod| !pod_has_deletion_timestamp(&pod.data))
        .filter(|pod| pod_matches_resource_quota_scopes(&pod.data, resource_quota))
        .count() as i64
}

async fn calculate_resource_quota_status(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    namespace: &str,
    rq: &Value,
) -> Option<(serde_json::Map<String, Value>, Value)> {
    let hard = rq
        .pointer("/spec/hard")
        .and_then(|h| h.as_object())?
        .clone();

    let mut used = serde_json::Map::new();
    for resource_name in hard.keys() {
        if let Some(pod_used) =
            sum_pod_resource_quota_key(pod_reader, namespace, rq, resource_name).await
        {
            used.insert(resource_name.clone(), json!(pod_used));
            continue;
        }

        let count = if resource_name == "services.nodeports" {
            // Count NodePort and LoadBalancer services (both allocate NodePorts)
            let np = count_services_by_type(db, namespace, "NodePort").await;
            let lb = count_services_by_type(db, namespace, "LoadBalancer").await;
            np + lb
        } else if resource_name == "services.loadbalancers" {
            count_services_by_type(db, namespace, "LoadBalancer").await
        } else if resource_name == "pods" {
            // Pod counting must exclude terminating pods
            count_pods_with_scope(pod_reader, namespace, rq).await
        } else if let Some((api_version, kind)) = quota_resource_to_kind(resource_name) {
            count_resources(db, api_version, kind, namespace).await
        } else if resource_name.starts_with("count/") {
            // Handle count/<plural>.<group> style quota keys
            if let Some((api_version, kind)) = parse_count_quota_key(resource_name) {
                count_resources(db, &api_version, &kind, namespace).await
            } else {
                // Unknown count/ resource, preserve existing or 0
                rq.pointer(&format!("/status/used/{}", resource_name))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0)
            }
        } else {
            // For resource types we don't know how to count (cpu, memory, storage, etc.),
            // preserve existing value or 0
            rq.pointer(&format!("/status/used/{}", resource_name))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0)
        };
        let value = count.to_string();
        used.insert(resource_name.clone(), json!(value));
    }

    Some((hard, Value::Object(used)))
}

fn resource_quota_status_value(hard: serde_json::Map<String, Value>, used: Value) -> Value {
    json!({
        "hard": hard,
        "used": used,
    })
}

/// Reconcile all ResourceQuotas in a namespace by updating status.used counts.
/// Called after any namespaced resource create or delete.
pub async fn reconcile_resource_quotas_for_namespace(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    namespace: &str,
) -> Result<()> {
    // List all ResourceQuotas in the namespace
    let rq_list = db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for rq_resource in rq_list.items {
        let rq = &rq_resource.data;

        let Some((hard, used_map)) =
            calculate_resource_quota_status(db, pod_reader, namespace, rq).await
        else {
            continue;
        };
        let status = resource_quota_status_value(hard, used_map);
        crate::controllers::common::write_status_for_resource(db, &rq_resource, &status).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
