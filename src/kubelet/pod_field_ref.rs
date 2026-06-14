use serde_json::Value;

/// Resolve a fieldRef to a pod field value
pub fn resolve_field_ref(field_path: &str, pod_data: &Value) -> String {
    match field_path {
        "metadata.name" => pod_data
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "metadata.namespace" => pod_data
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "metadata.uid" => pod_data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "spec.nodeName" => pod_data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "spec.serviceAccountName" => pod_data
            .pointer("/spec/serviceAccountName")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string(),
        "status.podIP" => pod_data
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "status.hostIP" => pod_data
            .pointer("/status/hostIP")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        // Handle metadata.labels['key']
        path if path.starts_with("metadata.labels['") && path.ends_with("']") => {
            let key = path
                .strip_prefix("metadata.labels['")
                .and_then(|s| s.strip_suffix("']"))
                .unwrap_or("");
            // RFC 6901: escape ~ as ~0 and / as ~1 for JSON pointer
            let escaped = key.replace("~", "~0").replace("/", "~1");
            pod_data
                .pointer(&format!("/metadata/labels/{}", escaped))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        }
        // Handle metadata.annotations['key']
        path if path.starts_with("metadata.annotations['") && path.ends_with("']") => {
            let key = path
                .strip_prefix("metadata.annotations['")
                .and_then(|s| s.strip_suffix("']"))
                .unwrap_or("");
            // RFC 6901: escape ~ as ~0 and / as ~1 for JSON pointer
            let escaped = key.replace("~", "~0").replace("/", "~1");
            pod_data
                .pointer(&format!("/metadata/annotations/{}", escaped))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        }
        _ => String::new(), // Unknown field, return empty string
    }
}

/// Resolve a resourceFieldRef to a container resource value.
/// Returns "0" when resources are not set (K8s returns node allocatable for
/// missing limits and 0 for missing requests).
pub fn resolve_resource_field_ref(resource: &str, container_spec: &Value) -> String {
    let raw_value = match resource {
        "limits.cpu" => container_spec
            .pointer("/resources/limits/cpu")
            .and_then(|v| v.as_str()),
        "limits.memory" => container_spec
            .pointer("/resources/limits/memory")
            .and_then(|v| v.as_str()),
        "requests.cpu" => container_spec
            .pointer("/resources/requests/cpu")
            .and_then(|v| v.as_str()),
        "requests.memory" => container_spec
            .pointer("/resources/requests/memory")
            .and_then(|v| v.as_str()),
        "limits.ephemeral-storage" => container_spec
            .pointer("/resources/limits/ephemeral-storage")
            .and_then(|v| v.as_str()),
        "requests.ephemeral-storage" => container_spec
            .pointer("/resources/requests/ephemeral-storage")
            .and_then(|v| v.as_str()),
        _ => None,
    };

    // K8s returns node allocatable for missing limits, 0 for missing requests
    let raw = match raw_value {
        Some(v) => v,
        None => {
            return match resource {
                "limits.memory" => {
                    let bytes = crate::kubelet::node::memory_ki() * 1024;
                    bytes.to_string()
                }
                "limits.cpu" => {
                    let cores = std::thread::available_parallelism()
                        .map(|n| n.get() as u64)
                        .unwrap_or(1);
                    cores.to_string()
                }
                _ => "0".to_string(),
            };
        }
    };

    // K8s converts resourceFieldRef values to numeric format:
    // - memory: quantity → bytes (32Mi → 33554432)
    // - cpu: quantity → millicores (500m → 1, 2 → 2)
    // - ephemeral-storage: quantity → bytes
    if resource.contains("memory") || resource.contains("ephemeral-storage") {
        // Convert memory/storage quantity to bytes
        crate::kubelet::volumes::parse_k8s_quantity(raw)
            .map(|bytes| bytes.to_string())
            .unwrap_or_else(|_| raw.to_string())
    } else if resource.contains("cpu") {
        // Convert CPU quantity to millicore integer
        // "500m" → 1 (cores, default divisor=1), "2" → 2
        // K8s default divisor for CPU is 1 (core), so return cores as integer
        if let Some(millis_str) = raw.strip_suffix('m') {
            // Millicores → cores (ceiling division per K8s downward API spec)
            millis_str
                .parse::<u64>()
                .map(|m| m.div_ceil(1000).max(1).to_string())
                .unwrap_or_else(|_| raw.to_string())
        } else {
            // Already in cores
            raw.to_string()
        }
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_resource_field_ref_limits_cpu_when_set() {
        // When limits.cpu is explicitly set in the container spec,
        // resolve_resource_field_ref must return that value, not the node CPU count.
        let container_spec = serde_json::json!({
            "name": "app",
            "image": "nginx",
            "resources": {
                "limits": {
                    "cpu": "2",
                    "memory": "512Mi"
                }
            }
        });
        let result = resolve_resource_field_ref("limits.cpu", &container_spec);
        assert_eq!(
            result, "2",
            "limits.cpu=2 must return '2', not the node CPU count"
        );
    }
}
