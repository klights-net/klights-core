use crate::kubelet::pod_env::expand_env_var_references;
use crate::kubelet::pod_field_ref::{resolve_field_ref, resolve_resource_field_ref};
use crate::kubelet::pod_resources::{parse_cpu_resource, parse_memory_resource};
use k8s_cri::v1::{
    ContainerConfig, ContainerMetadata, ImageSpec, KeyValue, LinuxContainerConfig,
    LinuxContainerResources,
};
use serde_json::Value;

/// Check runAsNonRoot constraint. Returns Ok(()) if the container is allowed
/// to start, or Err(message) if it would run as root and runAsNonRoot is true.
pub fn check_run_as_non_root(
    pod_data: &Value,
    container_spec: &Value,
    container_name: &str,
) -> Result<(), String> {
    let pod_sc = pod_data.pointer("/spec/securityContext");
    let container_sc = container_spec.get("securityContext");

    // Resolve runAsNonRoot: container overrides pod
    let run_as_non_root = container_sc
        .and_then(|c| c.get("runAsNonRoot").and_then(|v| v.as_bool()))
        .or_else(|| pod_sc.and_then(|p| p.get("runAsNonRoot").and_then(|v| v.as_bool())))
        .unwrap_or(false);

    if !run_as_non_root {
        return Ok(());
    }

    // Resolve runAsUser: container overrides pod
    let run_as_user = container_sc
        .and_then(|c| c.get("runAsUser").and_then(|v| v.as_i64()))
        .or_else(|| pod_sc.and_then(|p| p.get("runAsUser").and_then(|v| v.as_i64())));

    match run_as_user {
        Some(uid) if uid != 0 => Ok(()),
        _ => {
            let pod_name = pod_data
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            Err(format!(
                "container has runAsNonRoot and image will run as root (pod: \"{}\", container: {})",
                pod_name, container_name
            ))
        }
    }
}

pub fn build_container_config(
    container_spec: &Value,
    pod_data: &Value,
    container_name: &str,
    kubernetes_service_ip: &str,
    resolved_env_from: &[(String, String)],
    resolved_env: &std::collections::HashMap<String, String>,
) -> ContainerConfig {
    let image = container_spec
        .get("image")
        .and_then(|i| i.as_str())
        .unwrap_or("nginx:latest")
        .to_string();

    // Extract command if present
    let command: Vec<String> = container_spec
        .get("command")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Extract args if present
    let args: Vec<String> = container_spec
        .get("args")
        .and_then(|a| a.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Extract existing env vars and append Kubernetes service discovery vars
    let mut envs = Vec::new();
    // Track resolved values in order for $(VAR_NAME) expansion
    let mut expansion_map: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // Add envFrom vars first (can be overridden by individual env vars)
    for (key, value) in resolved_env_from {
        expansion_map.insert(key.clone(), value.clone());
        envs.push(KeyValue {
            key: key.clone(),
            value: value.clone(),
        });
    }

    // Parse existing env vars from container spec (these override envFrom)
    if let Some(env_array) = container_spec.get("env").and_then(|e| e.as_array()) {
        for env in env_array {
            let name = match env.get("name").and_then(|n| n.as_str()) {
                Some(n) => n,
                None => continue,
            };

            if let Some(value) = env.get("value").and_then(|v| v.as_str()) {
                // Direct value — apply $(VAR_NAME) expansion against previously resolved vars
                let expanded = expand_env_var_references(value, &expansion_map);
                expansion_map.insert(name.to_string(), expanded.clone());
                envs.push(KeyValue {
                    key: name.to_string(),
                    value: expanded,
                });
            } else if let Some(resolved_value) = resolved_env.get(name) {
                // Resolved from secretKeyRef/configMapKeyRef
                expansion_map.insert(name.to_string(), resolved_value.clone());
                envs.push(KeyValue {
                    key: name.to_string(),
                    value: resolved_value.clone(),
                });
            } else if let Some(value_from) = env.get("valueFrom") {
                // Handle fieldRef
                if let Some(field_ref) = value_from.get("fieldRef") {
                    if let Some(field_path) = field_ref.get("fieldPath").and_then(|f| f.as_str()) {
                        let resolved_value = resolve_field_ref(field_path, pod_data);
                        expansion_map.insert(name.to_string(), resolved_value.clone());
                        envs.push(KeyValue {
                            key: name.to_string(),
                            value: resolved_value,
                        });
                    }
                }
                // Handle resourceFieldRef
                else if let Some(resource_field_ref) = value_from.get("resourceFieldRef")
                    && let Some(resource) =
                        resource_field_ref.get("resource").and_then(|r| r.as_str())
                {
                    let resolved_value = resolve_resource_field_ref(resource, container_spec);
                    expansion_map.insert(name.to_string(), resolved_value.clone());
                    envs.push(KeyValue {
                        key: name.to_string(),
                        value: resolved_value,
                    });
                }
            }
        }
    }

    // Apply $(VAR_NAME) expansion to command and args entries
    let command: Vec<String> = command
        .iter()
        .map(|s| expand_env_var_references(s, &expansion_map))
        .collect();
    let args: Vec<String> = args
        .iter()
        .map(|s| expand_env_var_references(s, &expansion_map))
        .collect();

    // Append Kubernetes service discovery env vars
    envs.push(KeyValue {
        key: "KUBERNETES_SERVICE_HOST".to_string(),
        value: kubernetes_service_ip.to_string(),
    });
    envs.push(KeyValue {
        key: "KUBERNETES_SERVICE_PORT".to_string(),
        value: "443".to_string(),
    });
    envs.push(KeyValue {
        key: "KUBERNETES_SERVICE_PORT_HTTPS".to_string(),
        value: "443".to_string(),
    });

    // Extract resource limits/requests from container spec
    let resources = container_spec.get("resources").and_then(|r| {
        let mut linux_res = LinuxContainerResources::default();
        let mut has_resources = false;

        // Parse limits
        if let Some(limits) = r.get("limits") {
            // Memory limit
            if let Some(memory_str) = limits.get("memory").and_then(|m| m.as_str())
                && let Some(bytes) = parse_memory_resource(memory_str)
            {
                linux_res.memory_limit_in_bytes = bytes;
                has_resources = true;
            }

            // CPU limit
            if let Some(cpu_str) = limits.get("cpu").and_then(|c| c.as_str())
                && let Some(quota_ns) = parse_cpu_resource(cpu_str)
            {
                // CRI uses quota/period: quota is time in ns per period (100ms = 100,000,000 ns)
                // 1 CPU = 100ms quota per 100ms period
                linux_res.cpu_quota = quota_ns / 1000; // Convert to microseconds for CRI
                linux_res.cpu_period = 100_000; // 100ms period in microseconds
                has_resources = true;
            }
        }

        // Parse requests
        if let Some(requests) = r.get("requests") {
            // Memory request (soft guarantee via memory.min)
            if let Some(memory_str) = requests.get("memory").and_then(|m| m.as_str())
                && let Some(_bytes) = parse_memory_resource(memory_str)
            {
                // memory.min is set via memory_swap_limit_in_bytes in some CRI impls
                // For now, we only enforce limits (memory.max), not soft guarantees
                has_resources = true;
            }

            // CPU request (relative shares via cpu.weight)
            if let Some(cpu_str) = requests.get("cpu").and_then(|c| c.as_str())
                && let Some(cpu_ns) = parse_cpu_resource(cpu_str)
            {
                // CRI uses cpu_shares: 1 CPU = 1024 shares
                // Convert from ns to shares: (cpu_ns / 1_000_000_000) * 1024
                linux_res.cpu_shares = ((cpu_ns as f64 / 1_000_000_000.0) * 1024.0) as i64;
                has_resources = true;
            }
        }

        if has_resources { Some(linux_res) } else { None }
    });

    // Build security context from pod-level and container-level securityContext
    // Container-level values override pod-level values
    let pod_security_context = pod_data.pointer("/spec/securityContext");
    let container_security_context = container_spec.get("securityContext");

    let security_context = if pod_security_context.is_some() || container_security_context.is_some()
    {
        use k8s_cri::v1::{Int64Value, LinuxContainerSecurityContext};

        // Helper to get i64 value with container override
        let get_security_i64 = |field: &str| -> Option<Int64Value> {
            container_security_context
                .and_then(|c| c.get(field).and_then(|v| v.as_i64()))
                .or_else(|| {
                    pod_security_context.and_then(|p| p.get(field).and_then(|v| v.as_i64()))
                })
                .map(|value| Int64Value { value })
        };

        // Helper to get bool value with container override
        let get_security_bool = |field: &str| -> bool {
            container_security_context
                .and_then(|c| c.get(field).and_then(|v| v.as_bool()))
                .or_else(|| {
                    pod_security_context.and_then(|p| p.get(field).and_then(|v| v.as_bool()))
                })
                .unwrap_or(false)
        };

        let run_as_user = get_security_i64("runAsUser");
        let run_as_group = get_security_i64("runAsGroup");
        let privileged = get_security_bool("privileged");
        let readonly_rootfs = get_security_bool("readOnlyRootFilesystem");

        // allowPrivilegeEscalation: K8s spec says it defaults to true when not explicitly set
        // (containers can gain more privileges via setuid executables). Only false when
        // explicitly set to false, or when a restrictive seccomp profile is applied.
        // Conformance: "should allow privilege escalation when not explicitly set and uid != 0"
        // requires that an unset allowPrivilegeEscalation does NOT block setuid binaries.
        let allow_priv_esc = container_security_context
            .and_then(|c| c.get("allowPrivilegeEscalation").and_then(|v| v.as_bool()))
            .or_else(|| {
                pod_security_context
                    .and_then(|p| p.get("allowPrivilegeEscalation").and_then(|v| v.as_bool()))
            })
            .unwrap_or(true); // default true per K8s spec (not false/privileged)

        let no_new_privs = !allow_priv_esc;

        // fsGroup: pod-level only. Added to the container's supplemental_groups
        // so that mounted volumes are accessible to non-root containers whose
        // primary GID doesn't match the volume's file ownership.
        // P0-E2E-20260423-14: [sig-storage] Secrets non-root+fsGroup requires
        // the container runtime to apply fsGroup as a supplemental group.
        let supplemental_groups: Vec<i64> = pod_security_context
            .and_then(|p| p.get("fsGroup").and_then(|v| v.as_i64()))
            .map(|gid| vec![gid])
            .unwrap_or_default();

        Some(LinuxContainerSecurityContext {
            run_as_user,
            run_as_group,
            privileged,
            readonly_rootfs,
            no_new_privs,
            supplemental_groups,
            ..Default::default()
        })
    } else {
        None
    };

    ContainerConfig {
        metadata: Some(ContainerMetadata {
            name: container_name.to_string(),
            ..Default::default()
        }),
        image: Some(ImageSpec {
            image: image.clone(),
            annotations: std::collections::HashMap::new(),
            runtime_handler: String::new(),
            user_specified_image: String::new(),
        }),
        command,
        args,
        working_dir: container_spec
            .get("workingDir")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        envs,
        mounts: vec![],
        devices: vec![],
        labels: std::collections::HashMap::new(),
        annotations: std::collections::HashMap::new(),
        log_path: format!("{}/0.log", container_name),
        stdin: container_spec
            .get("stdin")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        stdin_once: container_spec
            .get("stdinOnce")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        tty: container_spec
            .get("tty")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        linux: Some(LinuxContainerConfig {
            resources,
            security_context,
        }),
        windows: None,
        cdi_devices: vec![],
    }
}

#[cfg(test)]
mod tests_env_and_basics;

#[cfg(test)]
mod tests_fieldref_and_security;
