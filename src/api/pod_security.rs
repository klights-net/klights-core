use serde_json::Value;

use crate::datastore::DatastoreBackend;

use super::AppError;

const LABEL_PREFIX: &str = "pod-security.kubernetes.io/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PodSecurityLevel {
    Privileged,
    Baseline,
    Restricted,
}

pub async fn enforce_pod_security_admission(
    db: &dyn DatastoreBackend,
    namespace: &str,
    pod: &Value,
) -> Result<(), AppError> {
    let Some(namespace_resource) = db
        .get_resource("v1", "Namespace", None, namespace)
        .await
        .map_err(|e| {
            AppError::BadRequest(format!("failed to read namespace for PodSecurity: {e}"))
        })?
    else {
        return Ok(());
    };
    let labels = namespace_resource
        .data
        .pointer("/metadata/labels")
        .and_then(|v| v.as_object());

    for mode in ["warn", "audit"] {
        let Some(level) = labels
            .and_then(|labels| labels.get(&format!("{LABEL_PREFIX}{mode}")))
            .and_then(|v| v.as_str())
            .and_then(parse_level)
        else {
            continue;
        };
        let violations = validate_pod_security(level, pod);
        if !violations.is_empty() {
            tracing::warn!(
                mode,
                namespace,
                level = ?level,
                violations = %violations.join(", "),
                "PodSecurity non-enforcing violation"
            );
        }
    }

    let Some(enforce_level) = labels
        .and_then(|labels| labels.get(&format!("{LABEL_PREFIX}enforce")))
        .and_then(|v| v.as_str())
        .and_then(parse_level)
    else {
        return Ok(());
    };
    let violations = validate_pod_security(enforce_level, pod);
    if violations.is_empty() {
        Ok(())
    } else {
        Err(AppError::Forbidden(format!(
            "PodSecurity {}: {}",
            enforce_level.as_str(),
            violations.join(", ")
        )))
    }
}

impl PodSecurityLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Privileged => "privileged",
            Self::Baseline => "baseline",
            Self::Restricted => "restricted",
        }
    }
}

fn parse_level(value: &str) -> Option<PodSecurityLevel> {
    match value {
        "privileged" => Some(PodSecurityLevel::Privileged),
        "baseline" => Some(PodSecurityLevel::Baseline),
        "restricted" => Some(PodSecurityLevel::Restricted),
        _ => None,
    }
}

fn validate_pod_security(level: PodSecurityLevel, pod: &Value) -> Vec<String> {
    if level == PodSecurityLevel::Privileged {
        return Vec::new();
    }

    let mut violations = Vec::new();
    validate_baseline(pod, &mut violations);
    if level >= PodSecurityLevel::Restricted {
        validate_restricted(pod, &mut violations);
    }
    violations
}

fn validate_baseline(pod: &Value, violations: &mut Vec<String>) {
    for field in ["hostNetwork", "hostPID", "hostIPC"] {
        if bool_at(pod, &format!("/spec/{field}")) == Some(true) {
            violations.push(format!("host namespaces ({field}=true)"));
        }
    }

    validate_host_process(pod, violations);
    validate_containers(pod, violations, |container, name, violations| {
        if bool_at(container, "/securityContext/privileged") == Some(true) {
            violations.push(format!("privileged container {name:?}"));
        }
        validate_baseline_capabilities(container, name, violations);
        validate_host_ports(container, name, violations);
        validate_probe_hosts(container, name, violations);
        validate_apparmor(container, name, violations);
        validate_selinux(container, name, violations);
        validate_proc_mount(container, name, violations);
        validate_seccomp_baseline(container, name, violations);
    });

    if pod
        .pointer("/spec/volumes")
        .and_then(|v| v.as_array())
        .is_some_and(|volumes| {
            volumes
                .iter()
                .any(|volume| volume.get("hostPath").is_some_and(|v| !v.is_null()))
        })
    {
        violations.push("hostPath volumes".to_string());
    }

    validate_apparmor(pod, "pod", violations);
    validate_selinux(pod, "pod", violations);
    validate_seccomp_baseline(pod, "pod", violations);
    validate_sysctls(pod, violations);
}

fn validate_restricted(pod: &Value, violations: &mut Vec<String>) {
    validate_restricted_volumes(pod, violations);
    if is_windows_pod(pod) {
        return;
    }

    let pod_run_as_non_root = bool_at(pod, "/spec/securityContext/runAsNonRoot") == Some(true);
    let pod_seccomp_ok =
        seccomp_type(pod).is_some_and(|value| matches!(value, "RuntimeDefault" | "Localhost"));

    if int_at(pod, "/spec/securityContext/runAsUser") == Some(0) {
        violations.push("runAsUser=0 at pod securityContext".to_string());
    }

    validate_containers(pod, violations, |container, name, violations| {
        if bool_at(container, "/securityContext/allowPrivilegeEscalation") != Some(false) {
            violations.push(format!(
                "allowPrivilegeEscalation != false for container {name:?}"
            ));
        }
        let container_run_as_non_root =
            bool_at(container, "/securityContext/runAsNonRoot") == Some(true);
        if !pod_run_as_non_root && !container_run_as_non_root {
            violations.push(format!("runAsNonRoot != true for container {name:?}"));
        }
        if int_at(container, "/securityContext/runAsUser") == Some(0) {
            violations.push(format!("runAsUser=0 for container {name:?}"));
        }
        let container_seccomp_ok = seccomp_type(container)
            .is_some_and(|value| matches!(value, "RuntimeDefault" | "Localhost"));
        if !pod_seccomp_ok && !container_seccomp_ok {
            violations.push(format!("seccompProfile missing for container {name:?}"));
        }
        validate_restricted_capabilities(container, name, violations);
    });
}

fn validate_containers<F>(pod: &Value, violations: &mut Vec<String>, mut f: F)
where
    F: FnMut(&Value, &str, &mut Vec<String>),
{
    for path in [
        "/spec/initContainers",
        "/spec/containers",
        "/spec/ephemeralContainers",
    ] {
        let Some(containers) = pod.pointer(path).and_then(|v| v.as_array()) else {
            continue;
        };
        for container in containers {
            let name = container
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unnamed>");
            f(container, name, violations);
        }
    }
}

fn validate_host_process(pod: &Value, violations: &mut Vec<String>) {
    if bool_at(pod, "/spec/securityContext/windowsOptions/hostProcess") == Some(true) {
        violations.push("hostProcess at pod securityContext".to_string());
    }
    validate_containers(pod, violations, |container, name, violations| {
        if bool_at(container, "/securityContext/windowsOptions/hostProcess") == Some(true) {
            violations.push(format!("hostProcess for container {name:?}"));
        }
    });
}

fn validate_baseline_capabilities(container: &Value, name: &str, violations: &mut Vec<String>) {
    const ALLOWED: &[&str] = &[
        "AUDIT_WRITE",
        "CHOWN",
        "DAC_OVERRIDE",
        "FOWNER",
        "FSETID",
        "KILL",
        "MKNOD",
        "NET_BIND_SERVICE",
        "SETFCAP",
        "SETGID",
        "SETPCAP",
        "SETUID",
        "SYS_CHROOT",
    ];
    for cap in string_array_at(container, "/securityContext/capabilities/add") {
        if !ALLOWED
            .iter()
            .any(|allowed| cap.eq_ignore_ascii_case(allowed))
        {
            violations.push(format!(
                "unrestricted capability {cap:?} for container {name:?}"
            ));
        }
    }
}

fn validate_restricted_capabilities(container: &Value, name: &str, violations: &mut Vec<String>) {
    let drops = string_array_at(container, "/securityContext/capabilities/drop");
    if !drops.iter().any(|cap| cap.eq_ignore_ascii_case("ALL")) {
        violations.push(format!(
            "capabilities.drop missing ALL for container {name:?}"
        ));
    }
    for cap in string_array_at(container, "/securityContext/capabilities/add") {
        if !cap.eq_ignore_ascii_case("NET_BIND_SERVICE") {
            violations.push(format!("capabilities.add {cap:?} for container {name:?}"));
        }
    }
}

fn validate_host_ports(container: &Value, name: &str, violations: &mut Vec<String>) {
    let Some(ports) = container.get("ports").and_then(|v| v.as_array()) else {
        return;
    };
    if ports.iter().any(|port| {
        port.get("hostPort")
            .and_then(|v| v.as_i64())
            .is_some_and(|port| port != 0)
    }) {
        violations.push(format!("hostPort for container {name:?}"));
    }
}

fn validate_probe_hosts(container: &Value, name: &str, violations: &mut Vec<String>) {
    for path in [
        "/livenessProbe/httpGet/host",
        "/readinessProbe/httpGet/host",
        "/startupProbe/httpGet/host",
        "/livenessProbe/tcpSocket/host",
        "/readinessProbe/tcpSocket/host",
        "/startupProbe/tcpSocket/host",
        "/lifecycle/postStart/httpGet/host",
        "/lifecycle/preStop/httpGet/host",
        "/lifecycle/postStart/tcpSocket/host",
        "/lifecycle/preStop/tcpSocket/host",
    ] {
        if string_at(container, path).is_some_and(|value| !value.is_empty()) {
            violations.push(format!(
                "host field in probe/lifecycle for container {name:?}"
            ));
            return;
        }
    }
}

fn validate_apparmor(value: &Value, name: &str, violations: &mut Vec<String>) {
    if let Some(profile) = string_at(value, "/securityContext/appArmorProfile/type")
        && !matches!(profile, "RuntimeDefault" | "Localhost")
    {
        violations.push(format!("AppArmor profile {profile:?} for {name}"));
    }
}

fn validate_selinux(value: &Value, name: &str, violations: &mut Vec<String>) {
    if let Some(profile) = string_at(value, "/securityContext/seLinuxOptions/type")
        && !matches!(
            profile,
            "" | "container_t" | "container_init_t" | "container_kvm_t" | "container_engine_t"
        )
    {
        violations.push(format!("SELinux type {profile:?} for {name}"));
    }
    if string_at(value, "/securityContext/seLinuxOptions/user").is_some_and(|v| !v.is_empty())
        || string_at(value, "/securityContext/seLinuxOptions/role").is_some_and(|v| !v.is_empty())
    {
        violations.push(format!("SELinux user/role for {name}"));
    }
}

fn validate_proc_mount(container: &Value, name: &str, violations: &mut Vec<String>) {
    if let Some(proc_mount) = string_at(container, "/securityContext/procMount")
        && proc_mount != "Default"
    {
        violations.push(format!("procMount {proc_mount:?} for container {name:?}"));
    }
}

fn validate_seccomp_baseline(value: &Value, name: &str, violations: &mut Vec<String>) {
    if seccomp_type(value) == Some("Unconfined") {
        violations.push(format!("seccompProfile Unconfined for {name}"));
    }
}

fn validate_sysctls(pod: &Value, violations: &mut Vec<String>) {
    const SAFE: &[&str] = &[
        "kernel.shm_rmid_forced",
        "net.ipv4.ip_local_port_range",
        "net.ipv4.ip_unprivileged_port_start",
        "net.ipv4.tcp_syncookies",
        "net.ipv4.ping_group_range",
        "net.ipv4.ip_local_reserved_ports",
        "net.ipv4.tcp_keepalive_time",
        "net.ipv4.tcp_fin_timeout",
        "net.ipv4.tcp_keepalive_intvl",
        "net.ipv4.tcp_keepalive_probes",
    ];
    let Some(sysctls) = pod
        .pointer("/spec/securityContext/sysctls")
        .and_then(|v| v.as_array())
    else {
        return;
    };
    for sysctl in sysctls {
        let Some(name) = sysctl.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        if !SAFE.contains(&name) {
            violations.push(format!("unsafe sysctl {name:?}"));
        }
    }
}

fn validate_restricted_volumes(pod: &Value, violations: &mut Vec<String>) {
    const ALLOWED: &[&str] = &[
        "configMap",
        "csi",
        "downwardAPI",
        "emptyDir",
        "ephemeral",
        "persistentVolumeClaim",
        "projected",
        "secret",
    ];
    let Some(volumes) = pod.pointer("/spec/volumes").and_then(|v| v.as_array()) else {
        return;
    };
    for volume in volumes {
        if !ALLOWED
            .iter()
            .any(|key| volume.get(*key).is_some_and(|v| !v.is_null()))
        {
            let name = volume
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("<unnamed>");
            violations.push(format!("restricted volume type for volume {name:?}"));
        }
    }
}

fn is_windows_pod(pod: &Value) -> bool {
    string_at(pod, "/spec/os/name") == Some("windows")
}

fn seccomp_type(value: &Value) -> Option<&str> {
    string_at(value, "/securityContext/seccompProfile/type")
        .or_else(|| string_at(value, "/spec/securityContext/seccompProfile/type"))
}

fn bool_at(value: &Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(|v| v.as_bool())
}

fn int_at(value: &Value, pointer: &str) -> Option<i64> {
    value.pointer(pointer).and_then(|v| v.as_i64())
}

fn string_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(|v| v.as_str())
}

fn string_array_at<'a>(value: &'a Value, pointer: &str) -> Vec<&'a str> {
    value
        .pointer(pointer)
        .and_then(|v| v.as_array())
        .map(|values| values.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn restricted_accepts_pod_level_seccomp_profile() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "restricted-pod"},
            "spec": {
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000,
                    "seccompProfile": {"type": "RuntimeDefault"}
                },
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "securityContext": {
                        "allowPrivilegeEscalation": false,
                        "capabilities": {"drop": ["ALL"]}
                    }
                }]
            }
        });

        let violations = validate_pod_security(PodSecurityLevel::Restricted, &pod);
        assert!(
            violations.is_empty(),
            "pod-level seccompProfile must satisfy restricted PodSecurity: {violations:?}"
        );
    }

    #[test]
    fn restricted_still_rejects_missing_seccomp_profile() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "restricted-pod"},
            "spec": {
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000
                },
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.10",
                    "securityContext": {
                        "allowPrivilegeEscalation": false,
                        "capabilities": {"drop": ["ALL"]}
                    }
                }]
            }
        });

        let violations = validate_pod_security(PodSecurityLevel::Restricted, &pod);
        assert!(
            violations.iter().any(|v| v.contains("seccompProfile")),
            "restricted PodSecurity must still require seccompProfile: {violations:?}"
        );
    }
}
