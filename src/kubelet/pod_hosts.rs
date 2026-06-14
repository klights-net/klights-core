use serde_json::Value;

pub fn parse_boolish(value: Option<&Value>) -> bool {
    match value {
        Some(v) => v
            .as_bool()
            .or_else(|| {
                v.as_str()
                    .and_then(|s| match s.trim().to_ascii_lowercase().as_str() {
                        "true" | "1" | "yes" => Some(true),
                        "false" | "0" | "no" => Some(false),
                        _ => None,
                    })
            })
            .or_else(|| v.as_i64().map(|n| n != 0))
            .unwrap_or(false),
        None => false,
    }
}

pub fn truncate_pod_hostname(hostname: String) -> String {
    // Linux hostname maximum is 64 bytes including the trailing NUL byte.
    const HOSTNAME_MAX_LEN: usize = 63;
    if hostname.len() <= HOSTNAME_MAX_LEN {
        return hostname;
    }

    let truncated = hostname[..HOSTNAME_MAX_LEN].trim_end_matches(['-', '.']);
    if truncated.is_empty() {
        "localhost".to_string()
    } else {
        truncated.to_string()
    }
}

/// Resolve hostname from pod spec, falling back to pod name
pub fn resolve_hostname(pod_spec: &Value, pod_name: &str) -> String {
    let raw = pod_spec
        .get("hostname")
        .and_then(|h| h.as_str())
        .filter(|h| !h.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| pod_name.to_string());

    truncate_pod_hostname(raw)
}

/// Check if the kubelet should manage /etc/hosts for this pod.
/// Returns false for hostNetwork pods (they use the host's /etc/hosts).
pub fn is_host_network(pod: &Value) -> bool {
    parse_boolish(pod.pointer("/spec/hostNetwork"))
}

/// Check if a container already has a volumeMount targeting /etc/hosts.
/// When a container explicitly mounts a volume to /etc/hosts, the kubelet
/// should NOT inject its managed /etc/hosts bind mount.
pub fn container_has_etc_hosts_mount(mounts: &[k8s_cri::v1::Mount]) -> bool {
    mounts.iter().any(|m| m.container_path == "/etc/hosts")
}

/// Build /etc/hosts file content for a pod
/// Includes baseline entries (localhost, IPv6) + pod IP/hostname + FQDN (if subdomain) + hostAliases
pub fn build_etc_hosts(
    hostname: &str,
    pod_ip: &str,
    subdomain: Option<&str>,
    namespace: &str,
    host_aliases: Option<&Vec<Value>>,
) -> String {
    let mut hosts = String::new();

    // Baseline /etc/hosts entries (Kubernetes standard)
    hosts.push_str("# Kubernetes-managed hosts file.\n");
    hosts.push_str("127.0.0.1\tlocalhost\n");
    hosts.push_str("::1\t\tlocalhost ip6-localhost ip6-loopback\n");
    hosts.push_str("fe00::0\t\tip6-localnet\n");
    hosts.push_str("fe00::0\t\tip6-mcastprefix\n");
    hosts.push_str("fe00::1\t\tip6-allnodes\n");
    hosts.push_str("fe00::2\t\tip6-allrouters\n");

    // Pod's own IP and hostname
    // If subdomain is specified, add FQDN: <hostname>.<subdomain>.<namespace>.svc.cluster.local
    if let Some(sub) = subdomain {
        let fqdn = format!("{}.{}.{}.svc.cluster.local", hostname, sub, namespace);
        hosts.push_str(&format!("{}\t{} {}\n", pod_ip, fqdn, hostname));
    } else {
        hosts.push_str(&format!("{}\t{}\n", pod_ip, hostname));
    }

    // Add hostAliases entries if provided
    if let Some(aliases) = host_aliases
        && !aliases.is_empty()
    {
        hosts.push_str("\n# Entries added by HostAliases\n");
        for alias in aliases {
            if let Some(ip) = alias.get("ip").and_then(|v| v.as_str())
                && let Some(hostnames) = alias.get("hostnames").and_then(|v| v.as_array())
            {
                let hostnames_str: Vec<&str> =
                    hostnames.iter().filter_map(|h| h.as_str()).collect();
                if !hostnames_str.is_empty() {
                    hosts.push_str(&format!("{}\t{}\n", ip, hostnames_str.join("\t")));
                }
            }
        }
    }

    hosts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_etc_hosts_basic() {
        let host_aliases = None;
        let hosts_content = build_etc_hosts("test-pod", "10.43.0.5", None, "default", host_aliases);

        // Should contain baseline entries
        assert!(hosts_content.contains("127.0.0.1\tlocalhost"));
        assert!(hosts_content.contains("::1\t\tlocalhost ip6-localhost ip6-loopback"));
        assert!(hosts_content.contains("fe00::0\t\tip6-localnet"));
        assert!(hosts_content.contains("fe00::0\t\tip6-mcastprefix"));
        assert!(hosts_content.contains("fe00::1\t\tip6-allnodes"));
        assert!(hosts_content.contains("fe00::2\t\tip6-allrouters"));

        // Should contain pod IP and hostname (no subdomain, so no FQDN)
        assert!(hosts_content.contains("10.43.0.5\ttest-pod"));

        // Should NOT contain hostAliases header
        assert!(!hosts_content.contains("# Entries added by HostAliases"));
    }

    #[test]
    fn test_build_etc_hosts_with_aliases() {
        let host_aliases = serde_json::json!([
            {
                "ip": "127.0.0.1",
                "hostnames": ["foo.local", "bar.local"]
            },
            {
                "ip": "10.0.0.5",
                "hostnames": ["example.com"]
            }
        ]);
        let host_aliases_vec = host_aliases.as_array().unwrap();
        let hosts_content = build_etc_hosts(
            "test-pod",
            "10.43.0.5",
            None,
            "default",
            Some(host_aliases_vec),
        );

        // Should contain baseline entries
        assert!(hosts_content.contains("127.0.0.1\tlocalhost"));
        assert!(hosts_content.contains("10.43.0.5\ttest-pod"));

        // Should contain hostAliases header
        assert!(hosts_content.contains("# Entries added by HostAliases"));

        // Should contain host alias entries
        assert!(hosts_content.contains("127.0.0.1\tfoo.local\tbar.local"));
        assert!(hosts_content.contains("10.0.0.5\texample.com"));
    }

    #[test]
    fn test_build_etc_hosts_empty_aliases() {
        let host_aliases = serde_json::json!([]);
        let host_aliases_vec = host_aliases.as_array().unwrap();
        let hosts_content = build_etc_hosts(
            "test-pod",
            "10.43.0.5",
            None,
            "default",
            Some(host_aliases_vec),
        );

        // Empty aliases should behave same as None
        assert!(hosts_content.contains("127.0.0.1\tlocalhost"));
        assert!(hosts_content.contains("10.43.0.5\ttest-pod"));
        assert!(!hosts_content.contains("# Entries added by HostAliases"));
    }

    #[test]
    fn test_build_etc_hosts_with_subdomain() {
        // Test FQDN entry when subdomain is specified
        let hosts_content =
            build_etc_hosts("my-pod", "10.43.0.5", Some("my-service"), "default", None);

        // Should contain FQDN + hostname
        assert!(
            hosts_content.contains("10.43.0.5\tmy-pod.my-service.default.svc.cluster.local my-pod")
        );
    }

    #[test]
    fn test_resolve_hostname_from_spec() {
        // Test spec.hostname takes precedence over pod name
        let pod_spec = serde_json::json!({
            "hostname": "custom-hostname"
        });
        assert_eq!(resolve_hostname(&pod_spec, "pod-name"), "custom-hostname");
    }

    #[test]
    fn test_resolve_hostname_defaults_to_pod_name() {
        // Test falls back to pod name when spec.hostname is not set
        let pod_spec = serde_json::json!({});
        assert_eq!(resolve_hostname(&pod_spec, "pod-name"), "pod-name");
    }

    #[test]
    fn test_resolve_hostname_empty_string_falls_back_to_pod_name() {
        let pod_spec = serde_json::json!({"hostname": ""});
        assert_eq!(resolve_hostname(&pod_spec, "pod-name"), "pod-name");
    }

    #[test]
    fn test_resolve_hostname_truncates_to_63_chars() {
        let long_hostname = "termination-message-containerfe3dcd00-9764-4fa1-82c4-a48b5f55568f";
        let pod_spec = serde_json::json!({"hostname": long_hostname});
        let resolved = resolve_hostname(&pod_spec, "pod-name");
        assert_eq!(resolved.len(), 63);
        assert_eq!(resolved, &long_hostname[..63]);
    }

    #[test]
    fn test_resolve_hostname_truncation_trims_invalid_suffix_chars() {
        let pod_spec = serde_json::json!({
            "hostname": format!("{}-x", "a".repeat(62))
        });
        let resolved = resolve_hostname(&pod_spec, "pod-name");
        assert_eq!(resolved.len(), 62);
        assert!(!resolved.ends_with('-'));
    }

    #[test]
    fn test_build_etc_hosts_with_hostname_only_no_subdomain() {
        // When spec.hostname is set but no subdomain, /etc/hosts should use the hostname
        // (not pod name) as the entry, without any FQDN
        let hosts_content = build_etc_hosts("custom-host", "10.43.0.10", None, "default", None);

        // Should use the custom hostname, not pod name
        assert!(
            hosts_content.contains("10.43.0.10\tcustom-host"),
            "Should use custom hostname in hosts entry"
        );
        // Should NOT contain FQDN (no subdomain)
        assert!(
            !hosts_content.contains("svc.cluster.local"),
            "No FQDN without subdomain"
        );
    }

    #[test]
    fn test_build_etc_hosts_with_host_aliases_and_subdomain() {
        // Both hostAliases and subdomain present — FQDN entry + alias entries
        let host_aliases = serde_json::json!([
            {"ip": "192.168.1.1", "hostnames": ["db.internal"]}
        ]);
        let host_aliases_vec = host_aliases.as_array().unwrap();
        let hosts_content = build_etc_hosts(
            "web-0",
            "10.43.0.20",
            Some("nginx"),
            "production",
            Some(host_aliases_vec),
        );

        // Should contain FQDN entry with subdomain
        assert!(
            hosts_content.contains("10.43.0.20\tweb-0.nginx.production.svc.cluster.local web-0"),
            "Should have FQDN + short hostname, got:\n{}",
            hosts_content
        );
        // Should also contain hostAliases
        assert!(
            hosts_content.contains("192.168.1.1\tdb.internal"),
            "Should contain hostAliases entry"
        );
        // Verify ordering: FQDN entry before hostAliases
        let fqdn_pos = hosts_content
            .find("web-0.nginx.production.svc.cluster.local")
            .unwrap();
        let alias_pos = hosts_content.find("db.internal").unwrap();
        assert!(
            fqdn_pos < alias_pos,
            "FQDN entry should appear before hostAliases"
        );
    }

    #[test]
    fn test_is_host_network_true() {
        let pod = serde_json::json!({
            "spec": {
                "hostNetwork": true,
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        assert!(is_host_network(&pod));
    }

    #[test]
    fn test_is_host_network_false_when_absent() {
        let pod = serde_json::json!({
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        assert!(!is_host_network(&pod));
    }

    #[test]
    fn test_is_host_network_false_when_explicit_false() {
        let pod = serde_json::json!({
            "spec": {
                "hostNetwork": false,
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        assert!(!is_host_network(&pod));
    }

    #[test]
    fn test_is_host_network_true_when_string_true() {
        let pod = serde_json::json!({
            "spec": {
                "hostNetwork": "true",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        assert!(is_host_network(&pod));
    }

    #[test]
    fn test_container_has_etc_hosts_mount_present() {
        let mounts = vec![
            k8s_cri::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: 0,
                gid_mappings: vec![],
                uid_mappings: vec![],
                image: None,
                recursive_read_only: false,
            },
            k8s_cri::v1::Mount {
                container_path: "/etc/hosts".to_string(),
                host_path: "/host/etc/hosts".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: 0,
                gid_mappings: vec![],
                uid_mappings: vec![],
                image: None,
                recursive_read_only: false,
            },
        ];
        assert!(container_has_etc_hosts_mount(&mounts));
    }

    #[test]
    fn test_container_has_etc_hosts_mount_absent() {
        let mounts = vec![k8s_cri::v1::Mount {
            container_path: "/data".to_string(),
            host_path: "/host/data".to_string(),
            readonly: false,
            selinux_relabel: false,
            propagation: 0,
            gid_mappings: vec![],
            uid_mappings: vec![],
            image: None,
            recursive_read_only: false,
        }];
        assert!(!container_has_etc_hosts_mount(&mounts));
    }

    #[test]
    fn test_container_has_etc_hosts_mount_empty() {
        let mounts: Vec<k8s_cri::v1::Mount> = vec![];
        assert!(!container_has_etc_hosts_mount(&mounts));
    }

    #[test]
    fn test_build_etc_hosts_header_has_trailing_period() {
        let hosts = build_etc_hosts("test-pod", "10.43.0.5", None, "default", None);
        assert!(
            hosts.starts_with("# Kubernetes-managed hosts file.\n"),
            "Header must have trailing period: got {:?}",
            hosts.lines().next()
        );
    }
}
