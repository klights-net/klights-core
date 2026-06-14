#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
pub fn extract_netns_path_from_sandbox_status_info(
    info: &HashMap<String, String>,
) -> Option<String> {
    for key in ["netns", "netnsPath", "netNamespacePath"] {
        if let Some(path) = info.get(key).filter(|path| !path.is_empty()) {
            return Some(path.clone());
        }
    }

    for raw in info.values() {
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw) else {
            continue;
        };
        if let Some(path) = parsed
            .pointer("/runtimeSpec/linux/namespaces")
            .and_then(|v| v.as_array())
            .and_then(|namespaces| {
                namespaces.iter().find_map(|ns| {
                    let ns_type = ns.get("type").and_then(|v| v.as_str())?;
                    let path = ns.get("path").and_then(|v| v.as_str())?;
                    (ns_type == "network" && !path.is_empty()).then(|| path.to_string())
                })
            })
        {
            return Some(path);
        }

        for key in ["netns", "netnsPath", "netNamespacePath"] {
            if let Some(path) = parsed.get(key).and_then(|v| v.as_str())
                && !path.is_empty()
            {
                return Some(path.to_string());
            }
        }
    }

    None
}

/// Pure matching logic for the Tier-3 CRI fallback. UID-bearing delete work
/// must match by UID only; namespace/name fallback is safe only when the
/// deleted snapshot has no UID.
#[cfg(test)]
pub fn match_sandbox_by_uid_then_name(
    sandboxes: &[k8s_cri::v1::PodSandbox],
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
) -> Option<String> {
    // 3a. uid match — strongest signal; survives same-name-different-pod recreation.
    if !pod_uid.is_empty() {
        for sb in sandboxes {
            if sb
                .metadata
                .as_ref()
                .map(|m| m.uid == pod_uid)
                .unwrap_or(false)
            {
                if sb.id.trim().is_empty() {
                    continue;
                }
                tracing::warn!(
                    "Found sandbox_id {} for {}/{} via CRI uid match \
                     (missing from SQLite and annotation — pre-fix pod)",
                    sb.id,
                    namespace,
                    pod_name
                );
                return Some(sb.id.clone());
            }
        }
    }

    // 3b. namespace+name fallback is only safe for UID-less legacy snapshots.
    // If pod_uid is present, a name match with a different UID may be a
    // replacement Pod and must not be torn down by old delete work.
    if pod_uid.is_empty() {
        for sb in sandboxes {
            if let Some(meta) = sb.metadata.as_ref()
                && meta.namespace == namespace
                && meta.name == pod_name
            {
                if sb.id.trim().is_empty() {
                    continue;
                }
                tracing::warn!(
                    sandbox_id = %sb.id,
                    ns = %namespace,
                    name = %pod_name,
                    "Found sandbox via CRI namespace+name match for UID-less delete snapshot"
                );
                return Some(sb.id.clone());
            }
        }
    }

    None
}

#[cfg(test)]
pub fn container_id_sets_for_delete(
    containers: &[k8s_cri::v1::Container],
) -> (Vec<String>, Vec<String>) {
    let mut all_ids = Vec::new();
    let mut running_ids = Vec::new();
    for c in containers {
        if c.state == 1 {
            running_ids.push(c.id.clone());
        }
        all_ids.push(c.id.clone());
    }

    let prestop_ids = if running_ids.is_empty() {
        all_ids.clone()
    } else {
        running_ids
    };
    (all_ids, prestop_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cri_sandbox(id: &str, ns: &str, name: &str, uid: &str) -> k8s_cri::v1::PodSandbox {
        k8s_cri::v1::PodSandbox {
            id: id.to_string(),
            metadata: Some(k8s_cri::v1::PodSandboxMetadata {
                name: name.to_string(),
                uid: uid.to_string(),
                namespace: ns.to_string(),
                attempt: 0,
            }),
            state: 0,
            created_at: 0,
            labels: std::collections::HashMap::new(),
            annotations: std::collections::HashMap::new(),
            runtime_handler: String::new(),
        }
    }

    fn make_cri_container(id: &str, name: &str, state: i32) -> k8s_cri::v1::Container {
        k8s_cri::v1::Container {
            id: id.to_string(),
            state,
            metadata: Some(k8s_cri::v1::ContainerMetadata {
                name: name.to_string(),
                attempt: 0,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_extract_netns_path_from_containerd_runtime_spec_info() {
        let mut info = std::collections::HashMap::new();
        info.insert(
            "info".to_string(),
            serde_json::json!({
                "runtimeSpec": {
                    "linux": {
                        "namespaces": [
                            {"type": "pid"},
                            {"type": "network", "path": "/var/run/netns/cni-abc123"},
                            {"type": "ipc"}
                        ]
                    }
                }
            })
            .to_string(),
        );

        assert_eq!(
            extract_netns_path_from_sandbox_status_info(&info).as_deref(),
            Some("/var/run/netns/cni-abc123")
        );
    }

    /// A delete for a UID-bearing Pod must never fall back to namespace+name,
    /// because a replacement Pod can reuse the name while the old WatchDeleted
    /// work is still queued. UID-mismatch orphans are handled by sandbox_gc.
    #[test]
    fn test_match_sandbox_rejects_namespace_name_when_uid_mismatches() {
        let sandboxes = vec![make_cri_sandbox(
            "sb-abc",
            "default",
            "my-pod",
            "old-uid-from-deleted-pod",
        )];

        let resolved =
            match_sandbox_by_uid_then_name(&sandboxes, "default", "my-pod", "fresh-uid-99");
        assert_eq!(
            resolved, None,
            "delete resolution must not match a different UID by namespace/name"
        );
    }

    /// uid match takes priority over namespace+name match when both are present.
    #[test]
    fn test_match_sandbox_uid_takes_priority_over_name() {
        let sandboxes = vec![
            make_cri_sandbox("sb-by-name", "default", "my-pod", "stale-uid"),
            make_cri_sandbox("sb-by-uid", "other-ns", "renamed-pod", "live-uid"),
        ];

        let resolved = match_sandbox_by_uid_then_name(&sandboxes, "default", "my-pod", "live-uid");
        assert_eq!(
            resolved,
            Some("sb-by-uid".to_string()),
            "uid match must win over namespace+name match"
        );
    }

    /// No match: neither uid nor namespace+name lines up.
    #[test]
    fn test_match_sandbox_returns_none_when_nothing_matches() {
        let sandboxes = vec![make_cri_sandbox(
            "sb-other",
            "other-ns",
            "other-pod",
            "other-uid",
        )];

        let resolved = match_sandbox_by_uid_then_name(&sandboxes, "default", "my-pod", "fresh-uid");
        assert_eq!(resolved, None);
    }

    /// Empty pod_uid still allows the namespace+name fallback to run.
    #[test]
    fn test_match_sandbox_namespace_name_works_with_empty_uid() {
        let sandboxes = vec![make_cri_sandbox(
            "sb-name-only",
            "kube-system",
            "coredns-1",
            "anything",
        )];

        let resolved = match_sandbox_by_uid_then_name(&sandboxes, "kube-system", "coredns-1", "");
        assert_eq!(resolved, Some("sb-name-only".to_string()));
    }

    #[test]
    fn test_container_id_sets_for_delete_prefers_running_for_prestop() {
        let containers = vec![
            make_cri_container("cid-exited", "app", 2),
            make_cri_container("cid-running", "app", 1),
        ];

        let (all_ids, prestop_ids) = container_id_sets_for_delete(&containers);
        assert_eq!(all_ids, vec!["cid-exited", "cid-running"]);
        assert_eq!(prestop_ids, vec!["cid-running"]);
    }

    #[test]
    fn test_container_id_sets_for_delete_falls_back_when_no_running_containers() {
        let containers = vec![
            make_cri_container("cid-created", "app", 0),
            make_cri_container("cid-exited", "app", 2),
        ];

        let (all_ids, prestop_ids) = container_id_sets_for_delete(&containers);
        assert_eq!(all_ids, vec!["cid-created", "cid-exited"]);
        assert_eq!(prestop_ids, vec!["cid-created", "cid-exited"]);
    }
}
