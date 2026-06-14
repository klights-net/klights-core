use anyhow::Result;
use k8s_cri::v1::Mount;
use serde_json::Value;
use std::collections::HashMap;

pub struct PodVolumeManager<'a> {
    sources: &'a dyn crate::kubelet::volume_sources::VolumeSourceReader,
    containerd_namespace: &'a str,
}

impl<'a> PodVolumeManager<'a> {
    pub fn new(
        sources: &'a dyn crate::kubelet::volume_sources::VolumeSourceReader,
        containerd_namespace: &'a str,
    ) -> Self {
        Self {
            sources,
            containerd_namespace,
        }
    }

    pub async fn process_volumes(
        &self,
        pod_dir_id: &str,
        pod_name: &str,
        namespace: &str,
        pod: &Value,
    ) -> Result<HashMap<String, String>> {
        let registry = crate::kubelet::volume_registry::VolumeRegistry::with_defaults();
        let ctx = crate::kubelet::volume_registry::VolumeContext {
            sources: self.sources,
            namespace,
            pod_name,
            pod_dir_id,
            pod,
            containerd_namespace: self.containerd_namespace,
        };
        let mut volume_paths = HashMap::new();

        if let Some(volumes) = pod
            .get("spec")
            .and_then(|s| s.get("volumes"))
            .and_then(|v| v.as_array())
        {
            for volume in volumes {
                let volume_name = volume
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Volume missing name"))?;

                let has_supported_type = registry.supported_type(volume).is_some();
                if let Some(path) = registry.resolve_path(volume, volume_name, &ctx).await? {
                    volume_paths.insert(volume_name.to_string(), path);
                } else if !has_supported_type {
                    tracing::warn!("Unsupported volume type for volume {}", volume_name);
                }
            }
        }

        Ok(volume_paths)
    }

    /// Build CRI mounts from a container's `volumeMounts` + a resolved volume path map.
    ///
    /// `resolved_envs` is the fully-resolved env map (all ConfigMap/Secret/fieldRef
    /// references already resolved) used to expand `subPathExpr` patterns.
    /// Pass an empty map when not available; literal `value` env vars in the
    /// container spec are also consulted as a fallback.
    pub fn build_mounts(
        container: &Value,
        volume_paths: &HashMap<String, String>,
        resolved_envs: &HashMap<String, String>,
    ) -> Result<(Vec<Mount>, Vec<std::path::PathBuf>), String> {
        let mut mounts = Vec::new();
        let mut dirs_to_create = Vec::new();

        let env_map = {
            let mut m = HashMap::new();
            if let Some(env_array) = container.get("env").and_then(|e| e.as_array()) {
                for env in env_array {
                    if let (Some(name), Some(value)) = (
                        env.get("name").and_then(|n| n.as_str()),
                        env.get("value").and_then(|v| v.as_str()),
                    ) {
                        m.insert(name.to_string(), value.to_string());
                    }
                }
            }
            for (k, v) in resolved_envs {
                m.insert(k.clone(), v.clone());
            }
            m
        };

        if let Some(volume_mounts) = container.get("volumeMounts").and_then(|v| v.as_array()) {
            for vm in volume_mounts {
                let name = vm.get("name").and_then(|n| n.as_str());
                let mount_path = vm.get("mountPath").and_then(|p| p.as_str());
                let read_only = vm
                    .get("readOnly")
                    .and_then(|r| r.as_bool())
                    .unwrap_or(false);
                let sub_path_expr = vm.get("subPathExpr").and_then(|s| s.as_str());
                let sub_path = if let Some(expr) = sub_path_expr {
                    let expanded =
                        crate::kubelet::pod_env::expand_env_var_references(expr, &env_map);
                    if expanded.starts_with('/') {
                        return Err(format!(
                            "invalid subPath \"{}\": must not be an absolute path",
                            expanded
                        ));
                    }
                    if expanded.split('/').any(|c| c == "..") {
                        return Err(format!(
                            "invalid subPath \"{}\": must not contain '..'",
                            expanded
                        ));
                    }
                    Some(std::borrow::Cow::Owned(expanded))
                } else {
                    vm.get("subPath")
                        .and_then(|s| s.as_str())
                        .map(std::borrow::Cow::Borrowed)
                };
                let sub_path = sub_path.as_deref();

                if let (Some(name), Some(mount_path)) = (name, mount_path)
                    && let Some(host_path) = volume_paths.get(name)
                {
                    let final_host_path = if let Some(sub_path) = sub_path {
                        let base_path = std::path::Path::new(host_path);
                        if base_path.is_dir() {
                            let subpath_full = base_path.join(sub_path);
                            if !read_only {
                                dirs_to_create.push(subpath_full.clone());
                            } else if let Some(parent) = subpath_full.parent() {
                                dirs_to_create.push(parent.to_path_buf());
                            }
                            subpath_full.to_string_lossy().to_string()
                        } else {
                            host_path.clone()
                        }
                    } else {
                        host_path.clone()
                    };

                    mounts.push(Mount {
                        container_path: mount_path.to_string(),
                        host_path: final_host_path,
                        readonly: read_only,
                        selinux_relabel: false,
                        propagation: 0,
                        gid_mappings: vec![],
                        uid_mappings: vec![],
                        image: None,
                        recursive_read_only: false,
                    });
                }
            }
        }

        Ok((mounts, dirs_to_create))
    }
}
