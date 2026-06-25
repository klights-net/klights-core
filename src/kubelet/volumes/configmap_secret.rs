use super::basics::volumes_root;
use super::run_blocking_fs_keyed;
use super::shared::{
    build_projection_paths, clear_volume_dir_contents_blocking, remove_projection_path_blocking,
    remove_stale_projection_files, resolve_projection_mode, write_projection_file_blocking,
};
use crate::kubelet::volume_sources::VolumeSourceReader;
use anyhow::{Context, Result};

#[derive(Clone)]
struct ProjectionFileWrite {
    file_name: String,
    bytes: Vec<u8>,
    mode: u32,
}

pub struct ConfigMapVolumeAtRequest<'a> {
    pub volumes_root: &'a str,
    pub sources: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub cm_name: &'a str,
    pub pod_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub items: Option<&'a serde_json::Value>,
}

struct ConfigMapRenderRequest<'a> {
    volumes_root: &'a str,
    cm_resource: &'a serde_json::Value,
    namespace: &'a str,
    cm_name: &'a str,
    pod_name: &'a str,
    volume_name: &'a str,
    default_mode: Option<u32>,
    items: Option<&'a serde_json::Value>,
}

pub struct SecretVolumeAtRequest<'a> {
    pub volumes_root: &'a str,
    pub sources: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub secret_name: &'a str,
    pub pod_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub items: Option<&'a serde_json::Value>,
}

struct SecretRenderRequest<'a> {
    volumes_root: &'a str,
    secret_resource: &'a serde_json::Value,
    namespace: &'a str,
    secret_name: &'a str,
    pod_name: &'a str,
    volume_name: &'a str,
    default_mode: Option<u32>,
    items: Option<&'a serde_json::Value>,
}

fn render_projection_volume_blocking(
    volume_path: String,
    files: Vec<ProjectionFileWrite>,
    desired_paths: std::collections::HashSet<String>,
) -> Result<()> {
    std::fs::create_dir_all(&volume_path)
        .with_context(|| format!("Failed to create volume path {}", volume_path))?;
    for entry in files {
        write_projection_file_blocking(&volume_path, &entry.file_name, &entry.bytes, entry.mode)?;
    }
    remove_stale_projection_files(&volume_path, &desired_paths)?;
    Ok(())
}

async fn render_projection_volume_keyed(
    task_label: &'static str,
    volume_path: &str,
    files: Vec<ProjectionFileWrite>,
    desired_paths: std::collections::HashSet<String>,
) -> Result<()> {
    let key = volume_path.to_string();
    let volume_path = volume_path.to_string();
    run_blocking_fs_keyed(task_label, &key, move || {
        render_projection_volume_blocking(volume_path, files, desired_paths)
    })
    .await
}

/// Creates a ConfigMap volume by rendering ConfigMap data as files
pub async fn create_config_map_volume(
    sources: &dyn VolumeSourceReader,
    namespace: &str,
    cm_name: &str,
    pod_name: &str,
    volume_name: &str,
    default_mode: Option<u32>,
    items: Option<&serde_json::Value>,
) -> Result<String> {
    let volumes_root = volumes_root();
    create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: &volumes_root,
        sources,
        namespace,
        cm_name,
        pod_name,
        volume_name,
        default_mode,
        items,
    })
    .await
}

pub async fn create_config_map_volume_at(request: ConfigMapVolumeAtRequest<'_>) -> Result<String> {
    let cm_resource = request
        .sources
        .config_map(request.namespace, request.cm_name)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ConfigMap {}/{} not found",
                request.namespace,
                request.cm_name
            )
        })?;

    render_config_map_resource_to_volume_at(ConfigMapRenderRequest {
        volumes_root: request.volumes_root,
        cm_resource: cm_resource.data.as_ref(),
        namespace: request.namespace,
        cm_name: request.cm_name,
        pod_name: request.pod_name,
        volume_name: request.volume_name,
        default_mode: request.default_mode,
        items: request.items,
    })
    .await
}

async fn render_config_map_resource_to_volume_at(
    request: ConfigMapRenderRequest<'_>,
) -> Result<String> {
    let ConfigMapRenderRequest {
        volumes_root,
        cm_resource,
        namespace,
        cm_name,
        pod_name,
        volume_name,
        default_mode,
        items,
    } = request;
    let volume_path = format!(
        "{}/{}/volumes/config-map/{}",
        volumes_root, pod_name, volume_name
    );

    let cm_data = cm_resource.get("data").and_then(|d| d.as_object());

    let cm_binary_data = cm_resource.get("binaryData").and_then(|d| d.as_object());

    if cm_data.is_none() && cm_binary_data.is_none() {
        return Err(anyhow::anyhow!(
            "ConfigMap {}/{} has no data or binaryData",
            namespace,
            cm_name
        ));
    }

    // Build key→path mapping from items or use identity mapping.
    // Items filtering applies to both data and binaryData keys.
    let projection_paths = build_projection_paths(
        items,
        cm_data
            .into_iter()
            .flat_map(|d| d.keys().cloned())
            .chain(cm_binary_data.into_iter().flat_map(|bd| bd.keys().cloned())),
    );

    let mut writes: Vec<ProjectionFileWrite> = Vec::new();
    let mut written_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
    use base64::Engine as _;
    for (key, (file_name, per_file_mode)) in projection_paths {
        // Check data first, then binaryData
        let bytes: Vec<u8> =
            if let Some(value) = cm_data.and_then(|d| d.get(&key)).and_then(|v| v.as_str()) {
                value.as_bytes().to_vec()
            } else if let Some(b64) = cm_binary_data
                .and_then(|bd| bd.get(&key))
                .and_then(|v| v.as_str())
            {
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .with_context(|| {
                        format!(
                            "Failed to decode binaryData key '{}' in ConfigMap {}/{}",
                            key, namespace, cm_name
                        )
                    })?
            } else {
                continue;
            };

        let mode = resolve_projection_mode(per_file_mode, default_mode);
        written_paths.insert(file_name.clone());
        writes.push(ProjectionFileWrite {
            file_name,
            bytes,
            mode,
        });
    }

    render_projection_volume_keyed(
        "create_config_map_volume_render",
        &volume_path,
        writes,
        written_paths,
    )
    .await?;

    Ok(volume_path)
}

/// Creates a Secret volume by rendering Secret data as files
pub async fn create_secret_volume(
    sources: &dyn VolumeSourceReader,
    namespace: &str,
    secret_name: &str,
    pod_name: &str,
    volume_name: &str,
    default_mode: Option<u32>,
    items: Option<&serde_json::Value>,
) -> Result<String> {
    let volumes_root = volumes_root();
    create_secret_volume_at(SecretVolumeAtRequest {
        volumes_root: &volumes_root,
        sources,
        namespace,
        secret_name,
        pod_name,
        volume_name,
        default_mode,
        items,
    })
    .await
}

pub async fn create_secret_volume_at(request: SecretVolumeAtRequest<'_>) -> Result<String> {
    let secret_resource = request
        .sources
        .secret(request.namespace, request.secret_name)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Secret {}/{} not found",
                request.namespace,
                request.secret_name
            )
        })?;

    render_secret_resource_to_volume_at(SecretRenderRequest {
        volumes_root: request.volumes_root,
        secret_resource: secret_resource.data.as_ref(),
        namespace: request.namespace,
        secret_name: request.secret_name,
        pod_name: request.pod_name,
        volume_name: request.volume_name,
        default_mode: request.default_mode,
        items: request.items,
    })
    .await
}

async fn render_secret_resource_to_volume_at(request: SecretRenderRequest<'_>) -> Result<String> {
    let SecretRenderRequest {
        volumes_root,
        secret_resource,
        namespace,
        secret_name,
        pod_name,
        volume_name,
        default_mode,
        items,
    } = request;
    let volume_path = format!(
        "{}/{}/volumes/secret/{}",
        volumes_root, pod_name, volume_name
    );

    let secret_data = secret_resource
        .get("data")
        .and_then(|d| d.as_object())
        .ok_or_else(|| anyhow::anyhow!("Secret {}/{} has no data", namespace, secret_name))?;

    // Build key→path mapping from items or use identity mapping
    let projection_paths = build_projection_paths(items, secret_data.keys().cloned());

    let mut writes: Vec<ProjectionFileWrite> = Vec::new();
    let mut written_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
    use base64::Engine;
    for (key, (file_name, per_file_mode)) in projection_paths {
        if let Some(encoded) = secret_data.get(&key).and_then(|v| v.as_str()) {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .with_context(|| format!("Failed to decode secret data for key {}", key))?;
            let mode = resolve_projection_mode(per_file_mode, default_mode);
            written_paths.insert(file_name.clone());
            writes.push(ProjectionFileWrite {
                file_name,
                bytes: decoded,
                mode,
            });
        }
    }

    render_projection_volume_keyed(
        "create_secret_volume_render",
        &volume_path,
        writes,
        written_paths,
    )
    .await?;

    Ok(volume_path)
}

/// Refresh Secret and ConfigMap volumes for all running pods that reference a given resource.
/// Watch events are authoritative for updates and deletes so workers do not
/// re-render stale local datastore snapshots.
#[derive(Clone, Copy)]
enum RefreshResourceSource<'a> {
    Event(&'a serde_json::Value),
    Deleted,
}

pub async fn refresh_secret_configmap_volumes_from_event(
    kind: &str,
    namespace: &str,
    name: &str,
    event_resource: &serde_json::Value,
    volumes_root: &str,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
) -> Result<()> {
    refresh_secret_configmap_volumes_inner(
        kind,
        namespace,
        name,
        RefreshResourceSource::Event(event_resource),
        volumes_root,
        pod_reader,
    )
    .await
}

pub async fn refresh_secret_configmap_volumes_after_delete(
    kind: &str,
    namespace: &str,
    name: &str,
    volumes_root: &str,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
) -> Result<()> {
    refresh_secret_configmap_volumes_inner(
        kind,
        namespace,
        name,
        RefreshResourceSource::Deleted,
        volumes_root,
        pod_reader,
    )
    .await
}

async fn refresh_secret_configmap_volumes_inner(
    kind: &str,
    namespace: &str,
    name: &str,
    source: RefreshResourceSource<'_>,
    volumes_root: &str,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
) -> Result<()> {
    let latest_resource = match source {
        RefreshResourceSource::Event(resource) => Some(resource),
        RefreshResourceSource::Deleted => None,
    };
    let resource_exists = latest_resource.is_some();

    // List all pods in this namespace through the pod repository so the
    // v1/Pod read boundary stays inside `PodStore`.
    let pods = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?;

    for pod_resource in &pods.items {
        let pod = &pod_resource.data;
        let pod_name = pod
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let pod_ns = pod
            .pointer("/metadata/namespace")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let Some(pod_uid) = pod.pointer("/metadata/uid").and_then(|uid| uid.as_str()) else {
            tracing::warn!(
                "Skipping {} volume refresh for pod {}/{} without metadata.uid",
                kind,
                pod_ns,
                pod_name
            );
            continue;
        };
        let pod_dir_id =
            crate::kubelet::pod_runtime::service::pod_volume_dir_id(pod_ns, pod_name, pod_uid);

        let volumes = match pod.pointer("/spec/volumes").and_then(|v| v.as_array()) {
            Some(v) => v,
            None => continue,
        };

        for volume in volumes {
            let volume_name = match volume.get("name").and_then(|n| n.as_str()) {
                Some(n) => n,
                None => continue,
            };

            // Check if this volume references the changed resource
            let references_resource = match kind {
                "Secret" => {
                    volume
                        .get("secret")
                        .and_then(|s| s.get("secretName"))
                        .and_then(|n| n.as_str())
                        == Some(name)
                }
                "ConfigMap" => {
                    volume
                        .get("configMap")
                        .and_then(|c| c.get("name"))
                        .and_then(|n| n.as_str())
                        == Some(name)
                }
                _ => false,
            };

            if !references_resource {
                // Also check projected volume sources
                let has_projected_ref = volume
                    .get("projected")
                    .and_then(|p| p.get("sources"))
                    .and_then(|s| s.as_array())
                    .map(|sources| {
                        sources.iter().any(|src| match kind {
                            "Secret" => {
                                src.get("secret")
                                    .and_then(|s| s.get("name"))
                                    .and_then(|n| n.as_str())
                                    == Some(name)
                            }
                            "ConfigMap" => {
                                src.get("configMap")
                                    .and_then(|c| c.get("name"))
                                    .and_then(|n| n.as_str())
                                    == Some(name)
                            }
                            _ => false,
                        })
                    })
                    .unwrap_or(false);

                if !has_projected_ref {
                    continue;
                }
            }

            // Determine the volume directory and re-render
            if references_resource {
                let vol_type_dir = if kind == "Secret" {
                    "secret"
                } else {
                    "config-map"
                };
                let volume_path = format!(
                    "{}/{}/volumes/{}/{}",
                    volumes_root, pod_dir_id, vol_type_dir, volume_name
                );

                // Only refresh if the directory exists (volume was mounted at pod startup)
                if !std::path::Path::new(&volume_path).exists() {
                    continue;
                }

                if !resource_exists {
                    {
                        let vp = volume_path.clone();
                        let key = vp.clone();
                        run_blocking_fs_keyed(
                            "refresh_clear_volume_dir_contents",
                            &key,
                            move || clear_volume_dir_contents_blocking(&vp),
                        )
                        .await?;
                    }
                    tracing::debug!(
                        "Cleared {} volume {}/{} after source deletion",
                        kind,
                        pod_name,
                        volume_name
                    );
                    continue;
                }

                if kind == "Secret" {
                    let items = volume.get("secret").and_then(|s| s.get("items"));
                    let default_mode = volume
                        .get("secret")
                        .and_then(|s| s.get("defaultMode"))
                        .and_then(|m| m.as_u64())
                        .map(|m| m as u32);
                    if let Some(resource) = latest_resource {
                        match render_secret_resource_to_volume_at(SecretRenderRequest {
                            volumes_root,
                            secret_resource: resource,
                            namespace,
                            secret_name: name,
                            pod_name: &pod_dir_id,
                            volume_name,
                            default_mode,
                            items,
                        })
                        .await
                        {
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to refresh secret volume {}/{}: {}",
                                    pod_name,
                                    volume_name,
                                    e
                                );
                            }
                            _ => {
                                tracing::debug!(
                                    "Refreshed secret volume {}/{} for pod {}",
                                    name,
                                    volume_name,
                                    pod_name
                                );
                            }
                        }
                    } else {
                        continue;
                    }
                } else {
                    let items = volume.get("configMap").and_then(|c| c.get("items"));
                    let default_mode = volume
                        .get("configMap")
                        .and_then(|c| c.get("defaultMode"))
                        .and_then(|m| m.as_u64())
                        .map(|m| m as u32);
                    if let Some(resource) = latest_resource {
                        match render_config_map_resource_to_volume_at(ConfigMapRenderRequest {
                            volumes_root,
                            cm_resource: resource,
                            namespace,
                            cm_name: name,
                            pod_name: &pod_dir_id,
                            volume_name,
                            default_mode,
                            items,
                        })
                        .await
                        {
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to refresh configmap volume {}/{}: {}",
                                    pod_name,
                                    volume_name,
                                    e
                                );
                            }
                            _ => {
                                tracing::debug!(
                                    "Refreshed configmap volume {}/{} for pod {}",
                                    name,
                                    volume_name,
                                    pod_name
                                );
                            }
                        }
                    } else {
                        continue;
                    }
                }
            } else {
                // Projected volume — re-render the projected source
                let projected_vol_path = format!(
                    "{}/{}/volumes/projected/{}",
                    volumes_root, pod_dir_id, volume_name
                );
                if !std::path::Path::new(&projected_vol_path).exists() {
                    continue;
                }

                let sources = volume
                    .get("projected")
                    .and_then(|p| p.get("sources"))
                    .and_then(|s| s.as_array());
                let default_mode = volume
                    .get("projected")
                    .and_then(|p| p.get("defaultMode"))
                    .and_then(|m| m.as_u64())
                    .map(|m| m as u32);

                if let Some(sources) = sources {
                    let source_count = sources.len();
                    for source in sources {
                        let (_src_kind, src_name) = if kind == "Secret" {
                            if let Some(s) = source.get("secret") {
                                ("Secret", s.get("name").and_then(|n| n.as_str()))
                            } else {
                                continue;
                            }
                        } else if let Some(c) = source.get("configMap") {
                            ("ConfigMap", c.get("name").and_then(|n| n.as_str()))
                        } else {
                            continue;
                        };

                        if src_name != Some(name) {
                            continue;
                        }

                        if !resource_exists {
                            let items_spec = if kind == "Secret" {
                                source.get("secret").and_then(|s| s.get("items"))
                            } else {
                                source.get("configMap").and_then(|c| c.get("items"))
                            };

                            if items_spec.and_then(|i| i.as_array()).is_some() {
                                let projection_paths = build_projection_paths(
                                    items_spec,
                                    std::iter::empty::<String>(),
                                );
                                for (_, (file_name, _)) in projection_paths {
                                    let pv = projected_vol_path.clone();
                                    let name = file_name.clone();
                                    let key = pv.clone();
                                    if let Err(e) = run_blocking_fs_keyed(
                                        "refresh_remove_projection_path",
                                        &key,
                                        move || remove_projection_path_blocking(&pv, &name),
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            "Failed to remove projected file {}: {}",
                                            format!("{}/{}", projected_vol_path, file_name),
                                            e
                                        );
                                    }
                                }
                            } else if source_count == 1 {
                                {
                                    let pvp = projected_vol_path.clone();
                                    let key = pvp.clone();
                                    run_blocking_fs_keyed(
                                        "refresh_clear_projected_dir_contents",
                                        &key,
                                        move || clear_volume_dir_contents_blocking(&pvp),
                                    )
                                    .await?;
                                }
                            } else {
                                tracing::warn!(
                                    "Cannot safely clear projected source {}/{} in multi-source volume {} without explicit items mapping",
                                    namespace,
                                    name,
                                    volume_name
                                );
                            }
                            continue;
                        }

                        // Re-render this projected source
                        if let Some(resource) = latest_resource {
                            let data = resource.get("data").and_then(|d| d.as_object());
                            let items_spec = if kind == "Secret" {
                                source.get("secret").and_then(|s| s.get("items"))
                            } else {
                                source.get("configMap").and_then(|c| c.get("items"))
                            };

                            if let Some(data) = data {
                                let projection_paths =
                                    build_projection_paths(items_spec, data.keys().cloned());

                                for (key, (file_name, per_file_mode)) in projection_paths {
                                    if let Some(value) = data.get(&key) {
                                        let content = if kind == "Secret" {
                                            // base64 decode
                                            use base64::Engine;
                                            match value.as_str().and_then(|s| {
                                                base64::engine::general_purpose::STANDARD
                                                    .decode(s)
                                                    .ok()
                                            }) {
                                                Some(decoded) => decoded,
                                                None => continue,
                                            }
                                        } else {
                                            match value.as_str() {
                                                Some(s) => s.as_bytes().to_vec(),
                                                None => continue,
                                            }
                                        };

                                        let mode =
                                            resolve_projection_mode(per_file_mode, default_mode);
                                        let pvp = projected_vol_path.clone();
                                        let name = file_name.clone();
                                        let key = pvp.clone();
                                        if let Err(e) = run_blocking_fs_keyed(
                                            "refresh_projected_source_write_file",
                                            &key,
                                            move || {
                                                write_projection_file_blocking(
                                                    &pvp, &name, &content, mode,
                                                )
                                            },
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                "Failed to refresh projected volume file {}: {}",
                                                format!("{}/{}", projected_vol_path, file_name),
                                                e
                                            );
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
