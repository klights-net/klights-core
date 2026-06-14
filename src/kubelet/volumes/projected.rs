use super::basics::volumes_root;
use super::downward_api::{extract_field_ref, extract_resource_field_ref};
use super::run_blocking_fs_keyed;
use super::shared::{
    build_projection_paths, remove_stale_projection_files, resolve_projection_mode,
    write_projection_file_blocking,
};
use crate::kubelet::volume_sources::VolumeSourceReader;
use anyhow::{Context, Result};
use std::collections::HashSet;

#[derive(Clone)]
struct ProjectionFileWrite {
    file_name: String,
    bytes: Vec<u8>,
    mode: u32,
}

struct ProjectedRenderPlanRequest<'a> {
    source_reader: &'a dyn VolumeSourceReader,
    namespace: &'a str,
    pod_lookup_name: &'a str,
    default_mode: Option<u32>,
    sources: &'a serde_json::Value,
    token: Option<&'a str>,
}

struct ProjectedVolumePathRequest<'a> {
    volumes_root: &'a str,
    source_reader: &'a dyn VolumeSourceReader,
    namespace: &'a str,
    pod_dir_id: &'a str,
    pod_lookup_name: &'a str,
    volume_name: &'a str,
    default_mode: Option<u32>,
    sources: &'a serde_json::Value,
    token: Option<&'a str>,
}

pub struct ProjectedVolumeNsRequest<'a> {
    pub source_reader: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub pod_dir_id: &'a str,
    pub pod_db_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub sources: &'a serde_json::Value,
    pub token: Option<&'a str>,
}

#[cfg(test)]
pub struct ProjectedVolumeAtRequest<'a> {
    pub volumes_root: &'a str,
    pub source_reader: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub pod_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub sources: &'a serde_json::Value,
    pub token: Option<&'a str>,
}

fn render_projected_volume_blocking(
    volume_path: String,
    writes: Vec<ProjectionFileWrite>,
    desired_paths: HashSet<String>,
) -> Result<()> {
    std::fs::create_dir_all(&volume_path)
        .with_context(|| format!("Failed to create projected volume at {}", volume_path))?;
    for entry in writes {
        write_projection_file_blocking(&volume_path, &entry.file_name, &entry.bytes, entry.mode)?;
    }
    remove_stale_projection_files(&volume_path, &desired_paths)?;
    Ok(())
}

async fn render_projected_volume_keyed(
    task_label: &'static str,
    volume_path: &str,
    writes: Vec<ProjectionFileWrite>,
    desired_paths: HashSet<String>,
) -> Result<()> {
    let key = volume_path.to_string();
    let volume_path = volume_path.to_string();
    run_blocking_fs_keyed(task_label, &key, move || {
        render_projected_volume_blocking(volume_path, writes, desired_paths)
    })
    .await
}

async fn build_projected_render_plan(
    request: ProjectedRenderPlanRequest<'_>,
) -> Result<(Vec<ProjectionFileWrite>, HashSet<String>)> {
    let ProjectedRenderPlanRequest {
        source_reader,
        namespace,
        pod_lookup_name,
        default_mode,
        sources,
        token,
    } = request;
    let sources_array = sources
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("projected volume sources must be an array"))?;

    let mut writes: Vec<ProjectionFileWrite> = Vec::new();
    let mut desired_paths: HashSet<String> = HashSet::new();

    for source in sources_array {
        if let Some(sa_token) = source.get("serviceAccountToken") {
            let path = sa_token
                .get("path")
                .and_then(|p| p.as_str())
                .unwrap_or("token");
            let token_value = sa_token
                .get("tokenValue")
                .and_then(|v| v.as_str())
                .or(token)
                .ok_or_else(|| {
                    anyhow::anyhow!("ServiceAccount token required for projected volume")
                })?;
            let file_name = path.to_string();
            desired_paths.insert(file_name.clone());
            writes.push(ProjectionFileWrite {
                file_name,
                bytes: token_value.as_bytes().to_vec(),
                mode: default_mode.unwrap_or(0o644),
            });
        }

        if let Some(config_map) = source.get("configMap") {
            let cm_name = config_map
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| anyhow::anyhow!("configMap source missing name"))?;
            let optional = config_map
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);
            let items = config_map.get("items");

            let Some(cm_resource) = source_reader.config_map(namespace, cm_name).await? else {
                if optional {
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "ConfigMap {}/{} not found",
                    namespace,
                    cm_name
                ));
            };

            let cm_data = cm_resource.data.get("data").and_then(|d| d.as_object());
            let cm_binary_data = cm_resource
                .data
                .get("binaryData")
                .and_then(|d| d.as_object());
            if cm_data.is_none() && cm_binary_data.is_none() {
                if optional {
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "ConfigMap {}/{} has no data or binaryData",
                    namespace,
                    cm_name
                ));
            }

            let projection_paths = build_projection_paths(
                items,
                cm_data
                    .into_iter()
                    .flat_map(|d| d.keys().cloned())
                    .chain(cm_binary_data.into_iter().flat_map(|bd| bd.keys().cloned())),
            );

            use base64::Engine as _;
            for (key, (file_name, per_file_mode)) in projection_paths {
                let bytes: Vec<u8> = if let Some(value_str) =
                    cm_data.and_then(|d| d.get(&key)).and_then(|v| v.as_str())
                {
                    value_str.as_bytes().to_vec()
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
                    if optional {
                        continue;
                    }
                    tracing::error!(
                        configmap = cm_name,
                        namespace = namespace,
                        key = key,
                        "ConfigMap key not found in data or binaryData"
                    );
                    return Err(anyhow::anyhow!(
                        "ConfigMap {}/{} does not contain key '{}'",
                        namespace,
                        cm_name,
                        key
                    ));
                };
                let mode = resolve_projection_mode(per_file_mode, default_mode);
                desired_paths.insert(file_name.clone());
                writes.push(ProjectionFileWrite {
                    file_name,
                    bytes,
                    mode,
                });
            }
        }

        if let Some(secret) = source.get("secret") {
            let secret_name = secret
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| anyhow::anyhow!("secret source missing name"))?;
            let optional = secret
                .get("optional")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);
            let items = secret.get("items");

            let Some(secret_resource) = source_reader.secret(namespace, secret_name).await? else {
                if optional {
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "Secret {}/{} not found",
                    namespace,
                    secret_name
                ));
            };
            let Some(secret_data) = secret_resource.data.get("data").and_then(|d| d.as_object())
            else {
                if optional {
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "Secret {}/{} has no data",
                    namespace,
                    secret_name
                ));
            };

            let projection_paths = build_projection_paths(items, secret_data.keys().cloned());
            use base64::Engine;
            for (key, (file_name, per_file_mode)) in projection_paths {
                let Some(encoded) = secret_data.get(&key) else {
                    if optional {
                        continue;
                    }
                    tracing::error!(
                        secret = secret_name,
                        namespace = namespace,
                        key = key,
                        "Secret key not found in data"
                    );
                    return Err(anyhow::anyhow!(
                        "Secret {}/{} does not contain key '{}'",
                        namespace,
                        secret_name,
                        key
                    ));
                };
                let encoded_str = encoded.as_str().ok_or_else(|| {
                    tracing::error!(
                        secret = secret_name,
                        namespace = namespace,
                        key = key,
                        value_type = ?encoded,
                        "Secret value is not a string type"
                    );
                    anyhow::anyhow!(
                        "Secret {}/{} key '{}' has non-string value: {:?}",
                        namespace,
                        secret_name,
                        key,
                        encoded
                    )
                })?;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded_str)
                    .with_context(|| format!("Failed to decode secret data for key {}", key))?;
                let mode = resolve_projection_mode(per_file_mode, default_mode);
                desired_paths.insert(file_name.clone());
                writes.push(ProjectionFileWrite {
                    file_name,
                    bytes: decoded,
                    mode,
                });
            }
        }

        if let Some(downward_api) = source.get("downwardAPI") {
            let items = downward_api
                .get("items")
                .ok_or_else(|| anyhow::anyhow!("downwardAPI source missing items"))?;
            let pod_resource = source_reader
                .pod(namespace, pod_lookup_name)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("Pod {}/{} not found", namespace, pod_lookup_name)
                })?;
            let items_array = items
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("downwardAPI items must be an array"))?;
            for item in items_array {
                let path = item
                    .get("path")
                    .and_then(|p| p.as_str())
                    .ok_or_else(|| anyhow::anyhow!("downwardAPI item missing path"))?;
                let per_file_mode = item.get("mode").and_then(|m| m.as_u64()).map(|m| m as u32);
                let content = if let Some(field_ref) = item.get("fieldRef") {
                    let field_path = field_ref
                        .get("fieldPath")
                        .and_then(|f| f.as_str())
                        .ok_or_else(|| anyhow::anyhow!("fieldRef missing fieldPath"))?;
                    extract_field_ref(&pod_resource.data, field_path)?
                } else if let Some(resource_ref) = item.get("resourceFieldRef") {
                    let resource = resource_ref
                        .get("resource")
                        .and_then(|r| r.as_str())
                        .ok_or_else(|| anyhow::anyhow!("resourceFieldRef missing resource"))?;
                    let container_name = resource_ref.get("containerName").and_then(|c| c.as_str());
                    extract_resource_field_ref(&pod_resource.data, container_name, resource)?
                } else {
                    anyhow::bail!("downwardAPI item must have fieldRef or resourceFieldRef");
                };
                let file_name = path.to_string();
                let mode = resolve_projection_mode(per_file_mode, default_mode);
                desired_paths.insert(file_name.clone());
                writes.push(ProjectionFileWrite {
                    file_name,
                    bytes: content.as_bytes().to_vec(),
                    mode,
                });
            }
        }
    }

    Ok((writes, desired_paths))
}

async fn create_projected_volume_at_impl(
    request: ProjectedVolumePathRequest<'_>,
) -> Result<String> {
    let ProjectedVolumePathRequest {
        volumes_root,
        source_reader,
        namespace,
        pod_dir_id,
        pod_lookup_name,
        volume_name,
        default_mode,
        sources,
        token,
    } = request;
    let volume_path = format!(
        "{}/{}/volumes/projected/{}",
        volumes_root, pod_dir_id, volume_name
    );
    let (writes, desired_paths) = build_projected_render_plan(ProjectedRenderPlanRequest {
        source_reader,
        namespace,
        pod_lookup_name,
        default_mode,
        sources,
        token,
    })
    .await?;
    render_projected_volume_keyed(
        "create_projected_volume_render",
        &volume_path,
        writes,
        desired_paths,
    )
    .await?;
    Ok(volume_path)
}

/// Like create_projected_volume but uses separate dir ID and DB name
/// to avoid cross-namespace volume path collisions.
pub async fn create_projected_volume_ns(request: ProjectedVolumeNsRequest<'_>) -> Result<String> {
    let volumes_root = volumes_root();
    create_projected_volume_at_impl(ProjectedVolumePathRequest {
        volumes_root: &volumes_root,
        source_reader: request.source_reader,
        namespace: request.namespace,
        pod_dir_id: request.pod_dir_id,
        pod_lookup_name: request.pod_db_name,
        volume_name: request.volume_name,
        default_mode: request.default_mode,
        sources: request.sources,
        token: request.token,
    })
    .await
}

#[cfg(test)]
pub async fn create_projected_volume_at(request: ProjectedVolumeAtRequest<'_>) -> Result<String> {
    create_projected_volume_at_impl(ProjectedVolumePathRequest {
        volumes_root: request.volumes_root,
        source_reader: request.source_reader,
        namespace: request.namespace,
        pod_dir_id: request.pod_name,
        pod_lookup_name: request.pod_name,
        volume_name: request.volume_name,
        default_mode: request.default_mode,
        sources: request.sources,
        token: request.token,
    })
    .await
}
