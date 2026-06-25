use super::basics::parse_k8s_quantity;
use super::basics::volumes_root;
use super::run_blocking_fs_keyed;
use super::shared::write_projection_file_blocking;
use crate::kubelet::volume_sources::VolumeSourceReader;
use anyhow::{Context, Result};

#[derive(Clone)]
struct DownwardFileWrite {
    file_name: String,
    bytes: Vec<u8>,
    mode: u32,
}

pub struct DownwardApiVolumeNsRequest<'a> {
    pub sources: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub pod_dir_id: &'a str,
    pub pod_db_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub items: &'a serde_json::Value,
}

pub struct DownwardApiVolumeWithDbNameRequest<'a> {
    pub volumes_root: &'a str,
    pub sources: &'a dyn VolumeSourceReader,
    pub namespace: &'a str,
    pub pod_dir_id: &'a str,
    pub pod_db_name: &'a str,
    pub volume_name: &'a str,
    pub default_mode: Option<u32>,
    pub items: &'a serde_json::Value,
}

fn render_downward_api_volume_blocking(
    volume_path: String,
    writes: Vec<DownwardFileWrite>,
) -> Result<()> {
    std::fs::create_dir_all(&volume_path)
        .with_context(|| format!("Failed to create downwardAPI volume at {}", volume_path))?;
    for entry in writes {
        write_projection_file_blocking(&volume_path, &entry.file_name, &entry.bytes, entry.mode)?;
    }
    Ok(())
}

fn build_downward_api_writes(
    pod_resource: &serde_json::Value,
    default_mode: Option<u32>,
    items: &serde_json::Value,
) -> Result<Vec<DownwardFileWrite>> {
    let default_mode = default_mode.unwrap_or(420); // 0o644
    let items_array = items
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("downwardAPI items must be an array"))?;

    let mut writes = Vec::with_capacity(items_array.len());
    for item in items_array {
        let path = item
            .get("path")
            .and_then(|p| p.as_str())
            .ok_or_else(|| anyhow::anyhow!("downwardAPI item missing path"))?;
        let mode = item
            .get("mode")
            .and_then(|m| m.as_u64())
            .map(|m| m as u32)
            .unwrap_or(default_mode);

        let mut content = String::new();
        if let Some(field_ref) = item.get("fieldRef") {
            let field_path = field_ref
                .get("fieldPath")
                .and_then(|f| f.as_str())
                .ok_or_else(|| anyhow::anyhow!("fieldRef missing fieldPath"))?;
            content = extract_field_ref(pod_resource, field_path)?;
        }
        if let Some(resource_field_ref) = item.get("resourceFieldRef") {
            let container_name = resource_field_ref
                .get("containerName")
                .and_then(|c| c.as_str());
            let resource = resource_field_ref
                .get("resource")
                .and_then(|r| r.as_str())
                .ok_or_else(|| anyhow::anyhow!("resourceFieldRef missing resource"))?;
            content = extract_resource_field_ref(pod_resource, container_name, resource)?;
        }

        writes.push(DownwardFileWrite {
            file_name: path.to_string(),
            bytes: content.into_bytes(),
            mode,
        });
    }
    Ok(writes)
}

async fn render_downward_api_volume_keyed(
    task_label: &'static str,
    volume_path: &str,
    writes: Vec<DownwardFileWrite>,
) -> Result<()> {
    let key = volume_path.to_string();
    let volume_path = volume_path.to_string();
    run_blocking_fs_keyed(task_label, &key, move || {
        render_downward_api_volume_blocking(volume_path, writes)
    })
    .await
}

/// Refreshes downwardAPI and projected volumes when pod metadata changes.
/// Re-renders volume files on disk to reflect updated labels/annotations.
pub async fn refresh_downward_api_volumes(
    pod: &serde_json::Value,
    volumes_root: &str,
) -> Result<()> {
    let namespace = pod
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.namespace"))?;

    let pod_name = pod
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.name"))?;

    let pod_uid = pod
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Pod missing metadata.uid"))?;

    let pod_dir_id =
        crate::kubelet::pod_runtime::service::pod_volume_dir_id(namespace, pod_name, pod_uid);

    let volumes = pod
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array());

    if volumes.is_none() {
        return Ok(()); // No volumes to refresh
    }

    for volume in volumes.unwrap() {
        let volume_name = volume
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("Volume missing name"))?;

        // Handle downwardAPI volumes
        if let Some(downward_api) = volume.get("downwardAPI")
            && let Some(items) = downward_api.get("items")
        {
            let default_mode = downward_api
                .get("defaultMode")
                .and_then(|m| m.as_u64())
                .map(|m| m as u32);
            let volume_path = format!(
                "{}/{}/volumes/downward-api/{}",
                volumes_root, pod_dir_id, volume_name
            );
            let writes = build_downward_api_writes(pod, default_mode, items)?;
            render_downward_api_volume_keyed(
                "refresh_downward_api_volume_render",
                &volume_path,
                writes,
            )
            .await?;
        }

        // Handle projected volumes — refresh downwardAPI sources in-place at the
        // correct volumes/projected/{name}/ path (NOT volumes/downward-api/ which
        // was the old phantom directory bug).
        if let Some(projected) = volume.get("projected")
            && let Some(sources) = projected.get("sources").and_then(|s| s.as_array())
        {
            let volume_path = format!(
                "{}/{}/volumes/projected/{}",
                volumes_root, pod_dir_id, volume_name
            );
            // Only refresh if the directory already exists (created at pod startup)
            if !std::path::Path::new(&volume_path).exists() {
                continue;
            }
            let default_mode = projected
                .get("defaultMode")
                .and_then(|m| m.as_u64())
                .map(|m| m as u32);

            let mut writes = Vec::new();
            for source in sources {
                if let Some(downward_api) = source.get("downwardAPI")
                    && let Some(items) = downward_api.get("items").and_then(|i| i.as_array())
                {
                    for item in items {
                        let path = match item.get("path").and_then(|p| p.as_str()) {
                            Some(p) => p,
                            None => continue,
                        };
                        let content = if let Some(field_ref) = item.get("fieldRef") {
                            let field_path =
                                match field_ref.get("fieldPath").and_then(|f| f.as_str()) {
                                    Some(fp) => fp,
                                    None => continue,
                                };
                            match extract_field_ref(pod, field_path) {
                                Ok(c) => c,
                                Err(_) => continue,
                            }
                        } else {
                            continue; // resourceFieldRef is immutable
                        };
                        let mode = item
                            .get("mode")
                            .and_then(|m| m.as_u64())
                            .map(|m| m as u32)
                            .or(default_mode)
                            .unwrap_or(0o644);
                        writes.push(DownwardFileWrite {
                            file_name: path.to_string(),
                            bytes: content.into_bytes(),
                            mode,
                        });
                    }
                }
            }
            if !writes.is_empty() {
                render_downward_api_volume_keyed(
                    "refresh_projected_downward_api_files",
                    &volume_path,
                    writes,
                )
                .await?;
            }
        }
    }

    Ok(())
}

/// Like create_downward_api_volume but uses separate dir ID and DB name
/// to avoid cross-namespace volume path collisions.
pub async fn create_downward_api_volume_ns(
    request: DownwardApiVolumeNsRequest<'_>,
) -> Result<String> {
    let volumes_root = volumes_root();
    create_downward_api_volume_at_with_db_name(DownwardApiVolumeWithDbNameRequest {
        volumes_root: &volumes_root,
        sources: request.sources,
        namespace: request.namespace,
        pod_dir_id: request.pod_dir_id,
        pod_db_name: request.pod_db_name,
        volume_name: request.volume_name,
        default_mode: request.default_mode,
        items: request.items,
    })
    .await
}

#[cfg(test)]
pub async fn create_downward_api_volume_at(
    volumes_root: &str,
    sources: &dyn VolumeSourceReader,
    namespace: &str,
    pod_name: &str,
    volume_name: &str,
    default_mode: Option<u32>,
    items: &serde_json::Value,
) -> Result<String> {
    let pod_resource = sources
        .pod(namespace, pod_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Pod {}/{} not found", namespace, pod_name))?;

    let volume_path = format!(
        "{}/{}/volumes/downward-api/{}",
        volumes_root, pod_name, volume_name
    );
    let writes = build_downward_api_writes(&pod_resource.data, default_mode, items)?;
    render_downward_api_volume_keyed("create_downward_api_volume_render", &volume_path, writes)
        .await?;
    Ok(volume_path)
}

/// Like create_downward_api_volume_at but uses separate pod_dir_id for paths
/// and pod_db_name for DB lookups to avoid cross-namespace volume collisions.
pub async fn create_downward_api_volume_at_with_db_name(
    request: DownwardApiVolumeWithDbNameRequest<'_>,
) -> Result<String> {
    let pod_resource = request
        .sources
        .pod(request.namespace, request.pod_db_name)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Pod {}/{} not found",
                request.namespace,
                request.pod_db_name
            )
        })?;

    let volume_path = format!(
        "{}/{}/volumes/downward-api/{}",
        request.volumes_root, request.pod_dir_id, request.volume_name
    );
    let writes =
        build_downward_api_writes(&pod_resource.data, request.default_mode, request.items)?;
    render_downward_api_volume_keyed(
        "create_downward_api_volume_with_db_name_render",
        &volume_path,
        writes,
    )
    .await?;
    Ok(volume_path)
}

/// Extract pod field from fieldPath (e.g., "metadata.name", "metadata.labels")
pub fn extract_field_ref(pod_data: &serde_json::Value, field_path: &str) -> Result<String> {
    match field_path {
        "metadata.name" => pod_data
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field metadata.name not found")),
        "metadata.namespace" => pod_data
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field metadata.namespace not found")),
        "metadata.uid" => pod_data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field metadata.uid not found")),
        "metadata.labels" => {
            let labels = pod_data
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.as_object())
                .ok_or_else(|| anyhow::anyhow!("Field metadata.labels not found"))?;
            // Format as key="value"\n per entry with trailing newline (K8s format)
            let mut output = String::new();
            for (k, v) in labels {
                if let Some(val) = v.as_str() {
                    output.push_str(&format!("{}=\"{}\"\n", k, val));
                }
            }
            Ok(output)
        }
        "metadata.annotations" => {
            let annotations = pod_data
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.as_object());
            // Format as key="value"\n per entry with trailing newline (K8s format)
            // Filter out internal klights annotations (klights.dev/*) — K8s kubelet
            // also filters internal annotations like kubernetes.io/config.*
            let mut output = String::new();
            if let Some(annotations) = annotations {
                for (k, v) in annotations {
                    if k.starts_with("klights.dev/") {
                        continue;
                    }
                    if let Some(val) = v.as_str() {
                        output.push_str(&format!("{}=\"{}\"\n", k, val));
                    }
                }
            }
            Ok(output)
        }
        "status.podIP" => pod_data
            .get("status")
            .and_then(|s| s.get("podIP"))
            .and_then(|ip| ip.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field status.podIP not found")),
        "spec.nodeName" => pod_data
            .get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field spec.nodeName not found")),
        "spec.serviceAccountName" => pod_data
            .get("spec")
            .and_then(|s| s.get("serviceAccountName"))
            .and_then(|sa| sa.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Field spec.serviceAccountName not found")),
        _ => anyhow::bail!("Unsupported fieldPath: {}", field_path),
    }
}

/// Extract container resource from resourceFieldRef (e.g., "limits.cpu", "requests.memory")
/// Returns values in K8s downward API format: CPU in whole cores (ceiling), memory in bytes.
pub fn extract_resource_field_ref(
    pod_data: &serde_json::Value,
    container_name: Option<&str>,
    resource: &str,
) -> Result<String> {
    // Get containers array
    let containers = pod_data
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.as_array())
        .ok_or_else(|| anyhow::anyhow!("spec.containers not found"))?;

    // Find the target container (first one if containerName not specified)
    let container = if let Some(name) = container_name {
        containers
            .iter()
            .find(|c| {
                c.get("name")
                    .and_then(|n| n.as_str())
                    .map(|n| n == name)
                    .unwrap_or(false)
            })
            .ok_or_else(|| anyhow::anyhow!("Container {} not found", name))?
    } else {
        containers
            .first()
            .ok_or_else(|| anyhow::anyhow!("No containers in pod"))?
    };

    // Parse resource path (limits.cpu, requests.memory, etc.)
    let parts: Vec<&str> = resource.split('.').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid resource path: {}", resource);
    }

    let (resource_type, resource_name) = (parts[0], parts[1]);

    // Extract raw quantity string from container.resources.
    // K8s stores quantities as strings (e.g., "500m", "256Mi", "2").
    // Also handle numeric JSON values (e.g., cpu: 2 in YAML becomes JSON number).
    let explicit_value = container
        .get("resources")
        .and_then(|r| r.get(resource_type))
        .and_then(|t| t.get(resource_name))
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| v.to_string())
        });

    let raw = match explicit_value {
        Some(v) => v,
        None => {
            if resource_type == "limits" {
                // No limit set — return node allocatable (K8s spec)
                match resource_name {
                    "memory" => {
                        let bytes = crate::kubelet::node::memory_ki() * 1024;
                        return Ok(bytes.to_string());
                    }
                    "cpu" => {
                        // Node allocatable CPU in cores
                        let cores = std::thread::available_parallelism()
                            .map(|n| n.get() as u64)
                            .unwrap_or(1);
                        return Ok(cores.to_string());
                    }
                    _ => return Ok("0".to_string()),
                }
            } else {
                // requests not set → 0
                return Ok("0".to_string());
            }
        }
    };

    // Convert K8s quantity to downward API format (same as resolve_resource_field_ref):
    // - CPU: milliCPU → whole cores (ceiling), "500m" → "1", "2" → "2", "2000m" → "2"
    // - memory/storage: quantity → bytes, "256Mi" → "268435456"
    if resource_name == "cpu" {
        if let Some(millis_str) = raw.strip_suffix('m') {
            let millis = millis_str.parse::<u64>().unwrap_or(0);
            // Ceiling division: millis / 1000 rounded up, minimum 1
            Ok(millis.div_ceil(1000).max(1).to_string())
        } else {
            // Already in whole cores
            Ok(raw)
        }
    } else {
        // memory, ephemeral-storage → bytes
        let bytes_str = parse_k8s_quantity(&raw)
            .map(|bytes| bytes.to_string())
            .unwrap_or(raw);
        Ok(bytes_str)
    }
}
