use anyhow::{Context, Result};

type ProjectionPathMap = std::collections::HashMap<String, (String, Option<u32>)>;

pub fn build_projection_paths<I>(
    items: Option<&serde_json::Value>,
    identity_keys: I,
) -> ProjectionPathMap
where
    I: IntoIterator<Item = String>,
{
    if let Some(items_array) = items.and_then(|i| i.as_array()) {
        items_array
            .iter()
            .filter_map(|item| {
                let key = item.get("key")?.as_str()?;
                let path = item.get("path")?.as_str()?;
                let mode = item.get("mode").and_then(|m| m.as_u64()).map(|m| m as u32);
                Some((key.to_string(), (path.to_string(), mode)))
            })
            .collect()
    } else {
        identity_keys
            .into_iter()
            .map(|key| (key.clone(), (key, None)))
            .collect()
    }
}

pub fn resolve_projection_mode(per_file_mode: Option<u32>, default_mode: Option<u32>) -> u32 {
    per_file_mode.or(default_mode).unwrap_or(0o644)
}

/// A projection file path — the `items[].path` of a configMap/secret/projected/
/// downwardAPI volume, or `serviceAccountToken.path` — must stay inside the
/// volume directory. K8s apiserver rejects absolute paths and any `..`
/// component; we enforce the same so a crafted path cannot escape `base_dir`
/// and let the (root) kubelet write attacker-controlled bytes to an arbitrary
/// host location. This is the last-line, defense-in-depth check; admission
/// validation (`validate_volume_projection_paths`) rejects these at create.
pub fn projection_path_is_safe(relative_path: &str) -> bool {
    if relative_path.is_empty() || relative_path.starts_with('/') {
        return false;
    }
    !relative_path.split('/').any(|component| component == "..")
}

pub fn write_projection_file_blocking(
    base_dir: &str,
    relative_path: &str,
    bytes: &[u8],
    mode: u32,
) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if !projection_path_is_safe(relative_path) {
        anyhow::bail!(
            "refusing to write projection file with unsafe path {:?}: must be relative and must not contain '..'",
            relative_path
        );
    }

    let file_path = format!("{}/{}", base_dir, relative_path);
    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create parent dir for {}", file_path))?;
    }

    // Remove any stale directory at the file path from a previous run.
    let target = std::path::Path::new(&file_path);
    if target.is_dir() {
        std::fs::remove_dir_all(&file_path)
            .with_context(|| format!("Failed to remove stale directory at {}", file_path))?;
    }

    std::fs::write(&file_path, bytes)
        .with_context(|| format!("Failed to write file {}", file_path))?;

    let mut perms = std::fs::metadata(&file_path)?.permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(&file_path, perms)
        .with_context(|| format!("Failed to set permissions on {}", file_path))?;
    Ok(())
}

pub fn clear_volume_dir_contents_blocking(volume_path: &str) -> Result<()> {
    let base = std::path::Path::new(volume_path);
    if !base.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(base)
        .with_context(|| format!("Failed to read volume directory {}", volume_path))?
    {
        let entry = entry.with_context(|| format!("Failed to read entry under {}", volume_path))?;
        let path = entry.path();
        if path.is_dir() {
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("Failed to remove directory {}", path.display()))?;
        } else {
            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove file {}", path.display()))?;
        }
    }

    Ok(())
}

fn prune_projection_dir_blocking(
    base: &std::path::Path,
    current: &std::path::Path,
    desired_paths: &std::collections::HashSet<String>,
) -> Result<bool> {
    let mut is_empty = true;
    for entry in std::fs::read_dir(current)
        .with_context(|| format!("Failed to read directory {}", current.display()))?
    {
        let entry =
            entry.with_context(|| format!("Failed to read entry under {}", current.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("Failed to read type for {}", path.display()))?;
        if file_type.is_dir() {
            let child_empty = prune_projection_dir_blocking(base, &path, desired_paths)?;
            if child_empty {
                std::fs::remove_dir(&path)
                    .with_context(|| format!("Failed to remove directory {}", path.display()))?;
            } else {
                is_empty = false;
            }
            continue;
        }

        let rel = path
            .strip_prefix(base)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        if desired_paths.contains(&rel) {
            is_empty = false;
            continue;
        }

        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to remove stale file {}", path.display()))?;
    }

    Ok(is_empty)
}

pub fn remove_stale_projection_files(
    base_dir: &str,
    desired_paths: &std::collections::HashSet<String>,
) -> Result<()> {
    let base = std::path::Path::new(base_dir);
    if !base.exists() {
        return Ok(());
    }
    let _ = prune_projection_dir_blocking(base, base, desired_paths)?;
    Ok(())
}

pub fn remove_projection_path_blocking(base_dir: &str, relative_path: &str) -> Result<()> {
    if !projection_path_is_safe(relative_path) {
        anyhow::bail!(
            "refusing to remove projection file with unsafe path {:?}: must be relative and must not contain '..'",
            relative_path
        );
    }
    let base = std::path::Path::new(base_dir);
    let target = base.join(relative_path);
    if target.exists() {
        if target.is_dir() {
            std::fs::remove_dir_all(&target)
                .with_context(|| format!("Failed to remove directory {}", target.display()))?;
        } else {
            std::fs::remove_file(&target)
                .with_context(|| format!("Failed to remove file {}", target.display()))?;
        }
    }

    // Best-effort cleanup of now-empty parent directories under base.
    let mut cursor = target.parent();
    while let Some(parent) = cursor {
        if parent == base {
            break;
        }
        let mut iter = match std::fs::read_dir(parent) {
            Ok(i) => i,
            Err(_) => break,
        };
        if iter.next().is_none() {
            let _ = std::fs::remove_dir(parent);
            cursor = parent.parent();
        } else {
            break;
        }
    }
    Ok(())
}
