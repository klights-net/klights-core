use anyhow::{Context, Result};
use serde_json::Value;
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

pub const WRAPPER_MODE_ARG: &str = "__rootless-runc-wrapper";

const DEFAULT_RUNC_BINARY: &str = "/usr/bin/runc";

pub fn run_from_args(args: Vec<OsString>) -> i32 {
    if let Err(err) = sanitize_bundle_from_args(&args) {
        eprintln!("klights rootless runc wrapper: failed to sanitize OCI spec: {err:#}");
        return 1;
    }

    let runc = std::env::var_os("KLIGHTS_RUNC_BINARY")
        .unwrap_or_else(|| OsString::from(DEFAULT_RUNC_BINARY));
    let err = Command::new(runc).arg("--rootless=true").args(args).exec();
    eprintln!("klights rootless runc wrapper: failed to exec runc: {err}");
    127
}

fn sanitize_bundle_from_args(args: &[OsString]) -> Result<()> {
    let Some(bundle_dir) = resolve_bundle_dir(args)? else {
        return Ok(());
    };
    sanitize_bundle_config(&bundle_dir)?;
    Ok(())
}

fn resolve_bundle_dir(args: &[OsString]) -> Result<Option<PathBuf>> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--bundle" || arg == "-b" {
            return Ok(iter.next().map(PathBuf::from));
        }

        if let Some(raw) = arg.to_str().and_then(|s| s.strip_prefix("--bundle=")) {
            return Ok(Some(PathBuf::from(raw)));
        }
    }

    let cwd = std::env::current_dir().context("resolve current directory for runc bundle")?;
    if cwd.join("config.json").exists() {
        Ok(Some(cwd))
    } else {
        Ok(None)
    }
}

fn sanitize_bundle_config(bundle_dir: &Path) -> Result<bool> {
    let config_path = bundle_dir.join("config.json");
    if !config_path.exists() {
        return Ok(false);
    }

    let raw = std::fs::read(&config_path)
        .with_context(|| format!("read OCI config {}", config_path.display()))?;
    let mut spec: Value = serde_json::from_slice(&raw)
        .with_context(|| format!("parse OCI config {}", config_path.display()))?;

    if !sanitize_rootless_oci_spec(&mut spec) {
        return Ok(false);
    }

    let serialized = serde_json::to_vec_pretty(&spec)
        .with_context(|| format!("serialize OCI config {}", config_path.display()))?;
    let tmp_path = config_path.with_extension("json.klights-rootless-tmp");
    std::fs::write(&tmp_path, serialized)
        .with_context(|| format!("write sanitized OCI config {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &config_path).with_context(|| {
        format!(
            "replace OCI config {} with sanitized rootless spec",
            config_path.display()
        )
    })?;
    Ok(true)
}

pub fn sanitize_rootless_oci_spec(spec: &mut Value) -> bool {
    let mut changed = false;

    if let Some(linux) = spec.get_mut("linux").and_then(Value::as_object_mut) {
        changed |= linux.remove("cgroupsPath").is_some();
        changed |= linux.remove("resources").is_some();
    }

    if let Some(process) = spec.get_mut("process").and_then(Value::as_object_mut) {
        changed |= process.remove("oomScoreAdj").is_some();
    }

    changed |= drop_current_namespace_path_entries(spec);
    changed |= rewrite_host_ipc_mqueue_mount(spec);
    changed |= rewrite_host_pid_proc_mount(spec);

    changed
}

fn drop_current_namespace_path_entries(spec: &mut Value) -> bool {
    let Some(namespaces) = spec
        .pointer_mut("/linux/namespaces")
        .and_then(Value::as_array_mut)
    else {
        return false;
    };

    let before = namespaces.len();
    namespaces.retain(|namespace| !namespace_path_targets_current_namespace(namespace));
    namespaces.len() != before
}

fn namespace_path_targets_current_namespace(namespace: &Value) -> bool {
    let Some(namespace_type) = namespace.get("type").and_then(Value::as_str) else {
        return false;
    };
    let Some(proc_namespace_name) = proc_namespace_name(namespace_type) else {
        return false;
    };
    let Some(target_path) = namespace.get("path").and_then(Value::as_str) else {
        return false;
    };

    let Ok(target_namespace) = std::fs::read_link(target_path) else {
        return false;
    };
    let Ok(current_namespace) = std::fs::read_link(format!("/proc/self/ns/{proc_namespace_name}"))
    else {
        return false;
    };

    target_namespace == current_namespace
}

fn proc_namespace_name(oci_namespace_type: &str) -> Option<&'static str> {
    match oci_namespace_type {
        "cgroup" => Some("cgroup"),
        "ipc" => Some("ipc"),
        "mount" => Some("mnt"),
        "network" => Some("net"),
        "pid" => Some("pid"),
        "time" => Some("time"),
        "user" => Some("user"),
        "uts" => Some("uts"),
        _ => None,
    }
}

fn rewrite_host_pid_proc_mount(spec: &mut Value) -> bool {
    if has_private_pid_namespace(spec) {
        return false;
    }

    let Some(mounts) = spec.get_mut("mounts").and_then(Value::as_array_mut) else {
        return false;
    };

    let mut changed = false;
    for mount in mounts {
        let Some(mount_obj) = mount.as_object_mut() else {
            continue;
        };
        let is_proc_mount = mount_obj
            .get("destination")
            .and_then(Value::as_str)
            .is_some_and(|destination| destination == "/proc")
            && mount_obj
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|mount_type| mount_type == "proc");
        if !is_proc_mount {
            continue;
        }

        mount_obj.insert("type".to_string(), Value::String("bind".to_string()));
        mount_obj.insert("source".to_string(), Value::String("/proc".to_string()));
        mount_obj.insert(
            "options".to_string(),
            Value::Array(rootless_host_namespace_bind_mount_options(
                mount_obj.get("options"),
            )),
        );
        changed = true;
    }

    changed
}

fn rewrite_host_ipc_mqueue_mount(spec: &mut Value) -> bool {
    if has_private_ipc_namespace(spec) {
        return false;
    }

    let Some(mounts) = spec.get_mut("mounts").and_then(Value::as_array_mut) else {
        return false;
    };

    let mut changed = false;
    for mount in mounts {
        let Some(mount_obj) = mount.as_object_mut() else {
            continue;
        };
        let is_mqueue_mount = mount_obj
            .get("destination")
            .and_then(Value::as_str)
            .is_some_and(|destination| destination == "/dev/mqueue")
            && mount_obj
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|mount_type| mount_type == "mqueue");
        if !is_mqueue_mount {
            continue;
        }

        mount_obj.insert("type".to_string(), Value::String("bind".to_string()));
        mount_obj.insert(
            "source".to_string(),
            Value::String("/dev/mqueue".to_string()),
        );
        mount_obj.insert(
            "options".to_string(),
            Value::Array(rootless_host_namespace_bind_mount_options(
                mount_obj.get("options"),
            )),
        );
        changed = true;
    }

    changed
}

fn has_private_pid_namespace(spec: &Value) -> bool {
    has_private_namespace(spec, "pid")
}

fn has_private_ipc_namespace(spec: &Value) -> bool {
    has_private_namespace(spec, "ipc")
}

fn has_private_namespace(spec: &Value, target_namespace_type: &str) -> bool {
    spec.pointer("/linux/namespaces")
        .and_then(Value::as_array)
        .is_some_and(|namespaces| {
            namespaces.iter().any(|namespace| {
                namespace
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|namespace_type| namespace_type == target_namespace_type)
            })
        })
}

fn rootless_host_namespace_bind_mount_options(options: Option<&Value>) -> Vec<Value> {
    let mut merged = vec![Value::String("rbind".to_string())];
    if let Some(options) = options.and_then(Value::as_array) {
        for option in options {
            let Some(option) = option.as_str() else {
                continue;
            };
            if option == "bind" || option == "rbind" {
                continue;
            }
            if !merged
                .iter()
                .any(|existing| existing.as_str() == Some(option))
            {
                merged.push(Value::String(option.to_string()));
            }
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sanitize_rootless_oci_spec_removes_cgroup_fields_only() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "linux": {
                "cgroupsPath": "/k8s.io/sandbox-id",
                "resources": {
                    "cpu": {"shares": 2},
                    "devices": [{"allow": false, "access": "rwm"}]
                },
                "namespaces": [{"type": "pid"}],
                "maskedPaths": ["/proc/acpi"]
            },
            "process": {"args": ["/pause"]}
        });

        let changed = sanitize_rootless_oci_spec(&mut spec);

        assert!(changed);
        assert!(spec["linux"].get("cgroupsPath").is_none());
        assert!(spec["linux"].get("resources").is_none());
        assert_eq!(spec["linux"]["namespaces"], json!([{"type": "pid"}]));
        assert_eq!(spec["linux"]["maskedPaths"], json!(["/proc/acpi"]));
        assert_eq!(spec["process"]["args"], json!(["/pause"]));
    }

    #[test]
    fn sanitize_rootless_oci_spec_removes_negative_oom_score_adjustment() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "process": {
                "args": ["/pause"],
                "oomScoreAdj": -998,
                "user": {"uid": 65535, "gid": 65535}
            }
        });

        let changed = sanitize_rootless_oci_spec(&mut spec);

        assert!(changed);
        assert!(spec["process"].get("oomScoreAdj").is_none());
        assert_eq!(spec["process"]["args"], json!(["/pause"]));
        assert_eq!(spec["process"]["user"], json!({"uid": 65535, "gid": 65535}));
    }

    #[test]
    fn sanitize_rootless_oci_spec_rewrites_proc_mount_for_host_pid() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "mounts": [
                {
                    "destination": "/proc",
                    "type": "proc",
                    "source": "proc",
                    "options": ["nosuid", "noexec", "nodev"]
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": "mount"},
                    {"type": "network"},
                    {"type": "ipc"}
                ]
            }
        });

        let changed = sanitize_rootless_oci_spec(&mut spec);

        assert!(changed);
        assert_eq!(spec["mounts"][0]["destination"], json!("/proc"));
        assert_eq!(spec["mounts"][0]["type"], json!("bind"));
        assert_eq!(spec["mounts"][0]["source"], json!("/proc"));
        assert_eq!(
            spec["mounts"][0]["options"],
            json!(["rbind", "nosuid", "noexec", "nodev"])
        );
    }

    #[test]
    fn sanitize_rootless_oci_spec_keeps_proc_mount_for_private_pid_namespace() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "mounts": [
                {
                    "destination": "/proc",
                    "type": "proc",
                    "source": "proc",
                    "options": ["nosuid", "noexec", "nodev"]
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": "mount"},
                    {"type": "pid"},
                    {"type": "network"}
                ]
            }
        });

        assert!(!sanitize_rootless_oci_spec(&mut spec));
        assert_eq!(spec["mounts"][0]["type"], json!("proc"));
        assert_eq!(spec["mounts"][0]["source"], json!("proc"));
        assert_eq!(
            spec["mounts"][0]["options"],
            json!(["nosuid", "noexec", "nodev"])
        );
    }

    #[test]
    fn sanitize_rootless_oci_spec_rewrites_mqueue_mount_for_host_ipc() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "mounts": [
                {
                    "destination": "/dev/mqueue",
                    "type": "mqueue",
                    "source": "mqueue",
                    "options": ["nosuid", "noexec", "nodev"]
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": "mount"},
                    {"type": "uts"}
                ]
            }
        });

        let changed = sanitize_rootless_oci_spec(&mut spec);

        assert!(changed);
        assert_eq!(spec["mounts"][0]["destination"], json!("/dev/mqueue"));
        assert_eq!(spec["mounts"][0]["type"], json!("bind"));
        assert_eq!(spec["mounts"][0]["source"], json!("/dev/mqueue"));
        assert_eq!(
            spec["mounts"][0]["options"],
            json!(["rbind", "nosuid", "noexec", "nodev"])
        );
    }

    #[test]
    fn sanitize_rootless_oci_spec_keeps_mqueue_mount_for_private_ipc_namespace() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "mounts": [
                {
                    "destination": "/dev/mqueue",
                    "type": "mqueue",
                    "source": "mqueue",
                    "options": ["nosuid", "noexec", "nodev"]
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": "mount"},
                    {"type": "ipc"}
                ]
            }
        });

        assert!(!sanitize_rootless_oci_spec(&mut spec));
        assert_eq!(spec["mounts"][0]["type"], json!("mqueue"));
        assert_eq!(spec["mounts"][0]["source"], json!("mqueue"));
        assert_eq!(
            spec["mounts"][0]["options"],
            json!(["nosuid", "noexec", "nodev"])
        );
    }

    #[test]
    fn sanitize_rootless_oci_spec_drops_namespace_paths_already_used_by_wrapper() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "mounts": [
                {
                    "destination": "/proc",
                    "type": "proc",
                    "source": "proc",
                    "options": ["nosuid", "noexec", "nodev"]
                }
            ],
            "linux": {
                "namespaces": [
                    {"type": "pid", "path": "/proc/self/ns/pid"},
                    {"type": "uts", "path": "/proc/self/ns/uts"},
                    {"type": "ipc", "path": "/proc/self/ns/ipc"},
                    {"type": "mount"}
                ]
            }
        });

        let changed = sanitize_rootless_oci_spec(&mut spec);

        assert!(changed);
        assert_eq!(spec["linux"]["namespaces"], json!([{"type": "mount"}]));
        assert_eq!(spec["mounts"][0]["type"], json!("bind"));
        assert_eq!(spec["mounts"][0]["source"], json!("/proc"));
    }

    #[test]
    fn sanitize_rootless_oci_spec_reports_unchanged_without_cgroup_fields() {
        let mut spec = json!({
            "ociVersion": "1.2.0",
            "linux": {
                "namespaces": [{"type": "pid"}]
            }
        });

        assert!(!sanitize_rootless_oci_spec(&mut spec));
    }
}
