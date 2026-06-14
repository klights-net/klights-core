use anyhow::{Context, Result};
use inotify::{Inotify, WatchMask};
use nix::mount::{MsFlags, mount};
use nix::sched::{CloneFlags, unshare};
use std::collections::BTreeSet;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::unix::AsyncFd;

/// Parse inotify value from /proc/sys format (e.g., "128\n" → 128)
fn parse_inotify_value(s: &str) -> Result<u32> {
    s.trim()
        .parse::<u32>()
        .context("Failed to parse inotify value")
}

/// Check if inotify limit should be increased
fn should_increase_inotify(current: u32, minimum: u32) -> bool {
    current < minimum
}

/// Ensure inotify limits are sufficient for containerd CRI plugin
fn ensure_inotify_limits() -> Result<()> {
    const INOTIFY_PATH: &str = "/proc/sys/fs/inotify/max_user_instances";
    const MINIMUM_INSTANCES: u32 = 1024;

    // Read current value
    let current_str = crate::utils::read_utf8_file(INOTIFY_PATH)
        .context("Failed to read /proc/sys/fs/inotify/max_user_instances")?;
    let current = parse_inotify_value(&current_str)?;

    // Check if increase is needed
    if should_increase_inotify(current, MINIMUM_INSTANCES) {
        // Write new value
        std::fs::write(INOTIFY_PATH, MINIMUM_INSTANCES.to_string())
            .context("Failed to write to /proc/sys/fs/inotify/max_user_instances")?;
        tracing::info!(
            "Increased fs.inotify.max_user_instances from {} to {}",
            current,
            MINIMUM_INSTANCES
        );
    } else {
        tracing::debug!(
            "fs.inotify.max_user_instances already sufficient: {}",
            current
        );
    }

    Ok(())
}

/// Check whether the current process is already in a mount namespace that is
/// isolated from init (systemd). When klights runs inside a netns (or any
/// container), its mount namespace already differs from PID 1 — new mounts
/// created here do not reach the host's systemd, so containerd can safely
/// share klights' mount namespace with full propagation intact.
fn is_isolated_mount_namespace() -> bool {
    match (
        std::fs::read_link("/proc/self/ns/mnt"),
        std::fs::read_link("/proc/1/ns/mnt"),
    ) {
        (Ok(s), Ok(i)) => s != i,
        _ => false,
    }
}

fn containerd_netns_mounts_under_state_dir(rootless: bool, isolated_mount_namespace: bool) -> bool {
    rootless || isolated_mount_namespace
}

/// Configure a Command to spawn its child in a private mount namespace with
/// slave propagation. Host mounts (CSI/CNI) propagate INTO the child, but the
/// child's per-pod mounts (rootfs, shm, netns) DO NOT propagate to the host.
///
/// This prevents systemd PID 1 from registering thousands of transient `.mount`
/// units during pod churn — without that isolation PID 1 pegs near 100% CPU
/// during e2e tests as it parses mountinfo on every change.
///
/// When klights already runs in an isolated mount namespace (e.g. inside a
/// netns), the isolation is unnecessary — mount events cannot reach the host's
/// systemd, and unshare+MS_SLAVE would break propagation of klights' own
/// mounts (emptyDir tmpfs, etc.) into containerd.
///
/// Requires CAP_SYS_ADMIN (klights runs as root).
fn apply_private_mount_namespace(cmd: &mut std::process::Command) {
    if is_isolated_mount_namespace() {
        return;
    }
    // SAFETY: pre_exec runs in the forked child between fork() and exec().
    // unshare(2) and mount(2) are async-signal-safe, so calling them here is sound.
    unsafe {
        cmd.pre_exec(|| {
            unshare(CloneFlags::CLONE_NEWNS)
                .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            mount(
                None::<&str>,
                "/",
                None::<&str>,
                MsFlags::MS_REC | MsFlags::MS_SLAVE,
                None::<&str>,
            )
            .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
            Ok(())
        });
    }
}

/// Wait for socket file creation using inotify
async fn wait_for_socket_inotify(socket_path: &str, socket_dir: &Path) -> Result<()> {
    let inotify = Inotify::init().context("Failed to initialize inotify")?;

    // Watch parent directory for file creation
    inotify
        .watches()
        .add(socket_dir, WatchMask::CREATE)
        .context("Failed to add inotify watch")?;

    let mut async_fd = AsyncFd::new(inotify).context("Failed to create AsyncFd for inotify")?;

    // Extract socket filename for comparison
    let socket_filename = Path::new(socket_path)
        .file_name()
        .context("Socket path has no filename")?;

    loop {
        // Wait for readability (inotify event available)
        let mut guard = async_fd
            .readable_mut()
            .await
            .context("inotify AsyncFd error")?;

        // Read events (non-blocking)
        let inotify = guard.get_inner_mut();
        let mut buffer = [0u8; 4096];
        let events = inotify
            .read_events(&mut buffer)
            .context("Failed to read inotify events")?;

        for event in events {
            if let Some(name) = event.name
                && name == socket_filename
            {
                tracing::info!("Containerd socket created: {}", socket_path);
                return Ok(());
            }
        }

        // Clear readiness and loop again
        guard.clear_ready();

        // Double-check if socket now exists (in case event was missed)
        if Path::new(socket_path).exists() {
            tracing::info!("Containerd socket exists: {}", socket_path);
            return Ok(());
        }
    }
}

pub struct ContainerdManager {
    process: ContainerdProcess,
    socket_path: String,
    _config_path: String,
    _data_dir: String,
    _state_dir: String,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

pub struct ContainerdStartConfig<'a> {
    pub namespace: &'a str,
    pub bridge_name: &'a str,
    pub pod_subnet: &'a str,
    pub pod_link_mtu: u32,
    pub data_dir: &'a str,
    pub state_dir: &'a str,
    pub rootless: bool,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
}

enum ContainerdProcess {
    Spawned(tokio::process::Child),
    Reused,
}

impl ContainerdManager {
    fn rootless_runc_wrapper_path(data_dir: &str) -> PathBuf {
        let mut wrapper = PathBuf::from(data_dir);
        wrapper.pop(); // up to containerd/
        wrapper.pop(); // up to klights data root/
        wrapper.push("runc-wrapper.sh");
        wrapper
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\\''"))
    }

    fn rootless_runc_wrapper_script(klights_binary: &str) -> String {
        format!(
            "#!/bin/sh\nexec {} {} \"$@\"\n",
            Self::shell_quote(klights_binary),
            crate::kubelet::rootless_runc_wrapper::WRAPPER_MODE_ARG
        )
    }

    async fn write_rootless_runc_wrapper(data_dir: &str) -> Result<()> {
        let current_exe =
            std::env::current_exe().context("Failed to resolve current executable")?;
        let current_exe = current_exe.to_string_lossy().into_owned();
        let wrapper_path = Self::rootless_runc_wrapper_path(data_dir);
        let wrapper_parent = wrapper_path
            .parent()
            .context("rootless runc wrapper path has no parent")?
            .to_path_buf();
        let script = Self::rootless_runc_wrapper_script(&current_exe);
        let key = wrapper_path.display().to_string();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_write_rootless_runc_wrapper",
            key,
            move || {
                use std::os::unix::fs::PermissionsExt;

                std::fs::create_dir_all(&wrapper_parent).with_context(|| {
                    format!(
                        "Failed to create rootless runc wrapper directory {}",
                        wrapper_parent.display()
                    )
                })?;
                std::fs::write(&wrapper_path, script).with_context(|| {
                    format!(
                        "Failed to write rootless runc wrapper {}",
                        wrapper_path.display()
                    )
                })?;
                let mut permissions = std::fs::metadata(&wrapper_path)
                    .with_context(|| {
                        format!(
                            "Failed to stat rootless runc wrapper {}",
                            wrapper_path.display()
                        )
                    })?
                    .permissions();
                permissions.set_mode(0o755);
                std::fs::set_permissions(&wrapper_path, permissions).with_context(|| {
                    format!(
                        "Failed to chmod rootless runc wrapper {}",
                        wrapper_path.display()
                    )
                })?;
                Ok(())
            },
        )
        .await
    }

    /// Write the klights Rust CNI config.
    ///
    /// containerd CRI requires an IP-bearing CNI result before RunPodSandbox
    /// returns, so the plugin receives the node-local subnet selected by klights.
    async fn write_cni_config(
        cni_conf_dir: &str,
        namespace: &str,
        bridge_name: &str,
        pod_subnet: &str,
        pod_link_mtu: u32,
    ) -> Result<()> {
        let cni_conf_dir_owned = cni_conf_dir.to_string();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_write_cni_create_dir",
            cni_conf_dir_owned.clone(),
            move || {
                std::fs::create_dir_all(&cni_conf_dir_owned)
                    .context("Failed to create CNI config directory")
            },
        )
        .await?;

        let cni_config = serde_json::json!({
            "cniVersion": "1.0.0",
            "name": bridge_name,
            "type": "klights-cni",
            "bridge": bridge_name,
            "subnet": pod_subnet,
            "mtu": pod_link_mtu,
            "rpcSocket": crate::cni_plugin::rpc_socket_path(namespace)
        });

        let config_json =
            serde_json::to_string_pretty(&cni_config).context("Failed to serialize CNI config")?;

        let subdir_path = format!("{}/10-{}.conf", cni_conf_dir, bridge_name);
        let subdir_path_key = subdir_path.clone();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_write_cni_config",
            subdir_path_key,
            move || {
                std::fs::write(&subdir_path, &config_json)
                    .context("Failed to write CNI config file to namespace subdir")
            },
        )
        .await?;

        Ok(())
    }

    async fn install_klights_cni_binary(cni_bin_dir: &str) -> Result<()> {
        let current_exe =
            std::env::current_exe().context("Failed to resolve current executable")?;
        let plugin_dir = std::path::Path::new(cni_bin_dir);
        let plugin_dir_path = plugin_dir.to_path_buf();
        let plugin_dir_key = plugin_dir.display().to_string();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_install_cni_create_dir",
            plugin_dir_key,
            move || {
                std::fs::create_dir_all(&plugin_dir_path)
                    .with_context(|| format!("Failed to create {}", plugin_dir_path.display()))
            },
        )
        .await?;
        let plugin_path = plugin_dir.join("klights-cni");
        let current_exe_for_link = current_exe.clone();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_install_cni_symlink",
            plugin_path.display().to_string(),
            move || -> Result<()> {
                let _ = std::fs::remove_file(&plugin_path);
                std::os::unix::fs::symlink(&current_exe_for_link, &plugin_path)
                    .with_context(|| format!("Failed to symlink {}", plugin_path.display()))?;
                Ok(())
            },
        )
        .await
        .context("Failed to install klights CNI plugin")?;
        Ok(())
    }

    /// Generate containerd config TOML for klights.
    /// Uses containerd v2 config format with both CRI plugins:
    /// - io.containerd.cri.v1.runtime: container sandbox/exec operations
    /// - io.containerd.cri.v1.images: image pull/list operations (ImageService)
    ///
    /// conf_dir points at the namespace-specific CNI config directory where klights
    /// writes the klights-cni conflist before containerd starts.
    fn generate_config(
        socket_path: &str,
        data_dir: &str,
        state_dir: &str,
        cni_bin_dir: &str,
        cni_conf_dir: &str,
        rootless: bool,
        isolated_mount_namespace: bool,
    ) -> String {
        // In rootless mode containerd runs inside a user namespace where
        // /var/run/netns is owned by root and not writable.  Telling the CRI
        // plugin to place network namespace mounts under the state directory
        // (which lives under the user-writable data root) avoids the
        // "permission denied" error at sandbox creation.
        //
        // The same setting is required when klights/containerd already run in
        // an isolated mount namespace, such as the multinode netns harness.
        // In that case containerd's bind mount is not visible from the host
        // mount namespace, so a host-visible /var/run/netns/cni-* placeholder
        // becomes an invalid file that breaks host `ip netns`/`ip link` scans.
        let netns_mounts_under_state_dir =
            if containerd_netns_mounts_under_state_dir(rootless, isolated_mount_namespace) {
                "true"
            } else {
                "false"
            };
        // In rootless mode, keep cgroupfs driver (SystemdCgroup = false).
        // Rootless runc handles cgroup delegation via the user's cgroup subtree.
        //
        // Set the containerd-level cgroup path to empty so the CRI plugin
        // does not try to create cgroups under /sys/fs/cgroup/ directly.
        // In rootless mode this would fail with permission denied.
        let cgroup_section = if rootless {
            "\n[cgroup]\n  path = \"\""
        } else {
            ""
        };
        // In rootless mode, containerd-shim-runc-v2 escapes the rootlesskit user
        // namespace.  Wrap runc with --rootless=true so it skips cgroup operations
        // that would fail with permission denied in the host user namespace.
        let binary_name_line = if rootless {
            // runc-wrapper.sh lives in the klights data root (e.g. ~/klights/).
            // data_dir = ~/klights/containerd/data → parent = ~/klights
            let mut wrapper = std::path::PathBuf::from(data_dir);
            wrapper.pop(); // up to containerd/
            wrapper.pop(); // up to klights data root/
            wrapper.push("runc-wrapper.sh");
            let wrapper_str = wrapper.to_string_lossy().to_string();
            format!("            BinaryName = \"{}\"", wrapper_str)
        } else {
            String::new()
        };
        format!(
            r#"version = 3

root = "{data_dir}"
state = "{state_dir}"

[grpc]
  address = "{socket_path}"
  max_recv_message_size = 67108864
  max_send_message_size = 67108864

[plugins]
  [plugins.'io.containerd.nri.v1.nri']
    disable = true
  [plugins.'io.containerd.cri.v1.images']
    snapshotter = "overlayfs"
    disable_snapshot_annotations = true

  [plugins.'io.containerd.cri.v1.runtime']
    netns_mounts_under_state_dir = {netns_mounts_under_state_dir}
    [plugins.'io.containerd.cri.v1.runtime'.cni]
      bin_dirs = ["{cni_bin_dir}"]
      conf_dir = "{cni_conf_dir}"
      use_internal_loopback = true
    [plugins.'io.containerd.cri.v1.runtime'.containerd]
      [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes]
        [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
          runtime_type = "io.containerd.runc.v2"
          [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc.options]
            SystemdCgroup = false
{binary_name_line}
{cgroup_section}
"#
        )
    }

    /// Spawn containerd subprocess, wait for socket ready
    /// Paths are derived from the namespace parameter for isolation
    pub async fn start(config: ContainerdStartConfig<'_>) -> Result<Self> {
        let ContainerdStartConfig {
            namespace,
            bridge_name,
            pod_subnet,
            pod_link_mtu,
            data_dir,
            state_dir,
            rootless,
            task_supervisor,
            grpc_transport_policy,
        } = config;
        // Ensure inotify limits are sufficient before starting containerd
        crate::kubelet::file_blocking::run_blocking_file(
            "containerd_ensure_inotify_limits",
            ensure_inotify_limits,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to increase inotify limits: {}. Continuing anyway.",
                e
            );
        });

        // Derive all paths from namespace for isolation
        let socket_path = crate::paths::containerd_socket_path(namespace)
            .to_string_lossy()
            .into_owned();
        let data_dir = data_dir.to_string();
        let state_dir = state_dir.to_string();
        let config_path = format!("{}/config.toml", data_dir);
        let cni_bin_dir = crate::paths::cni_bin_dir_path(namespace)
            .to_string_lossy()
            .into_owned();
        let cni_conf_dir = crate::paths::cni_conf_dir_path(namespace)
            .to_string_lossy()
            .into_owned();

        // Create directories
        let state_dir_clone = state_dir.clone();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_create_state_dir",
            state_dir.clone(),
            move || {
                std::fs::create_dir_all(&state_dir_clone)
                    .context("Failed to create state directory")
            },
        )
        .await?;
        let data_dir_clone = data_dir.clone();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_create_data_dir",
            data_dir.clone(),
            move || {
                std::fs::create_dir_all(&data_dir_clone).context("Failed to create data directory")
            },
        )
        .await?;
        if rootless {
            Self::write_rootless_runc_wrapper(&data_dir).await?;
        }
        Self::install_klights_cni_binary(&cni_bin_dir).await?;
        Self::write_cni_config(
            &cni_conf_dir,
            namespace,
            bridge_name,
            pod_subnet,
            pod_link_mtu,
        )
        .await?;

        // Generate and write config file
        let config_content = Self::generate_config(
            &socket_path,
            &data_dir,
            &state_dir,
            &cni_bin_dir,
            &cni_conf_dir,
            rootless,
            is_isolated_mount_namespace(),
        );
        let config_key = config_path.clone();
        let config_path_for_write = config_path.clone();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_write_config",
            config_key,
            move || {
                std::fs::write(&config_path_for_write, config_content)
                    .context("Failed to write containerd config")
            },
        )
        .await?;

        if Self::try_reuse_existing(
            &socket_path,
            namespace,
            rootless,
            grpc_transport_policy.as_ref(),
        )
        .await?
        {
            tracing::info!(
                socket = %socket_path,
                namespace = %namespace,
                "Reusing existing klights containerd subprocess"
            );
            return Ok(Self {
                process: ContainerdProcess::Reused,
                socket_path,
                _config_path: config_path,
                _data_dir: data_dir,
                _state_dir: state_dir,
                task_supervisor,
            });
        }

        // Spawn containerd subprocess in a private mount namespace.
        // See `apply_private_mount_namespace` for rationale.
        let mut cmd = tokio::process::Command::new("containerd");
        cmd.arg("--config").arg(&config_path);
        apply_private_mount_namespace(cmd.as_std_mut());
        let child = cmd.spawn().context("Failed to spawn containerd process")?;

        // Wait for socket file using inotify (30s timeout)
        let socket_dir = Path::new(&socket_path)
            .parent()
            .context("Socket path has no parent directory")?;
        let socket_dir_path = socket_dir.to_path_buf();
        let socket_dir_key = socket_dir.display().to_string();
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "containerd_create_socket_dir",
            socket_dir_key,
            move || {
                std::fs::create_dir_all(&socket_dir_path)
                    .context("Failed to create containerd socket directory")
            },
        )
        .await?;

        // Check if socket already exists (fast path)
        if !Path::new(&socket_path).exists() {
            let socket_wait = wait_for_socket_inotify(&socket_path, socket_dir);
            task_supervisor
                .timeout(
                    "containerd_socket_wait",
                    Duration::from_secs(30),
                    socket_wait,
                )
                .await
                .context("Task supervisor error waiting for containerd socket")?
                .map_err(|_| anyhow::anyhow!("Timeout waiting for containerd socket"))?
                .context("Failed to watch for containerd socket")?;
        }

        // Wait for CRI gRPC to be ready with exponential backoff
        // Socket file exists before CRI plugin initializes, so we retry connect
        // Backoff: 100ms → 200ms → 400ms → 800ms → 1600ms → 3200ms → 5000ms (cap)
        // Total timeout: 30s
        let socket_for_ready = socket_path.clone();
        let ns_for_ready = namespace.to_string();
        let cri_ready = async {
            let start = std::time::Instant::now();
            let total_timeout = Duration::from_secs(30);
            let mut delay_ms = 100u64;
            let max_delay_ms = 5000u64;
            let mut attempt = 0u32;

            loop {
                attempt += 1;
                match super::cri::CriClient::connect_with_policy(
                    &socket_for_ready,
                    &ns_for_ready,
                    grpc_transport_policy.as_ref(),
                )
                .await
                {
                    Ok(_) => {
                        tracing::info!("Containerd CRI ready after {} attempts", attempt);
                        return Ok(());
                    }
                    Err(_) => {
                        if start.elapsed() >= total_timeout {
                            anyhow::bail!(
                                "Timeout waiting for containerd CRI to be ready after {} attempts",
                                attempt
                            );
                        }
                        let _ = task_supervisor
                            .sleep(
                                "containerd_cri_ready_retry_backoff",
                                Duration::from_millis(delay_ms),
                            )
                            .await;
                        delay_ms = std::cmp::min(delay_ms * 2, max_delay_ms);
                    }
                }
            }
        };
        cri_ready.await?;

        Ok(Self {
            process: ContainerdProcess::Spawned(child),
            socket_path,
            _config_path: config_path,
            _data_dir: data_dir,
            _state_dir: state_dir,
            task_supervisor,
        })
    }

    async fn try_reuse_existing(
        socket_path: &str,
        namespace: &str,
        rootless: bool,
        grpc_transport_policy: &crate::replication::grpc::transport_policy::GrpcTransportPolicy,
    ) -> Result<bool> {
        if !crate::utils::path_exists_async(socket_path).await? {
            return Ok(false);
        }

        match Self::socket_is_reusable(socket_path, namespace, rootless, grpc_transport_policy)
            .await
        {
            Ok(true) => Ok(true),
            Ok(false) => Ok(false),
            Err(e) => {
                tracing::warn!(
                    socket = %socket_path,
                    error = %e,
                    "Existing containerd socket is not ready; removing stale socket before spawn"
                );
                let socket_path_for_remove = socket_path.to_string();
                crate::kubelet::file_blocking::run_blocking_file_keyed(
                    "containerd_remove_stale_socket",
                    socket_path.to_string(),
                    move || match std::fs::remove_file(&socket_path_for_remove) {
                        Ok(()) => Ok(()),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(e) => Err(e).with_context(|| {
                            format!("Failed to remove stale socket {socket_path_for_remove}")
                        }),
                    },
                )
                .await?;
                Ok(false)
            }
        }
    }

    pub async fn namespace_containerd_is_reusable(
        namespace: &str,
        rootless: bool,
        grpc_transport_policy: &crate::replication::grpc::transport_policy::GrpcTransportPolicy,
    ) -> Result<bool> {
        let socket_path = crate::paths::containerd_socket_path(namespace)
            .to_string_lossy()
            .into_owned();
        if !crate::utils::path_exists_async(&socket_path).await? {
            return Ok(false);
        }
        Self::socket_is_reusable(&socket_path, namespace, rootless, grpc_transport_policy).await
    }

    async fn socket_is_reusable(
        socket_path: &str,
        namespace: &str,
        rootless: bool,
        grpc_transport_policy: &crate::replication::grpc::transport_policy::GrpcTransportPolicy,
    ) -> Result<bool> {
        match super::cri::CriClient::connect_with_policy(
            socket_path,
            namespace,
            grpc_transport_policy,
        )
        .await
        {
            Ok(_) if rootless => rootless_namespace_containerd_is_current(namespace).await,
            Ok(_) => Ok(true),
            Err(e) => Err(e)
                .with_context(|| format!("containerd socket {socket_path} is not ready for reuse")),
        }
    }

    /// Return the socket path for CRI client connections
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    /// Graceful shutdown: SIGTERM, wait, SIGKILL if needed
    pub async fn stop(&mut self) -> Result<()> {
        let ContainerdProcess::Spawned(child) = &mut self.process else {
            tracing::debug!(
                "ContainerdManager::stop called for reused containerd; cleanup owns teardown"
            );
            return Ok(());
        };

        // Get the process ID for sending SIGTERM
        let pid = child.id().context("Failed to get containerd PID")?;

        // SAFETY: pid is valid — obtained from self.child.id() which returns
        // the OS process ID of the spawned containerd. We send SIGTERM for graceful shutdown.
        let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if ret != 0 {
            tracing::warn!(
                "Failed to send SIGTERM to containerd (pid {}): {}",
                pid,
                std::io::Error::last_os_error()
            );
        }

        // Wait up to 10 seconds for graceful exit
        let wait_result = self
            .task_supervisor
            .timeout(
                "containerd_stop_wait",
                Duration::from_secs(10),
                child.wait(),
            )
            .await;

        match wait_result {
            Ok(Ok(Ok(_))) => {
                // Process exited gracefully
            }
            Ok(Ok(Err(e))) => {
                return Err(e).context("Error waiting for containerd to exit");
            }
            Ok(Err(_)) => {
                // Timeout - send SIGKILL
                child.kill().await.context("Failed to SIGKILL containerd")?;
                child.wait().await.context("Failed to wait after SIGKILL")?;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Task supervisor cancelled while waiting for containerd to exit: {e}"
                ));
            }
        }

        // Clean up socket file (best-effort — containerd may have already removed it)
        if Path::new(&self.socket_path).exists() {
            let socket_path = self.socket_path.clone();
            if let Err(e) = crate::kubelet::file_blocking::run_blocking_file_keyed(
                "containerd_remove_socket_file",
                socket_path.clone(),
                move || {
                    std::fs::remove_file(&socket_path)
                        .with_context(|| format!("Failed to remove socket {}", socket_path))?;
                    Ok(())
                },
            )
            .await
            {
                tracing::warn!("Failed to remove socket {}: {}", self.socket_path, e);
            }
        }

        Ok(())
    }

    pub async fn stop_namespace_containerd(
        namespace: &str,
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
    ) -> Result<usize> {
        let config_path = crate::paths::containerd_data_dir_path(namespace).join("config.toml");
        let socket_path = crate::paths::containerd_socket_path(namespace);
        let mut pids: BTreeSet<libc::pid_t> = find_containerd_pids_for_config(&config_path)
            .await?
            .into_iter()
            .collect();
        pids.extend(find_containerd_shim_pids_for_socket(&socket_path).await?);
        let pids: Vec<libc::pid_t> = pids.into_iter().collect();

        if pids.is_empty() {
            tracing::debug!(
                namespace = %namespace,
                config = %config_path.display(),
                socket = %socket_path.display(),
                "No namespace containerd or shim process found during cleanup"
            );
            return Ok(0);
        }

        for pid in &pids {
            send_signal(*pid, libc::SIGTERM);
        }

        wait_for_pids_to_exit(&pids, Duration::from_secs(5), task_supervisor).await;

        let remaining: Vec<libc::pid_t> = pids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid))
            .collect();
        for pid in &remaining {
            send_signal(*pid, libc::SIGKILL);
        }
        if !remaining.is_empty() {
            wait_for_pids_to_exit(&remaining, Duration::from_secs(2), task_supervisor).await;
        }

        Ok(pids.len())
    }
}

pub fn send_signal(pid: libc::pid_t, signal: libc::c_int) {
    // SAFETY: kill(2) is called with a PID discovered from /proc and a constant signal.
    let ret = unsafe { libc::kill(pid, signal) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(pid, signal, error = %err, "Failed to signal containerd process");
        }
    }
}

pub async fn wait_for_pids_to_exit(
    pids: &[libc::pid_t],
    timeout_duration: Duration,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) {
    let mut pidfds = Vec::new();
    for pid in pids {
        if !process_exists(*pid) {
            continue;
        }
        match open_pidfd(*pid).and_then(|fd| AsyncFd::new(fd).context("create AsyncFd for pidfd")) {
            Ok(pidfd) => pidfds.push(pidfd),
            Err(e) => {
                tracing::debug!(pid = *pid, error = %e, "pidfd wait unavailable for containerd process");
            }
        }
    }
    if pidfds.is_empty() {
        return;
    }

    let wait = async move {
        for pidfd in pidfds {
            let mut ready = pidfd
                .readable()
                .await
                .context("wait for containerd pidfd readiness")?;
            ready.clear_ready();
        }
        Ok::<(), anyhow::Error>(())
    };

    match task_supervisor
        .timeout("containerd_stop_pidfd_wait", timeout_duration, wait)
        .await
    {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => tracing::debug!("containerd pidfd wait failed: {}", e),
        Ok(Err(_elapsed)) => tracing::debug!("containerd pidfd wait timed out"),
        Err(e) => tracing::debug!("containerd pidfd wait supervisor error: {}", e),
    }
}

fn open_pidfd(pid: libc::pid_t) -> Result<OwnedFd> {
    // SAFETY: pidfd_open is a side-effect-free syscall for the supplied PID.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd_open failed");
    }
    // SAFETY: fd is newly returned by pidfd_open and is owned by this process.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
}

pub fn process_exists(pid: libc::pid_t) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

async fn rootless_namespace_containerd_is_current(namespace: &str) -> Result<bool> {
    let config_path = crate::paths::containerd_data_dir_path(namespace).join("config.toml");
    let pids = find_containerd_pids_for_config(&config_path).await?;
    rootless_containerd_pids_in_current_netns(Path::new("/proc"), &pids)
}

fn rootless_containerd_pids_in_current_netns(
    proc_root: &Path,
    pids: &[libc::pid_t],
) -> Result<bool> {
    if pids.is_empty() {
        return Ok(false);
    }

    let current_netns = std::fs::read_link(proc_root.join("self/ns/net"))
        .context("failed to read current rootless network namespace")?;
    for pid in pids {
        let pid_netns_path = proc_root.join(pid.to_string()).join("ns/net");
        let pid_netns = match std::fs::read_link(&pid_netns_path) {
            Ok(netns) => netns,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "failed to read existing rootless containerd network namespace at {}",
                        pid_netns_path.display()
                    )
                });
            }
        };
        if pid_netns != current_netns {
            tracing::warn!(
                pid = *pid,
                current_netns = %current_netns.display(),
                containerd_netns = %pid_netns.display(),
                "Existing rootless containerd belongs to an old network namespace; forcing startup cleanup"
            );
            return Ok(false);
        }
    }

    Ok(true)
}

async fn find_containerd_pids_for_config(config_path: &Path) -> Result<Vec<libc::pid_t>> {
    let config_path = config_path.to_path_buf();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "containerd_find_namespace_processes",
        config_path.display().to_string(),
        move || find_containerd_pids_for_config_sync(&config_path),
    )
    .await
}

async fn find_containerd_shim_pids_for_socket(socket_path: &Path) -> Result<Vec<libc::pid_t>> {
    let socket_path = socket_path.to_path_buf();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "containerd_find_namespace_shims",
        socket_path.display().to_string(),
        move || find_containerd_shim_pids_for_socket_sync(&socket_path),
    )
    .await
}

fn find_containerd_pids_for_config_sync(config_path: &Path) -> Result<Vec<libc::pid_t>> {
    find_proc_pids_matching_cmdline(|args| containerd_cmdline_matches_config(args, config_path))
}

fn find_containerd_shim_pids_for_socket_sync(socket_path: &Path) -> Result<Vec<libc::pid_t>> {
    find_proc_pids_matching_cmdline(|args| {
        containerd_shim_cmdline_matches_socket(args, socket_path)
    })
}

fn find_proc_pids_matching_cmdline(
    mut matches: impl FnMut(&[String]) -> bool,
) -> Result<Vec<libc::pid_t>> {
    let mut pids = Vec::new();
    let entries = match std::fs::read_dir("/proc") {
        Ok(entries) => entries,
        Err(e) => return Err(e).context("read /proc for containerd process discovery"),
    };

    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let file_name = entry.file_name();
        let Some(pid) = file_name
            .to_str()
            .and_then(|name| name.parse::<libc::pid_t>().ok())
        else {
            continue;
        };
        let cmdline_path = entry.path().join("cmdline");
        let Ok(cmdline) = std::fs::read(&cmdline_path) else {
            continue;
        };
        let args: Vec<String> = cmdline
            .split(|b| *b == 0)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8_lossy(arg).into_owned())
            .collect();
        if matches(&args) {
            pids.push(pid);
        }
    }

    Ok(pids)
}

fn containerd_cmdline_matches_config(args: &[impl AsRef<str>], config_path: &Path) -> bool {
    let Some(program) = args.first().map(|arg| arg.as_ref()) else {
        return false;
    };
    if Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        != Some("containerd")
    {
        return false;
    }

    for pair in args.windows(2) {
        if pair[0].as_ref() == "--config"
            && containerd_config_arg_matches(pair[1].as_ref(), config_path)
        {
            return true;
        }
    }

    args.iter().any(|arg| {
        arg.as_ref()
            .strip_prefix("--config=")
            .is_some_and(|value| containerd_config_arg_matches(value, config_path))
    })
}

fn containerd_config_arg_matches(arg: &str, config_path: &Path) -> bool {
    path_arg_matches(arg, config_path)
}

fn containerd_shim_cmdline_matches_socket(args: &[impl AsRef<str>], socket_path: &Path) -> bool {
    let Some(program) = args.first().map(|arg| arg.as_ref()) else {
        return false;
    };
    let Some(program_name) = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    else {
        return false;
    };
    if !program_name.starts_with("containerd-shim-") {
        return false;
    }

    for pair in args.windows(2) {
        let flag = pair[0].as_ref();
        if (flag == "-address" || flag == "--address")
            && path_arg_matches(pair[1].as_ref(), socket_path)
        {
            return true;
        }
    }

    args.iter().any(|arg| {
        let arg = arg.as_ref();
        arg.strip_prefix("-address=")
            .or_else(|| arg.strip_prefix("--address="))
            .is_some_and(|value| path_arg_matches(value, socket_path))
    })
}

fn path_arg_matches(arg: &str, expected_path: &Path) -> bool {
    let arg_path = Path::new(arg);
    arg_path == expected_path
        || normalize_path_for_match(arg_path) == normalize_path_for_match(expected_path)
}

fn normalize_path_for_match(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Per-process unique test root under /tmp, shared by all tests in
    /// this module (and across all test modules that call
    /// `paths::test_data_root_path`).
    fn test_root(ns: &str) -> String {
        crate::paths::test_data_root_path(ns)
            .to_string_lossy()
            .into_owned()
    }

    fn test_cni_conf_dir(ns: &str) -> String {
        let root = crate::paths::test_data_root_path(ns);
        root.join("cni")
            .join("net.d")
            .join(ns)
            .to_string_lossy()
            .into_owned()
    }

    /// Spawns a child with apply_private_mount_namespace and verifies the
    /// mount-namespace behaviour:
    /// - isolated parent (netns/container) → child shares parent's NS (skip unshare)
    /// - host parent                       → child lands in distinct NS (unshare+MS_SLAVE)
    ///
    /// Skips if not running as root (CAP_SYS_ADMIN is required for unshare).
    #[test]
    fn test_apply_private_mount_namespace_ns_behaviour() {
        // SAFETY: geteuid is always safe.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipping: requires root for unshare(CLONE_NEWNS)");
            return;
        }

        let parent_is_isolated = is_isolated_mount_namespace();

        let parent_ns = std::fs::read_link("/proc/self/ns/mnt")
            .expect("read parent mnt ns")
            .into_os_string()
            .into_string()
            .expect("utf8 mnt ns");

        let mut cmd = std::process::Command::new("readlink");
        cmd.arg("/proc/self/ns/mnt");
        apply_private_mount_namespace(&mut cmd);
        let output = cmd.output().expect("spawn readlink");
        assert!(
            output.status.success(),
            "readlink failed: {:?}",
            output.status
        );
        let child_ns = String::from_utf8(output.stdout)
            .expect("utf8 child ns")
            .trim()
            .to_string();

        if parent_is_isolated {
            assert_eq!(
                parent_ns, child_ns,
                "isolated parent: child should share the same mount namespace"
            );
        } else {
            assert_ne!(
                parent_ns, child_ns,
                "host parent: child should land in a distinct mount namespace"
            );
        }
    }

    #[test]
    fn test_should_increase_inotify_below_threshold() {
        // Values below threshold should trigger increase
        assert!(should_increase_inotify(0, 1024));
        assert!(should_increase_inotify(128, 1024));
        assert!(should_increase_inotify(512, 1024));
        assert!(should_increase_inotify(1023, 1024));
    }

    #[test]
    fn test_should_increase_inotify_at_or_above_threshold() {
        // Values at or above threshold should not trigger increase
        assert!(!should_increase_inotify(1024, 1024));
        assert!(!should_increase_inotify(2048, 1024));
        assert!(!should_increase_inotify(9999, 1024));
    }

    #[test]
    fn test_parse_inotify_value() {
        // Test parsing current value from /proc/sys format
        assert_eq!(parse_inotify_value("128\n").unwrap(), 128);
        assert_eq!(parse_inotify_value("1024\n").unwrap(), 1024);
        assert_eq!(parse_inotify_value("256").unwrap(), 256);
        assert_eq!(parse_inotify_value("0\n").unwrap(), 0);

        // Invalid inputs
        assert!(parse_inotify_value("").is_err());
        assert!(parse_inotify_value("abc\n").is_err());
        assert!(parse_inotify_value("123abc\n").is_err());
    }

    #[test]
    fn containerd_config_arg_match_normalizes_current_dir_components() {
        assert!(
            containerd_config_arg_matches(
                "/root/./klights/containerd/data/config.toml",
                Path::new("/root/klights/containerd/data/config.toml")
            ),
            "cleanup must match the config path used by a previously spawned containerd even when one side contains ./"
        );
    }

    #[test]
    fn containerd_cmdline_match_accepts_split_and_equals_config_args() {
        let config = Path::new("/root/klights/containerd/data/config.toml");

        assert!(containerd_cmdline_matches_config(
            &[
                "containerd",
                "--config",
                "/root/./klights/containerd/data/config.toml"
            ],
            config
        ));
        assert!(containerd_cmdline_matches_config(
            &[
                "containerd",
                "--config=/root/./klights/containerd/data/config.toml"
            ],
            config
        ));
        assert!(!containerd_cmdline_matches_config(
            &[
                "containerd",
                "--config",
                "/root/other/containerd/data/config.toml"
            ],
            config
        ));
    }

    #[test]
    fn containerd_shim_cmdline_match_accepts_split_and_equals_address_args() {
        let socket = Path::new("/root/klights/containerd.sock");

        assert!(containerd_shim_cmdline_matches_socket(
            &[
                "/usr/bin/containerd-shim-runc-v2",
                "-namespace",
                "k8s.io",
                "-id",
                "abc",
                "-address",
                "/root/./klights/containerd.sock",
            ],
            socket
        ));
        assert!(containerd_shim_cmdline_matches_socket(
            &[
                "containerd-shim-runc-v2",
                "-address=/root/./klights/containerd.sock"
            ],
            socket
        ));
        assert!(!containerd_shim_cmdline_matches_socket(
            &["containerd-shim-runc-v2", "-address", "/root/other.sock"],
            socket
        ));
    }

    #[test]
    fn rootless_reuse_requires_containerd_in_current_netns() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("self/ns")).unwrap();
        std::fs::create_dir_all(dir.path().join("123/ns")).unwrap();
        std::fs::create_dir_all(dir.path().join("456/ns")).unwrap();
        symlink("net:[current]", dir.path().join("self/ns/net")).unwrap();
        symlink("net:[current]", dir.path().join("123/ns/net")).unwrap();
        symlink("net:[old]", dir.path().join("456/ns/net")).unwrap();

        assert!(
            rootless_containerd_pids_in_current_netns(dir.path(), &[123]).unwrap(),
            "rootless containerd can be reused when it belongs to the current rootlesskit netns"
        );
        assert!(
            !rootless_containerd_pids_in_current_netns(dir.path(), &[123, 456]).unwrap(),
            "containerd in an old rootlesskit netns must force startup cleanup"
        );
        assert!(
            !rootless_containerd_pids_in_current_netns(dir.path(), &[]).unwrap(),
            "no matching pid means there is no rootless containerd process to reclaim"
        );
    }

    // Embedded containerd invariants are enforced by
    // `scripts/check_containerd_manager_invariants.sh`, run as part
    // of `./build.sh`. The lint script checks `try_reuse_existing`
    // before `cmd.spawn()`, plus the cross-file checks against
    // `src/bootstrap/` (R3-survivable) and `deploy/klights.service`.

    #[test]
    fn test_generate_config_valid_toml() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");

        assert_eq!(parsed.get("version").and_then(|v| v.as_integer()), Some(3));

        let grpc = parsed.get("grpc").expect("Must have grpc section");
        assert_eq!(
            grpc.get("address").and_then(|v| v.as_str()),
            Some(sock.as_str())
        );
        assert_eq!(
            parsed.get("root").and_then(|v| v.as_str()),
            Some(data.as_str())
        );
        assert_eq!(
            parsed.get("state").and_then(|v| v.as_str()),
            Some(state.as_str())
        );
    }

    #[test]
    fn test_generate_config_sets_grpc_message_size_limits() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");
        let grpc = parsed.get("grpc").expect("Must have grpc section");

        assert_eq!(
            grpc.get("max_recv_message_size")
                .and_then(|v| v.as_integer()),
            Some(67108864)
        );
        assert_eq!(
            grpc.get("max_send_message_size")
                .and_then(|v| v.as_integer()),
            Some(67108864)
        );
    }

    #[test]
    fn test_generate_config_contains_runc_runtime() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");

        // Navigate to plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runc
        let plugins = parsed.get("plugins").expect("Must have plugins section");
        let cri = plugins
            .get("io.containerd.cri.v1.runtime")
            .expect("Must have CRI plugin");
        let containerd = cri.get("containerd").expect("Must have containerd section");
        let runtimes = containerd
            .get("runtimes")
            .expect("Must have runtimes section");
        let runc = runtimes.get("runc").expect("Must have runc runtime");

        // Verify runc is configured
        assert!(runc.is_table());
    }

    #[test]
    fn test_generate_config_no_systemd_cgroup() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");

        // Navigate to plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runc.options
        let plugins = parsed.get("plugins").expect("Must have plugins section");
        let cri = plugins
            .get("io.containerd.cri.v1.runtime")
            .expect("Must have CRI plugin");
        let containerd = cri.get("containerd").expect("Must have containerd section");
        let runtimes = containerd
            .get("runtimes")
            .expect("Must have runtimes section");
        let runc = runtimes.get("runc").expect("Must have runc runtime");
        let options = runc.get("options").expect("Must have options section");

        // Verify SystemdCgroup is disabled — klights uses cgroupfs driver so containerd
        // owns the full cgroup lifecycle without creating systemd slice units per pod.
        assert_eq!(
            options.get("SystemdCgroup").and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn test_rootless_runc_wrapper_script_invokes_klights_wrapper_mode() {
        let script = ContainerdManager::rootless_runc_wrapper_script("/usr/local/bin/klights");

        assert!(script.starts_with("#!/bin/sh\n"));
        assert!(
            script.contains("exec '/usr/local/bin/klights' __rootless-runc-wrapper \"$@\""),
            "wrapper script must route runc args through klights sanitizer: {script}"
        );
    }

    #[test]
    fn test_generate_config_custom_namespace_derives_correct_paths() {
        let ns = "klights-test";
        let root = test_root(ns);
        let cni_conf_dir = test_cni_conf_dir(ns);
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");

        assert_eq!(
            parsed.get("root").and_then(|v| v.as_str()),
            Some(data.as_str())
        );
        assert_eq!(
            parsed.get("state").and_then(|v| v.as_str()),
            Some(state.as_str())
        );

        let grpc = parsed.get("grpc").expect("Must have grpc section");
        assert_eq!(
            grpc.get("address").and_then(|v| v.as_str()),
            Some(sock.as_str())
        );

        // conf_dir is configured explicitly per namespace.
        let plugins = parsed.get("plugins").unwrap();
        let cri = plugins.get("io.containerd.cri.v1.runtime").unwrap();
        let cni = cri.get("cni").unwrap();
        assert_eq!(
            cni.get("conf_dir").and_then(|v| v.as_str()),
            Some(cni_conf_dir.as_str())
        );
    }

    #[test]
    fn test_generate_config_has_cni_paths() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");

        let plugins = parsed.get("plugins").expect("Must have plugins section");
        let cri = plugins
            .get("io.containerd.cri.v1.runtime")
            .expect("Must have CRI plugin");
        let cni = cri.get("cni").expect("Must have CNI section");

        let bin_dirs = cni
            .get("bin_dirs")
            .and_then(|v| v.as_array())
            .expect("CNI bin_dirs must be an array");
        assert_eq!(
            bin_dirs[0].as_str(),
            Some(cni_bin.as_str()),
            "CNI bin_dirs[0] must point at the namespace-scoped CNI bin directory"
        );
        assert_eq!(
            cni.get("conf_dir").and_then(|v| v.as_str()),
            Some(cni_conf_dir.as_str()),
            "CNI conf_dir must be namespace-specific"
        );
    }

    #[test]
    fn test_generate_config_rootless_sets_netns_mounts_under_state_dir() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            true,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");
        let cri = parsed
            .get("plugins")
            .and_then(|p| p.get("io.containerd.cri.v1.runtime"))
            .expect("Must have CRI plugin");
        let netns = cri
            .get("netns_mounts_under_state_dir")
            .and_then(|v| v.as_bool())
            .expect("netns_mounts_under_state_dir must be a bool");
        assert!(
            netns,
            "rootless mode must set netns_mounts_under_state_dir = true"
        );
    }

    #[test]
    fn test_isolated_mount_namespace_sets_netns_mounts_under_state_dir() {
        assert!(
            containerd_netns_mounts_under_state_dir(false, true),
            "root-mode containerd in an isolated mount namespace must store pod netns mounts under its state dir so host /var/run/netns does not accumulate invalid placeholders"
        );
    }

    #[test]
    fn test_generate_config_isolated_root_sets_netns_mounts_under_state_dir() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            true,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");
        let cri = parsed
            .get("plugins")
            .and_then(|p| p.get("io.containerd.cri.v1.runtime"))
            .expect("Must have CRI plugin");
        let netns = cri
            .get("netns_mounts_under_state_dir")
            .and_then(|v| v.as_bool())
            .expect("netns_mounts_under_state_dir must be a bool");
        assert!(
            netns,
            "isolated root-mode containerd must set netns_mounts_under_state_dir = true"
        );
    }

    #[test]
    fn test_generate_config_root_mode_does_not_set_netns_mounts_under_state_dir() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");
        let cri = parsed
            .get("plugins")
            .and_then(|p| p.get("io.containerd.cri.v1.runtime"))
            .expect("Must have CRI plugin");
        let netns = cri
            .get("netns_mounts_under_state_dir")
            .and_then(|v| v.as_bool())
            .expect("netns_mounts_under_state_dir must be a bool");
        assert!(
            !netns,
            "root mode must set netns_mounts_under_state_dir = false"
        );
    }

    #[test]
    fn test_generate_config_rejects_empty_cni_dir_pattern() {
        let root = test_root("klights");
        let cni_conf_dir = test_cni_conf_dir("klights");
        let cni_bin = format!("{root}/cni/bin");
        let sock = format!("{root}/containerd.sock");
        let data = format!("{root}/containerd/data");
        let state = format!("{root}/containerd/state");
        let config = ContainerdManager::generate_config(
            &sock,
            &data,
            &state,
            &cni_bin,
            &cni_conf_dir,
            false,
            false,
        );

        let parsed: toml::Value =
            toml::from_str(&config).expect("Generated config must be valid TOML");
        let plugins = parsed.get("plugins").expect("Must have plugins section");
        let cri = plugins
            .get("io.containerd.cri.v1.runtime")
            .expect("Must have CRI plugin");
        let cni = cri.get("cni").expect("Must have CNI section");
        let conf_dir = cni
            .get("conf_dir")
            .and_then(|v| v.as_str())
            .expect("CNI conf_dir must be present");
        let bin_dirs = cni
            .get("bin_dirs")
            .and_then(|v| v.as_array())
            .expect("CNI bin_dirs must be present");
        let bin0 = bin_dirs
            .first()
            .and_then(|v| v.as_str())
            .expect("CNI bin_dirs[0] must be present");

        for candidate in [conf_dir, bin0] {
            assert!(
                !candidate.contains("cni-empty") && !candidate.ends_with("/cni/empty"),
                "Invalid CRI CNI path: empty-dir patterns break netPlugin.Status()/RunPodSandbox: {candidate}"
            );
        }
    }

    #[tokio::test]
    async fn test_write_cni_config_provides_node_local_subnet() {
        let dir = tempfile::tempdir().unwrap();
        let cni_dir = dir.path().to_string_lossy().into_owned();

        ContainerdManager::write_cni_config(
            &cni_dir,
            "klights-test",
            "klights-test",
            "10.43.0.0/24",
            crate::networking::wireguard::WIREGUARD_MTU,
        )
        .await
        .unwrap();

        let config_path = dir.path().join("10-klights-test.conf");
        let raw = crate::utils::read_utf8_file(config_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();

        assert_eq!(parsed["type"], "klights-cni");
        assert_eq!(parsed["bridge"], "klights-test");
        assert_eq!(parsed["subnet"], "10.43.0.0/24");
        assert_eq!(parsed["mtu"], crate::networking::wireguard::WIREGUARD_MTU);
        assert_eq!(
            parsed["rpcSocket"],
            crate::paths::cni_rpc_socket_path("klights-test")
                .to_string_lossy()
                .into_owned()
        );
        assert!(parsed.get("mode").is_none());
    }
}
