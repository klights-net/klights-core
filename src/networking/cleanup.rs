//! Networking-owned best-effort cleanup for full teardown and partial boot failure.
//!
//! This handle is intentionally cheaper than a fully booted `Datapath`: it is
//! built from process-start configuration before `NetworkPlane::boot()` touches
//! the bridge path. If boot fails half way through, bootstrap can still
//! ask this networking-owned handle to remove root-mode bridge/veth leftovers.
//! Rootless cleanup only runs when the process is inside rootlesskit, so link
//! and nft commands target the user network namespace rather than the host.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[async_trait::async_trait]
trait NetworkCommandRunner: Send + Sync {
    async fn run_network_output(
        &self,
        category: klights_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output>;
}

#[async_trait::async_trait]
impl NetworkCommandRunner for klights_supervisor::FileProcessExecutor {
    async fn run_network_output(
        &self,
        category: klights_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output> {
        let mut command = std::process::Command::new(program);
        command.args(args);
        self.run_process_output(category, name, command)
            .await
            .with_context(|| format!("supervised network process {name} ({program})"))
    }
}

async fn run_network_command_with(
    runner: &dyn NetworkCommandRunner,
    name: &'static str,
    program: &str,
    args: &[&str],
) -> Result<std::process::Output> {
    runner
        .run_network_output(
            klights_supervisor::TaskCategory::Network,
            name,
            program,
            args,
        )
        .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupDisposition {
    Absent,
    Removed,
}

fn command_failure(program: &str, args: &[&str], output: &std::process::Output) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::anyhow!(
        "{program} {} exited with {}: {}",
        args.join(" "),
        output.status,
        stderr.trim()
    )
}

fn inspection_reports_not_found(output: &std::process::Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    stderr.contains("does not exist")
        || stderr.contains("cannot find device")
        || stderr.contains("no such file or directory")
}

#[derive(Clone, Debug)]
enum NetworkCleanupMode {
    Root {
        bridge_name: String,
        wireguard_device: String,
        nft_table_name: String,
    },
    Rootless {
        bridge_name: String,
        wireguard_device: String,
        nft_table_name: String,
        inside_rootlesskit: bool,
    },
}

#[derive(Clone, Debug)]
struct CniCleanupScope {
    network_name: String,
    results_dir: PathBuf,
    networks_dir: PathBuf,
}

/// Early-created networking cleanup handle.
///
/// Unlike `Datapath`, this does not depend on rtnetlink handles, CNI state, or
/// a successful `NetworkPlane::boot()`. It only captures mode + static config,
/// which lets cleanup run even after partial network boot failure while still
/// keeping direct network operations inside `src/networking/`.
#[derive(Clone)]
pub struct NetworkCleanup {
    mode: NetworkCleanupMode,
    cni_scope: CniCleanupScope,
    file_process: klights_supervisor::FileProcessExecutor,
}

impl std::fmt::Debug for NetworkCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkCleanup")
            .field("mode", &self.mode)
            .field("cni_scope", &self.cni_scope)
            .field("file_process", &"<FileProcessExecutor>")
            .finish()
    }
}

impl NetworkCleanup {
    /// Build cleanup from immutable startup mode/config. Must be called before
    /// network boot so failure paths still have a networking-owned fallback.
    pub fn from_config(
        cfg: &crate::networking::NetworkCleanupConfig,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        match cfg.mode() {
            crate::networking::NetworkMode::Root => Self::root_with_runtime(
                cfg.bridge_name(),
                cfg.wireguard_device(),
                cfg.nft_table_name(),
                file_process,
            ),
            crate::networking::NetworkMode::Rootless => Self::rootless_with_cni(
                cfg.bridge_name(),
                cfg.wireguard_device(),
                cfg.nft_table_name(),
                cfg.inside_rootlesskit(),
                file_process,
            ),
        }
    }

    /// Root-mode cleanup for the configured host bridge.
    #[cfg(test)]
    pub fn root(bridge_name: impl Into<String>) -> Self {
        let bridge_name = bridge_name.into();
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        Self::root_with_runtime(
            bridge_name.clone(),
            crate::networking::wireguard::DEFAULT_WIREGUARD_DEVICE.to_string(),
            bridge_name,
            klights_supervisor::FileProcessExecutor::new(supervisor),
        )
    }

    fn root_with_runtime(
        bridge_name: impl Into<String>,
        wireguard_device: impl Into<String>,
        nft_table_name: impl Into<String>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let bridge_name = bridge_name.into();
        Self {
            mode: NetworkCleanupMode::Root {
                bridge_name: bridge_name.clone(),
                wireguard_device: wireguard_device.into(),
                nft_table_name: nft_table_name.into(),
            },
            cni_scope: CniCleanupScope::new(bridge_name),
            file_process,
        }
    }

    fn rootless_with_cni(
        network_name: impl Into<String>,
        wireguard_device: impl Into<String>,
        nft_table_name: impl Into<String>,
        inside_rootlesskit: bool,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let network_name = network_name.into();
        Self {
            mode: NetworkCleanupMode::Rootless {
                bridge_name: network_name.clone(),
                wireguard_device: wireguard_device.into(),
                nft_table_name: nft_table_name.into(),
                inside_rootlesskit,
            },
            cni_scope: CniCleanupScope::new(network_name),
            file_process,
        }
    }

    async fn run_network_command(
        &self,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output> {
        run_network_command_with(&self.file_process, name, program, args).await
    }

    /// Best-effort runtime networking cleanup: remove veths first while the
    /// bridge still exists, then delete datapath and CNI artifacts. Errors are
    /// logged and do not prevent directory cleanup or boot-error propagation.
    pub async fn cleanup_runtime_network_best_effort(&self) {
        self.cleanup_network_best_effort(true).await;
    }

    /// Best-effort startup recovery cleanup for stale subordinate runtime
    /// resources. The configured bridge is preserved so boot-time validation
    /// can reject wrong-kind links instead of silently deleting operator-owned
    /// conflicts.
    pub async fn cleanup_startup_network_best_effort(&self) {
        self.cleanup_network_best_effort(false).await;
    }

    async fn cleanup_network_best_effort(&self, delete_datapath_links: bool) {
        tracing::info!("Cleaning up orphaned veth pairs");
        if let Err(e) = self.cleanup_orphaned_veths().await {
            tracing::warn!("Failed to cleanup orphaned veths: {}", e);
        }

        if delete_datapath_links {
            tracing::info!("Removing bridge interface");
            if let Err(e) = self.cleanup_bridge().await {
                tracing::warn!("Failed to cleanup bridge: {}", e);
            }
        } else {
            tracing::debug!("Preserving configured bridge during startup recovery");
        }

        tracing::info!("Removing WireGuard interface");
        if let Err(e) = self.cleanup_wireguard_device().await {
            tracing::warn!("Failed to cleanup WireGuard interface: {}", e);
        }

        tracing::info!("Removing nftables service-routing table");
        if let Err(e) = self.cleanup_nft_table().await {
            tracing::warn!("Failed to cleanup nftables table: {}", e);
        }

        tracing::info!("Removing CNI cache artifacts");
        if let Err(e) = self.cleanup_cni_artifacts().await {
            tracing::warn!("Failed to cleanup CNI artifacts: {}", e);
        }
    }

    /// Clean stale pod-network resources that klights previously recorded in
    /// node-local state. This is intentionally record-driven: without a
    /// pod_networks row, cleanup does not know whether a generic `cni-*` netns
    /// or unmastered `veth*` belongs to this klights instance.
    pub async fn cleanup_recorded_pod_networks(
        &self,
        node_local: &dyn klights_node_store::PodNetworkCache,
    ) -> Result<()> {
        let assignments = node_local
            .list_network_assignments()
            .await
            .context("failed to list recorded pod networks")?;
        let mut cleaned = 0u32;

        for assignment in assignments {
            let request = assignment.request().clone();
            let sandbox_id = request.sandbox_id().to_string();

            if is_recorded_klights_veth_name(request.veth_host()) {
                self.cleanup_recorded_veth(&sandbox_id, request.veth_host())
                    .await;
            } else {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    veth = %request.veth_host(),
                    "Skipping recorded pod-network veth with unexpected klights naming"
                );
            }

            if let Some(netns_name) = cni_netns_basename(request.netns_path()) {
                self.cleanup_cni_netns(netns_name).await;
            } else {
                tracing::debug!(
                    sandbox_id = %sandbox_id,
                    netns = %request.netns_path(),
                    "Skipping recorded pod-network netns path outside /run/netns/cni-*"
                );
            }

            match node_local.delete_network_if_matches(request).await {
                Err(e) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %e,
                        "Failed to remove stale pod_networks record during cleanup"
                    );
                }
                Ok(true) => {
                    cleaned += 1;
                }
                Ok(false) => tracing::debug!(
                    sandbox_id = %sandbox_id,
                    "Recorded pod-network identity changed during cleanup; preserving current row"
                ),
            }
        }

        if cleaned > 0 {
            tracing::info!("Cleaned up {} recorded stale pod network(s)", cleaned);
        }
        Ok(())
    }

    async fn cleanup_recorded_veth(&self, sandbox_id: &str, iface: &str) {
        let check = match self
            .run_network_command(
                "network_cleanup_recorded_veth_inspect",
                "ip",
                &["link", "show", iface],
            )
            .await
        {
            Ok(out) => out,
            Err(e) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    iface = %iface,
                    error = %e,
                    "Failed to inspect recorded stale pod veth"
                );
                return;
            }
        };
        if !check.status.success() {
            return;
        }

        match self
            .run_network_command(
                "network_cleanup_recorded_veth_delete",
                "ip",
                &link_delete_args(iface),
            )
            .await
        {
            Ok(out) if out.status.success() => {
                tracing::info!(
                    sandbox_id = %sandbox_id,
                    iface = %iface,
                    "Deleted recorded stale pod veth"
                );
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    iface = %iface,
                    stderr = %stderr.trim(),
                    "Failed to delete recorded stale pod veth"
                );
            }
            Err(e) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    iface = %iface,
                    error = %e,
                    "Failed to delete recorded stale pod veth"
                );
            }
        }
    }

    /// Clean up the owned bridge interface in the current network namespace.
    /// Rootless cleanup is active only inside rootlesskit, never from the host.
    pub async fn cleanup_bridge(&self) -> Result<()> {
        self.cleanup_bridge_with_runner(&self.file_process).await?;
        Ok(())
    }

    async fn cleanup_bridge_with_runner(
        &self,
        runner: &dyn NetworkCommandRunner,
    ) -> Result<CleanupDisposition> {
        let Some(bridge_name) = self.current_namespace_bridge_name() else {
            tracing::debug!("network cleanup: skipping bridge delete outside owned namespace");
            return Ok(CleanupDisposition::Absent);
        };

        let show_args = bridge_show_args(bridge_name);
        let check =
            run_network_command_with(runner, "network_cleanup_bridge_inspect", "ip", &show_args)
                .await?;

        if !check.status.success() {
            if inspection_reports_not_found(&check) {
                return Ok(CleanupDisposition::Absent);
            }
            return Err(command_failure("ip", &show_args, &check))
                .context("Failed to inspect bridge");
        }

        let delete_args = bridge_delete_args(bridge_name);
        let delete =
            run_network_command_with(runner, "network_cleanup_bridge_delete", "ip", &delete_args)
                .await
                .context("Failed to delete bridge")?;
        if !delete.status.success() {
            bail!(command_failure("ip", &delete_args, &delete));
        }

        tracing::info!("Deleted bridge interface: {}", bridge_name);
        Ok(CleanupDisposition::Removed)
    }

    /// Clean up veth pairs attached to the owned bridge before the bridge is
    /// removed.
    pub async fn cleanup_orphaned_veths(&self) -> Result<()> {
        let Some(bridge_name) = self.current_namespace_bridge_name() else {
            tracing::debug!("network cleanup: skipping veth delete outside owned namespace");
            return Ok(());
        };

        let output = self
            .run_network_command(
                "network_cleanup_orphaned_veth_list",
                "ip",
                &orphaned_veth_list_args(bridge_name),
            )
            .await
            .context("Failed to list network interfaces")?;
        if !output.status.success() {
            return Err(command_failure(
                "ip",
                &orphaned_veth_list_args(bridge_name),
                &output,
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let veths = parse_veth_names(&stdout);

        let mut count = 0u32;
        for iface in veths {
            let delete = self
                .run_network_command(
                    "network_cleanup_orphaned_veth_delete",
                    "ip",
                    &link_delete_args(&iface),
                )
                .await;

            match delete {
                Ok(out) if out.status.success() => {
                    count += 1;
                }
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    tracing::warn!(
                        bridge = bridge_name,
                        iface = %iface,
                        stderr = %stderr.trim(),
                        "Failed to delete orphaned veth"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        bridge = bridge_name,
                        iface = %iface,
                        error = %e,
                        "Failed to delete orphaned veth"
                    );
                }
            }
        }

        if count > 0 {
            tracing::info!(
                "Cleaned up {} orphaned veth pairs from bridge {}",
                count,
                bridge_name
            );
        }
        Ok(())
    }

    pub async fn cleanup_wireguard_device(&self) -> Result<()> {
        let Some(wireguard_device) = self.current_namespace_wireguard_device() else {
            tracing::debug!("network cleanup: skipping WireGuard delete outside owned namespace");
            return Ok(());
        };

        let check = self
            .run_network_command(
                "network_cleanup_wireguard_inspect",
                "ip",
                &wireguard_show_args(wireguard_device),
            )
            .await?;

        if !check.status.success() {
            if inspection_reports_not_found(&check) {
                return Ok(());
            }
            return Err(command_failure(
                "ip",
                &wireguard_show_args(wireguard_device),
                &check,
            ));
        }

        let delete = self
            .run_network_command(
                "network_cleanup_wireguard_delete",
                "ip",
                &wireguard_delete_args(wireguard_device),
            )
            .await
            .context("Failed to delete WireGuard interface")?;
        if delete.status.success() {
            tracing::info!("Deleted WireGuard interface: {}", wireguard_device);
        } else {
            return Err(command_failure(
                "ip",
                &wireguard_delete_args(wireguard_device),
                &delete,
            ));
        }

        Ok(())
    }

    pub async fn cleanup_nft_table(&self) -> Result<()> {
        let Some(nft_table_name) = self.current_namespace_nft_table_name() else {
            tracing::debug!("network cleanup: skipping nft table delete outside owned namespace");
            return Ok(());
        };

        let check = self
            .run_network_command(
                "network_cleanup_nft_table_inspect",
                "nft",
                &nft_list_table_args(nft_table_name),
            )
            .await?;

        if !check.status.success() {
            if inspection_reports_not_found(&check) {
                return Ok(());
            }
            return Err(command_failure(
                "nft",
                &nft_list_table_args(nft_table_name),
                &check,
            ));
        }

        let delete = self
            .run_network_command(
                "network_cleanup_nft_table_delete",
                "nft",
                &nft_delete_table_args(nft_table_name),
            )
            .await
            .context("Failed to delete nftables table")?;
        if delete.status.success() {
            tracing::info!("Deleted nftables table inet {}", nft_table_name);
        } else {
            return Err(command_failure(
                "nft",
                &nft_delete_table_args(nft_table_name),
                &delete,
            ));
        }

        Ok(())
    }

    pub async fn cleanup_cni_artifacts(&self) -> Result<()> {
        let scope = self.cni_scope.clone();
        let key = scope.results_dir.to_string_lossy().into_owned();
        let artifacts = self
            .file_process
            .run_blocking_file_keyed("cleanup_cni_cache_files", key, move || {
                cleanup_cni_cache_files(scope)
            })
            .await?;

        let mut seen = HashSet::new();
        for artifact in artifacts {
            if let Some(netns_name) = artifact.record.netns_name
                && seen.insert(netns_name.clone())
            {
                self.cleanup_cni_netns(&netns_name).await;
            }
        }

        Ok(())
    }

    async fn cleanup_cni_netns(&self, netns_name: &str) {
        let netns_path = netns_runtime_path(netns_name);
        if !matches!(
            crate::runtime_fs::exists_async(&self.file_process, &netns_path).await,
            Ok(true)
        ) {
            return;
        }

        match self
            .run_network_command(
                "network_cleanup_cni_netns_delete",
                "ip",
                &netns_delete_args(netns_name),
            )
            .await
        {
            Ok(out) if out.status.success() => {
                tracing::info!("Deleted CNI netns: {}", netns_name);
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!(
                    netns = netns_name,
                    stderr = %stderr.trim(),
                    "Failed to delete CNI netns"
                );
            }
            Err(e) => {
                tracing::warn!(
                    netns = netns_name,
                    error = %e,
                    "Failed to delete CNI netns"
                );
            }
        }
    }

    #[cfg(test)]
    fn bridge_name_for_test(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root { bridge_name, .. } => Some(bridge_name.as_str()),
            NetworkCleanupMode::Rootless {
                bridge_name,
                inside_rootlesskit: true,
                ..
            } => Some(bridge_name.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }

    #[cfg(test)]
    fn wireguard_device_for_test(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root {
                wireguard_device, ..
            } => Some(wireguard_device.as_str()),
            NetworkCleanupMode::Rootless {
                wireguard_device,
                inside_rootlesskit: true,
                ..
            } => Some(wireguard_device.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }

    #[cfg(test)]
    fn nft_table_name_for_test(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root { nft_table_name, .. } => Some(nft_table_name.as_str()),
            NetworkCleanupMode::Rootless {
                nft_table_name,
                inside_rootlesskit: true,
                ..
            } => Some(nft_table_name.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }

    fn current_namespace_bridge_name(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root { bridge_name, .. } => Some(bridge_name.as_str()),
            NetworkCleanupMode::Rootless {
                bridge_name,
                inside_rootlesskit: true,
                ..
            } => Some(bridge_name.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }

    fn current_namespace_wireguard_device(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root {
                wireguard_device, ..
            } => Some(wireguard_device.as_str()),
            NetworkCleanupMode::Rootless {
                wireguard_device,
                inside_rootlesskit: true,
                ..
            } => Some(wireguard_device.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }

    fn current_namespace_nft_table_name(&self) -> Option<&str> {
        match &self.mode {
            NetworkCleanupMode::Root { nft_table_name, .. } => Some(nft_table_name.as_str()),
            NetworkCleanupMode::Rootless {
                nft_table_name,
                inside_rootlesskit: true,
                ..
            } => Some(nft_table_name.as_str()),
            NetworkCleanupMode::Rootless { .. } => None,
        }
    }
}

impl CniCleanupScope {
    fn new(network_name: impl Into<String>) -> Self {
        let network_name = network_name.into();
        Self {
            networks_dir: cni_networks_dir(&network_name),
            network_name,
            results_dir: cni_results_dir(),
        }
    }
}

#[derive(Debug)]
struct CniCacheArtifact {
    record: CniCacheRecord,
}

#[derive(Debug, PartialEq, Eq)]
struct CniCacheRecord {
    container_id: String,
    netns_name: Option<String>,
}

#[derive(Deserialize)]
struct RawCniCacheRecord {
    #[serde(rename = "containerId")]
    container_id: Option<String>,
    #[serde(rename = "networkName")]
    network_name: Option<String>,
    netns: Option<String>,
}

fn bridge_show_args(bridge_name: &str) -> Vec<&str> {
    vec!["link", "show", bridge_name]
}

fn bridge_delete_args(bridge_name: &str) -> Vec<&str> {
    vec!["link", "delete", bridge_name]
}

fn orphaned_veth_list_args(bridge_name: &str) -> Vec<&str> {
    vec!["-o", "link", "show", "master", bridge_name, "type", "veth"]
}

fn link_delete_args(iface: &str) -> Vec<&str> {
    vec!["link", "delete", iface]
}

fn wireguard_show_args(wireguard_device: &str) -> Vec<&str> {
    vec!["link", "show", wireguard_device]
}

fn wireguard_delete_args(wireguard_device: &str) -> Vec<&str> {
    link_delete_args(wireguard_device)
}

fn nft_list_table_args(table_name: &str) -> Vec<&str> {
    vec!["list", "table", "inet", table_name]
}

fn nft_delete_table_args(table_name: &str) -> Vec<&str> {
    vec!["delete", "table", "inet", table_name]
}

fn netns_delete_args(netns_name: &str) -> Vec<&str> {
    vec!["netns", "del", netns_name]
}

fn netns_runtime_path(netns_name: &str) -> PathBuf {
    PathBuf::from("/run/netns").join(netns_name)
}

fn cni_results_dir() -> PathBuf {
    PathBuf::from("/var/lib/cni/results")
}

fn cni_networks_dir(network_name: &str) -> PathBuf {
    PathBuf::from("/var/lib/cni/networks").join(network_name)
}

fn parse_veth_names(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let iface = line
                .split_whitespace()
                .nth(1)?
                .trim_end_matches(':')
                .split('@')
                .next()?;
            if iface.starts_with("veth") {
                Some(iface.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn is_recorded_klights_veth_name(name: &str) -> bool {
    let Some(suffix) = name.strip_prefix("veth") else {
        return false;
    };
    !suffix.is_empty() && name.len() <= 15 && suffix.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn cleanup_cni_cache_files(scope: CniCleanupScope) -> Result<Vec<CniCacheArtifact>> {
    let mut artifacts = Vec::new();

    ensure_cni_results_dir(&scope.results_dir)?;

    match std::fs::read_dir(&scope.results_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        tracing::warn!("Failed to read CNI cache entry: {}", e);
                        continue;
                    }
                };
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let bytes = match std::fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "Failed to read CNI cache file"
                        );
                        continue;
                    }
                };
                match parse_cni_cache_record(&bytes, &scope.network_name) {
                    Ok(Some(record)) => {
                        if let Err(e) = std::fs::remove_file(&path) {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "Failed to remove CNI cache file"
                            );
                        } else {
                            tracing::info!("Removed CNI cache file: {}", path.display());
                        }
                        artifacts.push(CniCacheArtifact { record });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "Failed to parse CNI cache file"
                        );
                    }
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).with_context(|| {
                format!(
                    "failed to read CNI result dir {}",
                    scope.results_dir.display()
                )
            });
        }
    }

    if scope.networks_dir.exists() {
        std::fs::remove_dir_all(&scope.networks_dir).with_context(|| {
            format!(
                "failed to remove CNI host-local dir {}",
                scope.networks_dir.display()
            )
        })?;
        tracing::info!(
            "Removed CNI host-local allocation dir: {}",
            scope.networks_dir.display()
        );
    }

    Ok(artifacts)
}

fn ensure_cni_results_dir(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let target = std::fs::read_link(path).with_context(|| {
                format!("failed to read CNI results symlink {}", path.display())
            })?;
            let target = if target.is_absolute() {
                target
            } else {
                path.parent().unwrap_or_else(|| Path::new("/")).join(target)
            };
            if target.is_dir() {
                return Ok(());
            }
            std::fs::remove_file(path).with_context(|| {
                format!(
                    "failed to remove dead CNI results symlink {} -> {}",
                    path.display(),
                    target.display()
                )
            })?;
            std::fs::create_dir_all(path)
                .with_context(|| format!("failed to create CNI results dir {}", path.display()))?;
        }
        Ok(metadata) if metadata.is_dir() => {}
        Ok(metadata) if metadata.is_file() => {
            std::fs::remove_file(path).with_context(|| {
                format!(
                    "failed to remove non-directory CNI results path {}",
                    path.display()
                )
            })?;
            std::fs::create_dir_all(path)
                .with_context(|| format!("failed to create CNI results dir {}", path.display()))?;
        }
        Ok(_) => {
            anyhow::bail!(
                "CNI results path {} exists but is not a directory, symlink, or regular file",
                path.display()
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path)
                .with_context(|| format!("failed to create CNI results dir {}", path.display()))?;
        }
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed to inspect CNI results path {}", path.display()));
        }
    }
    Ok(())
}

fn parse_cni_cache_record(bytes: &[u8], network_name: &str) -> Result<Option<CniCacheRecord>> {
    let raw: RawCniCacheRecord = serde_json::from_slice(bytes).context("invalid CNI cache JSON")?;
    if raw.network_name.as_deref() != Some(network_name) {
        return Ok(None);
    }
    let container_id = raw.container_id.unwrap_or_default();
    if container_id.trim().is_empty() {
        return Ok(None);
    }
    let netns_name = raw
        .netns
        .as_deref()
        .and_then(cni_netns_basename)
        .map(str::to_string);
    Ok(Some(CniCacheRecord {
        container_id,
        netns_name,
    }))
}

fn cni_netns_basename(raw: &str) -> Option<&str> {
    let path = Path::new(raw);
    let parent_name = path.parent()?.file_name()?.to_str()?;
    let name = path.file_name()?.to_str()?;
    if parent_name == "netns" && name.starts_with("cni-") {
        Some(name)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::file_blocking::process_test_support::output;
    use crate::networking::{NetworkCleanupConfig, NetworkMode};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct FakeProcessOutputRunner {
        results: Mutex<VecDeque<Result<std::process::Output>>>,
        calls: Mutex<Vec<(String, String, Vec<String>)>>,
    }

    impl FakeProcessOutputRunner {
        fn new(results: Vec<Result<std::process::Output>>) -> Self {
            Self {
                results: Mutex::new(results.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(String, String, Vec<String>)> {
            self.calls.lock().expect("fake calls lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl NetworkCommandRunner for FakeProcessOutputRunner {
        async fn run_network_output(
            &self,
            _category: klights_supervisor::TaskCategory,
            name: &'static str,
            program: &str,
            args: &[&str],
        ) -> Result<std::process::Output> {
            self.calls.lock().expect("fake calls lock").push((
                name.to_string(),
                program.to_string(),
                args.iter().map(|arg| (*arg).to_string()).collect(),
            ));
            self.results
                .lock()
                .expect("fake results lock")
                .pop_front()
                .unwrap_or_else(|| Err(anyhow::anyhow!("fake network runner exhausted")))
        }
    }

    #[test]
    fn network_cleanup_remains_publicly_debuggable_without_exposing_executor_state() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<NetworkCleanup>();

        let rendered = format!("{:?}", NetworkCleanup::root("klights"));
        assert!(rendered.contains("file_process: \"<FileProcessExecutor>\""));
        assert!(!rendered.contains("TaskSupervisorInner"));
    }

    #[tokio::test]
    async fn bridge_cleanup_obeys_spawn_status_not_found_and_mutation_results() {
        struct Case {
            name: &'static str,
            results: Vec<Result<std::process::Output>>,
            expect_error: bool,
            expect_removed: bool,
            expected_calls: usize,
        }

        let cases = vec![
            Case {
                name: "spawn error",
                results: vec![Err(anyhow::anyhow!("spawn failed"))],
                expect_error: true,
                expect_removed: false,
                expected_calls: 1,
            },
            Case {
                name: "nonzero inspect",
                results: vec![Ok(output(2, b"", b"permission denied"))],
                expect_error: true,
                expect_removed: false,
                expected_calls: 1,
            },
            Case {
                name: "not found",
                results: vec![Ok(output(
                    1,
                    b"",
                    b"Device \"klights-test\" does not exist.",
                ))],
                expect_error: false,
                expect_removed: false,
                expected_calls: 1,
            },
            Case {
                name: "nonzero mutation",
                results: vec![Ok(output(0, b"", b"")), Ok(output(1, b"", b"device busy"))],
                expect_error: true,
                expect_removed: false,
                expected_calls: 2,
            },
            Case {
                name: "success",
                results: vec![Ok(output(0, b"", b"")), Ok(output(0, b"", b""))],
                expect_error: false,
                expect_removed: true,
                expected_calls: 2,
            },
        ];

        for case in cases {
            let cleanup = NetworkCleanup::root("klights-test");
            let runner = FakeProcessOutputRunner::new(case.results);
            let result = cleanup.cleanup_bridge_with_runner(&runner).await;

            assert_eq!(result.is_err(), case.expect_error, "{}", case.name);
            assert_eq!(
                result.ok() == Some(CleanupDisposition::Removed),
                case.expect_removed,
                "{} must not report/log removal unless mutation succeeded",
                case.name
            );
            assert_eq!(runner.calls().len(), case.expected_calls, "{}", case.name);
        }
    }

    #[test]
    fn root_cleanup_handle_has_root_ip_command_plans_without_datapath() {
        let cleanup = NetworkCleanup::root("klights-test");
        assert_eq!(cleanup.bridge_name_for_test(), Some("klights-test"));
        assert_eq!(
            bridge_show_args("klights-test"),
            vec!["link", "show", "klights-test"]
        );
        assert_eq!(
            bridge_delete_args("klights-test"),
            vec!["link", "delete", "klights-test"]
        );
        assert_eq!(
            orphaned_veth_list_args("klights-test"),
            vec![
                "-o",
                "link",
                "show",
                "master",
                "klights-test",
                "type",
                "veth"
            ]
        );
        assert_eq!(
            link_delete_args("vethabc123"),
            vec!["link", "delete", "vethabc123"]
        );
    }

    #[test]
    fn root_cleanup_plans_bridge_wireguard_nft_and_cni_artifacts() {
        let mut cfg = crate::KlightsConfig::test_default();
        cfg.bridge_name = "klights".to_string();
        cfg.containerd_namespace = "klights".to_string();
        cfg.wireguard_device = "klights.wg".to_string();

        let focused = NetworkCleanupConfig::try_new(
            NetworkMode::Root,
            cfg.bridge_name.clone(),
            cfg.wireguard_device.clone(),
            cfg.containerd_namespace.clone(),
            false,
        )
        .unwrap();
        let cleanup = NetworkCleanup::from_config(
            &focused,
            crate::kubelet::file_blocking::test_file_process_executor(),
        );

        assert_eq!(cleanup.bridge_name_for_test(), Some("klights"));
        assert_eq!(
            cleanup.nft_table_name_for_test(),
            Some("klights"),
            "service-routing nft table is owned by containerd namespace, not bridge truncation"
        );
        assert_eq!(
            cleanup.wireguard_device_for_test(),
            Some("klights.wg"),
            "root cleanup must delete the configured WireGuard device"
        );
        assert_eq!(
            wireguard_delete_args("klights.wg"),
            vec!["link", "delete", "klights.wg"]
        );
        assert_eq!(
            nft_delete_table_args("klights"),
            vec!["delete", "table", "inet", "klights"]
        );
    }

    #[test]
    fn cni_cache_parser_finds_only_current_network_netns_records() {
        let matching = r#"{
            "kind": "cniCacheV1",
            "containerId": "sandbox-a",
            "networkName": "klights",
            "netns": "/var/run/netns/cni-sandbox-a",
            "ifName": "eth0"
        }"#;
        let other_network = r#"{
            "kind": "cniCacheV1",
            "containerId": "sandbox-b",
            "networkName": "other",
            "netns": "/var/run/netns/cni-sandbox-b",
            "ifName": "eth0"
        }"#;

        let record = parse_cni_cache_record(matching.as_bytes(), "klights")
            .expect("valid cache JSON")
            .expect("matching network should be selected");
        assert_eq!(record.container_id, "sandbox-a");
        assert_eq!(
            record.netns_name.as_deref(),
            Some("cni-sandbox-a"),
            "cleanup must delete the bind-mounted CNI netns by basename"
        );

        assert!(
            parse_cni_cache_record(other_network.as_bytes(), "klights")
                .expect("valid cache JSON")
                .is_none(),
            "cleanup must not touch host CNI cache owned by another network"
        );
    }

    #[test]
    fn cni_cleanup_replaces_dead_results_symlink_with_directory() {
        let dir = tempfile::tempdir().unwrap();
        let results_link = dir.path().join("results");
        let target = dir.path().join("rootless-worker").join("cni-results");
        std::os::unix::fs::symlink(&target, &results_link).unwrap();

        let scope = CniCleanupScope {
            network_name: "klights".to_string(),
            results_dir: results_link.clone(),
            networks_dir: dir.path().join("networks").join("klights"),
        };

        let artifacts = cleanup_cni_cache_files(scope).unwrap();

        assert!(
            artifacts.is_empty(),
            "empty newly-created CNI results dir must not report artifacts"
        );
        assert!(
            results_link.is_dir(),
            "cleanup must replace a dead results symlink with a real directory"
        );
        assert!(
            !results_link.is_symlink(),
            "cleanup must not leave a stale rootless-owned results symlink in place"
        );
        assert!(
            !target.exists(),
            "cleanup must not resurrect the dead rootless symlink target"
        );
    }

    #[test]
    fn cni_cleanup_preserves_live_results_symlink_and_cleans_records() {
        let dir = tempfile::tempdir().unwrap();
        let results_link = dir.path().join("results");
        let target = dir.path().join("rootless-worker").join("cni-results");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("sandbox-a-eth0"),
            r#"{
                "kind": "cniCacheV1",
                "containerId": "sandbox-a",
                "networkName": "klights",
                "netns": "/var/run/netns/cni-sandbox-a",
                "ifName": "eth0"
            }"#,
        )
        .unwrap();
        std::fs::write(
            target.join("sandbox-b-eth0"),
            r#"{
                "kind": "cniCacheV1",
                "containerId": "sandbox-b",
                "networkName": "other",
                "netns": "/var/run/netns/cni-sandbox-b",
                "ifName": "eth0"
            }"#,
        )
        .unwrap();
        std::os::unix::fs::symlink(&target, &results_link).unwrap();

        let scope = CniCleanupScope {
            network_name: "klights".to_string(),
            results_dir: results_link.clone(),
            networks_dir: dir.path().join("networks").join("klights"),
        };

        let artifacts = cleanup_cni_cache_files(scope).unwrap();

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].record.container_id, "sandbox-a");
        assert!(
            results_link.is_symlink(),
            "cleanup must preserve a live rootless-owned results symlink"
        );
        assert!(
            !target.join("sandbox-a-eth0").exists(),
            "cleanup must remove klights-owned cache records through the live symlink"
        );
        assert!(
            target.join("sandbox-b-eth0").exists(),
            "cleanup must leave other-network cache records alone"
        );
    }

    #[test]
    fn rootless_cleanup_handle_targets_current_user_netns_resources() {
        let mut cfg = crate::KlightsConfig::test_default();
        cfg.bridge_name = "klights".to_string();
        cfg.containerd_namespace = "klights".to_string();
        cfg.wireguard_device = "klights.wg".to_string();

        let focused = NetworkCleanupConfig::try_new(
            NetworkMode::Rootless,
            cfg.bridge_name.clone(),
            cfg.wireguard_device.clone(),
            cfg.containerd_namespace.clone(),
            true,
        )
        .unwrap();
        let cleanup = NetworkCleanup::from_config(
            &focused,
            crate::kubelet::file_blocking::test_file_process_executor(),
        );

        assert_eq!(
            cleanup.bridge_name_for_test(),
            Some("klights"),
            "rootless cleanup must delete current user-netns bridge leftovers"
        );
        assert_eq!(
            cleanup.wireguard_device_for_test(),
            Some("klights.wg"),
            "rootless cleanup must delete current user-netns WireGuard leftovers"
        );
        assert_eq!(
            cleanup.nft_table_name_for_test(),
            Some("klights"),
            "rootless cleanup must delete current user-netns nft table leftovers"
        );
    }

    #[test]
    fn parse_veth_names_handles_ip_o_output() {
        let output = "\
4: vethbcfc09c1@if2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP mode DEFAULT group default
22: lights-tester-2: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc noqueue state DOWN mode DEFAULT group default qlen 1000
27: veth167109c5@if2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP mode DEFAULT group default";

        let names = parse_veth_names(output);
        assert_eq!(names, vec!["vethbcfc09c1", "veth167109c5"]);
    }

    #[test]
    fn recorded_klights_veth_name_rejects_unowned_harness_or_host_names() {
        assert!(is_recorded_klights_veth_name("veth12345678abc"));
        assert!(is_recorded_klights_veth_name("vethuid"));
        assert!(!is_recorded_klights_veth_name("eth0"));
        assert!(!is_recorded_klights_veth_name("veth-l-host"));
        assert!(!is_recorded_klights_veth_name("veth"));
        assert!(!is_recorded_klights_veth_name("veth123456789012345"));
    }
}
