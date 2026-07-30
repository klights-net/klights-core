//! Phase 2: Config loading, supervisor creation, and filesystem paths.
//!
//! Returns a `ConfigPhase` struct consumed by all downstream phases.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::{CliFlags, NodeMode};
use crate::networking::NetworkCleanup;
use klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy;
use klights_supervisor::TaskSupervisor;

pub struct ConfigPhase {
    pub config: Arc<KlightsConfig>,
    pub node_mode: NodeMode,
    pub supervisor: Arc<TaskSupervisor>,
    pub file_process: klights_supervisor::FileProcessExecutor,
    pub grpc_transport_policy: SharedGrpcTransportPolicy,
    pub network_cleanup: NetworkCleanup,
    pub shutdown_token: CancellationToken,
    pub etc_dir: String,
    pub containerd_state_dir: String,
    pub runtime_paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
}

pub async fn load(cli: &CliFlags) -> Result<ConfigPhase> {
    let mut config = KlightsConfig::from_env_with_namespace_override(cli.namespace.as_deref())
        .context("invalid klights configuration")?;
    if let Some(anonymous_auth) = cli.anonymous_auth {
        config.anonymous_auth = anonymous_auth;
    }
    let config = Arc::new(config);
    let node_mode =
        NodeMode::detect(cli.rootless).context("failed to detect klights operating mode")?;
    tracing::info!(?node_mode, "operating mode detected");

    super::super::init::predicates::validate_worker_dataplane_ingress(&cli.role, &config)?;

    let shutdown_token = CancellationToken::new();

    let task_config = klights_supervisor::TaskCategoryConfig::from_env()
        .context("invalid task supervisor category limits")?;
    let supervisor = Arc::new(TaskSupervisor::new(task_config));
    let file_process = klights_supervisor::FileProcessExecutor::new(supervisor.clone());
    let network_cleanup = NetworkCleanup::from_config(
        &crate::bootstrap::network_adapters::cleanup_config(&node_mode, &config)?,
        file_process.clone(),
    );
    let grpc_transport_policy =
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default();
    let task_limits = supervisor.config();
    tracing::info!(
        "task supervisor category limits: background={}, file={}, db={}, timer={}, network={}, pod_delete_workqueue={}, others={}",
        task_limits.background,
        task_limits.file,
        task_limits.db,
        task_limits.timer,
        task_limits.network,
        task_limits.pod_delete_workqueue,
        task_limits.others
    );

    let etc_dir = config.data_root.join("etc").to_string_lossy().into_owned();
    let containerd_state_dir = config
        .data_root
        .join("containerd")
        .join("state")
        .to_string_lossy()
        .into_owned();
    let runtime_paths =
        crate::kubelet::runtime_paths::KubeletRuntimePaths::new(config.data_root.clone())
            .context("invalid kubelet runtime path layout")?;

    Ok(ConfigPhase {
        config,
        node_mode,
        supervisor,
        file_process,
        grpc_transport_policy,
        network_cleanup,
        shutdown_token,
        etc_dir,
        containerd_state_dir,
        runtime_paths,
    })
}
