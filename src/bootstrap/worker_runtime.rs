//! Worker runtime (T5).
//!
//! Owns the worker boot sequence. A worker joins with a token, registers,
//! runs kubelet/networking/heartbeat, and does NOT run the API server,
//! scheduler, or cluster-wide controllers.
//!
//! T5 stops at the bootstrap shape: `run_worker(cli)` is the single entry
//! point, delegated to from the dispatcher in `runtime.rs::run_with_flags`.
//! The current implementation re-exports the existing
//! `runtime::run_worker_with_flags` body verbatim while the line-by-line
//! file move is staged.
//!
//! Replicas-as-learners (post-T1.6): `klights replica` maps to
//! `NodeRole::Controlplane { as_learner: true }` and boots the leader-class
//! stack, with kubelet storage supplied by the shared worker-store adapter.

use crate::bootstrap::CliFlags;

/// T5 entry point for `NodeRole::Worker { .. }`. Workers run kubelet,
/// networking, and heartbeat only.
pub(crate) async fn run_worker(cli: CliFlags) -> anyhow::Result<()> {
    crate::bootstrap::runtime::run_worker_with_flags(cli).await
}

/// Subsystems enabled for a worker node.
// dispatcher no longer validates it inline.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg(test)]
pub struct WorkerSubsystemConfig {
    pub api_server: bool,
    pub datastore_replication: bool,
    pub scheduler: bool,
    pub deployment_controller: bool,
    pub replicaset_controller: bool,
    pub statefulset_controller: bool,
    pub job_controller: bool,
    pub cronjob_controller: bool,
    pub pvc_controller: bool,
    pub pdb_controller: bool,
    pub resource_quota_controller: bool,
    pub gc_controller: bool,
    pub kubelet: bool,
    pub networking: bool,
    pub heartbeat: bool,
}

#[cfg(test)]
impl WorkerSubsystemConfig {
    /// Subsystem config for a worker-only node.
    ///
    /// Only kubelet, networking, and heartbeat are enabled.
    pub fn worker() -> Self {
        Self {
            api_server: false,
            datastore_replication: false,
            scheduler: false,
            deployment_controller: false,
            replicaset_controller: false,
            statefulset_controller: false,
            job_controller: false,
            cronjob_controller: false,
            pvc_controller: false,
            pdb_controller: false,
            resource_quota_controller: false,
            gc_controller: false,
            kubelet: true,
            networking: true,
            heartbeat: true,
        }
    }

    /// Returns true if any cluster-wide controller is enabled.
    pub fn has_cluster_controllers(&self) -> bool {
        self.scheduler
            || self.deployment_controller
            || self.replicaset_controller
            || self.statefulset_controller
            || self.job_controller
            || self.cronjob_controller
            || self.pvc_controller
            || self.pdb_controller
            || self.resource_quota_controller
            || self.gc_controller
    }
}

/// Validate that the worker subsystem config is correct.
#[cfg(test)]
pub fn validate_worker_config(config: &WorkerSubsystemConfig) -> Result<(), String> {
    if config.has_cluster_controllers() {
        return Err("worker must not run cluster-wide controllers".into());
    }

    if config.datastore_replication {
        return Err("worker must not keep a replicated datastore copy".into());
    }

    if config.api_server {
        return Err("worker must not run API server".into());
    }

    if !config.kubelet {
        return Err("worker must enable kubelet".into());
    }

    if !config.networking {
        return Err("worker must enable networking".into());
    }

    if !config.heartbeat {
        return Err("worker must enable heartbeat".into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_config_has_no_cluster_controllers() {
        let config = WorkerSubsystemConfig::worker();
        assert!(!config.has_cluster_controllers());
    }

    #[test]
    fn worker_config_has_no_api_server() {
        let config = WorkerSubsystemConfig::worker();
        assert!(!config.api_server);
    }

    #[test]
    fn worker_config_has_no_datastore_replication() {
        let config = WorkerSubsystemConfig::worker();
        assert!(!config.datastore_replication);
    }

    #[test]
    fn worker_config_has_node_local_pieces() {
        let config = WorkerSubsystemConfig::worker();
        assert!(config.kubelet, "worker must run kubelet");
        assert!(config.networking, "worker must run networking");
        assert!(config.heartbeat, "worker must run heartbeat");
    }

    #[test]
    fn validate_worker_config_succeeds() {
        let config = WorkerSubsystemConfig::worker();
        assert!(validate_worker_config(&config).is_ok());
    }

    #[test]
    fn validate_worker_config_fails_with_controllers() {
        let mut config = WorkerSubsystemConfig::worker();
        config.scheduler = true;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("cluster-wide controllers"));
    }

    #[test]
    fn validate_worker_config_fails_with_datastore() {
        let mut config = WorkerSubsystemConfig::worker();
        config.datastore_replication = true;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("replicated datastore"));
    }

    #[test]
    fn validate_worker_config_fails_with_api_server() {
        let mut config = WorkerSubsystemConfig::worker();
        config.api_server = true;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("API server"));
    }

    #[test]
    fn validate_worker_config_fails_without_kubelet() {
        let mut config = WorkerSubsystemConfig::worker();
        config.kubelet = false;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("kubelet"));
    }

    #[test]
    fn validate_worker_config_fails_without_networking() {
        let mut config = WorkerSubsystemConfig::worker();
        config.networking = false;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("networking"));
    }

    #[test]
    fn validate_worker_config_fails_without_heartbeat() {
        let mut config = WorkerSubsystemConfig::worker();
        config.heartbeat = false;
        let err = validate_worker_config(&config).unwrap_err();
        assert!(err.contains("heartbeat"));
    }
}
