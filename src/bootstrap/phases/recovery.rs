//! Phase 3: Startup resource recovery and host IP discovery.

use anyhow::{Context, Result};

use super::config::ConfigPhase;
use crate::bootstrap::NodeMode;
use crate::bootstrap::init::host::discover_host_ip;
use crate::bootstrap::init::recovery::run_startup_resource_recovery;

pub struct RecoveryPhase {
    pub node_ip: String,
}

pub async fn run(cfg: &ConfigPhase) -> Result<RecoveryPhase> {
    run_startup_resource_recovery(
        &cfg.config,
        &cfg.node_mode,
        &cfg.network_cleanup,
        &cfg.containerd_state_dir,
        cfg.supervisor.as_ref(),
        cfg.grpc_transport_policy.as_ref(),
    )
    .await
    .context("failed to recover previous startup resources")?;

    let discovered_ip = discover_host_ip()
        .await
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let node_ip = select_node_ip(
        cfg.config.node_ip.as_deref(),
        cfg.config.external_endpoint.as_deref(),
        &cfg.node_mode,
        discovered_ip,
    );

    Ok(RecoveryPhase { node_ip })
}

fn select_node_ip(
    node_ip_override: Option<&str>,
    external_endpoint: Option<&str>,
    node_mode: &NodeMode,
    discovered_ip: String,
) -> String {
    if let Some(node_ip) = node_ip_override {
        return node_ip.to_string();
    }

    if matches!(node_mode, NodeMode::Rootless { .. })
        && let Some(endpoint) = external_endpoint
    {
        return endpoint.to_string();
    }

    discovered_ip
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_node_ip_prefers_node_specific_override_for_root() {
        let selected = select_node_ip(
            Some("192.0.2.10"),
            Some("198.51.100.20"),
            &NodeMode::Root,
            "10.0.0.5".to_string(),
        );

        assert_eq!(selected, "192.0.2.10");
    }

    #[test]
    fn select_node_ip_prefers_node_specific_override_for_rootless() {
        let selected = select_node_ip(
            Some("192.0.2.11"),
            Some("198.51.100.21"),
            &NodeMode::Rootless {
                rootlesskit_pid: 0,
                user_netns: std::path::PathBuf::from("/proc/0/ns/net"),
            },
            "10.0.0.6".to_string(),
        );

        assert_eq!(selected, "192.0.2.11");
    }

    #[test]
    fn select_node_ip_preserves_rootless_external_endpoint_fallback() {
        let selected = select_node_ip(
            None,
            Some("198.51.100.22"),
            &NodeMode::Rootless {
                rootlesskit_pid: 0,
                user_netns: std::path::PathBuf::from("/proc/0/ns/net"),
            },
            "10.0.0.7".to_string(),
        );

        assert_eq!(selected, "198.51.100.22");
    }
}
