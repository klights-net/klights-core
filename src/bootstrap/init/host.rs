//! Host helpers extracted from runtime.rs (R3 refactor).

use crate::{KlightsConfig, paths, version};

pub async fn discover_host_ip() -> anyhow::Result<String> {
    crate::kubelet::node_ip::discover_primary_route_ip().await
}

pub fn print_ready_message(config: &KlightsConfig) {
    let etc_dir = paths::etc_dir_path(&config.containerd_namespace);
    tracing::info!("");
    tracing::info!("  klights is ready! {}", version::git_version());
    tracing::info!("  API server: https://localhost:{}", config.tls_port);
    tracing::info!("");
    tracing::info!("  Connect with kubectl:");
    tracing::info!(
        "    export KUBECONFIG={}",
        etc_dir.join("kubeconfig.yaml").display()
    );
    tracing::info!("    kubectl get nodes");
    tracing::info!("");
}
