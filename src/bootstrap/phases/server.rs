//! Phase 8: PID file, signal handling, HTTP server, and graceful shutdown.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::init::host::print_ready_message;
use crate::bootstrap::init::predicates::runs_api_server;
use crate::bootstrap::init::tls::serve_https;
use crate::task_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct ServeArgs<'a> {
    pub config: &'a Arc<KlightsConfig>,
    pub cli: &'a crate::bootstrap::CliFlags,
    pub app: axum::Router,
    pub pod_watcher_handle: Option<SupervisedJoinHandle<()>>,
    pub heartbeat_handle: SupervisedJoinHandle<()>,
    pub node_subnet_watch_handle: SupervisedJoinHandle<()>,
    pub node_lifecycle_handle: Option<SupervisedJoinHandle<()>>,
    pub crd_registry_watch_handle: SupervisedJoinHandle<()>,
    pub leader_peer_endpoint_observer_handle: Option<SupervisedJoinHandle<()>>,
    pub scheduler_controller_handle: Option<SupervisedJoinHandle<()>>,
    pub cni_rpc_token: CancellationToken,
    pub cni_rpc_handle: SupervisedJoinHandle<()>,
    pub controlplane_leader_control_stream_handle: Option<SupervisedJoinHandle<()>>,
    pub db_handle: crate::datastore::DatastoreHandle,
    pub shutdown_token: CancellationToken,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
}

pub async fn serve(args: ServeArgs<'_>) -> Result<()> {
    let ServeArgs {
        config,
        cli,
        app,
        pod_watcher_handle,
        heartbeat_handle,
        node_subnet_watch_handle,
        node_lifecycle_handle,
        crd_registry_watch_handle,
        leader_peer_endpoint_observer_handle,
        scheduler_controller_handle,
        cni_rpc_token,
        cni_rpc_handle,
        controlplane_leader_control_stream_handle,
        db_handle,
        shutdown_token,
        supervisor,
        grpc_transport_policy,
    } = args;
    use crate::pidfile;

    let pid_path = pidfile::default_pid_path(&config.containerd_namespace);
    if let Err(e) = pidfile::write(&pid_path) {
        tracing::warn!("Failed to write pidfile at {}: {}", pid_path.display(), e);
    } else {
        tracing::info!("Wrote pidfile at {}", pid_path.display());
    }

    let shutdown_signal = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("Received SIGTERM, shutting down gracefully"),
            _ = sigint.recv() => tracing::info!("Received SIGINT, shutting down gracefully"),
        }
    };

    let runs_api = runs_api_server(&cli.role);
    let serve_outcome = if !runs_api {
        print_ready_message(config);
        tracing::info!("worker role ready without local API server");
        shutdown_signal.await;
        Ok(())
    } else {
        let addr = api_bind_addr(config, cli);
        print_ready_message(config);
        serve_https(
            app,
            &addr,
            &config.containerd_namespace,
            supervisor.clone(),
            grpc_transport_policy,
            shutdown_signal,
        )
        .await
        .context("HTTPS startup failed")
    };

    // Soft shutdown
    tracing::info!("Starting soft shutdown — leaving pods and networking intact");
    shutdown_token.cancel();
    db_handle.close();

    let timeout = std::time::Duration::from_secs(10);

    if let Some(handle) = pod_watcher_handle {
        match supervisor
            .timeout("pod_watcher_shutdown_join", timeout, handle.join())
            .await
        {
            Ok(Ok(_)) => tracing::info!("Pod watcher task stopped"),
            Ok(Err(_)) => tracing::warn!("Pod watcher did not complete within timeout"),
            Err(e) => tracing::warn!("Pod watcher join cancelled: {e}"),
        }
    }
    match supervisor
        .timeout("heartbeat_shutdown_join", timeout, heartbeat_handle.join())
        .await
    {
        Ok(Ok(_)) => tracing::info!("Heartbeat task stopped"),
        Ok(Err(_)) => tracing::warn!("Heartbeat did not complete within timeout"),
        Err(e) => tracing::warn!("Heartbeat join cancelled: {e}"),
    }
    match supervisor
        .timeout(
            "node_subnet_watch_shutdown_join",
            timeout,
            node_subnet_watch_handle.join(),
        )
        .await
    {
        Ok(Ok(_)) => tracing::info!("Node subnet peer watch stopped"),
        Ok(Err(_)) => tracing::warn!("Node subnet peer watch did not complete within timeout"),
        Err(e) => tracing::warn!("Node subnet peer watch join cancelled: {e}"),
    }
    if let Some(handle) = node_lifecycle_handle {
        match supervisor
            .timeout("node_lifecycle_shutdown_join", timeout, handle.join())
            .await
        {
            Ok(Ok(_)) => tracing::info!("Node lifecycle controller stopped"),
            Ok(Err(_)) => {
                tracing::warn!("Node lifecycle controller did not complete within timeout")
            }
            Err(e) => tracing::warn!("Node lifecycle controller join cancelled: {e}"),
        }
    }
    match supervisor
        .timeout(
            "crd_registry_watch_shutdown_join",
            timeout,
            crd_registry_watch_handle.join(),
        )
        .await
    {
        Ok(Ok(_)) => tracing::info!("CRD registry watch stopped"),
        Ok(Err(_)) => tracing::warn!("CRD registry watch did not complete within timeout"),
        Err(e) => tracing::warn!("CRD registry watch join cancelled: {e}"),
    }
    if let Some(handle) = leader_peer_endpoint_observer_handle {
        match supervisor
            .timeout(
                "leader_peer_observed_endpoint_shutdown_join",
                timeout,
                handle.join(),
            )
            .await
        {
            Ok(Ok(_)) => tracing::info!("Leader peer observed endpoint watcher stopped"),
            Ok(Err(_)) => {
                tracing::warn!(
                    "Leader peer observed endpoint watcher did not complete within timeout"
                )
            }
            Err(e) => tracing::warn!("Leader peer observed endpoint watcher join cancelled: {e}"),
        }
    }
    if let Some(handle) = scheduler_controller_handle {
        match supervisor
            .timeout("scheduler_controller_shutdown_join", timeout, handle.join())
            .await
        {
            Ok(Ok(_)) => tracing::info!("Scheduler controller stopped"),
            Ok(Err(_)) => tracing::warn!("Scheduler controller did not complete within timeout"),
            Err(e) => tracing::warn!("Scheduler controller join cancelled: {e}"),
        }
    }
    if let Some(handle) = controlplane_leader_control_stream_handle {
        match supervisor
            .timeout(
                "controlplane_leader_control_stream_shutdown_join",
                timeout,
                handle.join(),
            )
            .await
        {
            Ok(Ok(_)) => tracing::info!("Control-plane leader control stream stopped"),
            Ok(Err(_)) => {
                tracing::warn!(
                    "Control-plane leader control stream did not complete within timeout"
                )
            }
            Err(e) => tracing::warn!("Control-plane leader control stream join cancelled: {e}"),
        }
    }

    cni_rpc_token.cancel();
    match supervisor
        .timeout("cni_rpc_shutdown_join", timeout, cni_rpc_handle.join())
        .await
    {
        Ok(Ok(_)) => tracing::info!("CNI RPC server stopped"),
        Ok(Err(_)) => tracing::warn!("CNI RPC server did not complete within timeout"),
        Err(e) => tracing::warn!("CNI RPC join cancelled: {e}"),
    }

    let ss = supervisor
        .shutdown(std::time::Duration::from_secs(10))
        .await;
    tracing::info!(
        "TaskSupervisor shutdown: total_managed={}, joined={}, aborted={}, timed_out={}, remaining_active={}",
        ss.total_managed,
        ss.joined,
        ss.aborted,
        ss.timed_out,
        ss.remaining_active
    );

    if let Err(e) = pidfile::remove(&pid_path) {
        tracing::warn!("Failed to remove pidfile: {}", e);
    }

    tracing::info!("Soft shutdown complete — pods and networking preserved");
    serve_outcome
}

pub(crate) fn api_bind_addr(config: &KlightsConfig, cli: &crate::bootstrap::CliFlags) -> String {
    let host = cli.bind_address.as_deref().unwrap_or("0.0.0.0");
    format!("{host}:{}", config.tls_port)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(
        role: crate::bootstrap::NodeRole,
        bind_address: Option<&str>,
    ) -> crate::bootstrap::CliFlags {
        crate::bootstrap::CliFlags {
            rootless: false,
            namespace: Some("test".to_string()),
            bind_address: bind_address.map(str::to_string),
            token_file: None,
            role,
        }
    }

    #[test]
    fn explicit_bind_address_overrides_default() {
        let mut config = KlightsConfig::test_default();
        config.tls_port = 7443;
        config.external_endpoint = Some("10.99.0.10".to_string());
        let cli = cli(
            crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: Vec::new(),
                token: None,
                skip_ca: false,
                as_learner: false,
            },
            Some("127.0.0.1"),
        );

        assert_eq!(api_bind_addr(&config, &cli), "127.0.0.1:7443");
    }

    #[test]
    fn controlplane_defaults_to_wildcard_so_pod_gateway_service_endpoint_is_reachable() {
        let mut config = KlightsConfig::test_default();
        config.tls_port = 7443;
        config.external_endpoint = Some("10.99.0.10".to_string());
        let cli = cli(
            crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: Vec::new(),
                token: None,
                skip_ca: false,
                as_learner: false,
            },
            None,
        );

        assert_eq!(api_bind_addr(&config, &cli), "0.0.0.0:7443");
    }

    #[test]
    fn non_controlplane_default_preserves_wildcard_bind() {
        let mut config = KlightsConfig::test_default();
        config.tls_port = 7443;
        config.external_endpoint = Some("10.99.0.10".to_string());
        let cli = cli(
            crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
        );

        assert_eq!(api_bind_addr(&config, &cli), "0.0.0.0:7443");
    }
}
