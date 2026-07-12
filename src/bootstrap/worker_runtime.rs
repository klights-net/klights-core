//! Worker runtime (T5).
//!
//! Owns the worker boot sequence. A worker joins with a token, registers,
//! runs kubelet/networking/heartbeat, and does NOT run the API server,
//! scheduler, or cluster-wide controllers.
//!
//! T5 stops at the bootstrap shape: `run_worker(cli)` is the single entry
//! point, delegated to from the dispatcher in `runtime.rs::run_with_flags`.
//!
//! Replicas-as-learners (post-T1.6): `klights replica` maps to
//! `NodeRole::Controlplane { as_learner: true }` and boots the leader-class
//! stack, with kubelet storage supplied by the shared worker-store adapter.

use anyhow::Context;

use crate::bootstrap::phases;
use crate::bootstrap::{CliFlags, NodeRole};
use crate::{controllers, kubelet, pidfile};

use super::init::dataplane::*;
use super::init::host::print_ready_message;
use super::init::leader_control_stream::start_worker_leader_control_stream;
use super::runtime::resolve_token_file_if_present;
use super::worker_store_adapter::{
    start_worker_store_adapter, worker_store_backend, worker_store_handle,
};

fn worker_pod_runtime_node_role() -> crate::kubelet::pod_cluster_runtime::RuntimeNodeRole {
    crate::kubelet::pod_cluster_runtime::RuntimeNodeRole::Worker
}

// ── Worker boot ──────────────────────────────────────────────────────────

pub(crate) async fn run_worker(mut cli: CliFlags) -> anyhow::Result<()> {
    phases::env::init_tracing(&cli);
    phases::env::init_process(&cli)?;
    let cfg = phases::config::load(&cli).await?;
    resolve_token_file_if_present(&mut cli).await?;
    phases::env::validate_role(&cli.role, &cfg.node_mode)?;
    let recovery = phases::recovery::run(&cfg).await?;
    let identity = phases::identity::setup_worker(&cfg, &recovery.node_ip).await?;

    let config = cfg.config;
    let node_mode = cfg.node_mode;
    let task_supervisor = cfg.supervisor;
    let grpc_transport_policy = cfg.grpc_transport_policy;
    let network_cleanup = cfg.network_cleanup;
    let shutdown_token = cfg.shutdown_token;
    let containerd_data_dir = cfg.containerd_data_dir;
    let containerd_state_dir = cfg.containerd_state_dir;
    let node_ip = identity.node_ip;
    let follower_dataplane = identity.follower_dataplane.unwrap();
    let grpc_ca_cert_path =
        crate::bootstrap::init::predicates::grpc_ca_cert_path_for_role(&config, &cli.role);

    let (leader_endpoint, token, skip_ca, all_leader_endpoints) = match &cli.role {
        NodeRole::Worker {
            leader_endpoints,
            token,
            skip_ca,
        } => {
            if leader_endpoints.is_empty() {
                anyhow::bail!("worker requires a leader endpoint");
            }
            // T2 step 5: save the full list for endpoint cycling on
            // stream failure.
            let all = leader_endpoints.clone();
            // P3-7b: probe each `--leader` endpoint at startup so an HA
            // worker pinned to a downed primary picks a live peer
            // immediately instead of waiting on the gRPC handshake to
            // time out. Falls back to leader_endpoints[0] if every probe
            // fails — the legacy connect path then surfaces the error.
            let chosen = crate::bootstrap::leader_reconnect::pick_reachable_leader_endpoint(
                &task_supervisor,
                leader_endpoints,
            )
            .await;
            (chosen, token.clone(), *skip_ca, all)
        }
        _ => anyhow::bail!("worker runtime can only start NodeRole::Worker"),
    };
    let le = leader_endpoint.clone();
    let tk = token.clone();

    // Resolve worker credential: use persisted node client cert when available,
    // otherwise bootstrap one via CSR before creating steady-state clients.
    let (client_cert_pem, client_key_pem) = {
        use crate::bootstrap::worker_identity::{
            CredentialSource, HttpCsrBootstrapClient, SupervisedFilesystemWorkerCredentialStore,
            bootstrap_with_csr_async_store, resolve_credential_async,
        };
        let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
            &config.containerd_namespace,
            &config.node_name,
            task_supervisor.clone(),
        );
        match resolve_credential_async(&store).await {
            Ok(CredentialSource::ExistingCert(cred)) => {
                tracing::info!(
                    node = %config.node_name,
                    "using persisted node client certificate for leader connection"
                );
                (Some(cred.certificate_pem), Some(cred.private_key_pem))
            }
            Ok(CredentialSource::BootstrapRequired) => {
                let csr_token = token.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "no persisted node certificate and no token source provided; \
                         join with --token-file first, or ensure the node cert is not corrupted"
                    )
                })?;
                tracing::info!(
                    node = %config.node_name,
                    "no persisted node cert, bootstrapping via CSR"
                );
                let csr_client = HttpCsrBootstrapClient::new(
                    le.clone(),
                    csr_token.clone(),
                    grpc_ca_cert_path.clone(),
                    skip_ca,
                    task_supervisor.clone(),
                )
                .await?;
                let cred =
                    bootstrap_with_csr_async_store(&config.node_name, &csr_client, &store).await?;
                (Some(cred.certificate_pem), Some(cred.private_key_pem))
            }
            Err(e) => {
                let csr_token = token.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "persisted credential invalid ({}) and no token source provided; \
                         join with --token-file to re-bootstrap",
                        e
                    )
                })?;
                tracing::warn!(
                    node = %config.node_name,
                    error = %e,
                    "persisted credential invalid, bootstrapping via CSR"
                );
                let csr_client = HttpCsrBootstrapClient::new(
                    le.clone(),
                    csr_token.clone(),
                    grpc_ca_cert_path.clone(),
                    skip_ca,
                    task_supervisor.clone(),
                )
                .await?;
                let cred =
                    bootstrap_with_csr_async_store(&config.node_name, &csr_client, &store).await?;
                (Some(cred.certificate_pem), Some(cred.private_key_pem))
            }
        }
    };

    let grpc_config = crate::replication::grpc::client::GrpcClientConfig {
        leader_endpoint: le.clone(),
        token: tk.unwrap_or_default(),
        node_name: config.node_name.clone(),
        role: crate::replication::protocol::JoinRole::Worker,
        dataplane: follower_dataplane.clone(),
        ca_cert_path: grpc_ca_cert_path.clone(),
        skip_ca,
        client_cert_pem,
        client_key_pem,
    };
    let follower_grpc_client = std::sync::Arc::new(
        crate::replication::grpc::client::ReplicationGrpcClient::new(
            grpc_config,
            task_supervisor.clone(),
            grpc_transport_policy.clone(),
        ),
    );
    // T2 step 5: register all known leader endpoints so the reconnect
    // loop can cycle through them after a stream failure.
    follower_grpc_client.set_all_leader_endpoints(all_leader_endpoints.clone());
    let remote_api_client = std::sync::Arc::new(
        crate::control_plane::client::remote::RemoteApiClient::from_grpc(
            follower_grpc_client.clone(),
            task_supervisor.clone(),
            config.node_name.clone(),
        ),
    );
    let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
        remote_api_client.clone();

    let nldb: Option<&std::path::Path> = if config.in_memory {
        None
    } else {
        Some(config.node_db_path.as_path())
    };
    let node_local = crate::datastore::node_local::selector::open_node_local(
        config.node_local_backend,
        nldb,
        task_supervisor.clone(),
        config.db_key_file.as_deref(),
        "sqlite:node-local",
    )
    .await
    .context("worker node-local")?;

    // Replicas-as-learners: `klights replica` maps to
    // `NodeRole::Controlplane { as_learner: true }` and runs the
    // leader-class boot. The BackupApplier path is gone.
    let _ = leader_endpoint;

    let ob_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let outbox = std::sync::Arc::new(crate::kubelet::outbox::Outbox::with_notify(
        node_local.clone(),
        ob_notify.clone(),
    ));
    crate::kubelet::outbox::OutboxDispatcher::new(
        node_local.clone(),
        remote_api_client.clone(),
        ob_notify,
    )
    // bug-grpc: pipelined dispatch — keep multiple worker→leader
    // `apply_outbox` round-trips in flight (one per Status channel-lane
    // connection) instead of one row per WAN RTT.
    .with_batch_mode(crate::kubelet::outbox::DEFAULT_DISPATCH_INFLIGHT)
    .start(task_supervisor.clone(), shutdown_token.clone())
    .await
    .context("worker outbox")?;
    if !follower_dataplane.endpoint.trim().is_empty() {
        enqueue_worker_dataplane_metadata_outbox(
            Some(outbox.as_ref()),
            &config.node_name,
            &follower_dataplane,
        )
        .await
        .context("worker dataplane outbox")?;
    }
    let worker_store = start_worker_store_adapter(
        remote_api_client.clone(),
        node_local.clone(),
        config.node_name.clone(),
        task_supervisor.clone(),
        shutdown_token.clone(),
        Some(follower_grpc_client.clone()),
        all_leader_endpoints.clone(),
    )
    .await?;

    let db_handle = worker_store_handle(worker_store.clone());
    let db = worker_store_backend(&db_handle);

    let net = phases::network::boot(phases::network::NetworkBootArgs {
        config: &config,
        node_mode: &node_mode,
        node_ip: &node_ip,
        cluster_api: cluster_api.clone(),
        node_local: node_local.clone(),
        db,
        network_cleanup: &network_cleanup,
        containerd_data_dir: &containerd_data_dir,
        containerd_state_dir: &containerd_state_dir,
        supervisor: task_supervisor.clone(),
        grpc_transport_policy: grpc_transport_policy.clone(),
        shutdown_token: shutdown_token.clone(),
    })
    .await?;
    let network = net.network;
    let services = net.services;
    let cni_rpc_token = net.cni_rpc_token;
    let cni_rpc_handle = net.cni_rpc_handle;
    let cri_for_pod_watcher = net.cri_for_pod_watcher;
    let cri_for_api = net.cri_for_api;
    let cni_readiness = net.cni_readiness;
    let dataplane_health = net.dataplane_health;
    // A worker is always multinode: start NetworkUnavailable=True until the
    // first successful peer-route sync confirms every Ready peer is reachable.
    dataplane_health.set_peers_pending();

    // Register this worker's Node BEFORE spawning the node_subnet peer watcher
    // below. The watcher's initial `sync_peer_routes`
    // calls `refresh_node_network_conditions`, which `get_resource`s this Node to
    // write its dataplane-readiness conditions. If registration ran after the
    // watcher's first sync, that read returns `None`, the readiness write is
    // silently dropped (`Ok(false)`), yet `reconcile_local_readiness` still
    // advances its cached `last_readiness` — so no later re-sync re-issues the
    // write and the worker stays NotReady permanently. Registering first
    // guarantees the Node exists when the watcher syncs, and that the watcher's
    // Node watch subscription is established after the registration event.
    //
    // Option C.2: apply the typed registration snapshot synchronously via
    // cluster_api.apply_outbox()
    // before enqueuing in the outbox. This ensures the Node exists on the
    // leader before the node_subnet watcher's initial sync_peer_routes call,
    // which reads the Node via gRPC to write its dataplane-readiness conditions.
    let registration_addresses =
        kubelet::node::NodeRegistrationAddresses::new(node_ip.clone(), None);
    let registration = kubelet::node::NodeRegistrationSnapshot::capture_local(
        &config.node_name,
        &node_mode,
        &cli.role,
        registration_addresses,
        None,
        None,
    )
    .await;
    if let Err(e) = kubelet::node::register_node_snapshot(
        db,
        Some(outbox.as_ref()),
        Some(cluster_api.clone()),
        Some(&dataplane_health),
        &registration,
    )
    .await
    {
        tracing::warn!("worker node registration: {}", e);
    }

    if let Some(cri) = &cri_for_api {
        let eh = std::sync::Arc::new(
            crate::replication::grpc::client::CriNodeExecSyncHandler::new(
                cri.clone(),
                task_supervisor.clone(),
            ),
        );
        follower_grpc_client
            .set_node_exec_sync_handler(eh.clone())
            .await;
        follower_grpc_client.set_node_exec_stream_handler(eh).await;
        follower_grpc_client
            .set_node_metrics_handler(std::sync::Arc::new(
                crate::replication::grpc::client::CriNodeMetricsHandler::new(
                    cri.clone(),
                    task_supervisor.clone(),
                ),
            ))
            .await;
    }
    follower_grpc_client
        .set_pod_log_handler(std::sync::Arc::new(
            crate::replication::grpc::client::LocalPodLogHandler::new_with_pod_event_store(
                config.containerd_namespace.clone(),
                task_supervisor.clone(),
                db_handle.clone(),
            ),
        ))
        .await;
    let worker_control_stream_handle = start_worker_leader_control_stream(
        follower_grpc_client.clone(),
        task_supervisor.clone(),
        shutdown_token.clone(),
    )
    .await
    .context("worker control stream")?;
    let node_subnet_watch_handle = {
        let dbh = db_handle.clone();
        let node_name = config.node_name.clone();
        let cluster_cidr = config.cluster_cidr.clone();
        let peering = network.peering.clone();
        let supervisor_for_task = task_supervisor.clone();
        let health_for_peer_watch = dataplane_health.clone();
        let outbox_for_peer_watch = outbox.clone();
        let cancel = shutdown_token.clone();
        task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "worker_node_subnet_peer_watch",
                async move {
                    controllers::node_subnet::run_peer_watch_with_components(
                        dbh,
                        node_name,
                        cluster_cidr,
                        peering,
                        supervisor_for_task,
                        Some(health_for_peer_watch),
                        Some(outbox_for_peer_watch),
                        cancel,
                    )
                    .await;
                },
            )
            .await
            .context("worker node subnet peer watch")?
    };

    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        Some(services.clone()),
        Some(task_supervisor.clone()),
        Some(db_handle.clone()),
    ));
    let (pod_lifecycle_tx, pod_lifecycle_rx) =
        tokio::sync::mpsc::channel::<crate::kubelet::lifecycle::LifecycleCommand>(128);
    let pod_lifecycle_rx = std::sync::Arc::new(tokio::sync::Mutex::new(Some(pod_lifecycle_rx)));
    let pod_watcher_runtime_ports = cri_for_pod_watcher.clone().map(|cri| {
        let runtime = std::sync::Arc::new(crate::kubelet::pod_runtime::cri::SharedCriRuntime::new(
            crate::kubelet::cri::SharedCriClient::new(cri),
        ));
        crate::kubelet::pod_manager::PodWatcherRuntimePorts::new(
            runtime.clone(),
            runtime,
            cni_readiness.clone(),
        )
    });
    let pod_repository_parts = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db_handle.clone(),
            supervisor: task_supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode:
                crate::kubelet::pod_repository::api::PodSchedulingMode::DeferredMultiNodeLeader,
            outbox: Some(outbox.clone()),
            cluster_api: Some(cluster_api.clone()),
        },
    );
    let pod_subsystem = crate::kubelet::pod_subsystem::PodSubsystem::new(
        crate::kubelet::pod_subsystem::PodSubsystemConfig {
            repository_parts: pod_repository_parts,
            supervisor: task_supervisor.clone(),
            outbox: Some(outbox.clone()),
            cluster_api: Some(cluster_api.clone()),
            node_name: config.node_name.clone(),
            service_cidr: config.service_cidr.clone(),
            lifecycle_concurrency: crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            cri: cri_for_pod_watcher.clone().map(crate::kubelet::cri::SharedCriClient::new),
            containerd_ns: config.containerd_namespace.clone(),
            lifecycle_tx: pod_lifecycle_tx,
            probe_manager: None,
            datapath: Some(network.datapath.clone()),
            service_router: Some(services.clone()),
            runtime_node_role: worker_pod_runtime_node_role(),
            runtime_service: None,
            runtime_store: std::sync::Arc::new(
                crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(db_handle.clone()),
            ),
            slot_admission: std::sync::Arc::new(
                crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
                    db_handle.clone(),
                    config.node_name.clone(),
                ),
            ),
            event_sink: std::sync::Arc::new(
                crate::kubelet::pod_runtime::events::RealPodEventSink::new(
                    Some(outbox.clone()),
                    db_handle.clone(),
                ),
            ),
        },
    )
    .context("pod subsystem construction")?;
    pod_subsystem.start();
    let pod_executor = pod_subsystem
        .build_executor()
        .await
        .context("pod lifecycle executor construction")?;
    pod_subsystem
        .lifecycle_router
        .set_work_executor(pod_executor);

    let pod_repository = pod_subsystem.repository.clone();
    let plr = pod_subsystem.lifecycle_router.clone();
    pod_repository.set_pod_lifecycle_router_for_node(plr.clone(), config.node_name.clone());
    worker_store.set_pod_lifecycle_router(plr.clone());
    side_effects.set_pod_repository(pod_repository.clone());

    services.request_services_sync();

    let kctx = std::sync::Arc::new(crate::kubelet::context::KubeletContext {
        cluster_api,
        node_local: node_local.clone(),
        outbox: outbox.clone(),
        task_supervisor: task_supervisor.clone(),
        config: config.clone(),
        node_mode: node_mode.clone(),
        role: cli.role.clone(),
        network: network.clone(),
        pod_repository: pod_repository.clone(),
        pod_lifecycle_router: plr,
        pod_probe_manager: pod_subsystem
            .probe_manager
            .clone()
            .expect("PodSubsystem must construct ProbeManager"),
        pod_lifecycle_rx,
        pod_start_retry_state: std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::kubelet::pod_creation_state::PodStartRetryState::new(),
        )),
    });

    let pod_watch_source = std::sync::Arc::new(
        crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(worker_store.clone()),
    );
    let persistent_volume_event_handler = std::sync::Arc::new(
        crate::kubelet::pod_watch_handlers::DatastorePersistentVolumeEventHandler::new(
            db_handle.clone(),
        ),
    );
    let pod_watcher_handle = if let Some(runtime_ports) = pod_watcher_runtime_ports {
        let ctx = kctx.clone();
        let watch_source = pod_watch_source.clone();
        let volume_events = persistent_volume_event_handler.clone();
        let c = shutdown_token.clone();
        Some(
            task_supervisor
                .spawn_async(
                    crate::task_supervisor::TaskCategory::Background,
                    "worker_pod_watcher",
                    async move {
                        kubelet::pod_manager::run_pod_watcher_with_context(
                            runtime_ports,
                            ctx,
                            watch_source,
                            volume_events,
                            c,
                        )
                        .await;
                    },
                )
                .await
                .context("worker pod watcher")?,
        )
    } else {
        None
    };
    let heartbeat_handle = {
        let dbc = db_handle.clone();
        let cfc = std::sync::Arc::clone(&config);
        let c = shutdown_token.clone();
        let s = task_supervisor.clone();
        let lease_client: std::sync::Arc<dyn kubelet::node::NodeLeaseRenewClient> =
            follower_grpc_client.clone();
        task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "worker_node_heartbeat",
                async move {
                    kubelet::node::run_heartbeat_with_lease_client(
                        dbc,
                        lease_client,
                        cfc.node_name.clone(),
                        c,
                        s,
                    )
                    .await;
                },
            )
            .await
            .context("worker heartbeat")?
    };

    let pid_path = pidfile::default_pid_path(&config.containerd_namespace);
    let _ = pidfile::write(&pid_path);
    let shutdown_signal = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut st = signal(SignalKind::terminate()).unwrap();
        let mut si = signal(SignalKind::interrupt()).unwrap();
        tokio::select! { _ = st.recv() => tracing::info!("Worker SIGTERM"), _ = si.recv() => tracing::info!("Worker SIGINT"), }
    };
    print_ready_message(&config);
    tracing::info!("worker ready");
    shutdown_signal.await;
    tracing::info!("Worker soft shutdown");
    shutdown_token.cancel();
    db_handle.close();
    let to = std::time::Duration::from_secs(10);
    if let Some(h) = pod_watcher_handle {
        let _ = task_supervisor.timeout("wp", to, h.join()).await;
    }
    let _ = task_supervisor
        .timeout("whb", to, heartbeat_handle.join())
        .await;
    let _ = task_supervisor
        .timeout("wnsw", to, node_subnet_watch_handle.join())
        .await;
    let _ = task_supervisor
        .timeout("wcs", to, worker_control_stream_handle.join())
        .await;
    cni_rpc_token.cancel();
    let _ = task_supervisor
        .timeout("wcni", to, cni_rpc_handle.join())
        .await;
    let _ = task_supervisor
        .shutdown(std::time::Duration::from_secs(10))
        .await;
    let _ = pidfile::remove(&pid_path);
    tracing::info!("Worker shutdown complete");
    Ok(())
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

    #[test]
    fn worker_pod_runtime_role_is_worker() {
        assert_eq!(
            super::worker_pod_runtime_node_role(),
            crate::kubelet::pod_cluster_runtime::RuntimeNodeRole::Worker,
            "worker kubelet runtime must forward cluster writes through the worker cluster view"
        );
    }
}
