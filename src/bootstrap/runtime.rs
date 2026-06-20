//! Leader / full-stack runtime. Worker code lives in worker_runtime.rs.
//!
//! T5: this module owns `run_with_flags` (the top-level role dispatcher) and
//! the leader / full-stack body that runs for `NodeRole::Leader` and
//! `NodeRole::Controlplane`. The Worker arm delegates out to
//! [`crate::bootstrap::worker_runtime::run_worker`].

use anyhow::Context;

use crate::bootstrap::phases;
use crate::bootstrap::{CliFlags, NodeRole};
use crate::{controllers, datastore, kubelet, pidfile};

pub use super::init::cleanup::run_cleanup_with_flags;
use super::init::dataplane::*;
use super::init::host::print_ready_message;
use super::init::leader_control_stream::start_worker_leader_control_stream;
use super::init::predicates::*;

fn worker_pod_runtime_node_role() -> crate::kubelet::pod_cluster_runtime::RuntimeNodeRole {
    crate::kubelet::pod_cluster_runtime::RuntimeNodeRole::Worker
}

fn should_start_controlplane_leader_control_stream(role: &NodeRole, has_client: bool) -> bool {
    has_client
        && matches!(
            role,
            NodeRole::Controlplane {
                leader_endpoints,
                ..
            } if !leader_endpoints.is_empty()
        )
}

fn should_use_worker_store_adapter_for_kubelet(role: &NodeRole) -> bool {
    matches!(role, NodeRole::Worker { .. })
        || matches!(
            role,
            NodeRole::Controlplane {
                leader_endpoints,
                ..
            } if !leader_endpoints.is_empty()
        )
}

fn leader_endpoints_for_role(role: &NodeRole) -> Vec<String> {
    match role {
        NodeRole::Worker {
            leader_endpoints, ..
        }
        | NodeRole::Controlplane {
            leader_endpoints, ..
        } => leader_endpoints.clone(),
        NodeRole::Leader { .. } => Vec::new(),
    }
}

async fn resolve_token_file_if_present(cli: &mut CliFlags) -> anyhow::Result<()> {
    let Some(path) = cli.token_file.take() else {
        return Ok(());
    };

    let supervisor = crate::kubelet::file_blocking::run_blocking_file_keyed;
    let key = path.to_string_lossy().to_string();
    let path_for_task = path.clone();
    let token = supervisor("join_token_file_read", key, move || {
        std::fs::read_to_string(path_for_task).context("read join token file")
    })
    .await
    .with_context(|| format!("failed to read --token-file {}", path.display()))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        anyhow::bail!("--token-file {} is empty", path.display());
    }

    match &mut cli.role {
        NodeRole::Worker { token: target, .. } | NodeRole::Controlplane { token: target, .. } => {
            *target = Some(token);
        }
        NodeRole::Leader { .. } => {
            anyhow::bail!("--token-file is only valid for joining worker/controlplane roles");
        }
    }
    Ok(())
}

async fn start_controlplane_leader_control_stream_if_needed(
    role: &NodeRole,
    client: Option<std::sync::Arc<crate::replication::grpc::client::ReplicationGrpcClient>>,
    cri_for_api: Option<&std::sync::Arc<tokio::sync::Mutex<crate::kubelet::CriClient>>>,
    config: &std::sync::Arc<crate::KlightsConfig>,
    pod_event_db: crate::datastore::DatastoreHandle,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> anyhow::Result<Option<crate::task_supervisor::SupervisedJoinHandle<()>>> {
    if !should_start_controlplane_leader_control_stream(role, client.is_some()) {
        return Ok(None);
    }
    let client = client.expect("checked above");

    if let Some(cri) = cri_for_api {
        let exec_handler = std::sync::Arc::new(
            crate::replication::grpc::client::CriNodeExecSyncHandler::new(
                cri.clone(),
                task_supervisor.clone(),
            ),
        );
        client
            .set_node_exec_sync_handler(exec_handler.clone())
            .await;
        client.set_node_exec_stream_handler(exec_handler).await;
    }
    client
        .set_pod_log_handler(std::sync::Arc::new(
            crate::replication::grpc::client::LocalPodLogHandler::new_with_pod_event_store(
                config.containerd_namespace.clone(),
                task_supervisor.clone(),
                pod_event_db,
            ),
        ))
        .await;

    start_worker_leader_control_stream(client, task_supervisor, shutdown_token)
        .await
        .context("controlplane leader control stream")
        .map(Some)
}

async fn start_worker_store_adapter(
    remote_api_client: std::sync::Arc<crate::control_plane::client::remote::RemoteApiClient>,
    node_local: crate::datastore::node_local::NodeLocalHandle,
    node_name: String,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    shutdown_token: tokio_util::sync::CancellationToken,
    discovery_client: Option<
        std::sync::Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
    >,
    initial_leader_endpoints: Vec<String>,
) -> anyhow::Result<std::sync::Arc<crate::control_plane::client::worker_store::WorkerStoreAdapter>>
{
    let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
        remote_api_client.clone();
    remote_api_client
        .start_required_worker_informers(shutdown_token.clone())
        .await
        .context("worker informers")?;

    let worker_store = std::sync::Arc::new(
        crate::control_plane::client::worker_store::WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            node_name,
        ),
    );
    let discovery_rx = worker_store.watch_signals(crate::watch::WatchTopic::new("v1", "Node"));
    worker_store
        .start_watch_mirrors(supervisor.clone(), shutdown_token.clone())
        .await
        .context("worker watch mirrors")?;

    if let Some(discovery_client) = discovery_client {
        use crate::bootstrap::controlplane_discovery::{
            ControlplaneDiscoveryEvent, extract_controlplane_endpoint,
        };
        use std::collections::HashMap;
        let mut discovery_rx = discovery_rx;
        let discovery_store = worker_store.clone();
        let cancel = shutdown_token.clone();
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "controlplane_endpoint_discovery",
                async move {
                    loop {
                        match discovery_rx.recv().await {
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => {
                                if cancel.is_cancelled() {
                                    return;
                                }
                                continue;
                            }
                        };
                        let nodes = match crate::datastore::DatastoreBackend::list_resources(
                            discovery_store.as_ref(),
                            "v1",
                            "Node",
                            None,
                            crate::datastore::ResourceListQuery::all(),
                        )
                        .await
                        {
                            Ok(nodes) => nodes,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "controlplane endpoint discovery Node relist failed"
                                );
                                continue;
                            }
                        };
                        let mut next_discovered: HashMap<String, String> = HashMap::new();
                        let mut leader_endpoint = None;
                        for node in nodes.items {
                            let event = crate::watch::WatchEvent {
                                event_type: crate::watch::EventType::Added,
                                object: node.data.clone(),
                                encoded_payload: None,
                            };
                            match extract_controlplane_endpoint(&event) {
                                ControlplaneDiscoveryEvent::Upsert {
                                    node_name,
                                    endpoint,
                                    is_leader,
                                } => {
                                    if is_leader {
                                        leader_endpoint = Some(endpoint.clone());
                                    }
                                    next_discovered.insert(node_name, endpoint);
                                }
                                ControlplaneDiscoveryEvent::Remove { .. }
                                | ControlplaneDiscoveryEvent::Ignore => {}
                            }
                        }
                        if let Some(endpoint) = leader_endpoint {
                            discovery_client.set_current_leader_endpoint(Some(endpoint));
                        }
                        let discovered = next_discovered;
                        let mut merged = initial_leader_endpoints.clone();
                        for ep in discovered.values() {
                            if !merged.contains(ep) {
                                merged.push(ep.clone());
                            }
                        }
                        discovery_client.set_all_leader_endpoints(merged);
                    }
                },
            )
            .await
            .context("controlplane endpoint discovery")?;
    }

    Ok(worker_store)
}

// ── Worker boot ──────────────────────────────────────────────────────────

pub(crate) async fn run_worker_with_flags(mut cli: CliFlags) -> anyhow::Result<()> {
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

    let db_handle: crate::datastore::DatastoreHandle = worker_store.clone();
    let db: &dyn datastore::DatastoreBackend = db_handle.as_ref();

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
    // Option C.2: use register_node_sync_with_outbox_at_addresses which
    // applies the registration synchronously via cluster_api.apply_outbox()
    // before enqueuing in the outbox. This ensures the Node exists on the
    // leader before the node_subnet watcher's initial sync_peer_routes call,
    // which reads the Node via gRPC to write its dataplane-readiness conditions.
    let registration_addresses =
        kubelet::node::NodeRegistrationAddresses::new(node_ip.clone(), None);
    if let Err(e) = kubelet::node::register_node_sync_with_outbox_at_addresses(
        db,
        outbox.as_ref(),
        cluster_api.clone(),
        &config.node_name,
        &node_mode,
        &cli.role,
        Some(&dataplane_health),
        &registration_addresses,
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
        crate::kubelet::pod_manager::PodWatcherRuntimePorts::new(runtime.clone(), runtime)
    });
    let pod_subsystem = crate::kubelet::pod_subsystem::PodSubsystem::new(
        crate::kubelet::pod_subsystem::PodSubsystemConfig {
            db: db_handle.clone(),
            supervisor: task_supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            scheduling_mode: crate::kubelet::pod_repository::api::PodSchedulingMode::DeferredMultiNodeLeader,
            outbox: Some(outbox.clone()),
            cluster_api: Some(cluster_api.clone()),
            node_name: config.node_name.clone(),
            service_cidr: config.service_cidr.clone(),
            lifecycle_concurrency: crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            network_events: crate::networking::global_pod_network_events(),
            cri: cri_for_pod_watcher.clone().map(crate::kubelet::cri::SharedCriClient::new),
            containerd_ns: config.containerd_namespace.clone(),
            lifecycle_tx: pod_lifecycle_tx,
            probe_manager: None,
            datapath: Some(network.datapath.clone()),
            service_router: Some(services.clone()),
            runtime_node_role: worker_pod_runtime_node_role(),
            runtime_service: None,
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

    let dbh = db_handle.clone();
    let pod_watcher_handle = if let Some(runtime_ports) = pod_watcher_runtime_ports {
        let ctx = kctx.clone();
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
                            dbh,
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

// ── Leader / full-stack boot ─────────────────────────────────────────────

pub(crate) async fn run_with_flags(mut cli: CliFlags) -> anyhow::Result<()> {
    match &cli.role {
        NodeRole::Worker { .. } => return crate::bootstrap::worker_runtime::run_worker(cli).await,
        NodeRole::Leader { .. } | NodeRole::Controlplane { .. } => {}
    }
    phases::env::init_tracing(&cli);
    log_role(&cli);
    phases::env::init_process(&cli)?;
    let cfg = phases::config::load(&cli).await?;
    resolve_token_file_if_present(&mut cli).await?;
    phases::env::validate_role(&cli.role, &cfg.node_mode)?;
    let recovery = phases::recovery::run(&cfg).await?;
    let identity = phases::identity::setup_leader(&cfg, &recovery.node_ip, &cli.role).await?;

    let config = cfg.config;
    let node_mode = cfg.node_mode;
    let task_supervisor = cfg.supervisor;
    let grpc_transport_policy = cfg.grpc_transport_policy;
    let network_cleanup = cfg.network_cleanup;
    let shutdown_token = cfg.shutdown_token;
    let containerd_data_dir = cfg.containerd_data_dir;
    let containerd_state_dir = cfg.containerd_state_dir;
    let node_ip = identity.node_ip;
    let local_dataplane = identity
        .follower_dataplane
        .expect("leader-class identity must prepare local dataplane metadata");
    let grpc_ca_cert_path = identity.grpc_ca_cert_path;
    let is_leader_runtime = uses_leader_runtime(&cli.role);
    // T6 step 4: leadership watch is created before `open_leader` so the
    // real `is_leader_rx` flows into `LocalApiClient`'s inner gate
    // (step 1) and the switching `LeaderProxyApiClient` (step 3). The
    // initial value reflects what each role expects at boot:
    //   - Seed control-plane / single-node leader: `true` because
    //     `bootstrap_single_voter` makes this node the leader during
    //     open_leader, before the shape watcher (later phase) can flip
    //     the bit. Bootstrap's own initial writes (namespaces, RBAC,
    //     ServiceCIDR) must succeed during this window.
    //   - Joining control-plane / replica learner: `false`. They are
    //     not the leader until raft membership confirms it; the shape
    //     watcher in bootstrap.rs flips the bit once `Raft::metrics()`
    //     reports `current_leader == self.node_id`.
    //   - Worker: irrelevant — workers don't use this watch.
    let initial_is_leader = match &cli.role {
        crate::bootstrap::NodeRole::Leader { .. } => true,
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints, ..
        } => leader_endpoints.is_empty(),
        crate::bootstrap::NodeRole::Worker { .. } => false,
    };
    let (is_leader_tx, is_leader_rx) = tokio::sync::watch::channel::<bool>(initial_is_leader);

    let ds = phases::datastore::open_leader(phases::datastore::OpenLeaderArgs {
        config: &config,
        role: &cli.role,
        supervisor: task_supervisor.clone(),
        grpc_transport_policy: grpc_transport_policy.clone(),
        shutdown_token: shutdown_token.clone(),
        is_leader_rx: is_leader_rx.clone(),
        local_dataplane: local_dataplane.clone(),
        node_ip: &node_ip,
    })
    .await?;
    let db_handle = ds.db_handle;
    let db: &dyn datastore::DatastoreBackend = &*db_handle;
    let cluster_api = ds.cluster_api;
    let remote_api_client = ds.remote_api_client;
    let replication_service_for_router = ds.replication_service.clone();
    let _replication_service = ds.replication_service;
    let node_local = ds.node_local;
    let outbox_runtime = ds.outbox;
    let node_lease_tracker = ds.node_lease_tracker;
    let control_plane_lease_client = ds.control_plane_lease_client;
    let raft_node = ds.raft_node;
    if let Some(rn) = raft_node.as_ref() {
        let metrics = rn.raft.metrics().borrow().clone();
        tracing::info!(
            node_id = rn.node_id,
            state = ?metrics.state,
            current_leader = ?metrics.current_leader,
            "P3 raft: RaftNode wired into leader runtime"
        );
    }

    // T2 step 2: construct the runtime leader election. Every
    // leader-class boot has a raft node (T2 step 1) so we always use
    // RaftLeaderLease. Workers have no raft node and get None.
    let leader_election: Option<std::sync::Arc<dyn crate::leader_election::LeaderElection>> =
        match (raft_node.as_ref(), is_leader_runtime) {
            (Some(rn), true) => {
                let election = crate::leader_election::RaftLeaderLease::new(
                    rn.clone(),
                    task_supervisor.root_cancellation_token(),
                    task_supervisor.clone(),
                );
                Some(std::sync::Arc::new(election))
            }
            _ => None,
        };

    let _ = grpc_ca_cert_path.clone();
    // Reuse the same LocalApiClient instance the outbox dispatcher was
    // wired with in the datastore phase. Creating a second instance here
    // would mean `set_controller_dispatcher` (called later in the bootstrap
    // phase) lands on a different OnceCell than the outbox dispatcher's
    // apply client reads — silently dropping every pod-status side effect
    // (RS readyReplicas, Service endpoint reconcile).
    let local_api_client = ds.local_api_client;
    let kubelet_uses_worker_store_adapter = should_use_worker_store_adapter_for_kubelet(&cli.role);
    let kubelet_db_handle = if kubelet_uses_worker_store_adapter {
        let remote_api_client = remote_api_client
            .clone()
            .ok_or_else(|| anyhow::anyhow!("joining controlplane requires direct leader client"))?;
        let worker_store = start_worker_store_adapter(
            remote_api_client,
            node_local.clone(),
            config.node_name.clone(),
            task_supervisor.clone(),
            shutdown_token.clone(),
            control_plane_lease_client.clone(),
            leader_endpoints_for_role(&cli.role),
        )
        .await?;
        worker_store as crate::datastore::DatastoreHandle
    } else {
        db_handle.clone()
    };
    if should_publish_local_dataplane_metadata(&cli.role) {
        // Self-heal: publish from KLIGHTS_EXTERNAL_ENDPOINT when set, otherwise
        // fall back to the ExternalIP already recorded on the local Node (e.g.
        // on a leader restart). Without this, a leader booted without
        // KLIGHTS_EXTERNAL_ENDPOINT never writes its node_dataplane row and the
        // cross-node WireGuard tunnel never forms.
        let published = publish_local_dataplane_metadata_self_heal(
            db,
            &config,
            &node_mode,
            task_supervisor.as_ref(),
        )
        .await
        .context("dataplane metadata")?;
        if !published {
            tracing::info!(
                "skipping local dataplane metadata publication until KLIGHTS_EXTERNAL_ENDPOINT or peer observation is available"
            );
        }
    }

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
    let dataplane_health = net.dataplane_health;
    let controlplane_leader_control_stream_handle =
        start_controlplane_leader_control_stream_if_needed(
            &cli.role,
            control_plane_lease_client.clone(),
            cri_for_api.as_ref(),
            &config,
            kubelet_db_handle.clone(),
            task_supervisor.clone(),
            shutdown_token.clone(),
        )
        .await?;

    let bp = phases::bootstrap::run(phases::bootstrap::BootstrapRunArgs {
        config: &config,
        cli: &cli,
        node_mode: &node_mode,
        node_ip: &node_ip,
        leader_election: leader_election.clone(),
        skip_seed_bootstrap: ds.skip_seed_bootstrap,
        db_handle: &db_handle,
        kubelet_db_handle: &kubelet_db_handle,
        kubelet_uses_worker_store_adapter,
        db,
        cluster_api: cluster_api.clone(),
        remote_api_client: remote_api_client.clone(),
        _node_local: node_local.clone(),
        replication_service_for_router: replication_service_for_router.clone(),
        outbox_runtime: outbox_runtime.clone(),
        control_plane_lease_client: control_plane_lease_client.clone(),
        node_lease_tracker: node_lease_tracker.clone(),
        network: network.clone(),
        services: services.clone(),
        local_api_client: local_api_client.clone(),
        dataplane_health: &dataplane_health,
        cri_for_pod_watcher,
        cri_for_api: cri_for_api.clone(),
        supervisor: task_supervisor.clone(),
        grpc_transport_policy: grpc_transport_policy.clone(),
        shutdown_token: shutdown_token.clone(),
        raft_node: raft_node.clone(),
        is_leader_tx: is_leader_tx.clone(),
        is_leader_rx: is_leader_rx.clone(),
    })
    .await?;
    let pod_repository = bp.pod_repository;
    let local_vtep_annotation_handle = bp.local_vtep_annotation_handle;
    let crd_registry_watch_handle = bp.crd_registry_watch_handle;
    let leader_peer_endpoint_observer_handle = bp.leader_peer_endpoint_observer_handle;
    let pod_watcher_handle = bp.pod_watcher_handle;
    let heartbeat_handle = bp.heartbeat_handle;
    let node_subnet_watch_handle = bp.node_subnet_watch_handle;
    let node_lifecycle_handle = bp.node_lifecycle_handle;
    let scheduler_controller_handle = bp.scheduler_controller_handle;
    let dispatcher_for_worker = bp.dispatcher_for_worker;
    let scheduler_state = bp._watcher_state.clone();
    let app = bp.app;
    let cri_for_shutdown = cri_for_api.clone();
    let dispatcher_for_cronjobs = dispatcher_for_worker.clone();

    phases::leader::start(phases::leader::LeaderStart {
        config: &config,
        leader_election,
        db_handle: &db_handle,
        task_supervisor: &task_supervisor,
        dispatcher_for_worker: &dispatcher_for_worker,
        dispatcher_for_cronjobs: &dispatcher_for_cronjobs,
        pod_repository: &pod_repository,
        scheduler_state: &scheduler_state,
        cri_for_shutdown: &cri_for_shutdown,
        datapath: &network.datapath,
        is_leader_rx: is_leader_rx.clone(),
        shutdown_token: shutdown_token.clone(),
    })
    .await?;

    phases::server::serve(phases::server::ServeArgs {
        config: &config,
        cli: &cli,
        app,
        pod_watcher_handle,
        heartbeat_handle,
        node_subnet_watch_handle,
        node_lifecycle_handle,
        local_vtep_annotation_handle,
        crd_registry_watch_handle,
        leader_peer_endpoint_observer_handle,
        scheduler_controller_handle,
        cni_rpc_token,
        cni_rpc_handle,
        controlplane_leader_control_stream_handle,
        db_handle,
        shutdown_token,
        supervisor: task_supervisor.clone(),
        grpc_transport_policy,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::super::init::predicates::validate_rootless_multinode_support;
    use super::super::init::tls::load_tls_pem_files;
    use super::should_use_worker_store_adapter_for_kubelet;
    use crate::bootstrap::{NodeMode, NodeRole};
    use std::sync::Arc;

    #[tokio::test]
    async fn tls_pem_loader_reads_existing_files() {
        let task_supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        tokio::fs::write(&cert_path, b"cert-bytes")
            .await
            .expect("write cert");
        tokio::fs::write(&key_path, b"key-bytes")
            .await
            .expect("write key");

        let (cert, key) = load_tls_pem_files(&task_supervisor, &cert_path, &key_path)
            .await
            .expect("load pem files");
        assert_eq!(cert, b"cert-bytes");
        assert_eq!(key, b"key-bytes");
    }

    #[test]
    fn kubelet_uses_worker_store_adapter_for_worker_and_joining_controlplane() {
        let worker = NodeRole::Worker {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };
        let replica = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
            as_learner: true,
        };
        let follower_controlplane = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
            as_learner: false,
        };

        assert!(should_use_worker_store_adapter_for_kubelet(&worker));
        assert!(should_use_worker_store_adapter_for_kubelet(&replica));
        assert!(should_use_worker_store_adapter_for_kubelet(
            &follower_controlplane
        ));
    }

    #[test]
    fn kubelet_keeps_local_store_adapter_for_seed_leaders() {
        let leader = NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        };
        let seed_controlplane = NodeRole::Controlplane {
            leader_endpoints: Vec::new(),
            token: None,
            skip_ca: false,
            as_learner: false,
        };

        assert!(!should_use_worker_store_adapter_for_kubelet(&leader));
        assert!(!should_use_worker_store_adapter_for_kubelet(
            &seed_controlplane
        ));
    }

    #[tokio::test]
    async fn tls_pem_loader_missing_cert_returns_error() {
        let task_supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let cert_path = dir.path().join("missing.crt");
        let key_path = dir.path().join("server.key");
        tokio::fs::write(&key_path, b"key-bytes")
            .await
            .expect("write key");

        let err = load_tls_pem_files(&task_supervisor, &cert_path, &key_path)
            .await
            .expect_err("missing cert");
        assert!(
            err.to_string()
                .contains(&format!("failed to read TLS cert: {}", cert_path.display()))
        );
    }

    #[test]
    fn seed_leader_flags_require_no_cluster_join_parameters() {
        let flags = super::super::CliFlags {
            rootless: false,
            namespace: None,
            bind_address: None,
            token_file: None,
            role: super::super::NodeRole::Leader {
                bootstrap: super::super::node_role::LeaderBootstrap::Seed,
            },
        };

        assert!(
            !flags.role.requires_leader(),
            "seed leader must not require a leader endpoint"
        );
        assert!(
            !flags.role.requires_token(),
            "seed leader must not require a bootstrap token"
        );
        assert!(
            flags.role.runs_full_stack(),
            "seed leader must run the full single-node stack"
        );
    }

    /// Replica mode is a control-plane learner join, not a worker join flag.
    #[test]
    fn replica_role_carries_controlplane_learner_join_parameters() {
        let flags = super::super::CliFlags {
            rootless: false,
            namespace: None,
            bind_address: None,
            token_file: None,
            role: super::super::NodeRole::Controlplane {
                leader_endpoints: vec!["https://192.0.2.4:7679".into()],
                token: Some("tok".into()),
                skip_ca: false,
                as_learner: true,
            },
        };
        assert!(flags.role.requires_leader());
        assert!(flags.role.requires_token());
        assert!(flags.role.is_learner_join());
    }

    /// Worker mode carries the join parameters needed by the wired runtime.
    #[test]
    fn worker_flags_carry_join_parameters() {
        let flags = super::super::CliFlags {
            rootless: false,
            namespace: None,
            bind_address: None,
            token_file: None,
            role: super::super::NodeRole::Worker {
                leader_endpoints: vec!["https://192.0.2.4:7679".into()],
                token: Some("tok".into()),
                skip_ca: false,
            },
        };
        assert!(flags.role.requires_leader());
        assert!(!flags.role.requires_token(), "worker token is optional");
    }

    #[tokio::test]
    async fn token_file_resolution_reads_trimmed_token_into_role() {
        use std::io::Write as _;

        let mut token_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(token_file, "file-token").unwrap();
        let mut flags = super::super::CliFlags {
            rootless: false,
            namespace: None,
            bind_address: None,
            token_file: Some(token_file.path().to_path_buf()),
            role: super::super::NodeRole::Worker {
                leader_endpoints: vec!["https://192.0.2.4:7679".into()],
                token: Some("arg-token".into()),
                skip_ca: false,
            },
        };

        super::resolve_token_file_if_present(&mut flags)
            .await
            .unwrap();

        assert_eq!(flags.token_file, None);
        assert_eq!(flags.role.token(), Some("file-token"));
    }

    #[test]
    fn rootless_multinode_roles_now_enabled_with_wireguard_over_pasta() {
        let rootless = NodeMode::Rootless {
            rootlesskit_pid: 42,
            user_netns: std::path::PathBuf::from("/proc/42/ns/net"),
        };
        let roles = [
            NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7679".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7679".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
        ];

        for role in &roles {
            validate_rootless_multinode_support(role, &rootless).unwrap_or_else(|e| {
                panic!(
                    "rootless multinode {:?} must now be enabled with WireGuard-over-pasta: {e}",
                    role
                )
            });
        }
        validate_rootless_multinode_support(
            &NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7679".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            &NodeMode::Root,
        )
        .expect("root-mode multinode roles must stay supported");
    }

    #[test]
    fn worker_default_dataplane_allows_api_discovered_endpoint() {
        let mut config = crate::KlightsConfig::test_default();
        config.external_endpoint = None;
        config.worker_dataplane_no_ingress = false;
        let role = NodeRole::Worker {
            leader_endpoints: vec!["https://dallas:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        super::validate_worker_dataplane_ingress(&role, &config).expect(
            "worker default ingress path should allow the leader to discover the endpoint from the API connection",
        );
    }

    #[test]
    fn worker_no_ingress_opt_in_allows_missing_external_endpoint() {
        let mut config = crate::KlightsConfig::test_default();
        config.external_endpoint = None;
        config.worker_dataplane_no_ingress = true;
        let role = NodeRole::Worker {
            leader_endpoints: vec!["https://dallas:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        super::validate_worker_dataplane_ingress(&role, &config)
            .expect("explicit no-ingress worker opt-in should allow missing external endpoint");
    }

    #[test]
    fn worker_default_dataplane_accepts_explicit_ingress_endpoint() {
        let mut config = crate::KlightsConfig::test_default();
        config.external_endpoint = Some("192.0.2.20".to_string());
        config.worker_dataplane_no_ingress = false;
        let role = NodeRole::Worker {
            leader_endpoints: vec!["https://dallas:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };

        super::validate_worker_dataplane_ingress(&role, &config)
            .expect("worker default path should accept explicit inbound external endpoint");
    }

    #[tokio::test]
    async fn publish_local_dataplane_metadata_writes_explicit_disabled_route_metadata() {
        let db = crate::datastore::test_support::in_memory().await;
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = Some("192.0.2.10".to_string());
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        );

        let published = super::publish_local_dataplane_metadata_self_heal(
            &db,
            &config,
            &crate::bootstrap::NodeMode::Root,
            &supervisor,
        )
        .await
        .expect("local dataplane metadata should publish");
        assert!(
            published,
            "configured external endpoint must publish metadata"
        );

        let stored = db
            .get_node_dataplane("leader-a")
            .await
            .expect("dataplane lookup should succeed")
            .expect("local dataplane metadata must be stored");
        assert_eq!(stored.node_name, "leader-a");
        assert_eq!(
            stored.encryption,
            crate::networking::wireguard::DataplaneEncryption::Disabled
        );
        assert!(stored.public_key.is_none());
        assert_eq!(stored.endpoint.to_string(), "192.0.2.10");
    }

    #[tokio::test]
    async fn worker_dataplane_metadata_is_enqueued_to_outbox() {
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let node_db = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-dataplane-outbox-test",
        )
        .await
        .expect("open node-local test db");
        let outbox = crate::kubelet::outbox::Outbox::new(node_db.clone());
        let dataplane = crate::replication::grpc::client::JoinDataplaneMetadata {
            public_key: Some("worker-public-key".to_string()),
            endpoint: "192.0.2.55".to_string(),
            port: Some(7679),
            mode: crate::networking::wireguard::DataplaneMode::Root,
            encryption: crate::networking::wireguard::DataplaneEncryption::Enabled,
        };

        super::enqueue_worker_dataplane_metadata_outbox(Some(&outbox), "worker-a", &dataplane)
            .await
            .expect("worker dataplane metadata should enqueue");

        let row = node_db
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox row")
            .expect("dataplane outbox row must exist");
        assert_eq!(row.operation, "NodeDataplane");
        assert_eq!(row.subject_kind, "Node");
        assert_eq!(row.subject_name, "worker-a");
        assert_eq!(row.subject_key, "v1/Node/worker-a/dataplane");
        let payload =
            crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&row.payload_proto)
                .expect("decode dataplane outbox payload");
        match payload.command {
            crate::datastore::command::StorageCommand::UpdateNodeDataplane {
                node_name,
                mode,
                encryption,
                public_key,
                endpoint,
                port,
            } => {
                assert_eq!(node_name, "worker-a");
                assert_eq!(mode, "root");
                assert_eq!(encryption, "enabled");
                assert_eq!(public_key.as_deref(), Some("worker-public-key"));
                assert_eq!(endpoint, "192.0.2.55");
                assert_eq!(port, Some(7679));
            }
            other => panic!("expected UpdateNodeDataplane outbox command, got {other:?}"),
        }
    }

    #[test]
    fn worker_pod_runtime_role_is_worker() {
        assert_eq!(
            super::worker_pod_runtime_node_role(),
            crate::kubelet::pod_cluster_runtime::RuntimeNodeRole::Worker,
            "worker kubelet runtime must forward cluster writes through the worker cluster view"
        );
    }

    #[test]
    fn joining_controlplane_starts_leader_control_stream_when_client_exists() {
        let role = NodeRole::Controlplane {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
            as_learner: false,
        };

        assert!(
            super::should_start_controlplane_leader_control_stream(&role, true),
            "joining controlplanes must register a node-control stream so the leader can proxy pod logs and exec"
        );
    }

    #[test]
    fn seed_controlplane_does_not_start_leader_control_stream_without_client() {
        let role = NodeRole::Controlplane {
            leader_endpoints: Vec::new(),
            token: None,
            skip_ca: false,
            as_learner: false,
        };

        assert!(!super::should_start_controlplane_leader_control_stream(
            &role, false
        ));
    }
}
