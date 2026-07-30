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
use super::worker_store_adapter::start_worker_store_adapter;

fn worker_pod_runtime_node_role() -> crate::kubelet::pod_cluster_runtime::RuntimeNodeRole {
    crate::kubelet::pod_cluster_runtime::RuntimeNodeRole::Worker
}

// ── Worker boot ──────────────────────────────────────────────────────────

pub(crate) async fn run_worker(mut cli: CliFlags) -> anyhow::Result<()> {
    phases::env::init_tracing(&cli);
    phases::env::init_process(&cli)?;
    let cfg = phases::config::load(&cli).await?;
    resolve_token_file_if_present(&mut cli, &cfg.file_process).await?;
    phases::env::validate_role(&cli.role, &cfg.node_mode)?;
    let recovery = phases::recovery::run(&cfg).await?;
    let identity = phases::identity::setup_worker(&cfg, &recovery.node_ip).await?;

    let config = cfg.config;
    let node_mode = cfg.node_mode;
    let task_supervisor = cfg.supervisor;
    let file_process = cfg.file_process;
    let grpc_transport_policy = cfg.grpc_transport_policy;
    let network_cleanup = cfg.network_cleanup;
    let shutdown_token = cfg.shutdown_token;
    let runtime_paths = cfg.runtime_paths;
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
        let crypto = klights_supervisor::CryptoExecutor::new(task_supervisor.clone());
        match resolve_credential_async(&store, &crypto).await {
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
                    bootstrap_with_csr_async_store(&config.node_name, &csr_client, &store, &crypto)
                        .await?;
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
                    bootstrap_with_csr_async_store(&config.node_name, &csr_client, &store, &crypto)
                        .await?;
                (Some(cred.certificate_pem), Some(cred.private_key_pem))
            }
        }
    };

    let grpc_config = klights_leader_rpc::client::GrpcClientConfig {
        leader_endpoint: le.clone(),
        token: tk.unwrap_or_default(),
        node_name: config.node_name.clone(),
        role: klights_leader_api::JoinRole::Worker,
        dataplane: follower_dataplane.clone(),
        ca_cert_path: grpc_ca_cert_path.clone(),
        skip_ca,
        client_cert_pem,
        client_key_pem,
    };
    let follower_grpc_client =
        std::sync::Arc::new(klights_leader_rpc::client::ReplicationGrpcClient::new(
            grpc_config,
            task_supervisor.clone(),
            grpc_transport_policy.clone(),
        ));
    // T2 step 5: register all known leader endpoints so the reconnect
    // loop can cycle through them after a stream failure.
    follower_grpc_client.set_all_leader_endpoints(all_leader_endpoints.clone());
    follower_grpc_client
        .require_command_codec_v3()
        .await
        .context("worker startup rejected incompatible leader command codec")?;
    let remote_api_client = std::sync::Arc::new(
        crate::control_plane::client::remote::RemoteApiClient::from_grpc(
            follower_grpc_client.clone(),
            task_supervisor.clone(),
            config.node_name.clone(),
            std::sync::Arc::new(crate::remote_informer_cache_adapter::WatchCacheAdapter::new()),
        ),
    );
    let leader_ports =
        crate::control_plane::client::LeaderClientPorts::from_client(remote_api_client.clone());

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
    let outbox_stores = crate::node_outbox::OutboxStores::new(
        node_local.outbox_producer(),
        node_local.outbox_dispatcher(),
        node_local.pod_status_checkpoints(),
        node_local.runtime_observation_checkpoints(),
        node_local.outbox_status_stamps(),
    );
    let outbox_codec = crate::replication::outbox_payload_codec::new_codec();
    let outbox_wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock> =
        std::sync::Arc::new(klights_supervisor::SystemWallClock);
    let outbox = std::sync::Arc::new(crate::node_outbox::Outbox::compose(
        outbox_stores.clone(),
        outbox_codec.clone(),
        ob_notify.clone(),
        outbox_wall_clock.clone(),
    ));
    crate::node_outbox::OutboxDispatcher::new(
        outbox_stores,
        outbox_codec,
        remote_api_client.clone(),
        ob_notify,
        outbox_wall_clock,
    )
    // bug-grpc: pipelined dispatch — keep multiple worker→leader
    // `apply_outbox` round-trips in flight (one per Status channel-lane
    // connection) instead of one row per WAN RTT.
    .with_batch_mode(crate::node_outbox::DEFAULT_DISPATCH_INFLIGHT)
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
        config.node_name.clone(),
        task_supervisor.clone(),
        shutdown_token.clone(),
        Some(follower_grpc_client.clone()),
        all_leader_endpoints.clone(),
    )
    .await?;

    let network_runtime_inputs = crate::bootstrap::runtime_inputs::NetworkRuntimeInputs::capture();
    let net = phases::network::boot(phases::network::NetworkBootArgs {
        config: &config,
        node_mode: &node_mode,
        node_ip: &node_ip,
        resource_query: leader_ports.resource_query.clone(),
        watch: leader_ports.watch.clone(),
        subnet_allocation: leader_ports.node_subnet_allocation.clone(),
        network_topology: leader_ports.network_topology.clone(),
        pod_network_cache: node_local.pod_network_cache(),
        pod_ipam: node_local.pod_ipam(),
        pod_runtime: node_local.pod_runtime(),
        pod_endpoints: node_local.pod_endpoints(),
        pod_endpoint_events: node_local.pod_endpoint_events(),
        network_cleanup: &network_cleanup,
        runtime_paths: &runtime_paths,
        runtime_inputs: network_runtime_inputs,
        supervisor: task_supervisor.clone(),
        grpc_transport_policy: grpc_transport_policy.clone(),
        shutdown_token: shutdown_token.clone(),
    })
    .await?;
    let db = worker_store.clone();
    let network = net.network;
    let services = net.services;
    let cni_rpc_token = net.cni_rpc_token;
    let cni_rpc_handle = net.cni_rpc_handle;
    let cri_for_pod_watcher = net.cri_for_pod_watcher;
    let cri_for_api = net.cri_for_api;
    let cni_readiness = net.cni_readiness;
    let dataplane_health = net.dataplane_health;
    let pod_network_cache = net.pod_network_cache;
    let pod_runtime_store = net.pod_runtime_store;
    let pod_endpoint_store = net.pod_endpoint_store;
    let assignment_waiter = net.assignment_waiter;
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
    let registration_profile =
        crate::bootstrap::node_registration_profile::build(&node_mode, &cli.role);
    let registration = kubelet::node::NodeRegistrationSnapshot::capture_local(
        &file_process,
        &config.node_name,
        &registration_profile,
        registration_addresses,
        None,
        None,
    )
    .await;
    let registration_health = dataplane_health.snapshot();
    if let Err(e) = crate::bootstrap::node_registration_adapter::register_worker_node_snapshot(
        db.as_ref(),
        outbox.as_ref(),
        Some(&registration_health),
        &registration,
    )
    .await
    {
        tracing::warn!("worker node registration: {}", e);
    }

    let (exec_runtime, metrics_runtime) = match &cri_for_api {
        Some(cri) => (
            klights_leader_rpc::client::NodeExecCapability::Available(std::sync::Arc::new(
                crate::kubelet::remote_runtime::CriNodeExecRuntime::new(
                    cri.clone(),
                    task_supervisor.clone(),
                ),
            )),
            klights_leader_rpc::client::NodeMetricsCapability::Available(std::sync::Arc::new(
                crate::kubelet::remote_runtime::CriNodeMetricsRuntime::new(std::sync::Arc::new(
                    crate::kubelet::metrics::CriNodeMetricsSampler::new(
                        cri.clone(),
                        task_supervisor.clone(),
                    ),
                )),
            )),
        ),
        None => (
            klights_leader_rpc::client::NodeExecCapability::Unavailable,
            klights_leader_rpc::client::NodeMetricsCapability::Unavailable,
        ),
    };
    let control_runtimes = klights_leader_rpc::client::NodeControlRuntimes::new(
        exec_runtime,
        klights_leader_rpc::client::NodeLogCapability::Available(std::sync::Arc::new(
            crate::api::pod_subresources::local_node_log_runtime::LocalNodeLogRuntime::new_with_pod_event_store(
                crate::paths::pod_logs_root_path(&config.containerd_namespace),
                task_supervisor.clone(),
                std::sync::Arc::new(crate::auth::clock::SystemClock),
                crate::api::pod_subresources::logs::PodLogFollowWatchSource::new(
                    std::sync::Arc::new(
                        crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(db.clone()),
                    ),
                ),
            ),
        )),
        metrics_runtime,
    );
    let worker_control_stream_handle = start_worker_leader_control_stream(
        follower_grpc_client.clone(),
        control_runtimes,
        task_supervisor.clone(),
        shutdown_token.clone(),
    )
    .await
    .context("worker control stream")?;
    let node_subnet_watch_handle = {
        let node_name = config.node_name.clone();
        let peering = network.peering().clone();
        let supervisor_for_task = task_supervisor.clone();
        let health_for_peer_watch = dataplane_health.clone();
        let query_for_peer_watch: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> =
            remote_api_client.clone();
        let topology_for_peer_watch: std::sync::Arc<
            dyn klights_leader_api::LeaderNetworkTopologyQuery,
        > = remote_api_client.clone();
        let watch_for_peer_watch: std::sync::Arc<dyn klights_leader_api::LeaderWatch> =
            remote_api_client.clone();
        let node_status_for_peer_watch: std::sync::Arc<
            dyn klights_leader_api::LeaderNodeSelfStatus,
        > = std::sync::Arc::new(crate::kubelet::node::OutboxNodeSelfStatusPublisher::new(
            config.node_name.clone(),
            query_for_peer_watch.clone(),
            outbox.clone(),
            std::sync::Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
        ));
        let readiness_publisher =
            crate::node_subnet_controller_adapter::KubeletNodeReadinessPublisher::new(
                query_for_peer_watch.clone(),
                node_status_for_peer_watch,
            );
        let cancel = shutdown_token.clone();
        task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "worker_node_subnet_peer_watch",
                async move {
                    controllers::node_subnet::run_focused_peer_watch(
                        topology_for_peer_watch,
                        query_for_peer_watch,
                        watch_for_peer_watch,
                        None,
                        node_name,
                        peering,
                        supervisor_for_task,
                        Some(std::sync::Arc::new(health_for_peer_watch)),
                        readiness_publisher,
                        cancel,
                    )
                    .await;
                },
            )
            .await
            .context("worker node subnet peer watch")?
    };

    let metrics = crate::side_effects::SideEffectMetrics::new();
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
    let pod_repository_parts = compose_worker_pod_repository_parts(
        crate::pod_repository_composition::WorkerPodRepositoryBuildConfig {
            resource_query: leader_ports.resource_query.clone(),
            pod_workqueue_store: node_local.pod_workqueue(),
            supervisor: task_supervisor.clone(),
            metrics: metrics.clone(),
            pod_network_cache,
            assignment_waiter,
            outbox: outbox.clone(),
        },
    );
    let kubelet_capacity =
        crate::kubelet::node_registration::NodeRegistrationHostFacts::capture_local(
            &file_process,
            &registration_profile,
        )
        .await
        .node_capacity();
    let sandbox_inputs =
        crate::bootstrap::runtime_inputs::capture_sandbox_inputs(&file_process, &node_mode).await;
    let kubelet_runtime_network = crate::kubelet::context::KubeletRuntimeNetworkServices::new(
        network.datapath().clone(),
        network.peering().clone(),
        services.clone(),
    );
    let kubelet_status_delivery = crate::kubelet::context::KubeletStatusDeliveryServices::new(
        leader_ports.resource_query.clone(),
        leader_ports.cache_readiness.clone(),
        leader_ports.pod_cleanup_intents.clone(),
        leader_ports.projected_tokens.clone(),
        outbox.clone(),
    );
    let pod_slot_store = node_local.pod_slots();
    let pod_slot_events = node_local.pod_slot_events();
    let pod_subsystem = crate::kubelet::pod_subsystem::PodSubsystem::new(
        crate::kubelet::pod_subsystem::PodSubsystemConfig {
            repository_parts: pod_repository_parts,
            supervisor: task_supervisor.clone(),
            outbox: Some(kubelet_status_delivery.outbox.clone()),
            resource_query: Some(kubelet_status_delivery.resource_query.clone()),
            projected_tokens: Some(kubelet_status_delivery.projected_tokens.clone()),
            node_name: config.node_name.clone(),
            service_cidr: config.service_cidr.clone(),
            lifecycle_concurrency: crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            pod_actor_idle_grace:
                crate::kubelet::pod_lifecycle_actor::actor::DEFAULT_POD_ACTOR_IDLE_GRACE,
            sandbox_inputs,
            node_capacity: kubelet_capacity,
            paths: runtime_paths.clone(),
            lifecycle_route_mode: crate::kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor,
            cri: cri_for_pod_watcher.clone().map(crate::kubelet::cri::SharedCriClient::new),
            containerd_ns: config.containerd_namespace.clone(),
            lifecycle_tx: pod_lifecycle_tx,
            probe_manager: None,
            datapath: Some(kubelet_runtime_network.datapath.clone()),
            service_router: Some(services.clone()),
            runtime_node_role: worker_pod_runtime_node_role(),
            runtime_service: None,
            runtime_store: std::sync::Arc::new(
                crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(
                    pod_runtime_store.clone(),
                    config.node_name.clone(),
                    std::sync::Arc::new(
                        crate::kubelet::pod_runtime::store::SystemRuntimeClock,
                    ),
                ),
            ),
            wall_clock: std::sync::Arc::new(
                crate::kubelet::pod_runtime::store::SystemRuntimeClock,
            ),
            slot_admission: std::sync::Arc::new(
                crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
                    pod_slot_store,
                    pod_slot_events,
                    config.node_name.clone(),
                ),
            ),
            event_sink: std::sync::Arc::new(
                crate::bootstrap::kubelet_ports::WorkerPodEventSink::new(
                    outbox.clone(),
                    leader_ports.resource_query.clone(),
                    std::sync::Arc::new(klights_supervisor::SystemWallClock),
                ),
            ),
        },
    )
    .context("pod subsystem construction")?;
    pod_subsystem
        .start()
        .await
        .context("pod subsystem startup")?;
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
    let kubelet_config = crate::kubelet::context::KubeletConfig::try_new(
        config.service_cidr.clone(),
        config.node_name.clone(),
        config.containerd_namespace.clone(),
        crate::kubelet::log_rotation::LogRotationPolicy::default(),
        kubelet_capacity,
        runtime_paths,
    )
    .context("worker kubelet configuration")?;
    let kctx = std::sync::Arc::new(crate::kubelet::context::KubeletServices::new(
        crate::kubelet::context::KubeletLifecycleServices::new(
            pod_repository.clone(),
            plr,
            pod_lifecycle_rx,
            std::sync::Arc::new(tokio::sync::Mutex::new(
                crate::kubelet::pod_creation_state::PodStartRetryState::new(),
            )),
        ),
        kubelet_runtime_network,
        kubelet_status_delivery,
        crate::kubelet::context::KubeletLocalExecutionServices::new(
            pod_runtime_store,
            pod_endpoint_store,
            std::sync::Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            task_supervisor.clone(),
            file_process.clone(),
            kubelet_config,
        ),
    ));
    kctx.runtime_network().services.request_services_sync()?;

    let pod_watch_source = std::sync::Arc::new(
        crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(worker_store.clone()),
    );
    let persistent_volume_event_handler = std::sync::Arc::new(
        crate::kubelet::pod_watch_handlers::NoopPersistentVolumeEventHandler::new(),
    );
    let pod_watcher_handle = if let Some(runtime_ports) = pod_watcher_runtime_ports {
        let ctx = kctx.clone();
        let watch_source = pod_watch_source.clone();
        let volume_events = persistent_volume_event_handler.clone();
        let c = shutdown_token.clone();
        Some(
            task_supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Background,
                    "worker_pod_watcher",
                    async move {
                        kubelet::pod_manager::run_pod_watcher_with_services(
                            runtime_ports,
                            ctx.lifecycle(),
                            ctx.status_delivery(),
                            ctx.local_execution(),
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
        let watch_source = std::sync::Arc::new(
            crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(worker_store.clone()),
        );
        let cfc = std::sync::Arc::clone(&config);
        let c = shutdown_token.clone();
        let s = task_supervisor.clone();
        let lease_client: std::sync::Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal> =
            remote_api_client.clone();
        task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "worker_node_heartbeat",
                async move {
                    kubelet::node::run_heartbeat_with_lease_client(
                        watch_source,
                        lease_client,
                        std::sync::Arc::new(
                            crate::bootstrap::kubelet_ports::SystemNodeHeartbeatClock::new(
                                std::sync::Arc::new(klights_supervisor::SystemWallClock),
                            ),
                        ),
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
    node_local.identity().close();
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

fn compose_worker_pod_repository_parts(
    config: crate::pod_repository_composition::WorkerPodRepositoryBuildConfig,
) -> crate::kubelet::pod_repository::facade::PodRepositoryParts {
    crate::pod_repository_composition::build_worker_pod_repository_parts(config)
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

    struct UnavailableWorkerQuery;

    impl klights_leader_api::LeaderResourceQuery for UnavailableWorkerQuery {
        fn get_resource(
            &self,
            _request: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            Box::pin(async {
                Err(klights_leader_api::ResourceQueryError::retryable(
                    "test query is intentionally unavailable",
                ))
            })
        }

        fn list_resources(
            &self,
            _request: klights_leader_api::ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            Box::pin(async {
                Err(klights_leader_api::ResourceQueryError::retryable(
                    "test query is intentionally unavailable",
                ))
            })
        }
    }

    #[tokio::test]
    async fn worker_repository_graph_starts_without_cluster_datastore_or_side_effect_registry() {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-runtime-repository-graph",
        )
        .await
        .expect("open worker node-local store");
        let outbox = std::sync::Arc::new(crate::node_outbox::Outbox::new(node_local.clone()));

        let parts = compose_worker_pod_repository_parts(
            crate::pod_repository_composition::WorkerPodRepositoryBuildConfig {
                resource_query: std::sync::Arc::new(UnavailableWorkerQuery),
                pod_workqueue_store: node_local.pod_workqueue(),
                supervisor,
                metrics: crate::side_effects::SideEffectMetrics::new(),
                pod_network_cache: crate::kubelet::pod_repository::empty_test_pod_network_cache(),
                assignment_waiter: crate::kubelet::pod_repository::test_assignment_bus(),
                outbox,
            },
        );

        parts
            .background
            .start()
            .await
            .expect("worker repository background starts without cluster datastore");
        assert!(parts.background.workqueue_start_called());
    }

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
