//! Phase 7: DB bootstrap, watcher state, node registration, services.
//! Combines namespace init, CRDs, dispatcher, pod repo, registration,
//! ServiceCIDR, CoreDNS, and CRD loading.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::{CliFlags, NodeMode};
use crate::datastore::DatastoreHandle;
use crate::kubelet::CriClient;
use crate::kubelet::cri::SharedCriClient;
use crate::kubelet::pod_cluster_runtime::RuntimeNodeRole;
use crate::task_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct BootstrapPhase {
    pub _watcher_state: Arc<crate::api::AppState>,
    pub _node_lifecycle_start_resource_version: i64,
    pub pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    pub local_vtep_annotation_handle: SupervisedJoinHandle<()>,
    pub crd_registry_watch_handle: SupervisedJoinHandle<()>,
    pub leader_peer_endpoint_observer_handle: Option<SupervisedJoinHandle<()>>,
    pub pod_watcher_handle: Option<SupervisedJoinHandle<()>>,
    pub heartbeat_handle: SupervisedJoinHandle<()>,
    pub node_subnet_watch_handle: SupervisedJoinHandle<()>,
    pub node_lifecycle_handle: Option<SupervisedJoinHandle<()>>,
    pub scheduler_controller_handle: Option<SupervisedJoinHandle<()>>,
    pub dispatcher_for_worker: Arc<crate::controller_dispatcher::ControllerDispatcher>,
    pub app: axum::Router,
}

pub struct BootstrapRunArgs<'a> {
    pub config: &'a Arc<KlightsConfig>,
    pub cli: &'a CliFlags,
    pub node_mode: &'a NodeMode,
    pub node_ip: &'a str,
    /// T2 step 2: runtime leader lease instead of compile-time bool.
    /// `None` for workers; `Some` for leader-class boots. The one-time
    /// init steps (namespaces, RBAC, ServiceCIDR, kube-dns) acquire the
    /// lease before running so a joiner that is not yet leader skips them
    /// without error.
    pub leader_election: Option<Arc<dyn crate::leader_election::LeaderElection>>,
    /// When true, this node is a joining Raft controlplane that has
    /// already caught up via the Phase A backup stream. Seed-only
    /// bootstrap writes (default namespaces, RBAC, kubernetes
    /// Service, ServiceCIDR) are skipped because the catch-up stream
    /// delivered the seed's state into the local cluster.db.
    pub skip_seed_bootstrap: bool,
    pub db_handle: &'a DatastoreHandle,
    pub kubelet_db_handle: &'a DatastoreHandle,
    pub kubelet_uses_worker_store_adapter: bool,
    pub db: &'a dyn crate::datastore::DatastoreBackend,
    pub cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient>,
    pub remote_api_client: Option<Arc<crate::control_plane::client::remote::RemoteApiClient>>,
    pub _node_local: crate::datastore::node_local::handle::NodeLocalHandle,
    pub replication_service_for_router: Option<Arc<crate::replication::ReplicationService>>,
    pub outbox_runtime: Arc<crate::kubelet::outbox::Outbox>,
    pub node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    pub network: Arc<crate::networking::Network>,
    pub services: Arc<dyn crate::networking::ServiceRouter>,
    pub local_api_client: Arc<crate::control_plane::client::local::LocalApiClient>,
    pub control_plane_lease_client:
        Option<Arc<crate::replication::grpc::client::ReplicationGrpcClient>>,
    pub dataplane_health: &'a crate::networking::dataplane_health::DataplaneHealth,
    pub cri_for_pod_watcher: Option<CriClient>,
    pub cri_for_api: Option<Arc<tokio::sync::Mutex<CriClient>>>,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
    /// P3-11c: when raft mode is active on a leader-class boot, this
    /// is the live `RaftNode`. The bootstrap phase wires its router +
    /// join handler onto the replication gRPC server so peer voters
    /// can drive `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot`
    /// and a joining controlplane can call `JoinAsControlplane`.
    pub raft_node: Option<Arc<crate::datastore::raft::node::RaftNode>>,
    /// T6 step 4: leadership watch sender, created in runtime.rs before
    /// `open_leader`. The shape watcher inside this phase updates it on
    /// every `Raft::metrics()` change so `LocalApiClient`'s inner gate
    /// (step 1) and the switching `LeaderProxyApiClient` (step 3) see
    /// the live leader state. The matching receiver was already passed
    /// to `open_leader` so it's wired into the datastore phase.
    pub is_leader_tx: tokio::sync::watch::Sender<bool>,
    pub is_leader_rx: tokio::sync::watch::Receiver<bool>,
}

pub async fn run(args: BootstrapRunArgs<'_>) -> Result<BootstrapPhase> {
    let BootstrapRunArgs {
        config,
        cli,
        node_mode,
        node_ip,
        leader_election,
        skip_seed_bootstrap,
        db_handle,
        kubelet_db_handle,
        kubelet_uses_worker_store_adapter,
        db,
        cluster_api,
        remote_api_client,
        _node_local,
        replication_service_for_router,
        outbox_runtime,
        node_lease_tracker,
        control_plane_lease_client,
        network,
        services,
        local_api_client,
        dataplane_health,
        cri_for_pod_watcher,
        cri_for_api,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
        raft_node,
        is_leader_tx,
        is_leader_rx,
    } = args;
    use crate::{api, controller_dispatcher, controllers, kubelet};

    // T2 step 2: leader-capable nodes gate one-time init on lease
    // acquisition. For a seed boot the raft node is already leader by
    // this point so acquire succeeds immediately. Joiners are not
    // leader and skip init cleanly (the seed already wrote these rows).
    let has_leader_election = leader_election.is_some();
    let leader_lease = if has_leader_election && !skip_seed_bootstrap {
        match leader_election
            .as_ref()
            .unwrap()
            .acquire(crate::leader_election::LeaderScope::Cluster)
            .await
        {
            Ok(lease) => {
                tracing::info!("bootstrap: acquired leader lease for one-time init");
                Some(lease)
            }
            Err(err) => {
                tracing::info!(
                    error = %err,
                    "bootstrap: not the raft leader — skipping one-time init (already seeded)"
                );
                None
            }
        }
    } else {
        None
    };

    // Initialize default namespaces (only on the seed leader).
    if leader_lease.is_some() {
        controllers::namespace::init_default_namespaces(db)
            .await
            .context("Failed to initialize default namespaces")?;
    }

    // Seed and reconcile default RBAC objects (only on leader).
    if leader_lease.is_some() {
        controllers::rbac_reconcile::reconcile_default_rbac_objects(db_handle.as_ref())
            .await
            .context("Failed to seed default RBAC objects")?;
    }

    let crd_registry = controllers::crd::CrdRegistry::new();
    let service_ipam = Arc::new(controllers::service::ServiceIpam::new(&config.service_cidr));
    controllers::service::rebuild_service_ipam_from_services(db, &service_ipam)
        .await
        .context("Failed to rebuild Service ClusterIP allocator")?;
    let nodeport_alloc: Arc<controllers::service::NodePortAllocator> =
        Arc::new(controllers::service::NodePortAllocator::new());
    controllers::service::rebuild_nodeport_allocator_from_services(db, &nodeport_alloc)
        .await
        .context("Failed to rebuild NodePort allocator")?;

    // Load CA cert/key for CSR signing (supervised file I/O)
    let csr_signer: Option<std::sync::Arc<dyn crate::auth::csr_signer::CsrSigner>> = {
        let ca_cert_path = crate::paths::ca_cert_path(&config.containerd_namespace);
        let ca_key_path = crate::paths::ca_key_path(&config.containerd_namespace);
        let cert_result = supervisor
            .run_blocking_file_keyed(
                "bootstrap_ca_cert",
                ca_cert_path.to_string_lossy().to_string(),
                {
                    let p = ca_cert_path.clone();
                    move || std::fs::read_to_string(&p)
                },
            )
            .await;
        let key_result = supervisor
            .run_blocking_file_keyed(
                "bootstrap_ca_key",
                ca_key_path.to_string_lossy().to_string(),
                {
                    let p = ca_key_path.clone();
                    move || std::fs::read_to_string(&p)
                },
            )
            .await;
        match (cert_result, key_result) {
            (Ok(Ok(ca_cert)), Ok(Ok(ca_key))) => Some(std::sync::Arc::new(
                crate::auth::csr_signer::CaCsrSigner::new(ca_cert, ca_key),
            )
                as std::sync::Arc<dyn crate::auth::csr_signer::CsrSigner>),
            _ => {
                tracing::warn!(
                    "CA cert/key not found at {:?}/{:?}; CSR signing disabled",
                    ca_cert_path,
                    ca_key_path
                );
                None
            }
        }
    };

    let controller_dispatcher = Arc::new(
        controller_dispatcher::ControllerDispatcher::new_with_nodeport(
            service_ipam.clone(),
            nodeport_alloc.clone(),
            supervisor.clone(),
            csr_signer,
        ),
    );
    controller_dispatcher.set_services(services.clone()).await;

    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        Some(services.clone()),
        Some(supervisor.clone()),
        Some(db_handle.clone()),
    ));
    side_effects.set_controller_dispatcher(controller_dispatcher.clone());
    local_api_client.set_controller_dispatcher(controller_dispatcher.clone());

    let scheduling_mode = if has_leader_election {
        crate::kubelet::pod_repository::api::PodSchedulingMode::DeferredMultiNodeLeader
    } else {
        crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode
    };
    let runtime_node_role = if kubelet_uses_worker_store_adapter || !has_leader_election {
        RuntimeNodeRole::Worker
    } else {
        RuntimeNodeRole::Leader
    };
    let (pod_lifecycle_tx, pod_lifecycle_rx) =
        tokio::sync::mpsc::channel::<crate::kubelet::lifecycle::LifecycleCommand>(128);
    let pod_lifecycle_rx = Arc::new(tokio::sync::Mutex::new(Some(pod_lifecycle_rx)));
    let pod_watcher_runtime_ports = cri_for_pod_watcher.clone().map(|cri| {
        let runtime = Arc::new(crate::kubelet::pod_runtime::cri::SharedCriRuntime::new(
            SharedCriClient::new(cri),
        ));
        crate::kubelet::pod_manager::PodWatcherRuntimePorts::new(runtime.clone(), runtime)
    });
    let pod_subsystem = crate::kubelet::pod_subsystem::PodSubsystem::new(
        crate::kubelet::pod_subsystem::PodSubsystemConfig {
            db: kubelet_db_handle.clone(),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            scheduling_mode,
            outbox: Some(outbox_runtime.clone()),
            cluster_api: Some(cluster_api.clone()),
            node_name: config.node_name.clone(),
            service_cidr: config.service_cidr.clone(),
            lifecycle_concurrency: crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            network_events: crate::networking::global_pod_network_events(),
            cri: cri_for_pod_watcher.clone().map(SharedCriClient::new),
            containerd_ns: config.containerd_namespace.clone(),
            lifecycle_tx: pod_lifecycle_tx,
            probe_manager: None,
            datapath: Some(network.datapath.clone()),
            service_router: Some(services.clone()),
            runtime_node_role,
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
    let pod_lifecycle_router = pod_subsystem.lifecycle_router.clone();
    pod_repository
        .set_pod_lifecycle_router_for_node(pod_lifecycle_router.clone(), config.node_name.clone());
    let api_pod_repository = if kubelet_uses_worker_store_adapter {
        let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
            crate::kubelet::pod_repository::PodRepositoryBuildConfig {
                db: db_handle.clone(),
                supervisor: supervisor.clone(),
                side_effects: side_effects.clone(),
                metrics: metrics.clone(),
                network_events: crate::networking::global_pod_network_events(),
                scheduling_mode,
                outbox: Some(outbox_runtime.clone()),
                cluster_api: Some(cluster_api.clone()),
            },
        );
        let repo = Arc::new(parts.repository);
        repo.set_pod_lifecycle_router_for_node(
            pod_lifecycle_router.clone(),
            config.node_name.clone(),
        );
        parts.background.start();
        repo
    } else {
        pod_repository.clone()
    };
    let pod_start_retry_state: crate::kubelet::pod_creation_state::PodStartRetryTracker = Arc::new(
        tokio::sync::Mutex::new(crate::kubelet::pod_creation_state::PodStartRetryState::new()),
    );
    side_effects.set_pod_repository(api_pod_repository.clone());
    let oidc_authenticator =
        crate::auth::oidc::build_oidc_authenticator_from_config(config, supervisor.as_ref())
            .await
            .context("failed to build OIDC authenticator")?;
    let webhook_authenticator =
        crate::auth::webhook_auth::build_webhook_auth_from_config(config, supervisor.as_ref())
            .await
            .context("failed to build webhook authenticator")?;

    // P3-11f: leadership watch channels for API proxy gating.
    // The shape watcher updates the senders; AppState holds the
    // RaftLeaderProxy for the middleware.
    //
    // T6 step 4: the `(is_leader_tx, is_leader_rx)` pair is created in
    // `runtime.rs` BEFORE `open_leader` so the same receiver flows into
    // `LocalApiClient`'s inner gate (step 1) and the switching
    // `LeaderProxyApiClient` (step 3). Here we take ownership of the
    // sender and refresh the initial value from live raft metrics in
    // case the raft node initialized between the runtime constructor
    // (which guesses) and now.
    let initial_raft_shape = raft_node.as_ref().map(|node| node.current_shape());
    let initial_is_leader = initial_raft_shape.as_ref().is_none_or(|s| s.is_leader);
    let _ = is_leader_tx.send(initial_is_leader);
    let initial_leader_addr = raft_node
        .as_ref()
        .and_then(|n| n.current_leader_info())
        .map(|(_, addr)| addr);
    if let (Some(leader_addr), Some(lease_client)) = (
        initial_leader_addr.as_ref(),
        control_plane_lease_client.as_ref(),
    ) {
        lease_client.set_current_leader_endpoint(Some(leader_addr.clone()));
    }
    let (leader_addr_tx, leader_addr_rx) = tokio::sync::watch::channel(initial_leader_addr);
    start_controlplane_remote_informers_if_present(remote_api_client, shutdown_token.clone())
        .await
        .context("control-plane remote API informers")?;
    // Load the cluster CA cert once: the follower proxy uses it to verify the
    // leader's serving cert, and the leader uses it to cryptographically
    // re-authenticate client certificates forwarded by follower proxies.
    let ca_cert_path = crate::paths::ca_cert_path(&config.containerd_namespace);
    let cluster_ca_pem = supervisor
        .run_blocking_file_keyed("proxy_read_ca_cert", ca_cert_path.display().to_string(), {
            let p = ca_cert_path.clone();
            move || crate::utils::read_utf8_file(&p)
        })
        .await
        .ok()
        .and_then(|r| r.ok());
    let raft_leader_proxy = if raft_node.is_some() {
        let ca_cert_pem = cluster_ca_pem.clone();
        let proxy_client_identity = crate::api::raft_proxy::load_proxy_client_identity(
            &config.containerd_namespace,
            supervisor.as_ref(),
        )
        .await;
        Some(std::sync::Arc::new(
            crate::api::raft_proxy::RaftLeaderProxy::new(
                is_leader_rx.clone(),
                leader_addr_rx.clone(),
                ca_cert_pem,
            )
            .with_proxy_client_identity(proxy_client_identity),
        ))
    } else {
        None
    };

    let watcher_state = Arc::new(api::AppState {
        db: db_handle.clone(),
        cluster_api: cluster_api.clone(),
        crd_registry,
        mode: node_mode.clone(),
        role: cli.role.clone(),
        replication: replication_service_for_router.clone(),
        network: network.clone(),
        config: Arc::clone(config),
        service_ipam,
        nodeport_alloc,
        cri: None,
        controller_dispatcher,
        side_effects,
        metrics,
        apiservice_proxy_identity_cache: Arc::new(tokio::sync::OnceCell::new()),
        apiservice_proxy_cache: Arc::new(api::apiservice_proxy::ApiServiceProxyCache::default()),
        task_supervisor: supervisor.clone(),
        pod_repository: api_pod_repository.clone(),
        outbox: outbox_runtime.clone(),
        node_lease_tracker: node_lease_tracker.clone(),
        pod_lifecycle_router: Some(pod_lifecycle_router),
        pod_probe_manager: pod_subsystem.probe_manager.clone(),
        pod_lifecycle_rx: Some(pod_lifecycle_rx),
        pod_start_retry_state: Some(pod_start_retry_state),
        is_raft_leader_rx: raft_leader_proxy,
        authorizer: std::sync::Arc::new(
            crate::auth::authorizer::AuthorizerChain::default_chain_with_rbac(
                db_handle.clone(),
                api_pod_repository.clone(),
            ),
        ),
        rbac_policy_store: std::sync::Arc::new(
            crate::auth::rbac_policy_store::DatastoreRbacPolicyStore::new(db_handle.clone()),
        ),
        oidc_authenticator,
        webhook_authenticator,
        cluster_ca_pem: cluster_ca_pem.map(std::sync::Arc::new),
    });
    watcher_state
        .controller_dispatcher
        .set_pod_repository(api_pod_repository.clone())
        .await;

    // VTEP reconciler
    let local_vtep_annotation_handle = {
        let state = watcher_state.clone();
        let cancel = shutdown_token.clone();
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "runtime_local_vtep_annotation_reconciler",
                async move {
                    controllers::node_subnet::run_local_node_vtep_reconciler(state, cancel).await;
                },
            )
            .await
            .context("failed to spawn local VTEP reconciler")?
    };
    tracing::info!("Local VTEP annotation reconciler task started");

    let node_lifecycle_start_resource_version = if has_leader_election {
        db.get_current_resource_version().await.unwrap_or(0)
    } else {
        0
    };

    // Register node — P3-11d: when a RaftNode is present, snapshot its
    // shape so the role label stamped here reflects the live cluster
    // membership. Control-plane voters keep `controlplane` as the stable
    // role and add `leader` only while elected.
    // The supervised shape-watcher task spawned below keeps the labels in
    // sync as elections / membership changes flip the shape.
    // P3-11f: joining raft controlplanes skip node registration
    // during bootstrap. The leader will create Node objects for all
    // raft voters through raft replication. The joiner's node info
    // is included in the JoinAsControlplane RPC.
    let register_result = if skip_seed_bootstrap {
        // Joining controlplane — skip node registration; the seed
        // leader will register this voter's node via raft.
        Ok(())
    } else {
        // Controlplane nodes publish their gRPC port so workers can
        // discover all controlplane endpoints via Node watch.
        let grpc_port = if cli.role.runs_full_stack() {
            Some(config.tls_port)
        } else {
            None
        };
        let registration_addresses = kubelet::node::NodeRegistrationAddresses::new(
            node_ip.to_string(),
            config.external_endpoint.clone(),
        );
        kubelet::node::register_node_with_outbox_and_shape_at_addresses(
            db,
            &outbox_runtime,
            &config.node_name,
            node_mode,
            &cli.role,
            Some(dataplane_health),
            &registration_addresses,
            initial_raft_shape.as_ref(),
            grpc_port,
        )
        .await
    };
    if let Err(e) = register_result {
        tracing::warn!("Failed to register node: {}", e);
    }

    let leader_peer_endpoint_observer_handle = if replication_service_for_router.is_some() {
        match crate::bootstrap::observed_endpoint::start_leader_peer_endpoint_observer(
            db_handle.clone(),
            config.clone(),
            node_mode.clone(),
            supervisor.clone(),
            grpc_transport_policy.clone(),
            shutdown_token.clone(),
        )
        .await
        {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "leader peer observed endpoint watcher not started"
                );
                None
            }
        }
    } else {
        None
    };

    // P3-11d: spawn the shape-driven role-label watcher. On every
    // openraft metrics change (leadership transfer, voter add/remove),
    // recompute the shape and re-register the Node so the
    // `node-role.kubernetes.io/*` label set tracks cluster membership
    // without an operator restart. Worker / replica boots have no
    // `raft_node` and skip this task entirely.
    if let Some(raft) = raft_node.as_ref() {
        let raft_task = raft.clone();
        let outbox_task = outbox_runtime.clone();
        let db_handle_task = db_handle.clone();
        let node_name = config.node_name.clone();
        let node_ip_task = node_ip.to_string();
        let external_endpoint_task = config.external_endpoint.clone();
        let node_mode_task = node_mode.clone();
        let role_task = cli.role.clone();
        let is_leader_tx_task = is_leader_tx.clone();
        let leader_addr_tx_task = leader_addr_tx.clone();
        let grpc_port_task = if cli.role.runs_full_stack() {
            Some(config.tls_port)
        } else {
            None
        };
        let control_plane_lease_client_for_leader_updates = control_plane_lease_client.clone();
        let mut last_shape = initial_raft_shape.clone();
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "raft_shape_role_label_watcher",
                async move {
                    // Deduped server-metrics: fires only on real
                    // state/leadership/membership changes (not every
                    // heartbeat tick), keeping this watcher idle-silent
                    // (HR #1). Shape + leader identity are recomputed from
                    // full metrics via current_shape()/current_leader_info()
                    // only when woken.
                    let mut metrics = raft_task.server_metrics_watch();
                    loop {
                        if metrics.changed().await.is_err() {
                            tracing::debug!(
                                "raft_shape_role_label_watcher: metrics channel closed, exiting"
                            );
                            return;
                        }
                        // Always update the leadership proxy channels on
                        // every metrics change — RaftShape only tracks
                        // (voter_count, is_leader, is_learner) and does
                        // NOT capture current_leader identity. When the
                        // leader changes from node A to node C, followers
                        // (node B) see is_leader=false both before and
                        // after, so the shape comparison would skip the
                        // proxy update, leaving the follower's API proxy
                        // pinned to the dead leader's address.
                        let _ = is_leader_tx_task.send(raft_task.is_leader());
                        match raft_task.current_leader_info() {
                            Some((_, addr)) => {
                                if let Some(lease_client) = control_plane_lease_client_for_leader_updates.as_ref()
                                {
                                    lease_client.set_current_leader_endpoint(Some(addr.clone()));
                                }
                                let _ = leader_addr_tx_task.send(Some(addr.clone()));
                            }
                            None => {
                                if let Some(lease_client) = control_plane_lease_client_for_leader_updates
                                    .as_ref()
                                {
                                    lease_client.clear_current_leader_endpoint();
                                }
                                let _ = leader_addr_tx_task.send(None);
                            }
                        }
                        let shape = raft_task.current_shape();
                        if Some(&shape) == last_shape.as_ref() {
                            continue;
                        }
                        tracing::info!(
                            voter_count = shape.voter_count,
                            is_leader = shape.is_leader,
                            is_learner = shape.is_learner,
                            "raft_shape_role_label_watcher: shape changed, re-stamping Node labels"
                        );
                        let registration_addresses =
                            crate::kubelet::node::NodeRegistrationAddresses::new(
                                node_ip_task.clone(),
                                external_endpoint_task.clone(),
                            );
                        let res = crate::kubelet::node::register_node_with_outbox_and_shape_at_addresses(
                            db_handle_task.as_ref(),
                            &outbox_task,
                            &node_name,
                            &node_mode_task,
                            &role_task,
                            None,
                            &registration_addresses,
                            Some(&shape),
                            grpc_port_task,
                        )
                        .await;
                        if let Err(err) = res {
                            tracing::warn!(
                                error = %err,
                                "raft_shape_role_label_watcher: re-register failed"
                            );
                        }
                        if shape.is_leader
                            && let Err(err) =
                                crate::kubelet::node::clear_leader_label_from_other_nodes(
                                    db_handle_task.as_ref(),
                                    &node_name,
                                )
                                .await
                        {
                            tracing::warn!(
                                error = %err,
                                node_name,
                                "raft_shape_role_label_watcher: failed to clear stale leader role labels"
                            );
                        }
                        last_shape = Some(shape);
                    }
                },
            )
            .await
            .context("failed to spawn raft_shape_role_label_watcher")?;
        tracing::info!("raft_shape_role_label_watcher started");
    }

    // T1.6 + T2 step 1: the controlplane log-apply follower lifecycle
    // is gone. With always-on raft (T2 step 1) every leader-class boot
    // has a raft node, so non-leader voters sync via raft AppendEntries
    // and no separate log_apply follower is needed.

    // ServiceCIDR + kubernetes Service (skip on joining controlplanes —
    // raft AppendEntries delivers these from the seed).
    if leader_lease.is_some() {
        controllers::kube_service::bootstrap_default_service_cidr(db, &config.service_cidr)
            .await
            .context("Failed to bootstrap default ServiceCIDR")?;
        controllers::kube_service::bootstrap_kubernetes_service(
            db,
            &config.service_cidr,
            config.tls_port,
            network.datapath.as_ref(),
        )
        .await
        .context("Failed to bootstrap kubernetes Service")?;
    }
    services.request_services_sync();

    if leader_lease.is_some() {
        controllers::coredns::bootstrap_coredns(
            db,
            watcher_state.pod_repository.clone(),
            config.tls_port,
            &config.service_cidr,
            &config.containerd_namespace,
            &config.node_name,
        )
        .await
        .context("Failed to bootstrap CoreDNS")?;
    }
    controllers::service::rebuild_service_ipam_from_services(
        db,
        watcher_state.service_ipam.as_ref(),
    )
    .await
    .context("Failed to rebuild Service ClusterIP allocator after bootstrap services")?;
    controllers::service::rebuild_nodeport_allocator_from_services(
        db,
        &watcher_state.nodeport_alloc,
    )
    .await
    .context("Failed to rebuild NodePort allocator after bootstrap services")?;

    controllers::crd::load_existing_crds(db, &watcher_state.crd_registry)
        .await
        .context("Failed to load existing CRDs")?;

    let crd_registry_watch_handle = {
        let dbh = db_handle.clone();
        let registry = watcher_state.crd_registry.clone();
        let supervisor_for_task = supervisor.clone();
        let cancel = shutdown_token.clone();
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "runtime_crd_registry_watch",
                async move {
                    controllers::crd::run_crd_registry_watch_with_components(
                        dbh,
                        registry,
                        supervisor_for_task,
                        cancel,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn CRD registry watch")?
    };

    // Spawn pod watcher
    let pod_watcher_handle = if let Some(runtime_ports) = pod_watcher_runtime_ports {
        let state = Arc::new(api::AppState {
            db: kubelet_db_handle.clone(),
            pod_repository: pod_repository.clone(),
            authorizer: std::sync::Arc::new(
                crate::auth::authorizer::AuthorizerChain::default_chain_with_rbac(
                    kubelet_db_handle.clone(),
                    pod_repository.clone(),
                ),
            ),
            rbac_policy_store: std::sync::Arc::new(
                crate::auth::rbac_policy_store::DatastoreRbacPolicyStore::new(
                    kubelet_db_handle.clone(),
                ),
            ),
            ..(*watcher_state).clone()
        });
        let cancel = shutdown_token.clone();
        Some(
            supervisor
                .spawn_async(
                    crate::task_supervisor::TaskCategory::Background,
                    "runtime_pod_watcher",
                    async move {
                        kubelet::run_pod_watcher(runtime_ports, state, cancel).await;
                    },
                )
                .await
                .context("failed to spawn pod watcher")?,
        )
    } else {
        None
    };

    // Heartbeat
    let is_leader_rx_for_heartbeat = is_leader_rx.clone();
    let control_plane_lease_client_for_heartbeat = control_plane_lease_client.clone();
    let heartbeat_handle = {
        let dbc = db_handle.clone();
        let cfg = Arc::clone(config);
        let cancel = shutdown_token.clone();
        let s = supervisor.clone();
        let lease_client: Arc<dyn kubelet::node::NodeLeaseRenewClient> =
            match control_plane_lease_client_for_heartbeat {
                Some(lease_client) => Arc::new(kubelet::node::LeaseRenewClient::new(
                    node_lease_tracker.clone(),
                    lease_client,
                    is_leader_rx_for_heartbeat,
                )),
                None => node_lease_tracker.clone(),
            };
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "runtime_node_heartbeat",
                async move {
                    kubelet::node::run_heartbeat_with_lease_client(
                        dbc,
                        lease_client,
                        cfg.node_name.clone(),
                        cancel,
                        s,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn heartbeat")?
    };

    // Node subnet peer watch
    let node_subnet_watch_handle = {
        let state = watcher_state.clone();
        let cancel = shutdown_token.clone();
        let health = dataplane_health.clone();
        supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "runtime_node_subnet_peer_watch",
                async move {
                    controllers::node_subnet::run_peer_watch(state, health, cancel).await;
                },
            )
            .await
            .context("failed to spawn node subnet peer watch")?
    };

    // Node lifecycle
    // P3-11f: in raft mode, every control-plane node can host the node
    // lifecycle watcher, but only the current leader should reconcile
    // changes. Followers wait on leadership before executing writes.
    let should_run_node_lifecycle = has_leader_election;
    let is_leader_rx_for_grpc = is_leader_rx.clone();
    let node_lifecycle_handle = if should_run_node_lifecycle {
        let state = watcher_state.clone();
        let cancel = shutdown_token.clone();
        let rv = node_lifecycle_start_resource_version;
        let raft_node_for_lifecycle = raft_node.clone();
        Some(
            supervisor
                .spawn_async(
                    crate::task_supervisor::TaskCategory::Background,
                    "runtime_node_lifecycle_controller",
                    async move {
                        controllers::node_lifecycle::run_node_lifecycle_controller(
                            state,
                            cancel,
                            rv,
                            is_leader_rx,
                            raft_node_for_lifecycle,
                        )
                        .await;
                    },
                )
                .await
                .context("failed to spawn node lifecycle controller")?,
        )
    } else {
        None
    };

    // Scheduler is leader-scoped and starts from `phases::leader::start`
    // through the same raft leadership lease loop as the controller
    // workqueue. Starting it here would leave joining voters without a
    // scheduler after failover, or duplicate the scheduler on the seed.
    let scheduler_controller_handle = None;

    let dispatcher_for_worker = watcher_state.controller_dispatcher.clone();

    let state_with_cri = api::AppState {
        cri: cri_for_api,
        ..(*watcher_state).clone()
    };
    let app = if let Some(rs) = replication_service_for_router {
        // P3-11c: if raft mode is active on this leader-class boot,
        // wire the RaftNode-backed Raft RPC dispatcher and the
        // controlplane join handler so peer voters can drive
        // RaftAppendEntries / RaftVote / RaftInstallSnapshot and a
        // joining controlplane can call JoinAsControlplane against
        // this server.
        let (raft_rpc_router, controlplane_join_handler) = match raft_node.as_ref() {
            Some(rn) => {
                let router: Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter> = Arc::new(
                    crate::datastore::raft::node::RaftNodeRpcRouter::from_node(rn.as_ref()),
                );
                let handler: Arc<dyn crate::replication::grpc::raft_rpc::ControlplaneJoinHandler> =
                    Arc::new(crate::datastore::raft::node::RaftNodeJoinHandler::new(
                        rn.clone(),
                        db_handle.clone(),
                    ));
                (Some(router), Some(handler))
            }
            None => (None, None),
        };
        crate::replication::grpc::server::mount_service_full(
            api::build_router(state_with_cri),
            rs,
            db_handle.clone(),
            Some(dispatcher_for_worker.clone()),
            Some(node_lease_tracker.clone()),
            raft_rpc_router,
            controlplane_join_handler,
            &config.containerd_namespace,
            Some(is_leader_rx_for_grpc),
            Some(config.node_name.clone()),
            grpc_transport_policy,
        )
    } else {
        api::build_router(state_with_cri)
    };

    Ok(BootstrapPhase {
        _watcher_state: watcher_state,
        pod_repository: api_pod_repository,
        local_vtep_annotation_handle,
        crd_registry_watch_handle,
        leader_peer_endpoint_observer_handle,
        _node_lifecycle_start_resource_version: node_lifecycle_start_resource_version,
        pod_watcher_handle,
        heartbeat_handle,
        node_subnet_watch_handle,
        node_lifecycle_handle,
        scheduler_controller_handle,
        dispatcher_for_worker,
        app,
    })
}

pub(crate) async fn start_controlplane_remote_informers_if_present(
    remote_api_client: Option<Arc<crate::control_plane::client::remote::RemoteApiClient>>,
    shutdown_token: CancellationToken,
) -> Result<Vec<SupervisedJoinHandle<()>>> {
    match remote_api_client {
        Some(remote_api_client) => {
            remote_api_client
                .start_required_worker_informers(shutdown_token)
                .await
        }
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::control_plane::client::remote::RemoteApiClient;
    use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode};
    use crate::replication::grpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient,
    };
    use crate::replication::grpc::transport_policy::GrpcTransportPolicy;
    use crate::replication::protocol::JoinRole;
    use crate::task_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};

    fn remote_client_for_informer_start_test(
        supervisor: Arc<TaskSupervisor>,
    ) -> Arc<RemoteApiClient> {
        let grpc = Arc::new(ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://127.0.0.1:16443".to_string(),
                token: String::new(),
                node_name: "cp1".to_string(),
                role: JoinRole::Worker,
                dataplane: JoinDataplaneMetadata {
                    endpoint: String::new(),
                    port: None,
                    mode: DataplaneMode::Root,
                    encryption: DataplaneEncryption::Disabled,
                    public_key: None,
                },
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            GrpcTransportPolicy::shared_default(),
        ));
        Arc::new(RemoteApiClient::from_grpc(
            grpc,
            supervisor,
            "cp1".to_string(),
        ))
    }

    #[tokio::test]
    async fn cp_boot_starts_required_worker_informers() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let remote_api_client = remote_client_for_informer_start_test(supervisor.clone());
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = super::start_controlplane_remote_informers_if_present(
            Some(remote_api_client.clone()),
            cancel.clone(),
        )
        .await
        .expect("start informers");

        assert!(
            !handles.is_empty(),
            "control-plane boot must start remote API informer tasks"
        );
        assert!(
            supervisor
                .active_tasks(Some(TaskCategory::Network))
                .iter()
                .any(|task| task.name == "remote_api_informer_watch"),
            "remote informer tasks must be registered with TaskSupervisor"
        );

        let duplicate = super::start_controlplane_remote_informers_if_present(
            Some(remote_api_client),
            cancel.clone(),
        )
        .await
        .expect("duplicate start");
        assert!(
            duplicate.is_empty(),
            "informer startup must be idempotent when worker-store setup already started it"
        );

        cancel.cancel();
        for handle in handles {
            handle.abort();
        }
    }
}
