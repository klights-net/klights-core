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
use klights_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct BootstrapPhase {
    #[cfg(test)]
    pub _watcher_state: Arc<crate::api::ApiState>,
    pub _node_lifecycle_start_resource_version: i64,
    pub pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    pub pod_api_service: Arc<crate::pod_api_service::PodApiService>,
    pub pod_scheduling: Arc<dyn klights_pod_api::PodScheduling>,
    pub crd_registry_watch_handle: SupervisedJoinHandle<()>,
    pub leader_peer_endpoint_observer_handle: Option<SupervisedJoinHandle<()>>,
    pub pod_watcher_handle: Option<SupervisedJoinHandle<()>>,
    pub heartbeat_handle: SupervisedJoinHandle<()>,
    pub node_subnet_watch_handle: SupervisedJoinHandle<()>,
    pub node_lifecycle_handle: Option<SupervisedJoinHandle<()>>,
    pub scheduler_controller_handle: Option<SupervisedJoinHandle<()>>,
    pub dispatcher_for_worker: Arc<crate::controllers::ControllerDispatcher>,
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
    pub leader_coordination: Option<Arc<dyn klights_leader_api::ControllerCoordination>>,
    /// When true, this node is a joining Raft controlplane that has
    /// already caught up via the Phase A backup stream. Seed-only
    /// bootstrap writes (default namespaces, RBAC, kubernetes
    /// Service, ServiceCIDR) are skipped because the catch-up stream
    /// delivered the seed's state into the local cluster.db.
    pub skip_seed_bootstrap: bool,
    pub db_handle: &'a DatastoreHandle,
    pub watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    pub positioned_watch: klights_watch::PositionedWatchService,
    pub pod_workqueue_store: Arc<dyn klights_node_store::PodWorkqueueStore>,
    pub pod_slot_store: Arc<dyn klights_node_store::PodSlotAdmissionStore>,
    pub pod_slot_events: Arc<dyn klights_node_store::PodSlotAdmissionEventSource>,
    pub worker_store_adapter:
        Option<Arc<crate::control_plane::client::worker_store::WorkerStoreAdapter>>,
    pub kubelet_uses_worker_store_adapter: bool,
    pub db: &'a dyn crate::datastore::DatastoreBackend,
    pub leader_ports: crate::control_plane::client::LeaderClientPorts,
    pub resource_commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    pub remote_api_client: Option<Arc<crate::control_plane::client::remote::RemoteApiClient>>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub replication_service_for_router: Option<Arc<crate::replication::ReplicationService>>,
    pub outbox_runtime: Arc<crate::node_outbox::Outbox>,
    pub node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    pub node_lease_renewal_client: Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    pub network: Arc<crate::networking::Network>,
    pub services: Arc<dyn klights_network_api::ServiceRouter>,
    pub local_api_client: Arc<crate::control_plane::client::local::LocalApiClient>,
    pub authenticated_outbox_delivery:
        Arc<dyn klights_leader_api::LeaderAuthenticatedOutboxDelivery>,
    pub control_plane_lease_client: Option<Arc<klights_leader_rpc::client::ReplicationGrpcClient>>,
    pub dataplane_health: &'a klights_networking::dataplane_health::DataplaneHealth,
    pub cri_for_pod_watcher: Option<CriClient>,
    pub cri_for_api: Option<Arc<tokio::sync::Mutex<CriClient>>>,
    pub cni_readiness: crate::kubelet::cni_readiness::CniReadiness,
    pub runtime_paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
    /// P3-11c: when raft mode is active on a leader-class boot, this
    /// is the live `RaftNode`. The bootstrap phase wires its router +
    /// join handler onto the replication gRPC server so peer voters
    /// can drive `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot`
    /// and a joining controlplane can call `JoinAsControlplane`.
    pub raft_node: Option<Arc<crate::datastore::raft::node::RaftNode>>,
    /// Probes each Raft member's replication protocol capabilities before a
    /// leader activates the committed-apply resource-version V1 mode.
    pub member_feature_probe:
        Option<Arc<crate::bootstrap::raft_transport::ReplicationGrpcMemberFeatureProbe>>,
    /// T6 step 4: leadership watch sender, created in runtime.rs before
    /// `open_leader`. The shape watcher inside this phase updates it on
    /// every `Raft::metrics()` change so `LocalApiClient`'s inner gate
    /// (step 1) and the switching `LeaderProxyApiClient` (step 3) see
    /// the live leader state. The matching receiver was already passed
    /// to `open_leader` so it's wired into the datastore phase.
    pub is_leader_tx: tokio::sync::watch::Sender<bool>,
    pub is_leader_rx: tokio::sync::watch::Receiver<bool>,
}

struct BootstrapNodeLeaderLabelStore {
    db: DatastoreHandle,
}

#[async_trait::async_trait]
impl crate::kubelet::node_leader_labels::NodeLeaderLabelStore for BootstrapNodeLeaderLabelStore {
    async fn list_nodes(&self) -> Result<Vec<klights_cluster_core::Resource>> {
        self.db
            .list_resources(
                "v1",
                "Node",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map(|list| list.items)
    }

    async fn update_node_with_preconditions(
        &self,
        name: &str,
        data: serde_json::Value,
        preconditions: crate::datastore::ResourcePreconditions,
    ) -> Result<crate::datastore::Resource> {
        self.db
            .update_resource_with_preconditions("v1", "Node", None, name, data, preconditions)
            .await
    }
}

async fn activate_committed_apply_rv_v1_if_possible(
    raft: &crate::datastore::raft::node::RaftNode,
    probe: Option<&Arc<crate::bootstrap::raft_transport::ReplicationGrpcMemberFeatureProbe>>,
) -> bool {
    let Some(probe) = probe else {
        return false;
    };
    match raft.activate_command_codec_v3(probe.as_ref()).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "raft_shape_role_label_watcher: failed to activate exact-v3 command codec and committed-apply resource-version V1"
            );
            false
        }
    }
}

async fn read_optional_auth_pem(
    supervisor: &TaskSupervisor,
    label: &'static str,
    description: &'static str,
    path: Option<&str>,
) -> Result<Option<String>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let path_buf = std::path::PathBuf::from(path);
    let key = path_buf.to_string_lossy().into_owned();
    let pem = supervisor
        .run_blocking_file_keyed(label, key, move || {
            klights_supervisor::runtime_fs::read_utf8(path_buf)
        })
        .await
        .with_context(|| format!("failed to join {description} read"))?
        .with_context(|| format!("failed to read {description} {path}"))?;
    Ok(Some(pem))
}

async fn compose_oidc_authenticator(
    config: &KlightsConfig,
    supervisor: &TaskSupervisor,
) -> Result<Option<Arc<dyn crate::auth::oidc::OidcValidator>>> {
    let Some(issuer_url) = config.oidc_issuer_url.as_ref() else {
        return Ok(None);
    };
    let client_id = config
        .oidc_client_id
        .as_deref()
        .filter(|client_id| !client_id.is_empty())
        .ok_or_else(|| anyhow::anyhow!("OIDC client ID is required when OIDC is configured"))?;
    let ca_bundle = read_optional_auth_pem(
        supervisor,
        "oidc_read_ca_bundle",
        "OIDC CA bundle",
        config.oidc_ca_bundle.as_deref(),
    )
    .await?;
    let oidc_config = crate::auth::oidc::prepare_oidc_config(Some(crate::auth::oidc::OidcConfig {
        issuer_url: issuer_url.clone(),
        client_id: client_id.to_string(),
        username_claim: config.oidc_username_claim.clone(),
        username_prefix: None,
        groups_claim: config.oidc_groups_claim.clone(),
        groups_prefix: config.oidc_groups_prefix.clone(),
        ca_bundle,
        signing_algs: crate::auth::oidc::default_signing_algs(),
    }))
    .ok_or_else(|| anyhow::anyhow!("invalid OIDC authenticator configuration"))?;
    let discovery_ca_bundle = oidc_config.ca_bundle.clone();
    let crypto = klights_supervisor::CryptoExecutor::from_supervisor(supervisor);
    let discovery = crypto
        .run_blocking("oidc-http-discovery-client-construction", move || {
            crate::auth::oidc::HttpOidcDiscovery::new(discovery_ca_bundle)
        })
        .await
        .map_err(|error| anyhow::anyhow!("OIDC client construction failed: {error}"))?
        .map_err(|error| anyhow::anyhow!("invalid OIDC authenticator configuration: {error}"))?;
    let authenticator: Arc<dyn crate::auth::oidc::OidcValidator> =
        Arc::new(crate::auth::oidc::JwtOidcValidator::new(
            oidc_config,
            Box::new(discovery),
            Arc::new(supervisor.clone()),
        ));
    Ok(Some(authenticator))
}

async fn compose_webhook_authenticator(
    config: &KlightsConfig,
    supervisor: &TaskSupervisor,
) -> Result<Option<Arc<dyn crate::auth::webhook_auth::WebhookAuthenticator>>> {
    let Some(url) = config.webhook_auth_url.as_ref() else {
        return Ok(None);
    };
    let ca_bundle = read_optional_auth_pem(
        supervisor,
        "webhook_auth_read_ca_bundle",
        "webhook auth CA bundle",
        config.webhook_auth_ca_bundle.as_deref(),
    )
    .await?;
    let client_cert = read_optional_auth_pem(
        supervisor,
        "webhook_auth_read_client_cert",
        "webhook auth client certificate",
        config.webhook_auth_client_cert.as_deref(),
    )
    .await?;
    let client_key = read_optional_auth_pem(
        supervisor,
        "webhook_auth_read_client_key",
        "webhook auth client key",
        config.webhook_auth_client_key.as_deref(),
    )
    .await?;
    let webhook_config = crate::auth::webhook_auth::prepare_webhook_auth_config(Some(
        crate::auth::webhook_auth::WebhookAuthConfig {
            url: url.clone(),
            ca_bundle,
            client_cert,
            client_key,
            audiences: config
                .webhook_auth_audiences
                .split(',')
                .map(str::trim)
                .filter(|audience| !audience.is_empty())
                .map(str::to_string)
                .collect(),
            cache_authorized_ttl_secs: config.webhook_auth_cache_authorized_ttl_secs,
            cache_unauthorized_ttl_secs: config.webhook_auth_cache_unauthorized_ttl_secs,
        },
    ))?;
    let Some(webhook_config) = webhook_config else {
        return Ok(None);
    };
    let reviewer_config = webhook_config.clone();
    let crypto = klights_supervisor::CryptoExecutor::from_supervisor(supervisor);
    let reviewer: Arc<dyn crate::auth::webhook_auth::WebhookTokenReviewer> = Arc::new(
        crypto
            .run_blocking("webhook-http-reviewer-client-construction", move || {
                crate::auth::webhook_auth::HttpWebhookTokenReviewer::new(reviewer_config)
            })
            .await
            .map_err(|error| anyhow::anyhow!("webhook client construction failed: {error}"))??,
    );
    let authenticator: Arc<dyn crate::auth::webhook_auth::WebhookAuthenticator> =
        Arc::new(crate::auth::webhook_auth::WebhookAuth::new(
            reviewer,
            std::time::Duration::from_secs(webhook_config.cache_authorized_ttl_secs),
            std::time::Duration::from_secs(webhook_config.cache_unauthorized_ttl_secs),
            webhook_config.audiences,
            Arc::new(crate::auth::clock::SystemMonotonicClock),
        ));
    Ok(Some(authenticator))
}

pub async fn run(args: BootstrapRunArgs<'_>) -> Result<BootstrapPhase> {
    let BootstrapRunArgs {
        config,
        cli,
        node_mode,
        node_ip,
        leader_coordination,
        skip_seed_bootstrap,
        db_handle,
        watch_signals,
        positioned_watch,
        pod_workqueue_store,
        pod_slot_store,
        pod_slot_events,
        worker_store_adapter,
        kubelet_uses_worker_store_adapter,
        db,
        leader_ports,
        resource_commands,
        remote_api_client,
        pod_network_cache,
        pod_runtime_store: node_pod_runtime_store,
        pod_endpoint_store: node_pod_endpoint_store,
        assignment_waiter,
        replication_service_for_router,
        outbox_runtime,
        node_lease_tracker,
        node_lease_renewal_client,
        control_plane_lease_client,
        network,
        services,
        local_api_client,
        authenticated_outbox_delivery,
        dataplane_health,
        cri_for_pod_watcher,
        cri_for_api,
        cni_readiness,
        runtime_paths,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
        raft_node,
        member_feature_probe,
        is_leader_tx,
        is_leader_rx,
    } = args;
    use crate::{api, controllers, kubelet};
    #[cfg(not(test))]
    let service_account_signing_key_path = runtime_paths.service_account_signing_key();
    let api_runtime_paths = crate::api::ApiRuntimePaths::from_data_root(config.data_root.clone())
        .context("invalid API runtime path layout")?;
    let api_runtime_inputs =
        crate::api::ApiRuntimeInputs::new(api_runtime_paths.clone(), config.api_slow_log_threshold)
            .context("invalid API runtime inputs")?;

    // T2 step 2: leader-capable nodes gate one-time init on lease
    // acquisition. For a seed boot the raft node is already leader by
    // this point so acquire succeeds immediately. Joiners are not
    // leader and skip init cleanly (the seed already wrote these rows).
    let has_leader_coordination = leader_coordination.is_some();
    let leader_lease = if has_leader_coordination && !skip_seed_bootstrap {
        match leader_coordination
            .as_ref()
            .unwrap()
            .try_acquire(klights_leader_api::ControllerScope::Cluster)
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
        controllers::namespace::init_default_namespaces_with_ca_path(
            &klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            db,
            &api_runtime_paths.ca_cert,
            chrono::Utc::now(),
        )
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
    let csr_issuer: Option<std::sync::Arc<dyn crate::controllers::csr_signer::CsrIssuer>> = {
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
            (Ok(Ok(ca_cert)), Ok(Ok(ca_key))) => {
                let signer: std::sync::Arc<dyn crate::auth::csr_signer::CsrSigner> =
                    std::sync::Arc::new(crate::auth::csr_signer::CaCsrSigner::new(
                        ca_cert,
                        ca_key,
                        std::sync::Arc::new(crate::auth::clock::SystemClock),
                    ));
                Some(
                    std::sync::Arc::new(crate::bootstrap::auth_adapters::AuthCsrIssuer::new(
                        signer,
                        std::sync::Arc::new(crate::auth::clock::SystemClock),
                        supervisor.clone(),
                    ))
                        as std::sync::Arc<dyn crate::controllers::csr_signer::CsrIssuer>,
                )
            }
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

    let local_node_metrics = cri_for_api.clone().map(|cri| {
        Arc::new(crate::kubelet::metrics::CriNodeMetricsSampler::new(
            cri,
            supervisor.clone(),
        )) as Arc<dyn klights_node_api::NodeMetricsSampler>
    });
    let node_metrics: Arc<dyn klights_node_api::NodeMetrics> =
        Arc::new(crate::node_metrics_adapter::RootNodeMetrics::new(
            config.node_name.clone(),
            local_node_metrics,
            replication_service_for_router
                .clone()
                .map(|service| service as Arc<dyn klights_node_api::NodeMetrics>),
            supervisor.clone(),
        ));

    let metrics = crate::side_effects::SideEffectMetrics::new();
    #[cfg(not(test))]
    let namespace_lifecycle_store =
        crate::api_state_adapter::RootNamespaceTerminationStore::new(db_handle.clone());
    #[cfg(not(test))]
    local_api_client.set_namespace_termination(
        crate::api_state_adapter::RootNamespaceTerminationReconciler::new(
            namespace_lifecycle_store.clone(),
            metrics.clone(),
        ),
    );
    let side_effects = Arc::new(crate::side_effect_registry_composition::default_registry(
        metrics.clone(),
        Some(services.clone()),
        Some(supervisor.clone()),
        Some(db_handle.clone()),
    ));
    let non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort> = Arc::new(
        crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(db_handle.clone()),
    );
    let controller_coordination = Arc::new(crate::controllers::ControllerCoordination::new());
    local_api_client.set_non_pod_finalization(non_pod_finalization.clone());

    let scheduling_mode = if has_leader_coordination {
        crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader
    } else {
        crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode
    };
    let runtime_node_role = if kubelet_uses_worker_store_adapter || !has_leader_coordination {
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
        crate::kubelet::pod_manager::PodWatcherRuntimePorts::new(
            runtime.clone(),
            runtime,
            cni_readiness.clone(),
        )
    });
    let (pod_repository_parts, root_pod_api_services) = if kubelet_uses_worker_store_adapter {
        (
            crate::pod_repository_composition::build_worker_pod_repository_parts(
                crate::pod_repository_composition::WorkerPodRepositoryBuildConfig {
                    resource_query: leader_ports.resource_query.clone(),
                    pod_workqueue_store: pod_workqueue_store.clone(),
                    supervisor: supervisor.clone(),
                    metrics: metrics.clone(),
                    pod_network_cache: pod_network_cache.clone(),
                    assignment_waiter: assignment_waiter.clone(),
                    outbox: outbox_runtime.clone(),
                },
            ),
            None,
        )
    } else {
        let root_parts = crate::pod_repository_composition::build_pod_repository_parts(
            crate::pod_repository_composition::PodRepositoryBuildConfig {
                db: db_handle.clone(),
                pod_workqueue_store: Some(pod_workqueue_store.clone()),
                supervisor: supervisor.clone(),
                side_effects: side_effects.clone(),
                metrics: metrics.clone(),
                pod_network_cache: pod_network_cache.clone(),
                assignment_waiter: assignment_waiter.clone(),
                scheduling_mode,
                outbox: Some(outbox_runtime.clone()),
                cluster_api: Some(leader_ports.resource_query.clone()),
                #[cfg(test)]
                scheduler_bind_gate: None,
                #[cfg(not(test))]
                gc_coordination: controller_coordination.clone(),
            },
            leader_coordination.clone(),
        );
        (
            root_parts.repository_parts,
            Some((
                root_parts.api,
                root_parts.subresource,
                root_parts.scheduling,
            )),
        )
    };
    let kubelet_file_process = klights_supervisor::FileProcessExecutor::new(supervisor.clone());
    let registration_profile =
        crate::bootstrap::node_registration_profile::build(node_mode, &cli.role);
    let kubelet_capacity =
        crate::kubelet::node_registration::NodeRegistrationHostFacts::capture_local(
            &kubelet_file_process,
            &registration_profile,
        )
        .await
        .node_capacity();
    let sandbox_inputs =
        crate::bootstrap::runtime_inputs::capture_sandbox_inputs(&kubelet_file_process, node_mode)
            .await;
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
        outbox_runtime.clone(),
    );
    let pod_subsystem = crate::kubelet::pod_subsystem::PodSubsystem::new(
        crate::kubelet::pod_subsystem::PodSubsystemConfig {
            repository_parts: pod_repository_parts,
            supervisor: supervisor.clone(),
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
            cri: cri_for_pod_watcher.clone().map(SharedCriClient::new),
            registry_proxy: config.registry_proxy.enabled().then(|| {
                crate::kubelet::registry_proxy::ContainerdRegistryProxyConfigurator::new(
                    config.registry_proxy.clone(),
                    runtime_paths.containerd_data_dir().join("certs.d"),
                    klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
                )
            }),
            containerd_ns: config.containerd_namespace.clone(),
            lifecycle_tx: pod_lifecycle_tx,
            probe_manager: None,
            datapath: Some(kubelet_runtime_network.datapath.clone()),
            service_router: Some(services.clone()),
            runtime_node_role,
            runtime_service: None,
            runtime_store: Arc::new(
                crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(
                    node_pod_runtime_store.clone(),
                    config.node_name.clone(),
                    Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
                ),
            ),
            wall_clock: Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            slot_admission: Arc::new(
                crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
                    pod_slot_store,
                    pod_slot_events,
                    config.node_name.clone(),
                ),
            ),
            event_sink: if kubelet_uses_worker_store_adapter {
                Arc::new(crate::bootstrap::kubelet_ports::WorkerPodEventSink::new(
                    outbox_runtime.clone(),
                    leader_ports.resource_query.clone(),
                    Arc::new(klights_supervisor::SystemWallClock),
                ))
            } else {
                Arc::new(crate::bootstrap::kubelet_ports::RootPodEventSink::new(
                    Some(outbox_runtime.clone()),
                    db_handle.clone(),
                    Arc::new(klights_supervisor::SystemWallClock),
                ))
            },
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
    let pod_lifecycle_router = pod_subsystem.lifecycle_router.clone();
    pod_repository
        .set_pod_lifecycle_router_for_node(pod_lifecycle_router.clone(), config.node_name.clone());
    if let Some(worker_store_adapter) = worker_store_adapter.as_ref() {
        worker_store_adapter.set_pod_lifecycle_router(pod_lifecycle_router.clone());
    }
    let (api_pod_repository, pod_api_service, pod_subresource_service, pod_scheduling) =
        if kubelet_uses_worker_store_adapter {
            let root_parts = crate::pod_repository_composition::build_pod_repository_parts(
                crate::pod_repository_composition::PodRepositoryBuildConfig {
                    db: db_handle.clone(),
                    pod_workqueue_store: Some(pod_workqueue_store.clone()),
                    supervisor: supervisor.clone(),
                    side_effects: side_effects.clone(),
                    metrics: metrics.clone(),
                    pod_network_cache: pod_network_cache.clone(),
                    assignment_waiter: assignment_waiter.clone(),
                    scheduling_mode,
                    outbox: Some(outbox_runtime.clone()),
                    cluster_api: Some(leader_ports.resource_query.clone()),
                    #[cfg(test)]
                    scheduler_bind_gate: None,
                    #[cfg(not(test))]
                    gc_coordination: controller_coordination.clone(),
                },
                leader_coordination.clone(),
            );
            let repo = Arc::new(root_parts.repository_parts.repository);
            repo.set_pod_lifecycle_router_for_node(
                pod_lifecycle_router.clone(),
                config.node_name.clone(),
            );
            root_parts
                .repository_parts
                .background
                .start()
                .await
                .context("API Pod repository background startup")?;
            (
                repo,
                root_parts.api,
                root_parts.subresource,
                root_parts.scheduling,
            )
        } else {
            let (api, subresource, scheduling) = root_pod_api_services
                .expect("root Pod API services must accompany the root repository");
            (pod_repository.clone(), api, subresource, scheduling)
        };
    let controller_pod_port = Arc::new(
        crate::controller_runtime_adapter::RootControllerPodPort::new(
            api_pod_repository.clone(),
            pod_api_service.clone(),
            pod_subresource_service.clone(),
        ),
    );
    let controller_leader_ports = Arc::new(
        crate::controller_runtime_adapter::RootControllerLeaderPort::new(db_handle.clone()),
    );
    let controller_dependencies = crate::controllers::ControllerRuntimeDependencies {
        wall_time: chrono::Utc::now,
        resource_query: controller_leader_ports.clone(),
        deployment_store: controller_leader_ports.clone(),
        replicaset_store: controller_leader_ports.clone(),
        statefulset_store: controller_leader_ports.clone(),
        daemonset_store: controller_leader_ports.clone(),
        job_store: controller_leader_ports.clone(),
        service_store: controller_leader_ports.clone(),
        pvc_store: controller_leader_ports.clone(),
        pdb_store: controller_leader_ports.clone(),
        replicationcontroller_store: controller_leader_ports.clone(),
        apiservice_store: controller_leader_ports.clone(),
        csr_status_store: controller_leader_ports,
        pod_query: api_pod_repository.clone(),
        pdb_pod_reader: api_pod_repository.clone(),
        deployment_pod_reader: api_pod_repository.clone(),
        deployment_pod_mutation: controller_pod_port.clone(),
        replicaset_pod_mutation: controller_pod_port.clone(),
        statefulset_pod_mutation: controller_pod_port.clone(),
        daemonset_pod_mutation: controller_pod_port.clone(),
        job_pod_mutation: controller_pod_port.clone(),
        replicationcontroller_pod_mutation: controller_pod_port.clone(),
        pod_delete_sink: api_pod_repository.clone(),
        reconcile: Arc::new(
            crate::controller_runtime_adapter::RootControllerReconcilePort::new(
                non_pod_finalization.clone(),
            ),
        ),
        network: Arc::new(
            crate::controller_runtime_adapter::RootControllerNetworkPort::new(services.clone()),
        ),
        effects: Arc::new(
            crate::controller_runtime_adapter::RootControllerEffectPort::new(
                klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
                config.data_root.join("local-path-provisioner"),
            ),
        ),
        coordination: controller_coordination.clone(),
        node_name: Arc::from(config.node_name.as_str()),
    };
    let hpa_controller = Arc::new(crate::hpa_controller_adapter::HpaController::new(
        db_handle.clone(),
        api_pod_repository.clone(),
        non_pod_finalization,
        controller_coordination.clone(),
        Arc::from(config.node_name.as_str()),
        node_metrics.clone(),
    ));
    let controller_dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new_complete(
        service_ipam.clone(),
        nodeport_alloc.clone(),
        supervisor.clone(),
        csr_issuer,
        hpa_controller,
        controller_dependencies,
    ));
    side_effects.set_controller_dispatcher(controller_dispatcher.clone());
    local_api_client.set_controller_dispatcher(controller_dispatcher.clone());
    let pod_start_retry_state: crate::kubelet::pod_creation_state::PodStartRetryTracker = Arc::new(
        tokio::sync::Mutex::new(crate::kubelet::pod_creation_state::PodStartRetryState::new()),
    );
    side_effects.set_pod_ports(api_pod_repository.clone(), api_pod_repository.clone());
    let oidc_authenticator = compose_oidc_authenticator(config, supervisor.as_ref())
        .await
        .context("failed to build OIDC authenticator")?;
    let webhook_authenticator = compose_webhook_authenticator(config, supervisor.as_ref())
        .await
        .context("failed to build webhook authenticator")?;

    // T6 step 4: the `(is_leader_tx, is_leader_rx)` pair is created in
    // `runtime.rs` BEFORE `open_leader` so the same receiver flows into
    // `LocalApiClient`'s inner gate (step 1) and the switching
    // `LeaderProxyApiClient` (step 3). Here we take ownership of the
    // sender and refresh the initial value from live raft metrics in
    // case the raft node initialized between the runtime constructor
    // (which guesses) and now.
    let initial_raft_shape = raft_node.as_ref().map(|node| node.current_shape());
    let initial_is_leader = initial_raft_shape.as_ref().is_none_or(|s| s.is_leader);
    let initial_declared_role =
        crate::bootstrap::node_registration_profile::build(node_mode, &cli.role).role();
    let initial_role_projection = initial_raft_shape
        .as_ref()
        .map(|shape| crate::authority_adapter::project_raft_shape(&initial_declared_role, shape));
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
    let (leader_authority, authority_publisher) =
        klights_replication::authority::WatchLeaderAuthority::channel(
            initial_is_leader,
            initial_leader_addr.clone(),
        );
    start_controlplane_remote_informers_if_present(remote_api_client, shutdown_token.clone())
        .await
        .context("control-plane remote API informers")?;
    // Load the cluster CA cert once: the follower proxy uses it to verify the
    // leader's serving cert, and the leader uses it to cryptographically
    // re-authenticate client certificates forwarded by follower proxies.
    let ca_cert_path = api_runtime_paths.ca_cert.clone();
    let cluster_ca_pem = supervisor
        .run_blocking_file_keyed("proxy_read_ca_cert", ca_cert_path.display().to_string(), {
            let p = ca_cert_path.clone();
            move || klights_supervisor::runtime_fs::read_utf8(&p)
        })
        .await
        .ok()
        .and_then(|r| r.ok());
    let authority_router = if raft_node.is_some() {
        let ca_cert_pem = cluster_ca_pem.clone();
        let proxy_client_identity = crate::api_server_shell::load_proxy_client_identity(
            &api_runtime_paths.api_proxy_cert,
            &api_runtime_paths.api_proxy_key,
            supervisor.as_ref(),
        )
        .await;
        Some(std::sync::Arc::new(
            crate::api_server_shell::HttpAuthorityRouter::from_authority(
                leader_authority.clone(),
                ca_cert_pem,
            )
            .with_proxy_client_identity(proxy_client_identity),
        ))
    } else {
        None
    };
    let api_authority = authority_router
        .as_ref()
        .map(|_| leader_authority.clone() as Arc<dyn klights_leader_api::LeaderAuthority>);

    let rbac_policy_store: std::sync::Arc<dyn crate::auth::rbac_policy_store::RbacPolicyStore> =
        std::sync::Arc::new(
            crate::auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(std::sync::Arc::new(
                crate::bootstrap::auth_adapters::DatastoreRbacResourceReader::new(
                    db_handle.clone(),
                ),
            )),
        );
    let node_policy_store: std::sync::Arc<dyn crate::auth::node_policy_store::NodePolicyStore> =
        std::sync::Arc::new(
            crate::bootstrap::auth_adapters::PodRepositoryNodePolicyStore::new(
                api_pod_repository.clone(),
            ),
        );
    let node_port_forward = crate::portforward::local_node_port_forward(supervisor.clone());
    #[cfg(test)]
    let api_role = match &cli.role {
        crate::bootstrap::NodeRole::Leader { .. } => crate::api::ApiNodeRole::Leader,
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints,
            as_learner,
            ..
        } => crate::api::ApiNodeRole::Controlplane {
            leader_endpoints: leader_endpoints.clone(),
            as_learner: *as_learner,
        },
        crate::bootstrap::NodeRole::Worker {
            leader_endpoints, ..
        } => crate::api::ApiNodeRole::Worker {
            leader_endpoints: leader_endpoints.clone(),
        },
    };
    #[cfg(test)]
    let api_replication = replication_service_for_router.clone().map(|replication| {
        crate::api::ApiRemoteNodeServices::new(
            replication.clone(),
            replication.clone(),
            replication,
        )
    });
    #[cfg(test)]
    let api_config = Arc::new(crate::api::ApiOperationalConfig::new(
        config.node_name.clone(),
        config.anonymous_auth,
        api_runtime_inputs.clone(),
        crate::version::api_version_info(),
    ));
    let finalizer_lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::DatastoreFinalizerLifecycleAdapter::new_with_coordination(
            db_handle.clone(),
            api_pod_repository.clone(),
            side_effects.clone(),
            metrics.clone(),
            controller_coordination.clone(),
        );
    let mutation_effects =
        crate::resource_mutation_effects_adapter::ResourceMutationEffectsAdapter::new(
            side_effects.clone(),
            metrics.clone(),
        );
    let list_resource_versions =
        crate::list_query_adapter::DatastoreListResourceVersionPort::new(db_handle.clone());
    let gc_owner_lifecycle =
        crate::gc_delete_adapter::GcOwnerLifecycleAdapter::new_with_coordination(
            db_handle.clone(),
            api_pod_repository.clone(),
            controller_coordination.clone(),
        );
    let generated_handler_adapter = crate::generated_handler_adapter::GeneratedHandlerAdapter::new(
        db_handle.clone(),
        watch_signals.clone(),
        positioned_watch.clone(),
        klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
        supervisor.clone(),
        api_runtime_paths.ca_cert.clone(),
    );
    #[cfg(test)]
    let watcher_state = Arc::new(api::ApiState::new(
        crate::api::ApiAuthPolicy::new(
            std::sync::Arc::new(
                crate::auth::authorizer::AuthorizerChain::default_chain_with_rbac(
                    rbac_policy_store.clone(),
                    node_policy_store,
                ),
            ),
            crate::audit::default_audit_sink(),
            std::sync::Arc::new(crate::api::priority_fairness::ApiPriorityFairness::new()),
            rbac_policy_store,
            crate::api::ApiAuthenticators::new(
                Arc::new(
                    crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
                        db_handle.clone(),
                    ),
                ),
                oidc_authenticator,
                webhook_authenticator,
            ),
            cluster_ca_pem.map(std::sync::Arc::new),
        ),
        crate::api::ApiResourceMutationServices {
            #[cfg(test)]
            db: db_handle.clone(),
            watch_stream: Arc::new(
                crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    db_handle.clone(),
                    watch_signals.clone(),
                    positioned_watch.clone(),
                ),
            ),
            #[cfg(not(test))]
            namespace_termination: crate::api_state_adapter::RootNamespaceTerminationStore::new(
                db_handle.clone(),
            ),
            #[cfg(test)]
            namespace_termination:
                crate::api_state_adapter_test_owner::RootNamespaceTerminationStore::new(
                    db_handle.clone(),
                ),
            resource_query: leader_ports.resource_query.clone(),
            resource_command: resource_commands.clone(),
            finalizer_lifecycle,
            mutation_effects,
            list_resource_versions,
            namespace_lists: crate::list_query_adapter::DatastoreNamespaceListPort::new(
                db_handle.clone(),
            ),
            quota_runtime:
                crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                    db_handle.clone(),
                ),
            admission: crate::resource_admission_adapter::ResourceAdmissionAdapter::new(
                db_handle.clone(),
            ),
            custom_resource_reads:
                crate::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                    db_handle.clone(),
                    watch_signals.clone(),
                    positioned_watch.clone(),
                    supervisor.clone(),
                ),
            builtin_admission_defaults: generated_handler_adapter.clone(),
            generated_lifecycle: generated_handler_adapter.clone(),
            generated_mutations: generated_handler_adapter.clone(),
            generated_watch: generated_handler_adapter,
            gc_owner_lifecycle: Arc::new(gc_owner_lifecycle),
            #[cfg(not(test))]
            pod_repository: crate::api_state_adapter::RootApiPodRepository::new(
                api_pod_repository.clone(),
                pod_api_service.clone(),
                pod_subresource_service.clone(),
            ),
            #[cfg(test)]
            pod_repository: api_pod_repository.clone(),
        },
        crate::api::ApiDiscoveryAggregationServices::new(
            crd_registry.clone(),
            Arc::new(tokio::sync::OnceCell::new()),
            Arc::new(api::apiservice_proxy::ApiServiceProxyCache::default()),
        ),
        crate::api::ApiControllerReconcileServices::new(
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                db_handle.clone(),
                service_ipam.clone(),
                nodeport_alloc.clone(),
            ),
            #[cfg(test)]
            service_ipam.clone(),
            #[cfg(test)]
            nodeport_alloc.clone(),
            controller_dispatcher.clone(),
            #[cfg(not(test))]
            crate::api_state_adapter::RootApiFailureMetrics::new(metrics.clone()),
            #[cfg(test)]
            metrics.clone(),
            #[cfg(not(test))]
            crate::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
            #[cfg(test)]
            node_lease_tracker.clone(),
        ),
        crate::api::ApiPodNodeSubresourceServices::new(
            Arc::new(
                crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                    services.clone(),
                ),
            ),
            crate::api::pod_subresources::logs::PodLogFollowWatchSource::new(Arc::new(
                crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(
                    leader_ports.watch.clone(),
                ),
            )),
            None,
            node_metrics.clone(),
            node_port_forward,
            #[cfg(test)]
            Some(pod_lifecycle_router.clone()),
            Some(pod_lifecycle_router.clone()),
            Some(
                crate::bootstrap::operational_adapters::ApiPodStartRetryDiagnostics::new(
                    pod_start_retry_state.clone(),
                ),
            ),
        ),
        crate::api::ApiOperationalServices::new(
            api_role,
            api_replication,
            api_config,
            Arc::new(crate::auth::clock::SystemClock),
            crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                db_handle.clone(),
            ),
            supervisor.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            api_authority.clone(),
        ),
    ));
    let kubelet_config = crate::kubelet::context::KubeletConfig::try_new(
        config.service_cidr.clone(),
        config.node_name.clone(),
        config.containerd_namespace.clone(),
        crate::kubelet::log_rotation::LogRotationPolicy::default(),
        kubelet_capacity,
        runtime_paths,
    )
    .context("kubelet configuration")?;
    let kubelet_services = Arc::new(crate::kubelet::context::KubeletServices::new(
        crate::kubelet::context::KubeletLifecycleServices::new(
            pod_repository.clone(),
            pod_lifecycle_router.clone(),
            pod_lifecycle_rx.clone(),
            pod_start_retry_state.clone(),
        ),
        kubelet_runtime_network,
        kubelet_status_delivery,
        crate::kubelet::context::KubeletLocalExecutionServices::new(
            node_pod_runtime_store,
            node_pod_endpoint_store,
            Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            supervisor.clone(),
            kubelet_file_process,
            kubelet_config,
        ),
    ));

    let node_lifecycle_start_resource_version = if has_leader_coordination {
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
        let registration_profile =
            crate::bootstrap::node_registration_profile::build(node_mode, &cli.role);
        let registration = kubelet::node::NodeRegistrationSnapshot::capture_local(
            &kubelet_services.local_execution().file_process,
            &config.node_name,
            &registration_profile,
            registration_addresses,
            initial_role_projection,
            grpc_port,
        )
        .await;
        let registration_health = dataplane_health.snapshot();
        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            db,
            Some(&outbox_runtime),
            Some(&registration_health),
            &registration,
        )
        .await
    };
    if let Err(e) = register_result {
        tracing::warn!("Failed to register node: {}", e);
    }

    let leader_peer_endpoint_observer_handle = if replication_service_for_router.is_some() {
        let endpoint_query = leader_ports.resource_query.clone();
        let endpoint_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus> =
            Arc::new(crate::kubelet::node::OutboxNodeSelfStatusPublisher::new(
                config.node_name.clone(),
                endpoint_query.clone(),
                outbox_runtime.clone(),
                Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            ));
        match crate::bootstrap::observed_endpoint::start_leader_peer_endpoint_observer(
            db_handle.clone(),
            watch_signals.clone(),
            crate::bootstrap::observed_endpoint::LeaderPeerEndpointObserverDeps::new(
                endpoint_query,
                endpoint_status,
                config.clone(),
                node_mode.clone(),
            ),
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
        let registration_profile_task =
            crate::bootstrap::node_registration_profile::build(node_mode, &cli.role);
        let file_process_task = kubelet_services.local_execution().file_process;
        let is_leader_tx_task = is_leader_tx.clone();
        let grpc_port_task = if cli.role.runs_full_stack() {
            Some(config.tls_port)
        } else {
            None
        };
        let control_plane_lease_client_for_leader_updates = control_plane_lease_client.clone();
        let authority_publisher_task = authority_publisher;
        let member_feature_probe_task = member_feature_probe.clone();
        let mut last_shape = initial_raft_shape.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "raft_shape_role_label_watcher",
                async move {
                    // Deduped server-metrics: fires only on real
                    // state/leadership/membership changes (not every
                    // heartbeat tick), keeping this watcher idle-silent
                    // (HR #1). Shape + leader identity are recomputed from
                    // full metrics via current_shape()/current_leader_info()
                    // only when woken.
                    let mut metrics = raft_task.server_metrics_watch();
                    // A single-voter seed can already be elected before this
                    // watcher starts. Attempt once now; a missing gRPC server
                    // or unready peer merely leaves this false, so future
                    // real metrics changes retry without a timer or polling.
                    let mut activation_succeeded = if raft_task.is_leader() {
                        activate_committed_apply_rv_v1_if_possible(
                            raft_task.as_ref(),
                            member_feature_probe_task.as_ref(),
                        )
                        .await
                    } else {
                        false
                    };
                    let mut last_was_leader = false;
                    loop {
                        if metrics.changed().await.is_err() {
                            tracing::debug!(
                                "raft_shape_role_label_watcher: metrics channel closed, exiting"
                            );
                            return;
                        }
                        // Always update the authority capability on
                        // every metrics change — RaftShape only tracks
                        // (voter_count, is_leader, is_learner) and does
                        // NOT capture current_leader identity. When the
                        // leader changes from node A to node C, followers
                        // (node B) see is_leader=false both before and
                        // after, so the shape comparison would skip the
                        // proxy update, leaving the follower's API proxy
                        // pinned to the dead leader's address.
                        let is_leader = raft_task.is_leader();
                        let _ = is_leader_tx_task.send(is_leader);
                        if !is_leader {
                            activation_succeeded = false;
                        } else if !activation_succeeded {
                            if !last_was_leader {
                                tracing::debug!(
                                    "raft_shape_role_label_watcher: local leadership gained; attempting committed-apply resource-version V1 activation"
                                );
                            }
                            activation_succeeded = activate_committed_apply_rv_v1_if_possible(
                                raft_task.as_ref(),
                                member_feature_probe_task.as_ref(),
                            )
                            .await;
                        }
                        last_was_leader = is_leader;
                        let leader_endpoint = match raft_task.current_leader_info() {
                            Some((_, addr)) => {
                                if let Some(lease_client) = control_plane_lease_client_for_leader_updates.as_ref()
                                {
                                    lease_client.set_current_leader_endpoint(Some(addr.clone()));
                                }
                                Some(addr)
                            }
                            None => {
                                if let Some(lease_client) = control_plane_lease_client_for_leader_updates
                                    .as_ref()
                                {
                                    lease_client.clear_current_leader_endpoint();
                                }
                                None
                            }
                        };
                        authority_publisher_task.publish(is_leader, leader_endpoint);
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
                        let registration = crate::kubelet::node::NodeRegistrationSnapshot::capture_local(
                            &file_process_task,
                            &node_name,
                            &registration_profile_task,
                            registration_addresses,
                            Some(crate::authority_adapter::project_raft_shape(
                                &registration_profile_task.role(),
                                &shape,
                            )),
                            grpc_port_task,
                        )
                        .await;
                        let res =
                            crate::bootstrap::node_registration_adapter::register_node_snapshot(
                            db_handle_task.as_ref(),
                            Some(&outbox_task),
                            None,
                            &registration,
                        )
                        .await;
                        if let Err(err) = res {
                            tracing::warn!(
                                error = %err,
                                "raft_shape_role_label_watcher: re-register failed"
                            );
                        }
                        if shape.is_leader
                            && let Err(err) = crate::kubelet::node::clear_leader_label_from_other_nodes(
                                &BootstrapNodeLeaderLabelStore {
                                    db: db_handle_task.clone(),
                                },
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
            network.datapath().as_ref(),
        )
        .await
        .context("Failed to bootstrap kubernetes Service")?;
    }
    services.request_services_sync()?;

    if leader_lease.is_some() {
        crate::coredns_bootstrap_adapter::bootstrap_coredns(
            db,
            api_pod_repository.clone(),
            controller_pod_port.clone(),
            api_pod_repository.clone(),
            &crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(db_handle.clone()),
            controller_coordination.as_ref(),
            crate::coredns_bootstrap_adapter::CoreDnsBootstrapConfig {
                tls_port: config.tls_port,
                service_cidr: &config.service_cidr,
                containerd_namespace: &config.containerd_namespace,
                node_name: &config.node_name,
            },
        )
        .await
        .context("Failed to bootstrap CoreDNS")?;
    }
    controllers::service::rebuild_service_ipam_from_services(db, service_ipam.as_ref())
        .await
        .context("Failed to rebuild Service ClusterIP allocator after bootstrap services")?;
    controllers::service::rebuild_nodeport_allocator_from_services(db, &nodeport_alloc)
        .await
        .context("Failed to rebuild NodePort allocator after bootstrap services")?;

    controllers::crd::load_existing_crds(db, &crd_registry)
        .await
        .context("Failed to load existing CRDs")?;

    let crd_registry_watch_handle = {
        let dbh = db_handle.clone();
        let registry = crd_registry.clone();
        let crd_runtime = crate::crd_registry_adapter::new_runtime(dbh, positioned_watch.clone());
        let cancel = shutdown_token.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "runtime_crd_registry_watch",
                async move {
                    controllers::crd::run_crd_registry_watch_with_components(
                        crd_runtime,
                        registry,
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
        let ctx = kubelet_services.clone();
        let watch_source: Arc<dyn crate::kubelet::pod_watch_source::PodWatchSource> =
            if let Some(worker_store) = worker_store_adapter.as_ref() {
                Arc::new(
                    crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(
                        worker_store.clone(),
                    ),
                )
            } else {
                Arc::new(
                    crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(
                        leader_ports.watch.clone(),
                    ),
                )
            };
        let volume_events = Arc::new(
            crate::bootstrap::kubelet_ports::LeaderPersistentVolumeEventHandler::new(
                db_handle.clone(),
                is_leader_rx.clone(),
                ctx.local_execution().file_process,
                config.data_root.join("local-path-provisioner"),
            ),
        );
        let cancel = shutdown_token.clone();
        Some(
            supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Background,
                    "runtime_pod_watcher",
                    async move {
                        kubelet::pod_manager::run_pod_watcher_with_services(
                            runtime_ports,
                            ctx.lifecycle(),
                            ctx.status_delivery(),
                            ctx.local_execution(),
                            watch_source,
                            volume_events,
                            cancel,
                        )
                        .await;
                    },
                )
                .await
                .context("failed to spawn pod watcher")?,
        )
    } else {
        None
    };

    // Heartbeat
    let heartbeat_handle = {
        let watch_source = Arc::new(
            crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(
                leader_ports.watch.clone(),
            ),
        );
        let cfg = Arc::clone(config);
        let cancel = shutdown_token.clone();
        let s = supervisor.clone();
        let lease_client = node_lease_renewal_client.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "runtime_node_heartbeat",
                async move {
                    kubelet::node::run_heartbeat_with_lease_client(
                        watch_source,
                        lease_client,
                        Arc::new(
                            crate::bootstrap::kubelet_ports::SystemNodeHeartbeatClock::new(
                                Arc::new(klights_supervisor::SystemWallClock),
                            ),
                        ),
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
        let cancel = shutdown_token.clone();
        let health = dataplane_health.clone();
        let topology = leader_ports.network_topology.clone();
        let query = leader_ports.resource_query.clone();
        let watch = leader_ports.watch.clone();
        let projection =
            crate::node_subnet_controller_adapter::DatastorePeerTopologyProjection::new(
                db_handle.clone(),
                config.node_name.clone(),
                config.cluster_cidr.clone(),
                Some(leader_authority.clone()),
            );
        let node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus> =
            Arc::new(crate::kubelet::node::OutboxNodeSelfStatusPublisher::new(
                config.node_name.clone(),
                query.clone(),
                kubelet_services.status_delivery().outbox.clone(),
                Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            ));
        let readiness_publisher =
            crate::node_subnet_controller_adapter::KubeletNodeReadinessPublisher::new(
                query.clone(),
                node_status,
            );
        let node_name = config.node_name.clone();
        let peering = kubelet_services.runtime_network().peering;
        let peer_supervisor = supervisor.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "runtime_node_subnet_peer_watch",
                async move {
                    controllers::node_subnet::run_focused_peer_watch(
                        topology,
                        query,
                        watch,
                        Some(projection),
                        node_name,
                        peering,
                        peer_supervisor,
                        Some(Arc::new(health)),
                        readiness_publisher,
                        cancel,
                    )
                    .await;
                },
            )
            .await
            .context("failed to spawn node subnet peer watch")?
    };

    // Node lifecycle
    // P3-11f: in raft mode, every control-plane node can host the node
    // lifecycle watcher, but only the current leader should reconcile
    // changes. Followers wait on leadership before executing writes.
    let should_run_node_lifecycle = has_leader_coordination;
    let node_lifecycle_handle = if should_run_node_lifecycle {
        let cancel = shutdown_token.clone();
        let node_lifecycle_status = local_api_client.clone();
        let node_lifecycle_db = db_handle.clone();
        let node_lifecycle_pod_repository = controller_pod_port.clone();
        let node_lifecycle_pod_mutations = api_pod_repository.mutation_reconcile_port();
        let node_lifecycle_pod_router = pod_lifecycle_router.clone();
        let node_lifecycle_lease_tracker = node_lease_tracker.clone();
        let node_lifecycle_supervisor = supervisor.clone();
        let node_lifecycle_pod_eviction_grace = config.node_not_ready_pod_eviction_grace;
        Some(
            supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::Background,
                    "runtime_node_lifecycle_controller",
                    async move {
                        crate::node_lifecycle_controller_adapter::run_node_lifecycle_controller(
                            crate::node_lifecycle_controller_adapter::NodeLifecycleControllerDependencies {
                                store: node_lifecycle_db,
                                pods: node_lifecycle_pod_repository,
                                pod_mutations: node_lifecycle_pod_mutations,
                                pod_lifecycle: node_lifecycle_pod_router,
                                lease_observations: node_lifecycle_lease_tracker,
                                supervisor: node_lifecycle_supervisor,
                                node_status: node_lifecycle_status.clone(),
                                watch: node_lifecycle_status,
                                pod_eviction_grace: node_lifecycle_pod_eviction_grace,
                            },
                            cancel,
                            is_leader_rx,
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

    let dispatcher_for_worker = controller_dispatcher.clone();

    let local_node_exec = cri_for_api.map(|cri| {
        let runtime: Arc<dyn klights_node_api::NodeExecRuntime> = Arc::new(
            crate::kubelet::remote_runtime::CriNodeExecRuntime::new(cri, supervisor.clone()),
        );
        crate::bootstrap::operational_adapters::InProcessNodeExec::new(runtime, supervisor.clone())
            as Arc<dyn klights_node_api::NodeExec>
    });
    #[cfg(not(test))]
    let root_api_role = match &cli.role {
        crate::bootstrap::NodeRole::Leader { .. } => crate::api::RootApiRole::Leader,
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints,
            as_learner,
            ..
        } => crate::api::RootApiRole::Controlplane {
            leader_endpoints: leader_endpoints.clone(),
            as_learner: *as_learner,
        },
        crate::bootstrap::NodeRole::Worker {
            leader_endpoints, ..
        } => crate::api::RootApiRole::Worker {
            leader_endpoints: leader_endpoints.clone(),
        },
    };
    #[cfg(not(test))]
    let root_api_replication = replication_service_for_router.clone().map(|replication| {
        (
            replication.clone() as Arc<dyn klights_node_api::NodeExec>,
            replication.clone() as Arc<dyn klights_node_api::NodeLog>,
            replication as Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>,
        )
    });
    #[cfg(not(test))]
    let api_signing_keys =
        crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::load(
            &service_account_signing_key_path,
            supervisor.as_ref(),
        )
        .await
        .context("load root-owned ServiceAccount signing state")?;
    #[cfg(not(test))]
    let (api_router, api_outer_layers) = api::build_router_from_root(
        Arc::new(
            crate::auth::authorizer::AuthorizerChain::default_chain_with_rbac(
                rbac_policy_store.clone(),
                node_policy_store,
            ),
        ),
        rbac_policy_store,
        Arc::new(
            crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
                db_handle.clone(),
            ),
        ),
        oidc_authenticator,
        webhook_authenticator,
        cluster_ca_pem.map(Arc::new),
        Arc::new(
            crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                db_handle.clone(),
                watch_signals.clone(),
                positioned_watch.clone(),
            ),
        ),
        namespace_lifecycle_store,
        leader_ports.resource_query.clone(),
        resource_commands.clone(),
        finalizer_lifecycle,
        mutation_effects,
        list_resource_versions,
        crate::list_query_adapter::DatastoreNamespaceListPort::new(db_handle.clone()),
        crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
            db_handle.clone(),
        ),
        crate::resource_admission_adapter::ResourceAdmissionAdapter::new(db_handle.clone()),
        crate::custom_resource_read_adapter::CustomResourceReadAdapter::new(
            db_handle.clone(),
            watch_signals.clone(),
            positioned_watch,
            supervisor.clone(),
        ),
        generated_handler_adapter.clone(),
        generated_handler_adapter.clone(),
        generated_handler_adapter.clone(),
        generated_handler_adapter,
        Arc::new(gc_owner_lifecycle),
        crate::api_state_adapter::RootApiPodRepository::new(
            api_pod_repository.clone(),
            pod_api_service.clone(),
            pod_subresource_service.clone(),
        ),
        crd_registry.clone(),
        crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
            db_handle.clone(),
            service_ipam.clone(),
            nodeport_alloc.clone(),
        ),
        controller_dispatcher.clone(),
        crate::api_state_adapter::RootApiFailureMetrics::new(metrics.clone()),
        crate::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
        Arc::new(
            crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(services.clone()),
        ),
        crate::api::pod_subresources::logs::PodLogFollowWatchSource::new(Arc::new(
            crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(
                leader_ports.watch.clone(),
            ),
        )),
        local_node_exec,
        node_metrics.clone(),
        node_port_forward,
        Some(pod_lifecycle_router.clone()),
        Some(
            crate::bootstrap::operational_adapters::ApiPodStartRetryDiagnostics::new(
                pod_start_retry_state.clone(),
            ),
        ),
        root_api_role,
        root_api_replication,
        config.node_name.clone(),
        config.anonymous_auth,
        api_runtime_inputs,
        crate::version::api_version_info(),
        Arc::new(crate::auth::clock::SystemClock),
        crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(db_handle.clone()),
        supervisor.clone(),
        api_signing_keys,
        api_authority,
    );
    #[cfg(test)]
    let state_with_cri = (*watcher_state)
        .clone()
        .with_local_node_exec(local_node_exec);
    #[cfg(test)]
    let (api_router, api_outer_layers) = api::build_router_parts(state_with_cri);
    let api_router = api_outer_layers.finish(crate::api_server_shell::wrap_authority_router(
        api_router,
        authority_router,
    ));
    let app = if let Some(rs) = replication_service_for_router {
        // P3-11c: if raft mode is active on this leader-class boot,
        // wire the RaftNode-backed Raft RPC dispatcher and the
        // controlplane join handler so peer voters can drive
        // RaftAppendEntries / RaftVote / RaftInstallSnapshot and a
        // joining controlplane can call JoinAsControlplane against
        // this server.
        let (raft_rpc_router, controlplane_join_handler) = match raft_node.as_ref() {
            Some(rn) => {
                let router: Arc<dyn klights_leader_rpc::raft_rpc::RaftRpcRouter> =
                    Arc::new(rn.rpc_router());
                let handler =
                    crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler(
                        rn.clone(),
                        db_handle.clone(),
                    );
                (Some(router), Some(handler))
            }
            None => (None, None),
        };
        let grpc_node_query: Arc<dyn klights_leader_api::LeaderResourceQuery> =
            local_api_client.clone();
        let grpc_node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus> =
            Arc::new(crate::kubelet::node::OutboxNodeSelfStatusPublisher::new(
                config.node_name.clone(),
                grpc_node_query.clone(),
                outbox_runtime.clone(),
                Arc::new(crate::kubelet::pod_runtime::store::SystemRuntimeClock),
            ));
        {
            let authenticated_projected_token = Arc::new(
                crate::control_plane::client::local::AuthenticatedProjectedTokenIssuer::new(
                    local_api_client.clone(),
                ),
            );
            let grpc_ports = klights_leader_rpc::server::ReplicationServerPorts::from_split(
                local_api_client.clone(),
                resource_commands,
                authenticated_outbox_delivery,
                authenticated_projected_token,
            );
            klights_leader_rpc::server::mount_service_full_production(
                api_router,
                klights_leader_rpc::server::GrpcReplicationRuntimePorts::from_shared(
                    crate::replication::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                        rs,
                    ),
                ),
                grpc_ports,
                Arc::new(
                    crate::bootstrap::auth_adapters::AuthReplicationPeerAuthenticator::new(
                        supervisor.clone(),
                    ),
                ),
                Arc::new(
                    crate::bootstrap::auth_adapters::AuthControlplaneCredentialIssuer::new(
                        Arc::new(crate::auth::clock::SystemClock),
                        supervisor.clone(),
                    ),
                ),
                Arc::new(chrono::Utc::now),
                raft_rpc_router,
                controlplane_join_handler,
                klights_leader_rpc::ReplicationRuntimeFiles {
                    ca_cert: crate::paths::ca_cert_path(&config.containerd_namespace),
                    ca_key: crate::paths::ca_key_path(&config.containerd_namespace),
                    service_account_signing_key: crate::paths::service_account_signing_key_path(
                        &config.containerd_namespace,
                    ),
                },
                Some(leader_authority.clone()),
                Some(config.node_name.clone()),
                Some(grpc_node_query),
                Some(grpc_node_status),
                Some(local_api_client),
                grpc_transport_policy,
            )
        }
    } else {
        api_router
    };

    Ok(BootstrapPhase {
        #[cfg(test)]
        _watcher_state: watcher_state,
        pod_repository: api_pod_repository,
        pod_api_service,
        pod_scheduling,
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
    use klights_leader_api::JoinRole;
    use klights_leader_rpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient,
    };
    use klights_leader_rpc::transport_policy::GrpcTransportPolicy;
    use klights_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};

    #[tokio::test]
    async fn oidc_composition_reads_root_configured_ca_bundle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ca_path = temp_dir.path().join("oidc-ca.pem");
        let cert =
            rcgen::generate_simple_self_signed(vec!["oidc.example.com".to_string()]).unwrap();
        std::fs::write(&ca_path, cert.cert.pem()).unwrap();
        let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
        let config = crate::KlightsConfig {
            oidc_issuer_url: Some("https://oidc.example.com".to_string()),
            oidc_client_id: Some("klights".to_string()),
            oidc_ca_bundle: Some(ca_path.to_string_lossy().into_owned()),
            ..crate::KlightsConfig::test_default()
        };

        assert!(
            super::compose_oidc_authenticator(&config, &supervisor)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn oidc_composition_requires_client_id() {
        let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
        let config = crate::KlightsConfig {
            oidc_issuer_url: Some("https://oidc.example.com".to_string()),
            oidc_client_id: None,
            ..crate::KlightsConfig::test_default()
        };

        let error = match super::compose_oidc_authenticator(&config, &supervisor).await {
            Ok(_) => panic!("configured OIDC requires a client ID"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("client ID"));
    }

    #[tokio::test]
    async fn webhook_composition_reads_root_configured_ca_bundle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ca_path = temp_dir.path().join("webhook-ca.pem");
        let cert = rcgen::generate_simple_self_signed(vec!["auth-webhook.example.com".to_string()])
            .unwrap();
        std::fs::write(&ca_path, cert.cert.pem()).unwrap();
        let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());
        let config = crate::KlightsConfig {
            webhook_auth_url: Some("https://auth-webhook.example.com/token".to_string()),
            webhook_auth_ca_bundle: Some(ca_path.to_string_lossy().into_owned()),
            ..crate::KlightsConfig::test_default()
        };

        assert!(
            super::compose_webhook_authenticator(&config, &supervisor)
                .await
                .unwrap()
                .is_some()
        );
    }

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
                    mode: klights_leader_api::NetworkNodeMode::Root,
                    encryption: klights_leader_api::DataplaneEncryption::Direct,
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
            Arc::new(crate::remote_informer_cache_adapter::WatchCacheAdapter::new()),
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
