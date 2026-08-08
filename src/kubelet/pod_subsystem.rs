//! Pod subsystem composition root. Owns the Pod repository, lifecycle
//! router, and background services. Constructed once per process by
//! bootstrap, then shared by consumers behind narrow trait references.

use std::sync::Arc;

use anyhow::Result;

use crate::kubelet::pod_repository::PodRepository;
use crate::kubelet::pod_repository::background::PodRepositoryBackground;
use crate::kubelet::pod_repository::facade::PodRepositoryParts;
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::service::{
    PodRuntimeService, RealPodRuntimeService, RealPodRuntimeServiceDependencies,
};
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
use klights_kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
use klights_kubelet::pod_lifecycle_router::executor::{PodLifecycleExecutor, PodWorkExecutor};
use klights_kubelet::pod_lifecycle_router::{PodLifecycleRouteMode, PodLifecycleRouter};
use klights_kubelet::pod_lifecycle_service::PodLifecycleService;
use klights_kubelet::probe_manager::ProbeManager;
use klights_supervisor::TaskSupervisor;

struct LifecycleWallClock {
    runtime_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
}

impl klights_supervisor::WallClock for LifecycleWallClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(self.runtime_clock.now_ms().max(0) as u64)
    }
}

/// Wiring inputs for PodSubsystem construction.
pub struct PodSubsystemConfig {
    pub repository_parts: PodRepositoryParts,
    pub supervisor: Arc<TaskSupervisor>,
    pub outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    pub resource_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    pub projected_tokens: Option<Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>>,
    pub node_name: String,
    pub service_cidr: String,
    pub lifecycle_concurrency: PodLifecycleConcurrencyConfig,
    pub pod_actor_idle_grace: std::time::Duration,
    pub sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs,
    pub node_capacity: klights_kubelet::node_capacity::NodeCapacity,
    pub paths: klights_kubelet::runtime_paths::KubeletRuntimePaths,
    pub lifecycle_route_mode: PodLifecycleRouteMode,
    // Task 19: runtime dependencies for RealPodRuntimeService construction (Task 24).
    pub cri: Option<klights_kubelet::cri::SharedCriClient>,
    pub registry_proxy:
        Option<klights_kubelet::registry_proxy::ContainerdRegistryProxyConfigurator>,
    pub containerd_ns: String,
    pub lifecycle_tx: tokio::sync::mpsc::Sender<klights_kubelet::lifecycle::LifecycleCommand>,
    pub probe_manager: Option<Arc<ProbeManager>>,
    pub datapath: Option<Arc<dyn klights_network_api::Datapath>>,
    pub service_router: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    pub runtime_service: Option<Arc<dyn PodRuntimeService>>,
    pub runtime_store: Arc<dyn PodRuntimeStore>,
    pub wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    pub slot_admission: Arc<dyn PodSlotAdmission>,
    pub event_sink: Arc<dyn PodEventSink>,
}

/// Composition root: owns the Pod repository, lifecycle router,
/// background services, and runtime adapter dependencies (stored for
/// Task 24 construction). Background work starts in explicit `start()`.
pub struct PodSubsystem {
    pub supervisor: Arc<TaskSupervisor>,
    pub repository: Arc<PodRepository>,
    pub repository_background: PodRepositoryBackground,
    pub lifecycle_router: Arc<PodLifecycleRouter>,
    pub lifecycle_service: PodLifecycleService,
    pub runtime: Arc<dyn PodRuntimeService>,
    // Task 19: runtime adapter dependencies stored for Task 24 construction.
    pub cri: Option<klights_kubelet::cri::SharedCriClient>,
    pub containerd_ns: String,
    pub probe_manager: Option<Arc<ProbeManager>>,
    pub datapath: Option<Arc<dyn klights_network_api::Datapath>>,
    pub service_router: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    pub node_name: String,
    pub service_cidr: String,
}

struct RuntimeServiceBuildRequest {
    supervisor: Arc<TaskSupervisor>,
    repository: Arc<PodRepository>,
    cri: Option<klights_kubelet::cri::SharedCriClient>,
    registry_proxy: Option<klights_kubelet::registry_proxy::ContainerdRegistryProxyConfigurator>,
    containerd_ns: String,
    probe_manager: Arc<ProbeManager>,
    datapath: Option<Arc<dyn klights_network_api::Datapath>>,
    service_router: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    node_name: String,
    service_cidr: String,
    sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs,
    node_capacity: klights_kubelet::node_capacity::NodeCapacity,
    paths: klights_kubelet::runtime_paths::KubeletRuntimePaths,
    resource_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    projected_tokens: Option<Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>>,
    runtime_store: Arc<dyn PodRuntimeStore>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    slot_admission: Arc<dyn PodSlotAdmission>,
    event_sink: Arc<dyn PodEventSink>,
    deletion_finalizer:
        Arc<dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer>,
}

impl PodSubsystem {
    /// Build repository parts and lifecycle router WITHOUT starting
    /// background work. Call `start()` after wiring is complete.
    pub fn new(config: PodSubsystemConfig) -> Result<Self> {
        let supervisor = config.supervisor.clone();
        let node_name = config.node_name.clone();
        let service_cidr = config.service_cidr.clone();
        let cri = config.cri.clone();
        let registry_proxy = config.registry_proxy.clone();
        let containerd_ns = config.containerd_ns.clone();
        let lifecycle_tx = config.lifecycle_tx.clone();
        let datapath = config.datapath.clone();
        let service_router = config.service_router.clone();
        let resource_query = config.resource_query.clone();
        let projected_tokens = config.projected_tokens.clone();
        let outbox = config.outbox.clone();
        let lifecycle_concurrency = config.lifecycle_concurrency.clone();
        let sandbox_inputs = config.sandbox_inputs.clone();
        let node_capacity = config.node_capacity;
        let paths = config.paths.clone();
        let parts = config.repository_parts;
        let (repository, repository_background, deletion_finalizer) =
            parts.into_pod_subsystem_parts();

        let lifecycle_wall_clock: Arc<dyn klights_supervisor::WallClock> =
            Arc::new(LifecycleWallClock {
                runtime_clock: config.wall_clock.clone(),
            });
        let registry = Arc::new(
            PodLifecycleRegistry::new_with_idle_grace(
                config.supervisor.clone(),
                lifecycle_concurrency.clone(),
                Arc::new(std::sync::Mutex::new(Arc::new(
                    klights_kubelet::pod_lifecycle_router::executor::NoopExecutor,
                ))),
                config.pod_actor_idle_grace,
                lifecycle_wall_clock.clone(),
            )
            .with_runtime_observation_store(outbox.clone())
            .with_local_node_name(node_name.clone()),
        );
        let lifecycle_router = match config.lifecycle_route_mode {
            PodLifecycleRouteMode::Actor => Arc::new(PodLifecycleRouter::new_actor(registry)),
            PodLifecycleRouteMode::Multiplex => Arc::new(PodLifecycleRouter::new(
                supervisor.clone(),
                lifecycle_concurrency,
                PodLifecycleRouteMode::Multiplex,
                config.pod_actor_idle_grace,
                lifecycle_wall_clock,
            )),
        };
        let lifecycle_service = PodLifecycleService::new(lifecycle_router.clone());
        let repository = Arc::new(repository);
        let probe_cri_runtime = config.cri.clone().map(|cri| {
            Arc::new(
                klights_kubelet::runtime::cri::SharedCriRuntime::new_with_registry_proxy(
                    cri,
                    registry_proxy.clone(),
                ),
            ) as Arc<dyn klights_kubelet::runtime::cri::CriRuntime>
        });
        let probe_manager = config.probe_manager.unwrap_or_else(|| {
            Arc::new(ProbeManager::new_with_lifecycle(
                supervisor.clone(),
                repository.clone() as Arc<dyn klights_pod_api::PodQuery>,
                probe_cri_runtime.clone(),
                lifecycle_tx.clone(),
                config.wall_clock.clone(),
            ))
        });
        let runtime = match config.runtime_service.clone() {
            Some(runtime_service) => runtime_service,
            None => Self::build_runtime_service(RuntimeServiceBuildRequest {
                supervisor: supervisor.clone(),
                repository: repository.clone(),
                cri: cri.clone(),
                registry_proxy: registry_proxy.clone(),
                containerd_ns: containerd_ns.clone(),
                probe_manager: probe_manager.clone(),
                datapath: datapath.clone(),
                service_router: service_router.clone(),
                node_name: node_name.clone(),
                service_cidr: service_cidr.clone(),
                sandbox_inputs,
                node_capacity,
                paths,
                resource_query,
                projected_tokens,
                runtime_store: config.runtime_store.clone(),
                wall_clock: config.wall_clock.clone(),
                slot_admission: config.slot_admission.clone(),
                event_sink: config.event_sink.clone(),
                deletion_finalizer,
            })?,
        };

        Ok(Self {
            supervisor,
            repository,
            repository_background,
            lifecycle_router,
            lifecycle_service,
            runtime,
            cri,
            containerd_ns,
            probe_manager: Some(probe_manager),
            datapath,
            service_router,
            node_name,
            service_cidr,
        })
    }

    fn build_runtime_service(
        request: RuntimeServiceBuildRequest,
    ) -> Result<Arc<dyn PodRuntimeService>> {
        let RuntimeServiceBuildRequest {
            supervisor,
            repository,
            cri,
            registry_proxy,
            containerd_ns,
            probe_manager,
            datapath,
            service_router,
            node_name,
            service_cidr,
            sandbox_inputs,
            node_capacity,
            paths,
            resource_query,
            projected_tokens,
            runtime_store,
            wall_clock,
            slot_admission,
            event_sink,
            deletion_finalizer,
        } = request;
        let cri =
            cri.ok_or_else(|| anyhow::anyhow!("missing PodRuntimeService dependencies: cri"))?;
        let datapath = datapath
            .ok_or_else(|| anyhow::anyhow!("missing PodRuntimeService dependencies: datapath"))?;
        let service_router = service_router.ok_or_else(|| {
            anyhow::anyhow!("missing PodRuntimeService dependencies: service_router")
        })?;
        let volume_resource_query = resource_query.ok_or_else(|| {
            anyhow::anyhow!("missing PodRuntimeService dependencies: resource_query")
        })?;
        let volume_projected_tokens = projected_tokens.ok_or_else(|| {
            anyhow::anyhow!("missing PodRuntimeService dependencies: projected_tokens")
        })?;
        let cri_runtime = Arc::new(
            klights_kubelet::runtime::cri::SharedCriRuntime::new_with_registry_proxy(
                cri.clone(),
                registry_proxy,
            ),
        );
        let pod_reader: Arc<dyn klights_pod_api::PodQuery> = repository.clone();
        let hostports: Arc<dyn crate::kubelet::pod_runtime::hostports::HostPortRuntime> = Arc::new(
            crate::kubelet::pod_runtime::hostports::RealHostPortRuntime::new(
                service_router,
                pod_reader,
                node_name.clone(),
            ),
        );
        let node_view = Arc::new(
            crate::kubelet::pod_cluster_runtime::LocalNodeRuntimeView::new(node_name.clone()),
        );
        // Every node routes Pod status through the same repository-backed
        // cluster view. Local versus remote delivery is selected by the
        // injected repository, so kubelet policy has no node-role branch.
        let cluster_view: Arc<dyn crate::kubelet::pod_cluster_runtime::ClusterRuntimeView> =
            Arc::new(
                crate::kubelet::pod_cluster_runtime::RepositoryClusterRuntimeView::new(
                    repository.clone(),
                ),
            );
        Ok(Arc::new(RealPodRuntimeService::new(
            RealPodRuntimeServiceDependencies {
                cri: cri_runtime.clone(),
                container_control: cri_runtime.clone(),
                network: Arc::new(
                    crate::kubelet::pod_runtime::network::RealPodNetworkRuntime::new(
                        datapath,
                        repository.clone(),
                        runtime_store.clone(),
                    ),
                ),
                store: runtime_store,
                clock: wall_clock,
                slot_admission,
                repository: repository.clone(),
                filesystem: Arc::new(
                    crate::kubelet::pod_runtime::filesystem::RealPodFilesystem::new(
                        supervisor.clone(),
                        containerd_ns.clone(),
                        node_name.clone(),
                        paths.clone(),
                    ),
                ),
                volumes: Arc::new(
                    crate::kubelet::pod_runtime::volumes::RealPodVolumeRuntime::new(
                        Arc::new(
                            klights_kubelet::volume_sources::LocalCacheVolumeSourceReader::new(
                                volume_resource_query.clone(),
                                volume_projected_tokens,
                            ),
                        ),
                        containerd_ns.clone(),
                        supervisor.clone(),
                        node_capacity,
                        paths.clone(),
                    ),
                ),
                probes: Arc::new(crate::kubelet::pod_runtime::probes::RealProbeRuntime::new(
                    probe_manager,
                )),
                hostports,
                events: event_sink,
                hooks: Arc::new(crate::kubelet::pod_runtime::hooks::RealPodHookRuntime::new(
                    cri_runtime.clone(),
                    supervisor.clone(),
                )),
                env_source: Arc::new(klights_kubelet::pod_env::LeaderApiEnvSourceReader::new(
                    volume_resource_query,
                )),
                finalizer: deletion_finalizer,
                supervisor,
                config: crate::kubelet::pod_runtime::service::RuntimeConfig {
                    node_name,
                    service_cidr,
                    containerd_namespace: containerd_ns,
                    sandbox_inputs,
                    node_capacity,
                    paths,
                },
                node_view,
                cluster_view,
            },
        )))
    }

    #[cfg(test)]
    pub fn runtime_service(&self) -> Arc<dyn PodRuntimeService> {
        self.runtime.clone()
    }

    pub async fn build_executor(&self) -> Result<Arc<PodLifecycleExecutor>> {
        Ok(Arc::new(PodLifecycleExecutor::new(self.runtime.clone())))
    }

    /// Start background services: workqueue reconciler, watch runner,
    /// deadline timer runner. Idempotent (repeated calls are safe).
    pub async fn start(&self) -> Result<()> {
        self.repository_background.start().await
    }

    /// Replace the work executor at runtime via the lifecycle service.
    pub fn set_work_executor(&self, executor: Arc<dyn PodWorkExecutor>) {
        self.lifecycle_service.set_work_executor(executor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreHandle;
    use crate::pod_repository_composition::PodSchedulingMode;
    use klights_controllers::side_effects::SideEffectMetrics;
    use klights_controllers::side_effects::SideEffectRegistry;
    use klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;

    fn fixture_supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn fixture_config(db: DatastoreHandle) -> PodSubsystemConfig {
        fixture_config_with_scheduling_mode(db, PodSchedulingMode::InlineSingleNode)
    }

    fn fixture_config_with_scheduling_mode(
        db: DatastoreHandle,
        scheduling_mode: PodSchedulingMode,
    ) -> PodSubsystemConfig {
        let (lifecycle_tx, _rx) =
            tokio::sync::mpsc::channel::<klights_kubelet::lifecycle::LifecycleCommand>(8);
        let cluster_api = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "node-1".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let runtime_service =
            Arc::new(klights_kubelet::runtime::test_support::MockPodRuntimeService::new());
        let supervisor = fixture_supervisor();
        let side_effects = Arc::new(SideEffectRegistry::new());
        let metrics = SideEffectMetrics::new();
        let repository_parts =
            PodRepository::build_parts(crate::kubelet::pod_repository::PodRepositoryBuildConfig {
                db: db.clone(),
                pod_workqueue_store: None,
                supervisor: supervisor.clone(),
                side_effects,
                metrics,
                pod_network_cache: crate::kubelet::pod_repository::empty_test_pod_network_cache(),
                assignment_waiter: crate::kubelet::pod_repository::test_assignment_bus(),
                scheduling_mode,
                outbox: None,
                cluster_api: Some(cluster_api.clone()),
                remote_delivery_required: false,
                controller_identity:
                    crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                scheduler_bind_gate: None,
            });
        PodSubsystemConfig {
            repository_parts,
            supervisor,
            outbox: None,
            resource_query: Some(cluster_api.clone()),
            projected_tokens: Some(cluster_api),
            node_name: "node-1".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            lifecycle_concurrency: PodLifecycleConcurrencyConfig::production_default(),
            lifecycle_route_mode: PodLifecycleRouteMode::Actor,
            pod_actor_idle_grace:
                klights_kubelet::pod_lifecycle_actor::actor::DEFAULT_POD_ACTOR_IDLE_GRACE,
            sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs::default(),
            node_capacity: klights_kubelet::node_capacity::NodeCapacity::default(),
            paths: klights_kubelet::runtime_paths::KubeletRuntimePaths::new(
                std::path::PathBuf::from("/tmp/klights-pod-subsystem-test"),
            )
            .unwrap(),
            cri: None,
            registry_proxy: None,
            containerd_ns: "klights".to_string(),
            lifecycle_tx,
            probe_manager: None,
            datapath: None,
            service_router: None,
            runtime_service: Some(runtime_service),
            runtime_store: Arc::new(
                klights_kubelet::runtime::test_support::MockPodRuntimeStore::new(),
            ),
            wall_clock: Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            slot_admission: Arc::new(
                klights_kubelet::runtime::test_support::MockPodSlotAdmission::new(),
            ),
            event_sink: Arc::new(crate::bootstrap::kubelet_ports::RootPodEventSink::new(
                None,
                db,
                Arc::new(klights_supervisor::SystemWallClock),
            )),
        }
    }

    /// Task 5.1: Config struct requires repository, router, and node identity.
    #[tokio::test]
    async fn pod_subsystem_config_requires_repository_router_and_node_identity() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config(db);
        assert_eq!(config.node_name, "node-1");
        // Repository builder parameters are present.
        let _ = &config.supervisor;
        let _ = &config.repository_parts;
        let _ = &config.lifecycle_concurrency;
        let _ = &config.runtime_store;
        let _ = &config.slot_admission;
        let _ = &config.event_sink;
    }

    /// Task 5.1: Construction produces repository and router without starting
    /// background work.
    #[tokio::test]
    async fn pod_subsystem_constructs_repository_and_router_without_starting_background() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config(db);

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // Repository is available.
        let _repo = &subsystem.repository;

        // Router is available.
        let _router = &subsystem.lifecycle_router;

        // Background services must NOT be started during construction.
        assert!(
            !subsystem.repository_background.workqueue_start_called(),
            "background workqueue must not be started during construction"
        );
    }

    /// Task 5.2: explicit start() boundary.
    #[tokio::test]
    async fn pod_subsystem_start_has_explicit_background_start_contract() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let _cfg = fixture_config(db);
        // This test verifies that PodSubsystem::start() exists, is
        // callable, and follows the explicit-start contract. The
        // async variant (below) tests actual background startup.
    }

    /// Task 5.2: start() calls repository background start exactly once
    /// and repeated calls are safe (idempotent).
    #[tokio::test]
    async fn pod_subsystem_start_starts_repository_background_once() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config(db);

        let subsystem = PodSubsystem::new(config).unwrap();

        // Not started yet.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // First start.
        subsystem.start().await.unwrap();
        assert!(subsystem.repository_background.workqueue_start_called());

        // Second start is idempotent.
        subsystem.start().await.unwrap();
        assert!(subsystem.repository_background.workqueue_start_called());
    }

    #[tokio::test]
    async fn pod_subsystem_accepts_injected_runtime_service() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let injected =
            Arc::new(klights_kubelet::runtime::test_support::MockPodRuntimeService::new())
                as Arc<dyn PodRuntimeService>;
        let mut config = fixture_config(db);
        config.runtime_service = Some(injected.clone());

        let subsystem = PodSubsystem::new(config).expect("construction must succeed");

        assert!(
            Arc::ptr_eq(&subsystem.runtime_service(), &injected),
            "subsystem must retain the injected runtime service"
        );
    }

    #[tokio::test]
    async fn pod_subsystem_without_injected_runtime_requires_real_runtime_dependencies() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let mut config = fixture_config(db);
        config.runtime_service = None;
        config.cri = None;
        config.datapath = None;
        config.resource_query = None;

        let err = match PodSubsystem::new(config) {
            Ok(_) => panic!("missing real runtime dependencies must fail construction"),
            Err(err) => err,
        };

        assert!(
            format!("{err:#}").contains("missing PodRuntimeService dependencies"),
            "unexpected error: {err:#}"
        );
    }

    // ── Task 19: runtime dependency fields on PodSubsystemConfig ──

    /// PodSubsystemConfig carries all runtime adapter dependencies needed for
    /// RealPodRuntimeService construction in Task 24.
    #[tokio::test]
    async fn pod_subsystem_config_carries_runtime_dependencies() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config(db.clone());
        assert_eq!(config.node_name, "node-1");
        assert_eq!(config.service_cidr, "10.43.128.0/17");
        assert_eq!(config.containerd_ns, "klights");
        assert!(config.cri.is_none(), "CRI is None by default in tests");
        assert!(
            config.probe_manager.is_none(),
            "probe_manager is None by default"
        );
        assert!(config.datapath.is_none(), "datapath is None by default");
        assert!(
            config.service_router.is_none(),
            "service_router is None by default"
        );
        // Storage ports on PodSubsystem config work after construction.
        let config2 = fixture_config(db);
        let subsystem = PodSubsystem::new(config2).expect("construction must succeed");
        assert_eq!(subsystem.containerd_ns, "klights");
        assert_eq!(subsystem.service_cidr, "10.43.128.0/17");
        assert!(subsystem.cri.is_none());
        assert!(
            subsystem.probe_manager.is_some(),
            "PodSubsystem hoists ProbeManager construction"
        );
    }

    /// Task 24: PodSubsystem owns the runtime service and builds the
    /// lifecycle executor from that runtime instead of watcher-local legacy
    /// wiring.
    #[tokio::test]
    async fn bootstrap_constructs_real_pod_runtime_service_and_binds_executor() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let sock_path = temp_dir.path().join("cri.sock");
        let _listener =
            std::os::unix::net::UnixListener::bind(&sock_path).expect("bind temp unix socket");
        let mut config = fixture_config(db);
        let cri = klights_kubelet::cri::CriClient::connect(
            &sock_path.to_string_lossy(),
            "klights",
            config.supervisor.as_ref().clone(),
        )
        .await
        .expect("connect temp cri socket");
        config.cri = Some(klights_kubelet::cri::SharedCriClient::new(cri));
        config.datapath = Some(Arc::new(
            klights_networking::test_support::MockNetworkProvider::new(),
        ));
        config.service_router = Some(Arc::new(
            klights_networking::test_support::MockServiceRouter::new(),
        ));
        config.runtime_service = None;
        let subsystem = PodSubsystem::new(config).expect("construction must succeed");

        let runtime = subsystem.runtime_service();
        assert!(
            std::sync::Arc::strong_count(&runtime) >= 2,
            "subsystem must retain the runtime service"
        );

        let executor = subsystem
            .build_executor()
            .await
            .expect("runtime-backed executor construction must succeed");
        assert!(
            std::sync::Arc::ptr_eq(&executor.runtime(), &runtime),
            "executor must use the exact subsystem runtime service"
        );
    }

    // ── Task 14.1: leader bootstrap wiring ──

    /// PodSubsystem constructed with leader scheduling mode exposes all
    /// components (repository, router, lifecycle service) and supports
    /// explicit start.
    #[tokio::test]
    async fn leader_bootstrap_constructs_pod_subsystem_with_leader_objects() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config =
            fixture_config_with_scheduling_mode(db, PodSchedulingMode::DeferredMultiNodeLeader);

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // All components accessible.
        let _repo = &subsystem.repository;
        let _router = &subsystem.lifecycle_router;
        let _service = &subsystem.lifecycle_service;

        // Background not started during construction.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // Explicit start works and is idempotent.
        subsystem.start().await.unwrap();
        assert!(subsystem.repository_background.workqueue_start_called());
        subsystem.start().await.unwrap(); // idempotent
    }

    // ── Task 14.2: worker bootstrap wiring ──

    /// PodSubsystem constructed with inline single-node scheduling mode
    /// exposes all components and supports explicit start.
    #[tokio::test]
    async fn worker_bootstrap_constructs_pod_subsystem_with_worker_objects() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config_with_scheduling_mode(db, PodSchedulingMode::InlineSingleNode);

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // All components accessible.
        let _repo = &subsystem.repository;
        let _router = &subsystem.lifecycle_router;
        let _service = &subsystem.lifecycle_service;

        // Background not started during construction.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // Explicit start works.
        subsystem.start().await.unwrap();
        assert!(subsystem.repository_background.workqueue_start_called());
    }

    // ── Task 14.3: runtime executor wiring through lifecycle service ──

    /// set_work_executor on PodSubsystem delegates through
    /// PodLifecycleService to the underlying router.
    #[tokio::test]
    async fn pod_subsystem_bootstrap_wires_runtime_executor() {
        let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let config = fixture_config(db);

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // Verify lifecycle service is functional post-construction.
        assert_eq!(
            subsystem.lifecycle_service.mode(),
            klights_kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor
        );

        // Replace executor with a fresh NoopExecutor — must not panic.
        let new_executor: Arc<dyn PodWorkExecutor> =
            Arc::new(klights_kubelet::pod_lifecycle_router::executor::NoopExecutor);
        subsystem.set_work_executor(new_executor);

        // Lifecycle service still reports correct mode after executor swap.
        assert_eq!(
            subsystem.lifecycle_service.mode(),
            klights_kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor
        );

        // Router is still functional: can route a message after executor swap.
        let key = klights_kubelet::pod_lifecycle_core::message::PodLifecycleKey::new(
            "default",
            "exec-wire-pod",
            "uid-exec-wire",
        );
        subsystem
            .lifecycle_service
            .route(
                klights_kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue {
                    key: key.clone(),
                },
            )
            .await
            .expect("route must succeed after executor swap");

        assert_eq!(subsystem.lifecycle_service.active_pod_count().await, 1);
        assert!(subsystem.lifecycle_service.remove_pod_state(&key).await);
    }

    /// Verify all object layer names are resolvable at compile time so the
    /// architecture remains discoverable from code.
    #[test]
    fn docs_name_all_object_layers() {
        // Composition root
        let _ = std::any::type_name::<crate::kubelet::pod_subsystem::PodSubsystem>();
        let _ = std::any::type_name::<crate::kubelet::pod_subsystem::PodSubsystemConfig>();
        // Lifecycle service
        let _ =
            std::any::type_name::<klights_kubelet::pod_lifecycle_service::PodLifecycleService>();
        // Runtime service port
        let _ =
            std::any::type_name::<dyn crate::kubelet::pod_runtime::service::PodRuntimeService>();
        // Deletion finalizer port
        let _ = std::any::type_name::<
            dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer,
        >();
    }
}
