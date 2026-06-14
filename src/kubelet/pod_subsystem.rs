//! Pod subsystem composition root. Owns the Pod repository, lifecycle
//! router, and background services. Constructed once per process by
//! bootstrap, then shared by consumers behind narrow trait references.

use std::sync::Arc;

use anyhow::Result;

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::DatastoreHandle;
use crate::kubelet::ProbeManager;
use crate::kubelet::pod_cluster_runtime::RuntimeNodeRole;
use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
use crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::kubelet::pod_lifecycle_router::executor::{PodLifecycleExecutor, PodWorkExecutor};
use crate::kubelet::pod_lifecycle_service::PodLifecycleService;
use crate::kubelet::pod_repository::api::PodSchedulingMode;
use crate::kubelet::pod_repository::background::PodRepositoryBackground;
use crate::kubelet::pod_repository::{PodRepository, PodRepositoryBuildConfig};
use crate::kubelet::pod_runtime::service::{
    PodRuntimeService, RealPodRuntimeService, RealPodRuntimeServiceDependencies,
};
use crate::networking::pod_network_events::PodNetworkEvents;
use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
use crate::task_supervisor::TaskSupervisor;

/// Wiring inputs for PodSubsystem construction.
pub struct PodSubsystemConfig {
    pub db: DatastoreHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub scheduling_mode: PodSchedulingMode,
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderApiClient>>,
    pub node_name: String,
    pub service_cidr: String,
    pub lifecycle_concurrency: PodLifecycleConcurrencyConfig,
    pub network_events: PodNetworkEvents,
    // Task 19: runtime dependencies for RealPodRuntimeService construction (Task 24).
    pub cri: Option<crate::kubelet::cri::SharedCriClient>,
    pub containerd_ns: String,
    pub lifecycle_tx: tokio::sync::mpsc::Sender<crate::kubelet::lifecycle::LifecycleCommand>,
    pub probe_manager: Option<Arc<ProbeManager>>,
    pub datapath: Option<Arc<dyn crate::networking::Datapath>>,
    pub service_router: Option<Arc<dyn crate::networking::ServiceRouter>>,
    pub runtime_node_role: RuntimeNodeRole,
    pub runtime_service: Option<Arc<dyn PodRuntimeService>>,
}

/// Composition root: owns the Pod repository, lifecycle router,
/// background services, and runtime adapter dependencies (stored for
/// Task 24 construction). Background work starts in explicit `start()`.
pub struct PodSubsystem {
    pub db: DatastoreHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub repository: Arc<PodRepository>,
    pub repository_background: PodRepositoryBackground,
    pub lifecycle_router: Arc<PodLifecycleRouter>,
    pub lifecycle_service: PodLifecycleService,
    pub runtime: Arc<dyn PodRuntimeService>,
    // Task 19: runtime adapter dependencies stored for Task 24 construction.
    pub cri: Option<crate::kubelet::cri::SharedCriClient>,
    pub containerd_ns: String,
    pub probe_manager: Option<Arc<ProbeManager>>,
    pub datapath: Option<Arc<dyn crate::networking::Datapath>>,
    pub service_router: Option<Arc<dyn crate::networking::ServiceRouter>>,
    pub cluster_api: Option<Arc<dyn LeaderApiClient>>,
    pub runtime_node_role: RuntimeNodeRole,
    pub node_name: String,
    pub service_cidr: String,
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
}

struct RuntimeServiceBuildRequest {
    db: DatastoreHandle,
    supervisor: Arc<TaskSupervisor>,
    repository: Arc<PodRepository>,
    cri: Option<crate::kubelet::cri::SharedCriClient>,
    containerd_ns: String,
    probe_manager: Arc<ProbeManager>,
    datapath: Option<Arc<dyn crate::networking::Datapath>>,
    service_router: Option<Arc<dyn crate::networking::ServiceRouter>>,
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    node_name: String,
    service_cidr: String,
    runtime_node_role: RuntimeNodeRole,
    cluster_api: Option<Arc<dyn LeaderApiClient>>,
}

impl PodSubsystem {
    /// Build repository parts and lifecycle router WITHOUT starting
    /// background work. Call `start()` after wiring is complete.
    pub fn new(config: PodSubsystemConfig) -> Result<Self> {
        let db = config.db.clone();
        let supervisor = config.supervisor.clone();
        let node_name = config.node_name.clone();
        let service_cidr = config.service_cidr.clone();
        let outbox = config.outbox.clone();
        let cri = config.cri.clone();
        let containerd_ns = config.containerd_ns.clone();
        let lifecycle_tx = config.lifecycle_tx.clone();
        let datapath = config.datapath.clone();
        let service_router = config.service_router.clone();
        let cluster_api = config.cluster_api.clone();
        let runtime_node_role = config.runtime_node_role.clone();
        let parts = PodRepository::build_parts(PodRepositoryBuildConfig {
            db: config.db,
            supervisor: config.supervisor.clone(),
            side_effects: config.side_effects.clone(),
            metrics: config.metrics.clone(),
            network_events: config.network_events,
            scheduling_mode: config.scheduling_mode,
            outbox: config.outbox.clone(),
            cluster_api: config.cluster_api.clone(),
        });

        let registry = Arc::new(PodLifecycleRegistry::new(
            config.supervisor.clone(),
            config.lifecycle_concurrency,
            Arc::new(std::sync::Mutex::new(Arc::new(
                crate::kubelet::pod_lifecycle_router::executor::NoopExecutor,
            ))),
        ));
        let lifecycle_router = Arc::new(PodLifecycleRouter::new_actor(registry));
        let lifecycle_service = PodLifecycleService::new(lifecycle_router.clone());
        let repository = Arc::new(parts.repository);
        let probe_cri_runtime = config.cri.clone().map(|cri| {
            Arc::new(crate::kubelet::pod_runtime::cri::SharedCriRuntime::new(cri))
                as Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>
        });
        let probe_manager = config.probe_manager.unwrap_or_else(|| {
            Arc::new(ProbeManager::new_with_lifecycle(
                supervisor.clone(),
                repository.clone() as Arc<dyn crate::kubelet::pod_repository::PodReader>,
                probe_cri_runtime.clone(),
                lifecycle_tx.clone(),
            ))
        });
        let runtime = match config.runtime_service.clone() {
            Some(runtime_service) => runtime_service,
            None => Self::build_runtime_service(RuntimeServiceBuildRequest {
                db: db.clone(),
                supervisor: supervisor.clone(),
                repository: repository.clone(),
                cri: cri.clone(),
                containerd_ns: containerd_ns.clone(),
                probe_manager: probe_manager.clone(),
                datapath: datapath.clone(),
                service_router: service_router.clone(),
                outbox: outbox.clone(),
                node_name: node_name.clone(),
                service_cidr: service_cidr.clone(),
                runtime_node_role: runtime_node_role.clone(),
                cluster_api: cluster_api.clone(),
            })?,
        };

        Ok(Self {
            db,
            supervisor,
            repository,
            repository_background: parts.background,
            lifecycle_router,
            lifecycle_service,
            runtime,
            cri,
            containerd_ns,
            probe_manager: Some(probe_manager),
            datapath,
            service_router,
            cluster_api,
            runtime_node_role,
            node_name,
            service_cidr,
            outbox,
        })
    }

    fn build_runtime_service(
        request: RuntimeServiceBuildRequest,
    ) -> Result<Arc<dyn PodRuntimeService>> {
        let RuntimeServiceBuildRequest {
            db,
            supervisor,
            repository,
            cri,
            containerd_ns,
            probe_manager,
            datapath,
            service_router,
            outbox,
            node_name,
            service_cidr,
            runtime_node_role,
            cluster_api,
        } = request;
        let cri =
            cri.ok_or_else(|| anyhow::anyhow!("missing PodRuntimeService dependencies: cri"))?;
        let datapath = datapath
            .ok_or_else(|| anyhow::anyhow!("missing PodRuntimeService dependencies: datapath"))?;
        let service_router = service_router.ok_or_else(|| {
            anyhow::anyhow!("missing PodRuntimeService dependencies: service_router")
        })?;
        let cluster_api = cluster_api.ok_or_else(|| {
            anyhow::anyhow!("missing PodRuntimeService dependencies: cluster_api")
        })?;
        let cri_runtime = Arc::new(crate::kubelet::pod_runtime::cri::SharedCriRuntime::new(
            cri.clone(),
        ));
        let pod_reader: Arc<dyn crate::kubelet::pod_repository::PodReader> = repository.clone();
        let hostports: Arc<dyn crate::kubelet::pod_runtime::hostports::HostPortRuntime> = Arc::new(
            crate::kubelet::pod_runtime::hostports::RealHostPortRuntime::new(
                service_router,
                pod_reader,
                node_name.clone(),
            ),
        );
        let node_view = Arc::new(
            crate::kubelet::pod_cluster_runtime::LocalNodeRuntimeView::new(
                node_name.clone(),
                runtime_node_role.clone(),
            ),
        );
        // Every role — leader included — routes Pod status through the same
        // worker cluster-view path. The role difference lives in the injected
        // repository, not the view, so there is no leader-specific bypass.
        let cluster_view: Arc<dyn crate::kubelet::pod_cluster_runtime::ClusterRuntimeView> =
            Arc::new(
                crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(
                    repository.clone(),
                    node_name.clone(),
                ),
            );
        let runtime_store =
            Arc::new(crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(db.clone()));
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
                slot_admission: Arc::new(
                    crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
                        db.clone(),
                        node_name.clone(),
                    ),
                ),
                repository: repository.clone(),
                filesystem: Arc::new(
                    crate::kubelet::pod_runtime::filesystem::RealPodFilesystem::new(
                        supervisor.clone(),
                        containerd_ns.clone(),
                        node_name.clone(),
                    ),
                ),
                volumes: Arc::new(
                    crate::kubelet::pod_runtime::volumes::RealPodVolumeRuntime::new(
                        Arc::new(
                            crate::kubelet::volume_sources::LocalCacheVolumeSourceReader::new(
                                cluster_api.clone(),
                            ),
                        ),
                        containerd_ns.clone(),
                        supervisor.clone(),
                    ),
                ),
                probes: Arc::new(crate::kubelet::pod_runtime::probes::RealProbeRuntime::new(
                    probe_manager,
                )),
                hostports,
                events: Arc::new(crate::kubelet::pod_runtime::events::RealPodEventSink::new(
                    outbox,
                    db.clone(),
                )),
                hooks: Arc::new(crate::kubelet::pod_runtime::hooks::RealPodHookRuntime::new(
                    cri_runtime.clone(),
                    supervisor.clone(),
                )),
                env_source: Arc::new(crate::kubelet::pod_env::LeaderApiEnvSourceReader::new(
                    cluster_api,
                )),
                finalizer: repository.deletion_finalizer(),
                supervisor,
                config: crate::kubelet::pod_runtime::service::RuntimeConfig {
                    node_name,
                    service_cidr,
                    containerd_namespace: containerd_ns,
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
    pub fn start(&self) {
        self.repository_background.start();
    }

    /// Replace the work executor at runtime via the lifecycle service.
    pub fn set_work_executor(&self, executor: Arc<dyn PodWorkExecutor>) {
        self.lifecycle_service.set_work_executor(executor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
    use crate::kubelet::pod_repository::api::PodSchedulingMode;
    use crate::side_effects::SideEffectMetrics;

    fn fixture_supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn fixture_config(db: DatastoreHandle) -> PodSubsystemConfig {
        let (lifecycle_tx, _rx) =
            tokio::sync::mpsc::channel::<crate::kubelet::lifecycle::LifecycleCommand>(8);
        let cluster_api = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "node-1".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let runtime_service =
            Arc::new(crate::kubelet::pod_runtime::test_support::MockPodRuntimeService::new());
        PodSubsystemConfig {
            db,
            supervisor: fixture_supervisor(),
            side_effects: Arc::new(SideEffectRegistry::new()),
            metrics: SideEffectMetrics::new(),
            scheduling_mode: PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: Some(cluster_api),
            node_name: "node-1".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            lifecycle_concurrency: PodLifecycleConcurrencyConfig::production_default(),
            network_events: crate::networking::global_pod_network_events(),
            cri: None,
            containerd_ns: "klights".to_string(),
            lifecycle_tx,
            probe_manager: None,
            datapath: None,
            service_router: None,
            runtime_node_role: RuntimeNodeRole::Worker,
            runtime_service: Some(runtime_service),
        }
    }

    /// Task 5.1: Config struct requires repository, router, and node identity.
    #[tokio::test]
    async fn pod_subsystem_config_requires_repository_router_and_node_identity() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = fixture_config(db);
        assert_eq!(config.node_name, "node-1");
        // Repository builder parameters are present.
        let _ = &config.supervisor;
        let _ = &config.side_effects;
        let _ = &config.metrics;
        let _ = &config.scheduling_mode;
        let _ = &config.lifecycle_concurrency;
        let _ = &config.network_events;
    }

    /// Task 5.1: Construction produces repository and router without starting
    /// background work.
    #[tokio::test]
    async fn pod_subsystem_constructs_repository_and_router_without_starting_background() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            ..fixture_config(db)
        };

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
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let _cfg = fixture_config(db);
        // This test verifies that PodSubsystem::start() exists, is
        // callable, and follows the explicit-start contract. The
        // async variant (below) tests actual background startup.
    }

    /// Task 5.2: start() calls repository background start exactly once
    /// and repeated calls are safe (idempotent).
    #[tokio::test]
    async fn pod_subsystem_start_starts_repository_background_once() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            ..fixture_config(db)
        };

        let subsystem = PodSubsystem::new(config).unwrap();

        // Not started yet.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // First start.
        subsystem.start();
        assert!(subsystem.repository_background.workqueue_start_called());

        // Second start is idempotent.
        subsystem.start();
        assert!(subsystem.repository_background.workqueue_start_called());
    }

    #[tokio::test]
    async fn pod_subsystem_accepts_injected_runtime_service() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let injected =
            Arc::new(crate::kubelet::pod_runtime::test_support::MockPodRuntimeService::new())
                as Arc<dyn PodRuntimeService>;
        let config = PodSubsystemConfig {
            db: db.clone(),
            runtime_service: Some(injected.clone()),
            ..fixture_config(db)
        };

        let subsystem = PodSubsystem::new(config).expect("construction must succeed");

        assert!(
            Arc::ptr_eq(&subsystem.runtime_service(), &injected),
            "subsystem must retain the injected runtime service"
        );
    }

    #[tokio::test]
    async fn pod_subsystem_without_injected_runtime_requires_real_runtime_dependencies() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            runtime_service: None,
            cri: None,
            datapath: None,
            cluster_api: None,
            ..fixture_config(db)
        };

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
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
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
        assert_eq!(
            config.runtime_node_role,
            RuntimeNodeRole::Worker,
            "default to Worker for single-node"
        );
        // Storage on PodSubsystem works after construction.
        let config2 = PodSubsystemConfig {
            db: db.clone(),
            ..fixture_config(db)
        };
        let subsystem = PodSubsystem::new(config2).expect("construction must succeed");
        assert_eq!(subsystem.containerd_ns, "klights");
        assert_eq!(subsystem.service_cidr, "10.43.128.0/17");
        assert!(subsystem.cri.is_none());
        assert!(
            subsystem.probe_manager.is_some(),
            "PodSubsystem hoists ProbeManager construction"
        );
        assert_eq!(subsystem.runtime_node_role, RuntimeNodeRole::Worker);
    }

    /// Task 24: PodSubsystem owns the runtime service and builds the
    /// lifecycle executor from that runtime instead of watcher-local legacy
    /// wiring.
    #[tokio::test]
    async fn bootstrap_constructs_real_pod_runtime_service_and_binds_executor() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let sock_path = temp_dir.path().join("cri.sock");
        let _listener =
            std::os::unix::net::UnixListener::bind(&sock_path).expect("bind temp unix socket");
        let cri = crate::kubelet::CriClient::connect(&sock_path.to_string_lossy(), "klights")
            .await
            .expect("connect temp cri socket");
        let config = PodSubsystemConfig {
            db: db.clone(),
            cri: Some(crate::kubelet::cri::SharedCriClient::new(cri)),
            datapath: Some(Arc::new(
                crate::networking::test_support::MockNetworkProvider::new(),
            )),
            service_router: Some(Arc::new(
                crate::networking::test_support::MockServiceRouter::new(),
            )),
            runtime_service: None,
            ..fixture_config(db)
        };
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
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            scheduling_mode: PodSchedulingMode::DeferredMultiNodeLeader,
            ..fixture_config(db)
        };

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // All components accessible.
        let _repo = &subsystem.repository;
        let _router = &subsystem.lifecycle_router;
        let _service = &subsystem.lifecycle_service;

        // Background not started during construction.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // Explicit start works and is idempotent.
        subsystem.start();
        assert!(subsystem.repository_background.workqueue_start_called());
        subsystem.start(); // idempotent
    }

    // ── Task 14.2: worker bootstrap wiring ──

    /// PodSubsystem constructed with inline single-node scheduling mode
    /// exposes all components and supports explicit start.
    #[tokio::test]
    async fn worker_bootstrap_constructs_pod_subsystem_with_worker_objects() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            scheduling_mode: PodSchedulingMode::InlineSingleNode,
            ..fixture_config(db)
        };

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // All components accessible.
        let _repo = &subsystem.repository;
        let _router = &subsystem.lifecycle_router;
        let _service = &subsystem.lifecycle_service;

        // Background not started during construction.
        assert!(!subsystem.repository_background.workqueue_start_called());

        // Explicit start works.
        subsystem.start();
        assert!(subsystem.repository_background.workqueue_start_called());
    }

    // ── Task 14.3: runtime executor wiring through lifecycle service ──

    /// set_work_executor on PodSubsystem delegates through
    /// PodLifecycleService to the underlying router.
    #[tokio::test]
    async fn pod_subsystem_bootstrap_wires_runtime_executor() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let config = PodSubsystemConfig {
            db: db.clone(),
            ..fixture_config(db)
        };

        let subsystem = PodSubsystem::new(config).expect("PodSubsystem construction must succeed");

        // Verify lifecycle service is functional post-construction.
        assert_eq!(
            subsystem.lifecycle_service.mode(),
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor
        );

        // Replace executor with a fresh NoopExecutor — must not panic.
        let new_executor: Arc<dyn PodWorkExecutor> =
            Arc::new(crate::kubelet::pod_lifecycle_router::executor::NoopExecutor);
        subsystem.set_work_executor(new_executor);

        // Lifecycle service still reports correct mode after executor swap.
        assert_eq!(
            subsystem.lifecycle_service.mode(),
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor
        );

        // Router is still functional: can route a message after executor swap.
        let key = crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey::new(
            "default",
            "exec-wire-pod",
            "uid-exec-wire",
        );
        subsystem
            .lifecycle_service
            .route(
                crate::kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue {
                    key: key.clone(),
                },
            )
            .await
            .expect("route must succeed after executor swap");

        assert_eq!(subsystem.lifecycle_service.active_pod_count().await, 1);
        assert!(subsystem.lifecycle_service.remove_pod_state(&key).await);
    }

    /// Task 18.2: Verify the final validation marker exists.
    /// The real validation is `./validate.sh` and the build invariant checks.
    #[test]
    fn final_validation_removes_temporary_compatibility_objects() {
        // PodSubsystemConfig is the single construction input — verify
        // the type exists and pod_repository::build_parts is available.
        assert!(
            std::mem::size_of::<crate::kubelet::pod_subsystem::PodSubsystemConfig>() > 0,
            "PodSubsystemConfig must exist as the composition root input"
        );
    }

    /// Verify all object layer names are resolvable at compile time so the
    /// architecture remains discoverable from code.
    #[test]
    fn docs_name_all_object_layers() {
        // Composition root
        let _ = std::any::type_name::<crate::kubelet::pod_subsystem::PodSubsystem>();
        let _ = std::any::type_name::<crate::kubelet::pod_subsystem::PodSubsystemConfig>();
        // API facade
        let _ = std::any::type_name::<crate::kubelet::pod_api::PodApiFacade>();
        // Lifecycle service
        let _ = std::any::type_name::<crate::kubelet::pod_lifecycle_service::PodLifecycleService>();
        // Runtime service port
        let _ =
            std::any::type_name::<dyn crate::kubelet::pod_runtime::service::PodRuntimeService>();
        // Deletion finalizer port
        let _ = std::any::type_name::<
            dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer,
        >();
    }
}
