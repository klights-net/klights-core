//! Pod subsystem composition root. Owns the Pod repository, lifecycle
//! router, and background services. Constructed once per process by
//! bootstrap, then shared by consumers behind narrow trait references.

use std::sync::Arc;

use anyhow::Result;

use crate::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
use crate::pod_lifecycle_actor::registry::PodLifecycleRegistry;
use crate::pod_lifecycle_router::executor::{PodLifecycleExecutor, PodWorkExecutor};
use crate::pod_lifecycle_router::{PodLifecycleRouteMode, PodLifecycleRouter};
use crate::pod_lifecycle_service::PodLifecycleService;
use crate::pod_repository::background::PodRepositoryBackground;
use crate::probe_manager::ProbeManager;
use crate::runtime::PodRuntimeService;
use crate::runtime::events::PodEventSink;
use crate::runtime::service::RealPodRuntimeService;
use crate::runtime::store::{PodRuntimeStore, PodSlotAdmission};
use klights_supervisor::TaskSupervisor;

struct LifecycleWallClock {
    runtime_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
}

impl klights_supervisor::WallClock for LifecycleWallClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(self.runtime_clock.now_ms().max(0) as u64)
    }
}

/// Wiring inputs for PodSubsystem construction.
pub struct PodSubsystemConfig {
    pub pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pub pod_network_assignment: Arc<dyn crate::pod_repository::PodNetworkAssignmentQuery>,
    pub pod_status_writer: Arc<dyn crate::pod_repository::status::PodStatusWriter>,
    pub pod_repository_background: PodRepositoryBackground,
    pub pod_deletion_finalizer: Arc<dyn crate::pod_deletion_finalizer::PodDeletionFinalizer>,
    pub supervisor: Arc<TaskSupervisor>,
    pub outbox: Option<Arc<crate::outbox::Outbox>>,
    pub resource_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    pub projected_tokens: Option<Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>>,
    pub node_name: String,
    pub service_cidr: String,
    pub lifecycle_concurrency: PodLifecycleConcurrencyConfig,
    pub pod_actor_idle_grace: std::time::Duration,
    pub sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs,
    pub node_capacity: crate::node_capacity::NodeCapacity,
    pub paths: crate::runtime_paths::KubeletRuntimePaths,
    pub lifecycle_route_mode: PodLifecycleRouteMode,
    pub cri: Option<crate::cri::SharedCriClient>,
    pub registry_proxy: Option<crate::registry_proxy::ContainerdRegistryProxyConfigurator>,
    pub containerd_ns: String,
    pub lifecycle_tx: tokio::sync::mpsc::Sender<crate::lifecycle::LifecycleCommand>,
    pub datapath: Option<Arc<dyn klights_network_api::Datapath>>,
    pub service_router: Option<Arc<dyn klights_network_api::ServiceRouter>>,
    pub runtime_store: Arc<dyn PodRuntimeStore>,
    pub wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    pub slot_admission: Arc<dyn PodSlotAdmission>,
    pub event_sink: Arc<dyn PodEventSink>,
}

/// Composition root: owns the Pod repository, lifecycle router,
/// background services, and the private runtime service. Background work
/// starts in explicit `start()`.
pub struct PodSubsystem {
    repository_background: PodRepositoryBackground,
    lifecycle_router: Arc<PodLifecycleRouter>,
    lifecycle_service: PodLifecycleService,
    runtime: Arc<dyn PodRuntimeService>,
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
        let pod_query = config.pod_query;
        let pod_network_assignment = config.pod_network_assignment;
        let pod_status_writer = config.pod_status_writer;
        let repository_background = config.pod_repository_background;
        let deletion_finalizer = config.pod_deletion_finalizer;

        let lifecycle_wall_clock: Arc<dyn klights_supervisor::WallClock> =
            Arc::new(LifecycleWallClock {
                runtime_clock: config.wall_clock.clone(),
            });
        let registry = Arc::new(
            PodLifecycleRegistry::new_with_idle_grace(
                config.supervisor.clone(),
                lifecycle_concurrency.clone(),
                Arc::new(std::sync::Mutex::new(Arc::new(
                    crate::pod_lifecycle_router::executor::NoopExecutor,
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
        let probe_cri_runtime = config.cri.clone().map(|cri| {
            Arc::new(
                crate::runtime::cri::SharedCriRuntime::new_with_registry_proxy(
                    cri,
                    registry_proxy.clone(),
                ),
            ) as Arc<dyn crate::runtime::cri::CriRuntime>
        });
        let probe_manager = Arc::new(ProbeManager::new_with_lifecycle(
            supervisor.clone(),
            pod_query.clone(),
            probe_cri_runtime,
            lifecycle_tx,
            config.wall_clock.clone(),
        ));
        let runtime = Self::build_runtime_service(
            supervisor,
            pod_query,
            pod_network_assignment,
            pod_status_writer,
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
            config.runtime_store,
            config.wall_clock,
            config.slot_admission,
            config.event_sink,
            deletion_finalizer,
        )?;

        Ok(Self {
            repository_background,
            lifecycle_router,
            lifecycle_service,
            runtime,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_runtime_service(
        supervisor: Arc<TaskSupervisor>,
        pod_query: Arc<dyn klights_pod_api::PodQuery>,
        pod_network_assignment: Arc<dyn crate::pod_repository::PodNetworkAssignmentQuery>,
        pod_status_writer: Arc<dyn crate::pod_repository::status::PodStatusWriter>,
        cri: Option<crate::cri::SharedCriClient>,
        registry_proxy: Option<crate::registry_proxy::ContainerdRegistryProxyConfigurator>,
        containerd_ns: String,
        probe_manager: Arc<ProbeManager>,
        datapath: Option<Arc<dyn klights_network_api::Datapath>>,
        service_router: Option<Arc<dyn klights_network_api::ServiceRouter>>,
        node_name: String,
        service_cidr: String,
        sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs,
        node_capacity: crate::node_capacity::NodeCapacity,
        paths: crate::runtime_paths::KubeletRuntimePaths,
        resource_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        projected_tokens: Option<Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>>,
        runtime_store: Arc<dyn PodRuntimeStore>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
        slot_admission: Arc<dyn PodSlotAdmission>,
        event_sink: Arc<dyn PodEventSink>,
        deletion_finalizer: Arc<dyn crate::pod_deletion_finalizer::PodDeletionFinalizer>,
    ) -> Result<Arc<dyn PodRuntimeService>> {
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
            crate::runtime::cri::SharedCriRuntime::new_with_registry_proxy(
                cri.clone(),
                registry_proxy,
            ),
        );
        let pod_reader: Arc<dyn klights_pod_api::PodQuery> = pod_query.clone();
        let hostports: Arc<dyn crate::runtime::hostports::HostPortRuntime> =
            Arc::new(crate::runtime::hostports::RealHostPortRuntime::new(
                service_router,
                pod_reader,
                node_name.clone(),
            ));
        Ok(Arc::new(RealPodRuntimeService::new(
            cri_runtime.clone(),
            cri_runtime.clone(),
            Arc::new(crate::runtime::network::RealPodNetworkRuntime::new(
                datapath,
                pod_network_assignment.clone(),
                runtime_store.clone(),
            )),
            runtime_store,
            wall_clock,
            slot_admission,
            pod_query,
            pod_status_writer,
            Arc::new(crate::runtime::filesystem::RealPodFilesystem::new(
                supervisor.clone(),
                containerd_ns.clone(),
                node_name.clone(),
                paths.clone(),
            )),
            Arc::new(crate::runtime::volumes::RealPodVolumeRuntime::new(
                Arc::new(crate::volume_sources::LocalCacheVolumeSourceReader::new(
                    volume_resource_query.clone(),
                    volume_projected_tokens,
                )),
                containerd_ns.clone(),
                supervisor.clone(),
                node_capacity,
                paths.clone(),
            )),
            Arc::new(crate::runtime::probes::RealProbeRuntime::new(probe_manager)),
            hostports,
            event_sink,
            Arc::new(crate::runtime::hooks::RealPodHookRuntime::new(
                cri_runtime.clone(),
                supervisor.clone(),
            )),
            Arc::new(crate::pod_env::LeaderApiEnvSourceReader::new(
                volume_resource_query,
            )),
            deletion_finalizer,
            supervisor,
            crate::runtime::service::RuntimeConfig {
                node_name,
                service_cidr,
                containerd_namespace: containerd_ns,
                sandbox_inputs,
                node_capacity,
                paths,
            },
        )))
    }

    #[cfg(test)]
    pub fn runtime_service(&self) -> Arc<dyn PodRuntimeService> {
        self.runtime.clone()
    }

    pub fn lifecycle_router(&self) -> &Arc<PodLifecycleRouter> {
        &self.lifecycle_router
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
