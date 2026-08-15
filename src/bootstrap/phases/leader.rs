//! Phase 9: Leader-only tasks — CronJob scheduler, workqueue worker, GC.
//!
//! T2 step 2: controllers are gated by runtime lease acquisition instead
//! of a compile-time `leader_scheduler_mode` boolean. The injected
//! `Arc<dyn ControllerCoordination>` is backed by `RaftLeaderLease` on every leader-class
//! boot (always-on raft, T2 step 1). When the lease is held, controllers
//! run; when it is lost, the lease cancel token tears them down.

use std::sync::Arc;

use crate::KlightsConfig;
use anyhow::{Context as _, Result};
use klights_leader_api::{ControllerCoordination, ControllerScope};
use klights_supervisor::TaskSupervisor;
use tokio_util::sync::CancellationToken;

pub struct LeaderStart<'a> {
    pub config: &'a Arc<KlightsConfig>,
    /// T2 step 2: runtime leader lease instead of a compile-time bool.
    /// `None` for workers (no controllers). When `Some`, the start
    /// function attempts to acquire the lease; if acquisition fails
    /// (not the raft leader), controller startup is skipped cleanly.
    pub leader_coordination: Option<Arc<dyn ControllerCoordination>>,
    pub resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    pub leader_bootstrap_store: Arc<crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore>,
    pub watch_maintenance: Arc<dyn klights_cluster_store::ClusterWatchMaintenance>,
    pub positioned_watch: klights_watch::PositionedWatchService,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub task_supervisor: &'a Arc<TaskSupervisor>,
    pub dispatcher_for_worker: &'a Arc<klights_controllers::ControllerDispatcher>,
    pub dispatcher_for_cronjobs: &'a Arc<klights_controllers::ControllerDispatcher>,
    pub cronjob_store: &'a Arc<dyn klights_controllers::cronjob::CronJobStore>,
    pub pod_query: &'a Arc<dyn klights_pod_api::PodQuery>,
    pub pod_sandbox_gc_dirty_counter: &'a Arc<std::sync::atomic::AtomicUsize>,
    pub pod_scheduling: &'a Arc<dyn klights_pod_api::PodScheduling>,
    pub cri_for_shutdown: &'a Option<Arc<tokio::sync::Mutex<klights_kubelet::cri::CriClient>>>,
    pub datapath: &'a Arc<dyn klights_network_api::Datapath>,
    pub shutdown_token: CancellationToken,
}

#[derive(Clone)]
struct LeaderScopedTaskContext {
    config: Arc<KlightsConfig>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    leader_bootstrap_store: Arc<crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore>,
    watch_maintenance: Arc<dyn klights_cluster_store::ClusterWatchMaintenance>,
    positioned_watch: klights_watch::PositionedWatchService,
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    task_supervisor: Arc<TaskSupervisor>,
    dispatcher_for_worker: Arc<klights_controllers::ControllerDispatcher>,
    dispatcher_for_cronjobs: Arc<klights_controllers::ControllerDispatcher>,
    cronjob_store: Arc<dyn klights_controllers::cronjob::CronJobStore>,
    pod_query: Arc<dyn klights_pod_api::PodQuery>,
    sandbox_gc_dirty_counter: Arc<std::sync::atomic::AtomicUsize>,
    pod_scheduling: Arc<dyn klights_pod_api::PodScheduling>,
    cri_for_shutdown: Option<Arc<tokio::sync::Mutex<klights_kubelet::cri::CriClient>>>,
    datapath: Arc<dyn klights_network_api::Datapath>,
}

pub async fn start(args: LeaderStart<'_>) -> Result<()> {
    let LeaderStart {
        config,
        leader_coordination,
        resource_reads,
        leader_bootstrap_store,
        watch_maintenance,
        positioned_watch,
        pod_network_cache,
        pod_runtime_store,
        task_supervisor,
        dispatcher_for_worker,
        dispatcher_for_cronjobs,
        cronjob_store,
        pod_query,
        pod_sandbox_gc_dirty_counter,
        pod_scheduling,
        cri_for_shutdown,
        datapath,
        shutdown_token,
    } = args;

    let Some(coordination) = leader_coordination else {
        tracing::debug!("no leader election injected — skipping controller startup");
        return Ok(());
    };

    // `pod_query` is already a focused trait object; the long-lived
    // leader-scoped task context (cloned into the lease-scoped background
    // closure) only carries focused capability fields.
    let pod_query: Arc<dyn klights_pod_api::PodQuery> = pod_query.clone();
    let sandbox_gc_dirty_counter = pod_sandbox_gc_dirty_counter.clone();
    let leader_context = LeaderScopedTaskContext {
        config: config.clone(),
        resource_reads,
        leader_bootstrap_store,
        watch_maintenance,
        positioned_watch,
        pod_network_cache,
        pod_runtime_store,
        task_supervisor: task_supervisor.clone(),
        dispatcher_for_worker: dispatcher_for_worker.clone(),
        dispatcher_for_cronjobs: dispatcher_for_cronjobs.clone(),
        cronjob_store: cronjob_store.clone(),
        pod_query,
        sandbox_gc_dirty_counter,
        pod_scheduling: pod_scheduling.clone(),
        cri_for_shutdown: cri_for_shutdown.clone(),
        datapath: datapath.clone(),
    };

    task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "runtime_leader_controller_lease_loop",
            async move {
                let scoped_coordination = coordination.clone();
                klights_controllers::run_under_lease(
                    coordination,
                    ControllerScope::Cluster,
                    shutdown_token,
                    move |_scope, lease, lease_cancel| {
                        let leader_context = leader_context.clone();
                        let coordination = scoped_coordination.clone();
                        async move {
                            if let Err(err) = start_leader_scoped_tasks(
                                leader_context,
                                coordination,
                                lease,
                                lease_cancel,
                            )
                            .await
                            {
                                tracing::warn!("leader-scoped controller startup failed: {err:#}");
                            }
                        }
                    },
                )
                .await;
            },
        )
        .await?;
    tracing::info!("Leader controller lease loop started");

    Ok(())
}

async fn start_leader_scoped_tasks(
    context: LeaderScopedTaskContext,
    coordination: Arc<dyn ControllerCoordination>,
    lease: klights_leader_api::ControllerLease,
    lease_cancel: CancellationToken,
) -> Result<()> {
    let LeaderScopedTaskContext {
        config,
        resource_reads,
        leader_bootstrap_store,
        watch_maintenance,
        positioned_watch,
        pod_network_cache,
        pod_runtime_store,
        task_supervisor,
        dispatcher_for_worker,
        dispatcher_for_cronjobs,
        cronjob_store,
        pod_query,
        sandbox_gc_dirty_counter,
        pod_scheduling,
        cri_for_shutdown,
        datapath,
    } = context;

    tracing::info!("Acquired leader lease");

    klights_controllers::kube_service::bootstrap_leader_kubernetes_service(
        leader_bootstrap_store.as_ref(),
        &config.service_cidr,
        config.tls_port,
        datapath.as_ref(),
    )
    .await
    .context("reconcile kubernetes Service endpoint for active leader")?;

    let scheduler =
        crate::bootstrap::controller_adapters::cronjob_scheduler_adapter::new_leader_scheduler(
            resource_reads,
            cronjob_store,
            positioned_watch.clone(),
            dispatcher_for_cronjobs,
            task_supervisor.clone(),
        );
    if let Err(e) = scheduler.startup_walk().await {
        tracing::warn!("CronJob scheduler startup walk failed: {:#}", e);
    }
    let wls = scheduler.clone();
    let wlc = lease_cancel.child_token();
    let cron_coordination = coordination.clone();
    let cron_lease = lease.clone();
    if let Err(err) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "runtime_cronjob_scheduler_watch",
            klights_leader_api::scope_controller_lease(cron_coordination, cron_lease, async move {
                wls.run_watch_loop(wlc).await;
            }),
        )
        .await
    {
        tracing::warn!("Failed to spawn CronJob scheduler: {}", err);
    }

    let d = dispatcher_for_worker;
    let c = lease_cancel.child_token();
    let worker_coordination = coordination.clone();
    let worker_lease = lease.clone();
    if let Err(e) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "runtime_controller_workqueue_worker",
            klights_leader_api::scope_controller_lease(
                worker_coordination,
                worker_lease,
                async move {
                    d.run_worker_pool(
                        klights_controllers::ControllerDispatcher::DEFAULT_WORKQUEUE_WORKERS,
                        c,
                    )
                    .await;
                },
            ),
        )
        .await
    {
        tracing::warn!("Failed to spawn workqueue worker: {}", e);
    }
    tracing::info!(
        workers = klights_controllers::ControllerDispatcher::DEFAULT_WORKQUEUE_WORKERS,
        "Controller workqueue worker pool started"
    );

    let scheduler_runtime: Arc<dyn klights_controllers::scheduler::SchedulerRuntime> = Arc::new(
        crate::bootstrap::scheduler_adapter::LeaderSchedulerRuntime::new(
            positioned_watch,
            pod_scheduling,
        ),
    );
    let scheduler_cancel = lease_cancel.child_token();
    let scheduler_coordination = coordination.clone();
    let scheduler_lease = lease.clone();
    if let Err(e) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "runtime_scheduler_controller",
            klights_leader_api::scope_controller_lease(
                scheduler_coordination,
                scheduler_lease,
                async move {
                    klights_controllers::scheduler::run_scheduler_watch(
                        scheduler_runtime,
                        scheduler_cancel,
                    )
                    .await;
                },
            ),
        )
        .await
    {
        tracing::warn!("Failed to spawn scheduler controller: {}", e);
    }
    tracing::info!("Scheduler controller started");

    let sandbox_maintenance = cri_for_shutdown.map(|cri_arc| {
        Arc::new(klights_kubelet::sandbox_gc::SandboxGc::new(
            pod_network_cache,
            pod_runtime_store,
            cri_arc.clone(),
            pod_query,
            config.containerd_namespace.clone(),
            sandbox_gc_dirty_counter,
            klights_supervisor::FileProcessExecutor::from_supervisor(task_supervisor.as_ref()),
        ))
    });
    let maintenance = crate::bootstrap::maintenance::MaintenanceRunner::new(
        watch_maintenance,
        sandbox_maintenance,
        task_supervisor.clone(),
        config.gc_interval,
        config.max_watch_events,
    )?;

    let cancel = lease_cancel.child_token();
    let maintenance_coordination = coordination;
    let maintenance_lease = lease;
    if let Err(e) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "runtime_root_maintenance",
            klights_leader_api::scope_controller_lease(
                maintenance_coordination,
                maintenance_lease,
                async move {
                    maintenance.run(cancel).await;
                },
            ),
        )
        .await
    {
        tracing::warn!("Failed to spawn root maintenance: {}", e);
    }

    Ok(())
}
