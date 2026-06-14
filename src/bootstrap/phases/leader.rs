//! Phase 9: Leader-only tasks — CronJob scheduler, workqueue worker, GC.
//!
//! T2 step 2: controllers are gated by runtime lease acquisition instead
//! of a compile-time `leader_scheduler_mode` boolean. The injected
//! `Arc<dyn LeaderElection>` is `RaftLeaderLease` on every leader-class
//! boot (always-on raft, T2 step 1). When the lease is held, controllers
//! run; when it is lost, the lease cancel token tears them down.

use std::sync::Arc;

use crate::KlightsConfig;
use crate::datastore::DatastoreHandle;
use crate::leader_election::{LeaderElection, LeaderScope};
use crate::task_supervisor::TaskSupervisor;
use anyhow::Result;
use tokio_util::sync::CancellationToken;

pub struct LeaderStart<'a> {
    pub config: &'a Arc<KlightsConfig>,
    /// T2 step 2: runtime leader lease instead of a compile-time bool.
    /// `None` for workers (no controllers). When `Some`, the start
    /// function attempts to acquire the lease; if acquisition fails
    /// (not the raft leader), controller startup is skipped cleanly.
    pub leader_election: Option<Arc<dyn LeaderElection>>,
    pub db_handle: &'a DatastoreHandle,
    pub task_supervisor: &'a Arc<TaskSupervisor>,
    pub dispatcher_for_worker: &'a Arc<crate::controller_dispatcher::ControllerDispatcher>,
    pub dispatcher_for_cronjobs: &'a Arc<crate::controller_dispatcher::ControllerDispatcher>,
    pub pod_repository: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
    pub scheduler_state: &'a Arc<crate::api::AppState>,
    pub cri_for_shutdown: &'a Option<Arc<tokio::sync::Mutex<crate::kubelet::CriClient>>>,
    pub is_leader_rx: tokio::sync::watch::Receiver<bool>,
    pub shutdown_token: CancellationToken,
}

#[derive(Clone)]
struct LeaderScopedTaskContext {
    config: Arc<KlightsConfig>,
    db_handle: DatastoreHandle,
    task_supervisor: Arc<TaskSupervisor>,
    dispatcher_for_worker: Arc<crate::controller_dispatcher::ControllerDispatcher>,
    dispatcher_for_cronjobs: Arc<crate::controller_dispatcher::ControllerDispatcher>,
    pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    scheduler_state: Arc<crate::api::AppState>,
    cri_for_shutdown: Option<Arc<tokio::sync::Mutex<crate::kubelet::CriClient>>>,
}

pub async fn start(args: LeaderStart<'_>) -> Result<()> {
    let LeaderStart {
        config,
        leader_election,
        db_handle,
        task_supervisor,
        dispatcher_for_worker,
        dispatcher_for_cronjobs,
        pod_repository,
        scheduler_state,
        cri_for_shutdown,
        is_leader_rx,
        shutdown_token,
    } = args;

    let Some(election) = leader_election else {
        tracing::debug!("no leader election injected — skipping controller startup");
        return Ok(());
    };

    let leader_context = LeaderScopedTaskContext {
        config: config.clone(),
        db_handle: db_handle.clone(),
        task_supervisor: task_supervisor.clone(),
        dispatcher_for_worker: dispatcher_for_worker.clone(),
        dispatcher_for_cronjobs: dispatcher_for_cronjobs.clone(),
        pod_repository: pod_repository.clone(),
        scheduler_state: scheduler_state.clone(),
        cri_for_shutdown: cri_for_shutdown.clone(),
    };

    task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_leader_controller_lease_loop",
            async move {
                crate::leader_election::run_under_lease(
                    election,
                    LeaderScope::Cluster,
                    is_leader_rx,
                    shutdown_token,
                    move |_scope, lease_cancel| {
                        let leader_context = leader_context.clone();
                        async move {
                            if let Err(err) =
                                start_leader_scoped_tasks(leader_context, lease_cancel).await
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
    lease_cancel: CancellationToken,
) -> Result<()> {
    let LeaderScopedTaskContext {
        config,
        db_handle,
        task_supervisor,
        dispatcher_for_worker,
        dispatcher_for_cronjobs,
        pod_repository,
        scheduler_state,
        cri_for_shutdown,
    } = context;

    tracing::info!("Acquired leader lease");
    use crate::{controllers, gc};

    let scheduler = controllers::cronjob_scheduler::CronJobScheduler::new(
        db_handle.clone(),
        dispatcher_for_cronjobs,
        task_supervisor.clone(),
    );
    if let Err(e) = scheduler.startup_walk().await {
        tracing::warn!("CronJob scheduler startup walk failed: {:#}", e);
    }
    let wls = scheduler.clone();
    let wlc = lease_cancel.child_token();
    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_cronjob_scheduler_watch",
            async move {
                wls.run_watch_loop(wlc).await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn CronJob scheduler: {}", err);
    }

    let d = dispatcher_for_worker;
    let dhw = db_handle.clone();
    let nn = config.node_name.clone();
    let c = lease_cancel.child_token();
    if let Err(e) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_controller_workqueue_worker",
            async move {
                d.run_worker(dhw, nn, c).await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn workqueue worker: {}", e);
    }
    tracing::info!("Controller workqueue worker started");

    let scheduler_state = scheduler_state.clone();
    let scheduler_cancel = lease_cancel.child_token();
    if let Err(e) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_scheduler_controller",
            async move {
                crate::controllers::scheduler::run_scheduler_watch(
                    scheduler_state,
                    scheduler_cancel,
                )
                .await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn scheduler controller: {}", e);
    }
    tracing::info!("Scheduler controller started");

    let gc_interval = std::env::var("KLIGHTS_GC_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30);
    let mut sched = gc::GcScheduler::new(std::time::Duration::from_secs(gc_interval));
    if let Some(cri_arc) = cri_for_shutdown {
        sched.register(Arc::new(gc::sandbox_gc::SandboxGc::new(
            db_handle.clone(),
            cri_arc.clone(),
            pod_repository.clone(),
            config.containerd_namespace.clone(),
            pod_repository.sandbox_gc_dirty_counter(),
        )));
    }
    sched.register(Arc::new(gc::watch_events_gc::WatchEventsGc::new(
        db_handle.clone(),
    )));

    // The global GC is intentionally hourly, not part of the 30s operational
    // GC cadence: on an idle cluster this adds at most one short
    // gc_applied_outbox transaction per hour. It currently runs only
    // twelve-hour applied_outbox idempotency-ledger pruning.
    let mut global_gc = gc::GcScheduler::new(std::time::Duration::from_secs(
        gc::applied_outbox_gc::APPLIED_OUTBOX_GC_INTERVAL_SECS,
    ));
    global_gc.register(Arc::new(gc::applied_outbox_gc::AppliedOutboxGc::new(
        db_handle.clone(),
    )));

    let cancel = lease_cancel.child_token();
    let ts = task_supervisor.clone();
    if let Err(e) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_gc_scheduler",
            async move {
                sched.run(ts, cancel).await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn GC scheduler: {}", e);
    }

    let global_cancel = lease_cancel.child_token();
    let global_ts = task_supervisor.clone();
    if let Err(e) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "runtime_global_gc_scheduler",
            async move {
                global_gc.run(global_ts, global_cancel).await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn global GC scheduler: {}", e);
    }

    Ok(())
}
