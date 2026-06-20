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
use anyhow::{Context as _, Result};
use tokio_util::sync::CancellationToken;

const CONTROLLER_WORKQUEUE_WORKERS: usize = 8;

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
    pub datapath: &'a Arc<dyn crate::networking::Datapath>,
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
    datapath: Arc<dyn crate::networking::Datapath>,
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
        datapath,
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
        datapath: datapath.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::networking::test_support::MockNetworkProvider;
    use serde_json::Value;
    use std::net::Ipv4Addr;

    fn endpoint_address(resource: &Value) -> &str {
        resource["subsets"][0]["addresses"][0]["ip"]
            .as_str()
            .expect("endpoint address should be a string")
    }

    fn endpointslice_address(resource: &Value) -> &str {
        resource["endpoints"][0]["addresses"][0]
            .as_str()
            .expect("endpointslice address should be a string")
    }

    #[tokio::test]
    async fn leader_kubernetes_service_reconcile_moves_endpoint_to_current_gateway() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .expect("default namespaces");
        let db_handle: DatastoreHandle = Arc::new(db);
        let mut config = KlightsConfig::test_default();
        config.service_cidr = "10.51.0.0/24".to_string();
        config.tls_port = 7679;

        let first_leader_datapath = MockNetworkProvider::new();
        first_leader_datapath.set_pod_gateway_ip(Ipv4Addr::new(10, 50, 0, 1));
        reconcile_kubernetes_service_for_leader(&config, &db_handle, &first_leader_datapath)
            .await
            .expect("first leader should seed kubernetes service endpoints");

        let next_leader_datapath = MockNetworkProvider::new();
        next_leader_datapath.set_pod_gateway_ip(Ipv4Addr::new(10, 50, 4, 1));
        reconcile_kubernetes_service_for_leader(&config, &db_handle, &next_leader_datapath)
            .await
            .expect("new leader should reconcile kubernetes service endpoints");

        let endpoints = db_handle
            .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
            .await
            .expect("get endpoints")
            .expect("kubernetes endpoints should exist")
            .data;
        assert_eq!(endpoint_address(&endpoints), "10.50.4.1");

        let endpointslice = db_handle
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "kubernetes",
            )
            .await
            .expect("get endpointslice")
            .expect("kubernetes endpointslice should exist")
            .data;
        assert_eq!(endpointslice_address(&endpointslice), "10.50.4.1");
    }
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
        datapath,
    } = context;

    tracing::info!("Acquired leader lease");
    use crate::{controllers, gc};

    reconcile_kubernetes_service_for_leader(config.as_ref(), &db_handle, datapath.as_ref())
        .await
        .context("reconcile kubernetes Service endpoint for active leader")?;

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
                d.run_worker_pool(CONTROLLER_WORKQUEUE_WORKERS, dhw, nn, c)
                    .await;
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn workqueue worker: {}", e);
    }
    tracing::info!(
        workers = CONTROLLER_WORKQUEUE_WORKERS,
        "Controller workqueue worker pool started"
    );

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

async fn reconcile_kubernetes_service_for_leader(
    config: &KlightsConfig,
    db_handle: &DatastoreHandle,
    datapath: &dyn crate::networking::Datapath,
) -> Result<()> {
    crate::controllers::kube_service::bootstrap_default_service_cidr(
        db_handle.as_ref(),
        &config.service_cidr,
    )
    .await?;
    crate::controllers::kube_service::bootstrap_kubernetes_service(
        db_handle.as_ref(),
        &config.service_cidr,
        config.tls_port,
        datapath,
    )
    .await
}
