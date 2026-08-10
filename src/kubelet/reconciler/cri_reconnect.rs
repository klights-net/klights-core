use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::kubelet::reconciler::cri_inventory::{
    CriContainerInventory, CriInventoryAction, cleanup_cold_sandbox,
    diff_cri_inventory_with_in_flight_starts,
};
use klights_kubelet::pod_lifecycle_core::message::LifecycleMessage;
use klights_kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use klights_kubelet::runtime::cri::{ContainerRuntimeControl, CriRuntime};
use klights_leader_api::{CacheReadinessRequest, LeaderCacheReadiness, LeaderResourceQuery};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CriStreamLifecycle {
    Reconnected {
        generation: u64,
        disconnected_at_ms: i64,
        reconnected_at_ms: i64,
    },
    IdentityUnresolved {
        container_id: String,
        timestamp_ns: i64,
    },
}

pub struct CriReconnectReconciler {
    node_name: String,
    resource_query: Arc<dyn LeaderResourceQuery>,
    cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    cri: Arc<dyn CriRuntime>,
    container_control: Arc<dyn ContainerRuntimeControl>,
    router: Arc<PodLifecycleRouter>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

pub struct CriReconnectDependencies {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub cri: Arc<dyn CriRuntime>,
    pub container_control: Arc<dyn ContainerRuntimeControl>,
    pub router: Arc<PodLifecycleRouter>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

async fn collect_container_inventory(
    container_control: &dyn ContainerRuntimeControl,
    sandboxes: &[klights_kubelet::runtime::cri::CriPodSandboxSummary],
) -> Result<Vec<CriContainerInventory>> {
    let mut containers = Vec::new();
    for sandbox in sandboxes {
        for (container_id, state) in container_control
            .list_containers(Some(&sandbox.sandbox_id))
            .await?
        {
            containers.push(CriContainerInventory {
                sandbox_id: sandbox.sandbox_id.clone(),
                container_id,
                state,
            });
        }
    }
    Ok(containers)
}

impl CriReconnectReconciler {
    pub fn new(node_name: String, dependencies: CriReconnectDependencies) -> Self {
        let CriReconnectDependencies {
            resource_query,
            cache_readiness,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            container_control,
            router,
            task_supervisor,
        } = dependencies;
        Self {
            node_name,
            resource_query,
            cache_readiness,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            container_control,
            router,
            task_supervisor,
        }
    }

    pub async fn run_once(&self) -> Result<Vec<CriInventoryAction>> {
        self.cache_readiness
            .wait_cache_ready(CacheReadinessRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some(format!("spec.nodeName={}", self.node_name)),
            )?)
            .await
            .context("wait for pod cache before CRI reconnect reconcile")?;

        let runtime_rows = self.pod_runtime_store.list_pod_runtime().await?;
        let leader_pods = self
            .resource_query
            .list_resources(klights_leader_api::pods_on_node_list_request(
                &self.node_name,
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await?
            .into_items()
            .into_iter()
            .map(|pod| (*pod.data).clone())
            .collect::<Vec<_>>();
        let sandboxes = self.cri.list_pod_sandbox_summaries().await?;
        let containers = collect_container_inventory(self.container_control.as_ref(), &sandboxes)
            .await
            .context("list containers during CRI reconnect inventory")?;

        let in_flight_starts = self
            .router
            .in_flight_start_keys()
            .await
            .into_iter()
            .collect();
        let actions = diff_cri_inventory_with_in_flight_starts(
            true,
            &runtime_rows,
            &leader_pods,
            &sandboxes,
            &containers,
            &in_flight_starts,
        );
        self.apply_actions(&actions).await?;
        Ok(actions)
    }

    async fn apply_actions(&self, actions: &[CriInventoryAction]) -> Result<()> {
        for action in actions {
            match action {
                CriInventoryAction::FinalizeOrphan { key, reason } => {
                    enqueue_orphan_finalize(self.router.as_ref(), key.clone(), *reason).await?;
                }
                CriInventoryAction::KillColdSandbox { sandbox_id, key } => {
                    if let Some(key) = key
                        && self.cold_sandbox_became_owned(key, sandbox_id).await?
                    {
                        self.router
                            .route(LifecycleMessage::CriEvent {
                                key: key.clone(),
                                container_id: String::new(),
                                kind: klights_kubelet::cri_events::KubeletEventKind::Stopped,
                            })
                            .await?;
                        continue;
                    }
                    cleanup_cold_sandbox(
                        self.router.as_ref(),
                        self.cri.as_ref(),
                        sandbox_id,
                        key.as_ref(),
                    )
                    .await?;
                }
                CriInventoryAction::DropLocalRows { key } => {
                    // The sandbox is gone from CRI and the leader has no Pod, but
                    // on-disk artifacts (volumes, cgroup, pod dir) may still be
                    // present. Reclaim them via actor-owned orphan finalize — the
                    // cleanup is UID-keyed and idempotent, so it works whether or
                    // not the bookkeeping rows still exist — then drop the rows.
                    enqueue_orphan_finalize(
                        self.router.as_ref(),
                        key.clone(),
                        OrphanReason::LeaderDeletedWhileDown,
                    )
                    .await?;
                    self.pod_runtime_store
                        .delete_pod_runtime_for_uid(klights_node_store::RuntimePodUid::try_new(
                            key.uid.clone(),
                        )?)
                        .await?;
                    self.pod_endpoint_store
                        .delete_endpoint_for_uid(klights_node_store::PodUidKey::try_new(
                            key.uid.clone(),
                        )?)
                        .await?;
                }
                CriInventoryAction::ReattachExistingSandbox { key, pod, .. }
                | CriInventoryAction::RecreateMissingSandbox { key, pod } => {
                    self.router
                        .route(LifecycleMessage::WatchAdded {
                            key: key.clone(),
                            resource_version: None,
                            pod: pod.clone(),
                        })
                        .await?;
                }
                CriInventoryAction::ReconcileRuntime { key } => {
                    self.router
                        .route(LifecycleMessage::CriEvent {
                            key: key.clone(),
                            container_id: String::new(),
                            kind: klights_kubelet::cri_events::KubeletEventKind::Stopped,
                        })
                        .await?;
                }
                CriInventoryAction::RefuseEmptyCache => {}
            }
        }
        Ok(())
    }

    async fn cold_sandbox_became_owned(
        &self,
        key: &klights_kubelet::pod_lifecycle_core::message::PodLifecycleKey,
        sandbox_id: &str,
    ) -> Result<bool> {
        if self
            .router
            .in_flight_start_keys()
            .await
            .iter()
            .any(|candidate| candidate == key)
        {
            return Ok(true);
        }
        let row = self
            .pod_runtime_store
            .get_pod_runtime(klights_node_store::RuntimePodUid::try_new(key.uid.clone())?)
            .await?;
        Ok(row.is_some_and(|row| {
            row.pod().namespace == key.namespace
                && row.pod().name == key.name
                && row.sandbox_id() == Some(sandbox_id)
        }))
    }

    pub async fn run_lifecycle_loop(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<CriStreamLifecycle>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => return,
                event = rx.recv() => {
                    let Some(mut event) = event else {
                        return;
                    };
                    let mut retry_attempt = 0u32;
                    loop {
                        let (generation, reason, container_id) = match &event {
                            CriStreamLifecycle::Reconnected { generation, .. } => {
                                (Some(*generation), "stream-reconnected", None)
                            }
                            CriStreamLifecycle::IdentityUnresolved { container_id, .. } => {
                                (None, "event-identity-unresolved", Some(container_id.as_str()))
                            }
                        };
                        let succeeded = match self.run_once().await {
                            Ok(actions) => {
                                tracing::warn!(
                                    ?generation,
                                    reason,
                                    container_id,
                                    action_count = actions.len(),
                                    "CRI event-driven inventory diff completed"
                                );
                                true
                            }
                            Err(err) => {
                                let delay = klights_supervisor::reconnect_backoff::delay(retry_attempt);
                                retry_attempt = retry_attempt.saturating_add(1);
                                tracing::warn!(
                                    ?generation,
                                    reason,
                                    container_id,
                                    ?delay,
                                    "CRI event-driven inventory diff failed; retained signal will retry: {err:#}"
                                );
                                tokio::select! {
                                    _ = cancel_token.cancelled() => return,
                                    sleep_result = self.task_supervisor.sleep(
                                        "cri_identity_reconcile_retry_backoff",
                                        delay,
                                    ) => {
                                        if let Err(error) = sleep_result {
                                            tracing::debug!(%error, "CRI inventory retry timer interrupted");
                                            return;
                                        }
                                    }
                                }
                                false
                            }
                        };
                        if !succeeded {
                            continue;
                        }

                        retry_attempt = 0;
                        let mut coalesced = None;
                        loop {
                            match rx.try_recv() {
                                Ok(next_event) => coalesced = Some(next_event),
                                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return,
                            }
                        }
                        if let Some(next_event) = coalesced {
                            event = next_event;
                            continue;
                        }
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use klights_kubelet::pod_lifecycle_core::message::PodLifecycleKey;

    static NEXT_DB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    struct FixedLeaderPod(klights_cluster_core::Resource);

    impl LeaderResourceQuery for FixedLeaderPod {
        fn get_resource(
            &self,
            _request: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            Box::pin(async { Ok(None) })
        }

        fn list_resources(
            &self,
            _request: klights_leader_api::ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            Box::pin(async move {
                klights_leader_api::ResourceListResult::try_new(
                    vec![self.0.clone()],
                    7,
                    None,
                    None,
                    None,
                )
            })
        }
    }

    struct ReadyCache;

    impl LeaderCacheReadiness for ReadyCache {
        fn wait_cache_ready(
            &self,
            _request: CacheReadinessRequest,
        ) -> klights_leader_api::CacheReadinessFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    struct SequencedContainerInventory {
        calls: AtomicUsize,
        fail_first: bool,
        first_entered: Option<Arc<tokio::sync::Notify>>,
        release_first: Option<Arc<tokio::sync::Notify>>,
        second_entered: Option<Arc<tokio::sync::Notify>>,
        release_second: Option<Arc<tokio::sync::Notify>>,
    }

    impl SequencedContainerInventory {
        fn immediate(fail_first: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                fail_first,
                first_entered: None,
                release_first: None,
                second_entered: None,
                release_second: None,
            })
        }

        #[allow(clippy::type_complexity)] // Exact paused-concurrency test fixture tuple.
        fn blocked_first_two() -> (
            Arc<Self>,
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
        ) {
            let first_entered = Arc::new(tokio::sync::Notify::new());
            let release_first = Arc::new(tokio::sync::Notify::new());
            let second_entered = Arc::new(tokio::sync::Notify::new());
            let release_second = Arc::new(tokio::sync::Notify::new());
            (
                Arc::new(Self {
                    calls: AtomicUsize::new(0),
                    fail_first: false,
                    first_entered: Some(first_entered.clone()),
                    release_first: Some(release_first.clone()),
                    second_entered: Some(second_entered.clone()),
                    release_second: Some(release_second.clone()),
                }),
                first_entered,
                release_first,
                second_entered,
                release_second,
            )
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ContainerRuntimeControl for SequencedContainerInventory {
        async fn list_containers(
            &self,
            _sandbox_id_filter: Option<&str>,
        ) -> anyhow::Result<Vec<(String, klights_kubelet::runtime::cri::ContainerRuntimeState)>>
        {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                if let Some(entered) = &self.first_entered {
                    entered.notify_one();
                }
                if let Some(release) = &self.release_first {
                    release.notified().await;
                }
                if self.fail_first {
                    anyhow::bail!("injected first inventory failure");
                }
            } else if call == 1 {
                if let Some(entered) = &self.second_entered {
                    entered.notify_one();
                }
                if let Some(release) = &self.release_second {
                    release.notified().await;
                }
            }
            Ok(vec![(
                "ctr-a".into(),
                klights_kubelet::runtime::cri::ContainerRuntimeState::Exited,
            )])
        }

        async fn pod_metadata_for_container(
            &self,
            _container_id: &str,
        ) -> anyhow::Result<Option<klights_types::PodIdentity>> {
            Ok(None)
        }
    }

    async fn reconnect_harness(
        container_control: Arc<SequencedContainerInventory>,
    ) -> (
        Arc<CriReconnectReconciler>,
        Arc<klights_kubelet::pod_lifecycle_router::executor::RecordingExecutor>,
        Arc<klights_supervisor::TaskSupervisor>,
    ) {
        use klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
        use klights_kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
        use klights_kubelet::pod_lifecycle_router::executor::{PodWorkExecutor, RecordingExecutor};
        use klights_node_store::{OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeStore};

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let db_name: &'static str = Box::leak(
            format!("cri-reconnect-{}", NEXT_DB.fetch_add(1, Ordering::Relaxed)).into_boxed_str(),
        );
        let executor = klights_node_datastore::open::open_with_opts(
            klights_supervisor::sqlite_open::OpenOpts::shared_memory(db_name),
            supervisor.clone(),
            "sqlite:cri-reconnect-test",
        )
        .await
        .unwrap();
        let clock = Arc::new(klights_supervisor::SystemWallClock);
        let runtime_store = Arc::new(klights_node_datastore::SqliteRuntimeWorkStore::new(
            executor.clone(),
            clock.clone(),
        ));
        let endpoint_store = Arc::new(klights_node_datastore::SqliteNodeNetworkStateStore::new(
            executor, clock,
        ));
        let identity = klights_types::PodIdentity::new("default", "web", "uid-a");
        runtime_store
            .admit_pod_runtime(PodRuntimeAdmission::try_new(identity.clone(), "worker-a").unwrap())
            .await
            .unwrap();
        runtime_store
            .record_owned_sandbox(
                OwnedPodSandbox::try_new(identity, "worker-a", "sb-a", 1).unwrap(),
            )
            .await
            .unwrap();

        let recorder = RecordingExecutor::new();
        let executor_holder = Arc::new(std::sync::Mutex::new(
            recorder.clone() as Arc<dyn PodWorkExecutor>
        ));
        let registry = Arc::new(PodLifecycleRegistry::new(
            supervisor.clone(),
            PodLifecycleConcurrencyConfig::production_default(),
            executor_holder,
        ));
        let router = Arc::new(PodLifecycleRouter::new_actor_with_executor(
            registry,
            recorder.clone(),
        ));
        let pod = klights_cluster_core::Resource::try_from_data(Arc::new(
            crate::kubelet::reconciler::cri_inventory::tests::pod("default", "web", "uid-a"),
        ))
        .unwrap();
        let cri = Arc::new(klights_kubelet::runtime::test_support::MockCriRuntime::new());
        cri.set_pod_sandboxes(vec![("sb-a", "default", "web", "uid-a", "Ready")]);
        let reconciler = Arc::new(CriReconnectReconciler::new(
            "worker-a".into(),
            CriReconnectDependencies {
                resource_query: Arc::new(FixedLeaderPod(pod)),
                cache_readiness: Arc::new(ReadyCache),
                pod_runtime_store: runtime_store,
                pod_endpoint_store: endpoint_store,
                cri,
                container_control,
                router,
                task_supervisor: supervisor.clone(),
            },
        ));
        (reconciler, recorder, supervisor)
    }

    async fn wait_for_runtime_reconcile(
        recorder: &klights_kubelet::pod_lifecycle_router::executor::RecordingExecutor,
    ) -> klights_kubelet::pod_lifecycle_core::action::PodAction {
        for _ in 0..1000 {
            if let Some(action) = recorder.take_actions().into_iter().find(|action| {
                matches!(
                    action,
                    klights_kubelet::pod_lifecycle_core::action::PodAction::ReconcileRuntime { .. }
                )
            }) {
                return action;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("runtime reconcile action was not dispatched")
    }

    fn unresolved_signal() -> CriStreamLifecycle {
        CriStreamLifecycle::IdentityUnresolved {
            container_id: "ctr-a".into(),
            timestamp_ns: 1_777_000_456,
        }
    }

    #[tokio::test]
    async fn identity_unresolved_routes_exited_container_to_uid_qualified_runtime_reconcile() {
        let inventory = SequencedContainerInventory::immediate(false);
        let (reconciler, recorder, _supervisor) = reconnect_harness(inventory).await;
        let (tx, rx) = mpsc::channel(4);
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(reconciler.run_lifecycle_loop(rx, cancel.clone()));
        tx.send(unresolved_signal()).await.unwrap();

        let action = wait_for_runtime_reconcile(&recorder).await;
        cancel.cancel();
        task.await.unwrap();
        assert!(matches!(
            action,
            klights_kubelet::pod_lifecycle_core::action::PodAction::ReconcileRuntime { key, .. }
                if key == PodLifecycleKey::new("default", "web", "uid-a")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn identity_unresolved_inventory_failure_retries_after_supervised_backoff() {
        let inventory = SequencedContainerInventory::immediate(true);
        let (reconciler, recorder, _supervisor) = reconnect_harness(inventory.clone()).await;
        let (tx, rx) = mpsc::channel(4);
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(reconciler.run_lifecycle_loop(rx, cancel.clone()));
        tx.send(unresolved_signal()).await.unwrap();

        while inventory.calls() == 0 {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        assert_eq!(inventory.calls(), 1);
        assert_eq!(recorder.action_count(), 0);
        tokio::time::advance(std::time::Duration::from_millis(499)).await;
        tokio::task::yield_now().await;
        assert_eq!(inventory.calls(), 1, "retry must honor the 500ms backoff");
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        while inventory.calls() < 2 {
            tokio::task::yield_now().await;
        }
        let action = loop {
            if let Some(action) = recorder.take_actions().into_iter().find(|action| {
                matches!(
                    action,
                    klights_kubelet::pod_lifecycle_core::action::PodAction::ReconcileRuntime { .. }
                )
            }) {
                break action;
            }
            tokio::task::yield_now().await;
        };
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(inventory.calls(), 2);
        assert!(matches!(
            action,
            klights_kubelet::pod_lifecycle_core::action::PodAction::ReconcileRuntime { key, .. }
                if key.uid == "uid-a"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn identity_unresolved_supervised_backoff_is_cancellation_aware() {
        let inventory = SequencedContainerInventory::immediate(true);
        let (reconciler, _recorder, _supervisor) = reconnect_harness(inventory.clone()).await;
        let (tx, rx) = mpsc::channel(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(reconciler.run_lifecycle_loop(rx, cancel.clone()));
        tx.send(unresolved_signal()).await.unwrap();
        while inventory.calls() == 0 {
            tokio::task::yield_now().await;
        }

        cancel.cancel();
        task.await.unwrap();
        tokio::time::advance(std::time::Duration::from_millis(500)).await;
        assert_eq!(
            inventory.calls(),
            1,
            "cancellation must suppress retained retry"
        );
    }

    #[tokio::test]
    async fn identity_unresolved_burst_is_coalesced_while_inventory_is_in_flight() {
        let (inventory, first_entered, release_first, second_entered, release_second) =
            SequencedContainerInventory::blocked_first_two();
        let (reconciler, recorder, _supervisor) = reconnect_harness(inventory.clone()).await;
        let (tx, rx) = mpsc::channel(8);
        let cancel = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(reconciler.run_lifecycle_loop(rx, cancel.clone()));
        tx.send(unresolved_signal()).await.unwrap();
        first_entered.notified().await;
        for _ in 0..4 {
            tx.send(unresolved_signal()).await.unwrap();
        }
        release_first.notify_one();
        second_entered.notified().await;
        assert_eq!(
            inventory.calls(),
            2,
            "queued burst must cause one follow-up pass"
        );
        release_second.notify_one();
        let _ = wait_for_runtime_reconcile(&recorder).await;
        cancel.cancel();
        task.await.unwrap();

        assert_eq!(
            inventory.calls(),
            2,
            "burst must coalesce to exactly one follow-up pass"
        );
    }

    struct FailingContainerInventory;

    #[async_trait::async_trait]
    impl ContainerRuntimeControl for FailingContainerInventory {
        async fn list_containers(
            &self,
            _sandbox_id_filter: Option<&str>,
        ) -> anyhow::Result<Vec<(String, klights_kubelet::runtime::cri::ContainerRuntimeState)>>
        {
            anyhow::bail!("injected reconnect ListContainers timeout")
        }

        async fn pod_metadata_for_container(
            &self,
            _container_id: &str,
        ) -> anyhow::Result<Option<klights_types::PodIdentity>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn reconnect_inventory_propagates_container_list_failure() {
        let control = FailingContainerInventory;
        let sandboxes = vec![klights_kubelet::runtime::cri::CriPodSandboxSummary {
            sandbox_id: "sandbox-timeout".into(),
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid".into(),
        }];

        let error = collect_container_inventory(&control, &sandboxes)
            .await
            .expect_err("reconnect must retry when inventory cannot be established");
        assert!(error.to_string().contains("injected reconnect"));
    }
}
