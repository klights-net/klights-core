#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use anyhow::Result;
    use klights_controllers::side_effects::SideEffectMetrics;
    use klights_kubelet::pod_lifecycle_core::action::PodAction;
    use klights_kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleWorkKind};
    use klights_kubelet::pod_lifecycle_router::LifecycleReplyHandle;
    use klights_kubelet::pod_lifecycle_router::executor::{ExecutorError, PodWorkExecutor};
    use klights_kubelet::pod_repository::workqueue::{
        PodWorkqueue, PodWorkqueueEntry, PodWorkqueueKind, PodWorkqueuePersistence,
    };
    use klights_leader_api::{ControllerCoordination, ControllerLease, ControllerScope};
    use klights_node_store::{PodWorkqueueLeaseToken, PodWorkqueueMutationOutcome};
    use klights_pod_api::{UnscheduledPodDeletionError, UnscheduledPodDeletionRequest};
    use klights_reconcile_api::{GcPodDeleteRequest, GcPodDeleteSink};
    use klights_supervisor::TaskCategory;
    use klights_types::PodIdentity;
    use serde_json::{Value, json};
    use tokio::sync::Notify;

    const MAX_ATTEMPTS: i64 = 720;
    const WORK_LEASE_MS: i64 = 30_000;
    const POD_DELETE_TARGET_NODE_PAYLOAD_KEY: &str = "target_node";

    fn now_ms() -> i64 {
        klights_supervisor::SystemWallClock::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn test_persistence(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
        clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::RootPodRepositoryPersistenceParts{
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(db);
        crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::new_root_parts_from_test_ports(
            clock, ports.applied_outbox, Arc::new(db.clone()), ports.read_ports.resource_reads(), ports.ownership_reads,
        )
    }

    fn pod_delete_target_payload(target_node: Option<&str>) -> Value {
        let mut payload = serde_json::Map::new();
        if let Some(target_node) = target_node.filter(|node| !node.trim().is_empty()) {
            payload.insert(
                POD_DELETE_TARGET_NODE_PAYLOAD_KEY.to_string(),
                Value::String(target_node.to_string()),
            );
        }
        Value::Object(payload)
    }

    fn focused_test_entry(lease: klights_node_store::PodWorkqueueLease) -> PodWorkqueueEntry {
        let (row, lease_token) = lease.into_parts();
        let (id, identity, payload, attempt_count, _next_due_ms) = row.into_parts();
        let (kind, pod) = identity.into_persisted();
        PodWorkqueueEntry {
            id: id.get(),
            kind: match kind {
                klights_node_store::PodWorkqueueKind::Pod => PodWorkqueueKind::Pod,
                klights_node_store::PodWorkqueueKind::Namespace => PodWorkqueueKind::Namespace,
            },
            namespace: pod.namespace,
            name: pod.name,
            uid: pod.uid,
            payload: serde_json::from_slice(&payload).expect("valid runtime work payload"),
            attempt_count,
            lease_token,
        }
    }

    async fn enqueue_runtime_work(
        store: &dyn klights_node_store::PodWorkqueueStore,
        kind: klights_node_store::PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        let identity = match kind {
            klights_node_store::PodWorkqueueKind::Pod => {
                klights_node_store::PodWorkIdentity::try_pod(pod.clone())?
            }
            klights_node_store::PodWorkqueueKind::Namespace => {
                klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
            }
        };
        store
            .enqueue_work(klights_node_store::PodWorkqueueEnqueue::try_new(
                identity,
                serde_json::to_vec(&payload)?,
                attempt_count,
                min_delay_ms,
                last_error.map(str::to_string),
            )?)
            .await
            .map_err(Into::into)
    }

    async fn peek_runtime_work(
        store: &dyn klights_node_store::PodWorkqueueStore,
    ) -> Result<Option<i64>> {
        store.peek_next_due_ms().await.map_err(Into::into)
    }

    async fn claim_runtime_work(
        store: &dyn klights_node_store::PodWorkqueueStore,
        now_ms: i64,
    ) -> Result<Option<PodWorkqueueEntry>> {
        let lease = store
            .claim_due_work_with_lease(klights_node_store::PodWorkqueueClaimRequest::try_new(
                now_ms.min(i64::MAX - 1),
                1,
            )?)
            .await?;
        Ok(lease.map(focused_test_entry))
    }

    struct FixedRuntimeClock(i64);

    impl klights_kubelet::runtime_clock::RuntimeClock for FixedRuntimeClock {
        fn now_ms(&self) -> i64 {
            self.0
        }
    }

    struct ProbePersistence {
        peeks: AtomicUsize,
        claims: AtomicUsize,
        claim_error: bool,
    }

    struct SpawnRefusalPersistence {
        claim_entered: Notify,
        release_claim: Notify,
        requeued: Notify,
        requeue_count: AtomicUsize,
    }

    struct SpawnRefusalPersistenceAdapter(Arc<SpawnRefusalPersistence>);

    impl SpawnRefusalPersistence {
        fn claimed_row() -> PodWorkqueueEntry {
            let identity = klights_node_store::PodWorkIdentity::try_namespace(
                "spawn-refusal",
                "uid-spawn-refusal",
            )
            .unwrap();
            PodWorkqueueEntry {
                id: 1,
                kind: PodWorkqueueKind::Namespace,
                namespace: String::new(),
                name: "spawn-refusal".to_string(),
                uid: "uid-spawn-refusal".to_string(),
                payload: json!({}),
                attempt_count: 0,
                lease_token: PodWorkqueueLeaseToken::try_new(1, identity, WORK_LEASE_MS).unwrap(),
            }
        }
    }

    #[async_trait::async_trait]
    impl PodWorkqueuePersistence for SpawnRefusalPersistenceAdapter {
        async fn enqueue(
            &self,
            _kind: PodWorkqueueKind,
            _pod: &PodIdentity,
            _payload: Value,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _last_error: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn ensure_absent(
            &self,
            _kind: PodWorkqueueKind,
            _pod: &PodIdentity,
            _payload: Value,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _last_error: Option<&str>,
        ) -> Result<bool> {
            Ok(true)
        }

        async fn peek_next_due(&self) -> Result<Option<i64>> {
            Ok(Some(0))
        }

        async fn claim_due(
            &self,
            _now_ms: i64,
            _lease_duration_ms: i64,
        ) -> Result<Option<PodWorkqueueEntry>> {
            self.0.claim_entered.notify_one();
            self.0.release_claim.notified().await;
            Ok(Some(SpawnRefusalPersistence::claimed_row()))
        }

        async fn acknowledge(
            &self,
            _token: PodWorkqueueLeaseToken,
        ) -> Result<PodWorkqueueMutationOutcome> {
            unreachable!("spawn refusal must park, not acknowledge")
        }

        async fn requeue(
            &self,
            _row: PodWorkqueueEntry,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _error: &str,
        ) -> Result<PodWorkqueueMutationOutcome> {
            self.0.requeue_count.fetch_add(1, Ordering::Relaxed);
            self.0.requeued.notify_one();
            Ok(PodWorkqueueMutationOutcome::Applied)
        }
    }

    impl ProbePersistence {
        fn idle() -> Self {
            Self {
                peeks: AtomicUsize::new(0),
                claims: AtomicUsize::new(0),
                claim_error: false,
            }
        }

        fn failing_claim() -> Self {
            Self {
                peeks: AtomicUsize::new(0),
                claims: AtomicUsize::new(0),
                claim_error: true,
            }
        }
    }

    struct ProbePersistenceAdapter(Arc<ProbePersistence>);

    #[async_trait::async_trait]
    impl PodWorkqueuePersistence for ProbePersistenceAdapter {
        async fn enqueue(
            &self,
            _kind: PodWorkqueueKind,
            _pod: &PodIdentity,
            _payload: Value,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _last_error: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn ensure_absent(
            &self,
            _kind: PodWorkqueueKind,
            _pod: &PodIdentity,
            _payload: Value,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _last_error: Option<&str>,
        ) -> Result<bool> {
            Ok(true)
        }

        async fn peek_next_due(&self) -> Result<Option<i64>> {
            self.0.peeks.fetch_add(1, Ordering::Relaxed);
            Ok(self.0.claim_error.then_some(0))
        }

        async fn claim_due(
            &self,
            _now_ms: i64,
            _lease_duration_ms: i64,
        ) -> Result<Option<PodWorkqueueEntry>> {
            self.0.claims.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!("injected claim failure"))
        }

        async fn acknowledge(
            &self,
            _token: PodWorkqueueLeaseToken,
        ) -> Result<PodWorkqueueMutationOutcome> {
            unreachable!("probe persistence never yields a claimed row")
        }

        async fn requeue(
            &self,
            _row: PodWorkqueueEntry,
            _attempt_count: i64,
            _min_delay_ms: i64,
            _error: &str,
        ) -> Result<PodWorkqueueMutationOutcome> {
            unreachable!("probe persistence never yields a claimed row")
        }
    }

    #[derive(Clone, Copy)]
    struct TestCoordinationState {
        local: bool,
        generation: u64,
    }

    struct TestCoordination {
        receiver: tokio::sync::watch::Receiver<TestCoordinationState>,
    }

    struct TestCoordinationFence(u64);

    impl ControllerCoordination for TestCoordination {
        fn try_acquire(
            &self,
            scope: ControllerScope,
        ) -> Result<ControllerLease, klights_leader_api::ControllerCoordinationError> {
            let state = *self.receiver.borrow();
            if state.local {
                Ok(ControllerLease::issue(
                    scope,
                    TestCoordinationFence(state.generation),
                ))
            } else {
                Err(klights_leader_api::ControllerCoordinationError::Unavailable)
            }
        }

        fn acquire(
            &self,
            scope: ControllerScope,
        ) -> klights_leader_api::ControllerAcquireFuture<'_> {
            let mut receiver = self.receiver.clone();
            Box::pin(async move {
                loop {
                    let state = *receiver.borrow_and_update();
                    if state.local {
                        return Ok(ControllerLease::issue(
                            scope,
                            TestCoordinationFence(state.generation),
                        ));
                    }
                    receiver
                        .changed()
                        .await
                        .map_err(|_| klights_leader_api::ControllerCoordinationError::Closed)?;
                }
            })
        }

        fn validate(
            &self,
            lease: &ControllerLease,
        ) -> Result<(), klights_leader_api::ControllerCoordinationError> {
            let state = *self.receiver.borrow();
            if !state.local {
                Err(klights_leader_api::ControllerCoordinationError::Unavailable)
            } else if lease
                .adapter_fence::<TestCoordinationFence>()
                .is_none_or(|fence| fence.0 != state.generation)
            {
                Err(klights_leader_api::ControllerCoordinationError::StalePermit)
            } else {
                Ok(())
            }
        }

        fn wait_for_revocation<'a>(
            &'a self,
            lease: &'a ControllerLease,
        ) -> klights_leader_api::ControllerRevocationFuture<'a> {
            let mut receiver = self.receiver.clone();
            let generation = lease
                .adapter_fence::<TestCoordinationFence>()
                .map_or(0, |fence| fence.0);
            Box::pin(async move {
                loop {
                    let state = *receiver.borrow_and_update();
                    if !state.local || state.generation != generation {
                        return;
                    }
                    if receiver.changed().await.is_err() {
                        return;
                    }
                }
            })
        }
    }

    fn test_coordination(
        local: bool,
    ) -> (
        Arc<dyn ControllerCoordination>,
        tokio::sync::watch::Sender<TestCoordinationState>,
    ) {
        let (sender, receiver) = tokio::sync::watch::channel(TestCoordinationState {
            local,
            generation: 1,
        });
        (Arc::new(TestCoordination { receiver }), sender)
    }

    #[derive(Default)]
    struct RecordingGcPodDeleteSink {
        calls: tokio::sync::Mutex<Vec<(String, String, String)>>,
    }

    impl RecordingGcPodDeleteSink {
        async fn calls(&self) -> Vec<(String, String, String)> {
            self.calls.lock().await.clone()
        }
    }

    impl GcPodDeleteSink for RecordingGcPodDeleteSink {
        fn request_gc_pod_delete(
            &self,
            request: GcPodDeleteRequest,
        ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
            Box::pin(async move {
                let identity = request.into_identity();
                self.calls
                    .lock()
                    .await
                    .push((identity.namespace, identity.name, identity.uid));
                Ok(())
            })
        }
    }

    struct WakeRecordingExecutor {
        stop_seen: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PodWorkExecutor for WakeRecordingExecutor {
        async fn dispatch(
            &self,
            action: PodAction,
            reply_to: LifecycleReplyHandle,
        ) -> Result<(), ExecutorError> {
            match action {
                PodAction::ReconcileCriLeftovers {
                    key, operation_id, ..
                } => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind: PodLifecycleWorkKind::ReconcileCriLeftovers,
                            sandbox_id: None,
                        })
                        .await;
                }
                PodAction::StopPod { key, .. } if key.uid == "uid-old" => {
                    self.stop_seen.notify_waiters();
                }
                _ => {}
            }
            Ok(())
        }
    }

    async fn test_workqueue() -> (
        Arc<PodWorkqueue>,
        klights_cluster_datastore::sqlite::embedded::Datastore,
        std::sync::Arc<crate::bootstrap::node_store::NodeLocalStores>,
    ) {
        test_workqueue_at(Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock)).await
    }

    async fn test_workqueue_at(
        clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> (
        Arc<PodWorkqueue>,
        klights_cluster_datastore::sqlite::embedded::Datastore,
        std::sync::Arc<crate::bootstrap::node_store::NodeLocalStores>,
    ) {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let persistence = test_persistence(&db, clock.clone());
        let store = persistence.store;
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = super::super::test_node_local_store(supervisor.clone()).await;
        let metrics = SideEffectMetrics::new();
        let (coordination, _publisher) = test_coordination(true);
        let workqueue = PodWorkqueue::new_leader(
            store,
            crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                node_local.pod_workqueue(),
                clock.clone(),
            ),
            supervisor,
            metrics,
            persistence.unscheduled_deletion,
            coordination,
            clock,
        );
        workqueue.set_local_node_name_for_tests(Some("node-a".to_string()));
        (workqueue, db, node_local)
    }

    async fn test_non_leader_workqueue() -> (
        Arc<PodWorkqueue>,
        klights_cluster_datastore::sqlite::embedded::Datastore,
        std::sync::Arc<crate::bootstrap::node_store::NodeLocalStores>,
    ) {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let store = Arc::new(crate::bootstrap::pod_repository_composition::new_pod_store(
            ports.applied_outbox,
            Arc::new(db.clone()),
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
        ));
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = super::super::test_node_local_store(supervisor.clone()).await;
        let metrics = SideEffectMetrics::new();
        let workqueue = PodWorkqueue::new(
            store,
            crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                node_local.pod_workqueue(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            ),
            supervisor,
            metrics,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        workqueue.set_local_node_name_for_tests(Some("node-a".to_string()));
        (workqueue, db, node_local)
    }

    async fn probe_workqueue(persistence: Arc<ProbePersistence>) -> Arc<PodWorkqueue> {
        probe_workqueue_for_persistence(ProbePersistenceAdapter(persistence)).await
    }

    async fn probe_workqueue_for_persistence(
        persistence: impl PodWorkqueuePersistence + 'static,
    ) -> Arc<PodWorkqueue> {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let store = Arc::new(crate::bootstrap::pod_repository_composition::new_pod_store(
            ports.applied_outbox,
            Arc::new(db.clone()),
            ports.read_ports.resource_reads(),
            ports.ownership_reads,
        ));
        PodWorkqueue::new(
            store,
            persistence,
            Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
            SideEffectMetrics::new(),
            Arc::new(FixedRuntimeClock(0)),
        )
    }

    async fn wait_for_claimed_due(
        node_local: &Arc<crate::bootstrap::node_store::NodeLocalStores>,
        initial_due: i64,
    ) -> i64 {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let due = peek_runtime_work(node_local.pod_workqueue().as_ref())
                .await
                .unwrap()
                .unwrap();
            if due > initial_due + 1_000 {
                return due;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "workqueue row was not claimed with a lease"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_parked_due(
        node_local: &Arc<crate::bootstrap::node_store::NodeLocalStores>,
        leased_due: i64,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if peek_runtime_work(node_local.pod_workqueue().as_ref())
                .await
                .unwrap()
                .is_some_and(|due| due != leased_due)
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "claimed workqueue row was not parked"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn capacity_blocked_workqueue(
        coordination: Option<Arc<dyn ControllerCoordination>>,
    ) -> (
        Arc<PodWorkqueue>,
        Arc<crate::bootstrap::node_store::NodeLocalStores>,
        Arc<klights_supervisor::TaskSupervisor>,
        Arc<Notify>,
    ) {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let persistence = test_persistence(
            &db,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let config = klights_supervisor::TaskCategoryConfig {
            pod_delete_workqueue: 1,
            ..Default::default()
        };
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(config));
        let persistence_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = super::super::test_node_local_store(persistence_supervisor).await;
        let release = Arc::new(Notify::new());
        let task_release = release.clone();
        supervisor
            .spawn_async(
                TaskCategory::PodDeleteWorkqueue,
                "pod_workqueue_capacity_blocker",
                async move { task_release.notified().await },
            )
            .await
            .unwrap();
        let workqueue = match coordination {
            Some(coordination) => PodWorkqueue::new_leader(
                persistence.store,
                crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                    node_local.pod_workqueue(),
                    Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
                ),
                supervisor.clone(),
                SideEffectMetrics::new(),
                persistence.unscheduled_deletion,
                coordination,
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            ),
            None => PodWorkqueue::new(
                persistence.store,
                crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                    node_local.pod_workqueue(),
                    Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
                ),
                supervisor.clone(),
                SideEffectMetrics::new(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            ),
        };
        (workqueue, node_local, supervisor, release)
    }

    #[tokio::test(start_paused = true)]
    async fn idle_reconciler_waits_for_notification_without_polling() {
        let persistence = Arc::new(ProbePersistence::idle());
        let workqueue = probe_workqueue(persistence.clone()).await;
        workqueue
            .ensure_reconciler_started_for_tests()
            .await
            .unwrap();
        for _ in 0..20 {
            if persistence.peeks.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(persistence.peeks.load(Ordering::Relaxed), 1);
        assert_eq!(persistence.claims.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_secs(3_600)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            persistence.peeks.load(Ordering::Relaxed),
            1,
            "an idle queue must remain notification-driven even as time advances"
        );
        workqueue
            .supervisor_for_tests()
            .root_cancellation_token()
            .cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn claim_errors_use_supervised_one_shot_backoff() {
        let persistence = Arc::new(ProbePersistence::failing_claim());
        let workqueue = probe_workqueue(persistence.clone()).await;
        workqueue
            .ensure_reconciler_started_for_tests()
            .await
            .unwrap();
        for _ in 0..20 {
            if persistence.claims.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(persistence.claims.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(249)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert_eq!(persistence.claims.load(Ordering::Relaxed), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..20 {
            if persistence.claims.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(persistence.claims.load(Ordering::Relaxed), 2);
        workqueue
            .supervisor_for_tests()
            .root_cancellation_token()
            .cancel();
    }

    #[tokio::test]
    async fn category_capacity_wake_parks_exact_claim_and_notifies_retry() {
        let (workqueue, node_local, supervisor, release) = capacity_blocked_workqueue(None).await;
        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "capacity-wait".to_string(),
                "uid-capacity".to_string(),
                Duration::ZERO,
            )
            .await
            .unwrap();
        let initial_due = peek_runtime_work(node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let leased_due = wait_for_claimed_due(&node_local, initial_due).await;
        workqueue.notify_for_tests();
        wait_for_parked_due(&node_local, leased_due).await;
        assert!(
            claim_runtime_work(node_local.pod_workqueue().as_ref(), i64::MAX)
                .await
                .unwrap()
                .is_some(),
            "category interruption must preserve the exact durable row"
        );
        release.notify_waiters();
        supervisor.root_cancellation_token().cancel();
    }

    #[tokio::test]
    async fn leadership_loss_during_category_wait_parks_exact_claim() {
        let (coordination, publisher) = test_coordination(true);
        let (workqueue, node_local, supervisor, release) =
            capacity_blocked_workqueue(Some(coordination)).await;
        workqueue.start().await.unwrap();
        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "leadership-wait".to_string(),
                "uid-leadership".to_string(),
                Duration::ZERO,
            )
            .await
            .unwrap();
        let initial_due = peek_runtime_work(node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let leased_due = wait_for_claimed_due(&node_local, initial_due).await;
        publisher.send_replace(TestCoordinationState {
            local: false,
            generation: 2,
        });
        wait_for_parked_due(&node_local, leased_due).await;
        assert!(
            claim_runtime_work(node_local.pod_workqueue().as_ref(), i64::MAX)
                .await
                .unwrap()
                .is_some(),
            "leadership loss must preserve the exact durable row"
        );
        release.notify_waiters();
        supervisor.root_cancellation_token().cancel();
    }

    #[tokio::test]
    async fn spawn_refusal_parks_claimed_row_before_reconciler_exits() {
        let persistence = Arc::new(SpawnRefusalPersistence {
            claim_entered: Notify::new(),
            release_claim: Notify::new(),
            requeued: Notify::new(),
            requeue_count: AtomicUsize::new(0),
        });
        let workqueue =
            probe_workqueue_for_persistence(SpawnRefusalPersistenceAdapter(persistence.clone()))
                .await;
        workqueue
            .ensure_reconciler_started_for_tests()
            .await
            .unwrap();
        persistence.claim_entered.notified().await;

        let supervisor = workqueue.supervisor_for_tests();
        let shutdown =
            tokio::spawn(async move { supervisor.shutdown(Duration::from_secs(2)).await });
        tokio::task::yield_now().await;
        persistence.release_claim.notify_one();
        tokio::time::timeout(Duration::from_secs(1), persistence.requeued.notified())
            .await
            .expect("spawn refusal must park the claimed row");
        assert_eq!(persistence.requeue_count.load(Ordering::Relaxed), 1);
        let report = shutdown.await.unwrap();
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn namespace_attempt_ceiling_acks_but_pod_attempt_ceiling_requeues() {
        let (workqueue, db, node_local) = test_workqueue().await;

        enqueue_runtime_work(
            node_local.pod_workqueue().as_ref(),
            klights_node_store::PodWorkqueueKind::Namespace,
            &PodIdentity::new("", "terminating", "uid-namespace"),
            json!({}),
            MAX_ATTEMPTS,
            0,
            None,
        )
        .await
        .unwrap();
        let due = peek_runtime_work(node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let namespace = claim_runtime_work(node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        workqueue
            .clone()
            .run_retry_for_tests(namespace, workqueue.current_test_leader_lease())
            .await;
        assert!(
            peek_runtime_work(node_local.pod_workqueue().as_ref())
                .await
                .unwrap()
                .is_none()
        );

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "never-dead-letter",
            pod_with_uid("never-dead-letter", "uid-pod", true),
        )
        .await
        .unwrap();
        enqueue_runtime_work(
            node_local.pod_workqueue().as_ref(),
            klights_node_store::PodWorkqueueKind::Pod,
            &PodIdentity::new("default", "never-dead-letter", "uid-pod"),
            json!({}),
            MAX_ATTEMPTS,
            0,
            None,
        )
        .await
        .unwrap();
        let due = peek_runtime_work(node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let pod = claim_runtime_work(node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        workqueue
            .clone()
            .run_retry_for_tests(pod, workqueue.current_test_leader_lease())
            .await;
        let retried = claim_runtime_work(node_local.pod_workqueue().as_ref(), i64::MAX)
            .await
            .unwrap()
            .expect("Pod work must remain durable beyond the namespace attempt ceiling");
        assert_eq!(retried.kind, PodWorkqueueKind::Pod);
        assert_eq!(retried.attempt_count, MAX_ATTEMPTS + 1);
    }

    #[tokio::test]
    async fn leadership_gain_discovers_terminating_unbound_pod_without_local_queue_row() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let persistence = test_persistence(
            &db,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let store = persistence.store;
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = super::super::test_node_local_store(supervisor.clone()).await;
        let (coordination, leader_tx) = test_coordination(false);
        let workqueue = PodWorkqueue::new_leader(
            store,
            crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                node_local.pod_workqueue(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            ),
            supervisor.clone(),
            SideEffectMetrics::new(),
            persistence.unscheduled_deletion,
            coordination,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "handoff",
            pod_with_uid_on_node("handoff", "uid-handoff", true, ""),
        )
        .await
        .unwrap();

        workqueue.start().await.unwrap();
        supervisor
            .sleep(
                "leadership_handoff_follower_quiet",
                Duration::from_millis(25),
            )
            .await
            .unwrap();
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "handoff")
                .await
                .unwrap()
                .is_some(),
            "follower must not process or lose the terminating Pod"
        );
        assert!(
            peek_runtime_work(node_local.pod_workqueue().as_ref())
                .await
                .unwrap()
                .is_none()
        );

        leader_tx.send_replace(TestCoordinationState {
            local: true,
            generation: 2,
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if db
                    .get_resource("v1", "Pod", Some("default"), "handoff")
                    .await
                    .unwrap()
                    .is_none()
                {
                    break;
                }
                supervisor
                    .sleep("leadership_handoff_wait", Duration::from_millis(10))
                    .await
                    .unwrap();
            }
        })
        .await
        .expect("new leader must discover and finalize the unbound Pod");
    }

    fn test_router(
        supervisor: &Arc<klights_supervisor::TaskSupervisor>,
        executor: Arc<dyn PodWorkExecutor>,
    ) -> Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter> {
        let registry = Arc::new(
            klights_kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(executor.clone())),
            ),
        );
        Arc::new(
            klights_kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry, executor,
            ),
        )
    }

    fn pod_with_uid_on_node(
        name: &str,
        uid: &str,
        deleting: bool,
        node_name: &str,
    ) -> serde_json::Value {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app", "image": "busybox"}]
            }
        });
        if deleting {
            pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        }
        pod
    }

    fn pod_with_uid(name: &str, uid: &str, deleting: bool) -> serde_json::Value {
        pod_with_uid_on_node(name, uid, deleting, "node-a")
    }

    fn unscheduled_pod_with_uid(name: &str, uid: &str, deleting: bool) -> serde_json::Value {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid
            },
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}]
            }
        });
        if deleting {
            pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        }
        pod
    }

    #[tokio::test]
    async fn deferred_pod_delete_removes_unscheduled_terminating_pod_directly() {
        // HR#11 exception: an unscheduled Pod (never bound to a node) has no
        // kubelet actor; the leader removes the row directly.
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "unscheduled",
            unscheduled_pod_with_uid("unscheduled", "uid-unsched", true),
        )
        .await
        .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "unscheduled".to_string(),
                "uid-unsched".to_string(),
                None,
            )
            .await
            .expect("unscheduled terminating Pod delete must succeed without an actor");

        assert!(
            db.get_resource("v1", "Pod", Some("default"), "unscheduled")
                .await
                .unwrap()
                .is_none(),
            "unscheduled terminating Pod row must be removed so its namespace can finalize"
        );
    }

    #[tokio::test]
    async fn stale_lease_cannot_delete_unscheduled_pod_after_demote_promote_aba() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let persistence = test_persistence(
            &db,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let deletion = persistence.unscheduled_deletion;
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "aba-unscheduled",
                unscheduled_pod_with_uid("aba-unscheduled", "uid-aba", true),
            )
            .await
            .unwrap();
        let (coordination, publisher) = test_coordination(true);
        let lease = coordination
            .try_acquire(ControllerScope::Cluster)
            .expect("initial leader lease");
        let reached_effect_boundary = Arc::new(tokio::sync::Notify::new());
        let resume_effect = Arc::new(tokio::sync::Notify::new());
        let request = UnscheduledPodDeletionRequest::try_new(
            PodIdentity::new("default", "aba-unscheduled", "uid-aba"),
            created.resource_version,
        )
        .unwrap();

        let operation = klights_leader_api::scope_controller_lease(coordination, lease, {
            let reached_effect_boundary = reached_effect_boundary.clone();
            let resume_effect = resume_effect.clone();
            async move {
                reached_effect_boundary.notify_one();
                resume_effect.notified().await;
                deletion.delete_unscheduled_pod(request).await
            }
        });
        let transition = async {
            reached_effect_boundary.notified().await;
            publisher.send_replace(TestCoordinationState {
                local: false,
                generation: 2,
            });
            publisher.send_replace(TestCoordinationState {
                local: true,
                generation: 3,
            });
            resume_effect.notify_one();
        };
        let (result, ()) = tokio::join!(operation, transition);

        assert!(matches!(
            result,
            Err(UnscheduledPodDeletionError::Unavailable { .. })
        ));
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "aba-unscheduled")
                .await
                .unwrap()
                .is_some(),
            "a stale leader lease must not reach the UID/RV delete CAS"
        );
    }

    #[tokio::test]
    async fn non_leader_workqueue_cannot_remove_unscheduled_pod_row() {
        let (workqueue, db, _node_local) = test_non_leader_workqueue().await;
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "worker-unscheduled",
                unscheduled_pod_with_uid("worker-unscheduled", "uid-worker", true),
            )
            .await
            .unwrap();

        let error = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "worker-unscheduled".to_string(),
                "uid-worker".to_string(),
                None,
            )
            .await
            .expect_err("a non-leader workqueue must not own unscheduled row removal");

        assert!(
            error
                .to_string()
                .contains("leader unscheduled-delete capability"),
            "unexpected error: {error:#}"
        );
        let live = db
            .get_resource("v1", "Pod", Some("default"), "worker-unscheduled")
            .await
            .unwrap()
            .expect("non-leader workqueue must preserve the Pod row");
        assert_eq!(live.uid, "uid-worker");
        assert_eq!(
            live.resource_version, created.resource_version,
            "denied non-leader deletion must not advance resourceVersion"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_for_kubelet_cleanup_while_uid_still_exists() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid("same-name", "uid-old", true),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("deferred workqueue must retry while kubelet-owned Pod cleanup is pending");

        assert!(
            err.to_string().contains("waiting for kubelet cleanup"),
            "unexpected error: {err:#}"
        );
        let live = db
            .get_resource("v1", "Pod", Some("default"), "same-name")
            .await
            .unwrap()
            .expect(
                "terminating Pod must remain until actor finalization confirms runtime cleanup",
            );
        assert_eq!(live.uid, "uid-old");
    }

    #[tokio::test]
    async fn deferred_pod_delete_wakes_local_actor_for_live_uid() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid_on_node("same-name", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("same-UID live Pod should keep durable reminder until actor deletes row");
        assert!(
            err.to_string().contains("waiting for kubelet cleanup"),
            "unexpected error: {err:#}"
        );

        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("deferred delete should wake the local lifecycle actor");
    }

    #[tokio::test]
    async fn terminating_pod_is_finalized_even_when_live_watch_event_is_dropped() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "watch-dropped",
            pod_with_uid_on_node("watch-dropped", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "watch-dropped".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("durable reminder must retry while actor-owned cleanup is pending");
        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("durable workqueue reminder should wake the actor without a live watch event");

        let finalization = test_persistence(
            &db,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let outcome = finalization
            .bound_finalization
            .finalize_bound_pod(
                klights_pod_api::BoundPodFinalizationRequest::try_new(PodIdentity::new(
                    "default",
                    "watch-dropped",
                    "uid-old",
                ))
                .unwrap(),
            )
            .await
            .expect("actor-owned UID finalization should remove the row");
        assert_eq!(
            outcome,
            klights_pod_api::BoundPodFinalizationOutcome::Removed
        );
        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "watch-dropped".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect("once actor finalization removed the row, the reminder completes");
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "watch-dropped")
                .await
                .unwrap()
                .is_none(),
            "Pod row must be gone only after actor-owned UID finalization"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_is_uid_bound_and_preserves_replacement_pod() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid("same-name", "uid-new", false),
        )
        .await
        .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect(
                "stale deferred delete for old UID should complete without touching replacement",
            );

        let live = db
            .get_resource("v1", "Pod", Some("default"), "same-name")
            .await
            .unwrap()
            .expect("replacement Pod must not be deleted by stale deferred work");
        assert_eq!(live.uid, "uid-new");
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_for_remote_actor_owned_finalization() {
        // Remote-targeted deferred delete must not hard-delete the Pod row from
        // the leader. The worker's Pod watch/actor owns finalization; the
        // leader-side workqueue only keeps a UID-bound reminder alive until
        // that actor-owned finalization removes the row.
        let (workqueue, db, _node_local) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote-targeted delete must keep retrying until the row is removed");
        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                executor.stop_seen.notified(),
            )
            .await
            .is_err(),
            "remote-targeted delete should not wake local actor"
        );
    }

    #[tokio::test]
    async fn remote_pod_workqueue_resignals_gc_delete_on_retry() {
        let (workqueue, db, _node_local) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote Pod should keep retrying until actor removes row");

        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            sink.calls().await,
            vec![(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string()
            )],
            "remote retry must re-signal the UID-bound GC delete path"
        );
    }

    #[tokio::test]
    async fn remote_pod_resignal_throttled_to_every_30_seconds() {
        let (workqueue, db, _node_local) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());
        let mut payload = pod_delete_target_payload(Some("node-b"));

        workqueue
            .run_pod_delete_full_with_target_node_and_payload_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                100_000,
            )
            .await
            .expect_err("remote Pod should retry after re-signal");
        assert_eq!(sink.calls().await.len(), 1);

        workqueue
            .run_pod_delete_full_with_target_node_and_payload_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                110_000,
            )
            .await
            .expect_err("remote Pod should retry while re-signal is throttled");
        assert_eq!(
            sink.calls().await.len(),
            1,
            "second retry inside throttle window must not re-signal"
        );

        workqueue
            .run_pod_delete_full_with_target_node_and_payload_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                130_000,
            )
            .await
            .expect_err("remote Pod should retry after throttle window re-signal");
        assert_eq!(
            sink.calls().await.len(),
            2,
            "retry at the 30s boundary should re-signal again"
        );
    }

    #[tokio::test]
    async fn remote_pod_without_deletion_timestamp_is_marked_on_first_retry() {
        let (workqueue, db, _node_local) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", false, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote Pod should retry until target actor finalizes it");

        assert_eq!(
            sink.calls().await,
            vec![(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string()
            )],
            "first remote retry must request the UID-bound delete mark even before deletionTimestamp is present"
        );
    }

    #[tokio::test]
    async fn remote_pod_resignal_is_uid_bound_and_self_extinguishes_when_row_removed() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-new", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("stale UID reminder must complete without touching replacement");
        assert!(
            sink.calls().await.is_empty(),
            "stale UID must not re-signal deletion for a replacement Pod"
        );

        db.delete_resource("v1", "Pod", Some("default"), "remote-pod")
            .await
            .unwrap();
        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("missing row means actor-owned finalization completed");
        assert!(
            sink.calls().await.is_empty(),
            "removed row should self-extinguish without re-signaling"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_for_remote_pod_completes_once_row_removed() {
        // Durability backstop: the remote deferred delete keeps the workqueue
        // entry alive (Err -> retry) while the Pod row still exists, and
        // completes (Ok) only once the actor-owned path has removed the row.
        let (workqueue, db, _node_local) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        // Row still present: must bail to retry (not complete).
        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("must retry while the remote Pod row still exists");

        // Simulate the row finally being removed by the remote actor. The next
        // retry must complete cleanly.
        db.delete_resource("v1", "Pod", Some("default"), "remote-pod")
            .await
            .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("once the remote Pod row is gone the deferred delete completes");
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_when_local_node_unknown_with_target() {
        // When local node is unknown but a target is specified, the deferred
        // delete still must not fall back to a leader-side hard-delete. It
        // keeps retrying until the target actor removes the row.
        let (workqueue, db, _node_local) = test_workqueue().await;
        workqueue.set_local_node_name_for_tests(None);

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "orphaned-pod",
            pod_with_uid_on_node("orphaned-pod", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "orphaned-pod".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("deferred delete with unknown local node must retry until the row is gone");
        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_skips_if_local_node_unknown_when_pod_has_node_name() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        workqueue.set_local_node_name_for_tests(None);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "orphaned-pod",
            pod_with_uid_on_node("orphaned-pod", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "orphaned-pod".to_string(),
                "uid-old".to_string(),
                None,
            )
            .await
            .expect_err("deferred delete should not run without local node identity");
        assert!(
            err.to_string().contains("local"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn namespace_termination_enqueues_all_terminating_pods() {
        // Regression: namespace termination must enqueue workqueue entries
        // for ALL terminating pods, including those on remote nodes. Remote
        // entries are actor-owned reminders, not leader hard-deletes.
        let (workqueue, db, _node_local) = test_workqueue().await;
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "local-pod",
            pod_with_uid_on_node("local-pod", "local-uid", true, "node-a"),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "remote-uid", true, "node-b"),
        )
        .await
        .unwrap();
        // Exercise the enqueue primitive without starting its concurrent
        // consumer; the public wrapper intentionally wakes that consumer, so
        // directly claiming its rows would race the behavior under test.
        workqueue
            .enqueue_actor_deletes_for_terminating_namespace_pods_for_tests("terminating-ns")
            .await
            .unwrap();

        let mut claimed_rows = Vec::new();
        loop {
            let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), i64::MAX)
                .await
                .unwrap();
            if let Some(row) = row {
                claimed_rows.push(row);
                continue;
            }
            break;
        }

        assert_eq!(
            claimed_rows.len(),
            2,
            "namespace termination should enqueue workqueue entries for both local and remote pods"
        );
        let names: std::collections::HashSet<&str> =
            claimed_rows.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains("local-pod"));
        assert!(names.contains("remote-pod"));
    }

    #[tokio::test]
    async fn namespace_actor_reminder_preserves_absolute_pod_deadline() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-08T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let fixed_now_ms = now.timestamp_millis();
        let (workqueue, db, node_local) =
            test_workqueue_at(Arc::new(FixedRuntimeClock(fixed_now_ms))).await;
        let mut pod = pod_with_uid_on_node("deadline-pod", "deadline-uid", true, "node-a");
        pod["metadata"]["deletionTimestamp"] =
            json!(klights_cluster_core::k8s_time::format_legacy_timestamp(
                now + chrono::Duration::seconds(17),
            ));
        db.create_resource("v1", "Pod", Some("terminating-ns"), "deadline-pod", pod)
            .await
            .unwrap();

        let system_before_ms = now_ms();
        workqueue
            .enqueue_actor_deletes_for_terminating_namespace_pods_for_tests("terminating-ns")
            .await
            .unwrap();
        let system_after_ms = now_ms();

        let due_ms = peek_runtime_work(node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .expect("namespace cleanup must enqueue a durable reminder");
        assert!(
            (system_before_ms + 17_000..=system_after_ms + 17_000).contains(&due_ms),
            "namespace cleanup must retain the Pod's 17-second remaining deletion deadline"
        );
        assert!(
            claim_runtime_work(node_local.pod_workqueue().as_ref(), due_ms - 1)
                .await
                .unwrap()
                .is_none(),
            "durable reminder is a backstop, not an immediate replacement for the actor timer"
        );
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_records_uid_bound_retry_row() {
        let (workqueue, _db, _node_local) = test_workqueue().await;
        let before = now_ms();

        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
            )
            .await
            .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .expect("deferred delete must be recorded in the durable workqueue");
        assert!(
            due >= before + 40,
            "deferred delete should not be due before the requested delay"
        );
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "same-name");
        assert_eq!(row.uid, "uid-old");
        assert!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .is_none()
                || row
                    .payload
                    .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                    .is_some_and(|value| value.is_null()),
            "default deferred delete should not set a target node"
        );
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_with_target_node_records_target_in_payload() {
        let (workqueue, _db, _node_local) = test_workqueue().await;
        let before = now_ms();

        workqueue
            .enqueue_deferred_delete_with_target_node(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
                Some("node-a".to_string()),
            )
            .await
            .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .expect("deferred delete must be recorded in the durable workqueue");
        assert!(
            due >= before + 40,
            "deferred delete should not be due before the requested delay"
        );
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "same-name");
        assert_eq!(row.uid, "uid-old");
        assert_eq!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .and_then(|value| value.as_str()),
            Some("node-a")
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_with_remote_target_retries_without_local_actor_or_outbox() {
        // Regression test: a deferred delete for a pod on a remote node
        // must not be silently dropped, must not wake the local actor, and
        // must not hard-delete through a leader-side outbox.
        let (workqueue, db, _node_local) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        enqueue_runtime_work(
            _node_local.pod_workqueue().as_ref(),
            klights_node_store::PodWorkqueueKind::Pod,
            &klights_types::PodIdentity::new("default", "remote-pod", "uid-old"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        let lease = workqueue.current_test_leader_lease();
        workqueue.clone().run_retry_for_tests(row, lease).await;

        // The local actor must NOT be woken for a remote pod.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                executor.stop_seen.notified(),
            )
            .await
            .is_err(),
            "remote-targeted delete should not wake local actor"
        );

        // The pod row still exists, so the workqueue entry must be RE-ENQUEUED
        // for retry rather than completed. The target worker's actor is the
        // only terminal delete owner for a picked-up Pod.
        assert!(
            claim_runtime_work(_node_local.pod_workqueue().as_ref(), i64::MAX)
                .await
                .unwrap()
                .is_some(),
            "remote-targeted deferred delete must retry until the cluster row is removed"
        );
    }

    #[tokio::test]
    async fn worker_finalizes_pod_from_durable_intent_without_restart() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        workqueue.set_local_node_name_for_tests(Some("node-b".to_string()));
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "worker-pod",
            pod_with_uid_on_node("worker-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-b".to_string());

        enqueue_runtime_work(
            _node_local.pod_workqueue().as_ref(),
            klights_node_store::PodWorkqueueKind::Pod,
            &klights_types::PodIdentity::new("default", "worker-pod", "uid-old"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                row.namespace.clone(),
                row.name.clone(),
                row.uid.clone(),
                row.payload
                    .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string),
            )
            .await
            .expect_err("durable intent should retry until actor-owned cleanup removes the row");
        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("running worker must consume durable intent and wake its actor");

        let finalization = test_persistence(
            &db,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let outcome = finalization
            .bound_finalization
            .finalize_bound_pod(
                klights_pod_api::BoundPodFinalizationRequest::try_new(PodIdentity::new(
                    "default",
                    "worker-pod",
                    "uid-old",
                ))
                .unwrap(),
            )
            .await
            .expect("worker actor-owned finalization should remove the row");
        assert_eq!(
            outcome,
            klights_pod_api::BoundPodFinalizationOutcome::Removed
        );
        workqueue
            .run_pod_delete_full_with_target_node_for_tests(
                "default".to_string(),
                "worker-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("durable intent must self-extinguish once actor finalization removed the row");
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_does_not_skip_remote_target() {
        // Regression test: enqueue_deferred_delete_with_target_node must
        // NOT silently drop entries for remote-targeted pods.
        let (workqueue, _db, _node_local) = test_workqueue().await;

        workqueue
            .enqueue_deferred_delete_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
                Some("node-b".to_string()),
            )
            .await
            .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap();
        assert!(
            due.is_some(),
            "remote-targeted deferred delete must be enqueued in the workqueue"
        );
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), i64::MAX)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "remote-pod");
        assert_eq!(row.uid, "uid-old");
        assert_eq!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .and_then(|value| value.as_str()),
            Some("node-b")
        );
    }

    #[tokio::test]
    async fn namespace_termination_enqueues_uid_bound_delete_for_unscheduled_pod() {
        let (workqueue, db, _node_local) = test_workqueue().await;
        struct DirectNamespaceTermination {
            store: Arc<crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore>,
            metrics: Arc<SideEffectMetrics>,
        }
        impl klights_reconcile_api::NamespaceTerminationSink for DirectNamespaceTermination {
            fn reconcile_namespace_termination(
                &self,
                request: klights_reconcile_api::NamespaceTerminationRequest,
            ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
                Box::pin(async move {
                    let uid = request.expected_uid.ok_or_else(|| {
                        klights_reconcile_api::ReconcileSinkError::unavailable(
                            "namespace test request requires UID",
                        )
                    })?;
                    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
                        self.store.as_ref(),
                        &request.namespace,
                        &uid,
                        self.metrics.as_ref(),
                        klights_supervisor::SystemWallClock::now_utc(),
                    )
                    .await
                    .map(|outcome| match outcome {
                        k8s_native_service::NamespaceTerminationOutcome::Finalized => {
                            klights_reconcile_api::NamespaceTerminationOutcome::Finalized
                        }
                        k8s_native_service::NamespaceTerminationOutcome::StillPending => {
                            klights_reconcile_api::NamespaceTerminationOutcome::StillPending
                        }
                    })
                    .map_err(|error| {
                        klights_reconcile_api::ReconcileSinkError::unavailable(format!("{error:?}"))
                    })
                })
            }
        }
        workqueue.set_namespace_termination_sink(Arc::new(DirectNamespaceTermination {
            store: crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new_with_commands(
                db.focused_read_store(),
                db.focused_read_store(),
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                    Arc::new(db.clone()),
                    Arc::new(db.clone()),
                    db.focused_read_store(),
                ),
            ),
            metrics: SideEffectMetrics::new(),
        }));
        db.create_namespace(
            "terminating-ns",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "terminating-ns", "uid": "ns-uid"},
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Active"}
            }),
        )
        .await
        .unwrap();
        let ns = db
            .get_namespace("terminating-ns")
            .await
            .unwrap()
            .expect("namespace exists");
        let mut ns_data = std::sync::Arc::unwrap_or_clone(ns.data);
        ns_data["metadata"]["deletionTimestamp"] = json!("2026-05-16T00:00:00Z");
        ns_data["status"]["phase"] = json!("Terminating");
        db.update_namespace("terminating-ns", ns_data, ns.resource_version)
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "unscheduled",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "terminating-ns",
                    "name": "unscheduled",
                    "uid": "pod-uid"
                },
                "spec": {
                    "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10.1"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_namespace_termination_for_tests("terminating-ns".to_string(), "ns-uid".to_string())
            .await
            .expect_err("namespace should stay pending until actor-owned pod deletion finalizes");
        assert!(
            err.to_string().contains("still terminating"),
            "unexpected namespace retry error: {err:#}"
        );

        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), now_ms() + 1_000)
            .await
            .unwrap()
            .expect("namespace termination must enqueue actor-owned Pod delete work");
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "terminating-ns");
        assert_eq!(row.name, "unscheduled");
        assert_eq!(row.uid, "pod-uid");

        let pod = db
            .get_resource("v1", "Pod", Some("terminating-ns"), "unscheduled")
            .await
            .unwrap()
            .expect("Pod row must remain until actor finalization");
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some(),
            "namespace termination should only mark the Pod terminating"
        );
    }

    #[tokio::test]
    async fn remote_pod_with_finalizers_is_not_hard_deleted_by_leader_workqueue() {
        // Regression: namespace deletion should NOT hard-delete a remote pod
        // that has remaining finalizers. Remote picked-up Pods remain
        // actor-owned regardless of finalizer state.
        //
        // Upstream test: [sig-api-machinery] OrderedNamespaceDeletion
        // "namespace deletion should delete pod first" — the test creates a
        // pod with a custom finalizer, deletes the namespace, and expects the
        // pod to still exist (with deletionTimestamp) while the ConfigMap
        // does NOT have deletionTimestamp.
        let (workqueue, db, _node_local) = test_workqueue().await;

        let mut remote_pod = pod_with_uid_on_node("finalizer-pod", "uid-f", true, "node-b");
        remote_pod["metadata"]["finalizers"] = json!(["test-finalizer"]);
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "finalizer-pod",
            remote_pod,
        )
        .await
        .unwrap();

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        // Enqueue as if namespace termination did it.
        enqueue_runtime_work(
            _node_local.pod_workqueue().as_ref(),
            klights_node_store::PodWorkqueueKind::Pod,
            &klights_types::PodIdentity::new("terminating-ns", "finalizer-pod", "uid-f"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = peek_runtime_work(_node_local.pod_workqueue().as_ref())
            .await
            .unwrap()
            .unwrap();
        let row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), due)
            .await
            .unwrap()
            .unwrap();
        let lease = workqueue.current_test_leader_lease();
        workqueue.clone().run_retry_for_tests(row, lease).await;

        // The pod must STILL exist in the datastore — remote leader workqueue
        // retries are actor wakeup/reminder state only.
        let pod = db
            .get_resource("v1", "Pod", Some("terminating-ns"), "finalizer-pod")
            .await
            .unwrap();
        assert!(
            pod.is_some(),
            "remote pod with finalizers must NOT be hard-deleted by leader workqueue"
        );
        let pod_data = pod.unwrap();
        assert!(
            pod_data.data.pointer("/metadata/finalizers").is_some(),
            "pod must still have its finalizers"
        );

        // The workqueue entry must be re-enqueued for retry (not completed),
        // because the pod still has finalizers.
        let retry_row = claim_runtime_work(_node_local.pod_workqueue().as_ref(), i64::MAX)
            .await
            .unwrap();
        assert!(
            retry_row.is_some(),
            "remote pod with finalizers must be re-enqueued for retry"
        );
    }

    /// The reconciler loop must respond to root cancellation at every wait
    /// point — bare `wake.notified().await`, sleep-until-due, and
    /// category-free-wait. Without cancellation branches, shutdown can
    /// be delayed by the sleep duration.
    #[tokio::test]
    async fn reconciler_exits_on_root_cancellation() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let store = Arc::new(crate::bootstrap::pod_repository_composition::new_pod_store(
            Arc::new(db.clone()),
            Arc::new(db.clone()),
            db.focused_read_store(),
            db.focused_read_store(),
        ));
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let persistence_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = super::super::test_node_local_store(persistence_supervisor).await;
        let cancel = supervisor.root_cancellation_token();
        let metrics = SideEffectMetrics::new();
        let workqueue = PodWorkqueue::new(
            store,
            crate::bootstrap::pod_repository_composition::test_workqueue_persistence(
                node_local.pod_workqueue(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            ),
            supervisor.clone(),
            metrics,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );

        // Enqueue a deferred delete to trigger reconciler start.
        // The reconciler will fail to process it (no lifecycle_router set)
        // and loop with error backoff, which is sufficient for testing
        // cancellation responsiveness.
        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "test-pod".to_string(),
                "uid-1".to_string(),
                Duration::from_millis(5000),
            )
            .await
            .unwrap();

        // Wait for the reconciler task to appear.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if supervisor
                .active_tasks(Some(TaskCategory::Background))
                .iter()
                .any(|t| t.name == "pod_workqueue_reconciler")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reconciler task did not appear"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Cancel root and verify the reconciler exits quickly.
        cancel.cancel();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if supervisor
                .active_tasks(Some(TaskCategory::Background))
                .iter()
                .all(|t| t.name != "pod_workqueue_reconciler")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reconciler did not exit within 3s of cancellation"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            peek_runtime_work(node_local.pod_workqueue().as_ref())
                .await
                .unwrap()
                .is_some(),
            "root cancellation must leave delayed durable work pending"
        );
    }
}
