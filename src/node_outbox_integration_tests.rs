use std::sync::Arc;

pub use klights_kubelet::node_outbox::{
    DispatchOutcome, Outbox, OutboxCommand, OutboxDispatcher, OutboxSubject,
};
use klights_leader_api::LeaderOutboxDelivery;

const MAX_BACKOFF_MS: i64 = klights_kubelet::node_outbox::MAX_BACKOFF_MS_FOR_INTEGRATION_TEST;
const MAX_OUTBOX_ATTEMPTS: i64 =
    klights_kubelet::node_outbox::MAX_OUTBOX_ATTEMPTS_FOR_INTEGRATION_TEST;

fn now_ms() -> i64 {
    klights_supervisor::SystemWallClock::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn adaptive_backoff_bounds(attempt: i64, rtt_est_ms: i64) -> (i64, i64) {
    klights_kubelet::node_outbox::adaptive_backoff_bounds_for_integration_test(attempt, rtt_est_ms)
}

fn adaptive_jittered_backoff_ms(attempt: i64, idempotency_key: &str, rtt_est_ms: i64) -> i64 {
    klights_kubelet::node_outbox::adaptive_jittered_backoff_ms_for_integration_test(
        attempt,
        idempotency_key,
        rtt_est_ms,
    )
}

trait OutboxTestConstructor {
    fn new(node_db: crate::datastore::node_local::NodeLocalStores) -> Self;
    fn with_notify(
        node_db: crate::datastore::node_local::NodeLocalStores,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self;
}

impl OutboxTestConstructor for Outbox {
    fn new(node_db: crate::datastore::node_local::NodeLocalStores) -> Self {
        crate::outbox_test_support::outbox_from_node_db(node_db)
    }

    fn with_notify(
        node_db: crate::datastore::node_local::NodeLocalStores,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        crate::outbox_test_support::outbox_with_notify(node_db, notify)
    }
}

trait OutboxDispatcherTestConstructor {
    fn for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
    ) -> Self;
    fn for_tests_with_rtt_estimator(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        rtt: Arc<klights_types::RttEstimator>,
    ) -> Self;
    fn for_tests_with_lease_renewal(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        lease_ms: i64,
    ) -> Self;
    fn batch_mode_for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        batch_size: usize,
    ) -> Self;
    fn production_for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self;
}

impl OutboxDispatcherTestConstructor for OutboxDispatcher {
    fn for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
    ) -> Self {
        crate::outbox_test_support::dispatcher_for_tests(node_db, client)
    }

    fn for_tests_with_rtt_estimator(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        rtt: Arc<klights_types::RttEstimator>,
    ) -> Self {
        crate::outbox_test_support::dispatcher_with_rtt_estimator(node_db, client, rtt)
    }

    fn for_tests_with_lease_renewal(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        lease_ms: i64,
    ) -> Self {
        crate::outbox_test_support::dispatcher_for_tests(node_db, client)
            .with_lease_renewal_for_test(supervisor, lease_ms)
    }

    fn batch_mode_for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        batch_size: usize,
    ) -> Self {
        crate::outbox_test_support::dispatcher_for_tests(node_db, client)
            .with_batch_mode(batch_size)
    }

    fn production_for_tests(
        node_db: crate::datastore::node_local::NodeLocalStores,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        crate::outbox_test_support::dispatcher_with_notify(node_db, client, notify)
            .with_batch_mode(klights_kubelet::node_outbox::PRODUCTION_DISPATCH_BATCH_SIZE)
    }
}

#[cfg(test)]
#[path = "node_outbox_integration_tests/tests/batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "node_outbox_integration_tests/tests/dead_letter_tests.rs"]
mod dead_letter_tests;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_leader_api::{
        LeaderOutboxDelivery, OutboxDeliveryError as OutboxApplyError, OutboxDeliveryFuture,
        OutboxDeliveryRequest, OutboxDeliveryResult as OutboxApplyResult,
    };
    use std::collections::HashSet;
    use tokio::sync::{Mutex, Notify};

    use crate::datastore::backend_kind::BackendKind;
    use crate::datastore::node_local::{LegacyDeliveryTestStore as _, NodeLocalStores, selector};
    use crate::outbox_test_support::OutboxPayload;
    use klights_cluster_core::ResourcePreconditions;
    use klights_cluster_core::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;
    use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    use super::{
        DispatchOutcome, Outbox, OutboxCommand, OutboxDispatcher,
        OutboxDispatcherTestConstructor as _, OutboxSubject, OutboxTestConstructor as _,
    };

    fn pod_status_classification() -> klights_node_store::OutboxClassification {
        klights_node_store::OutboxClassification::try_new(
            klights_node_store::OutboxPriority::Workload,
            klights_node_store::OutboxSupersedability::PodStatus,
            klights_node_store::TerminalDeleteClassification::NotTerminalDelete,
            klights_node_store::OutboxSequencePolicy::PerSubject,
        )
        .expect("valid Pod status classification")
    }

    #[tokio::test]
    async fn focused_outbox_checkpoint_preserves_pod_identity_rv_and_status_payload() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let checkpoint = klights_cluster_core::Resource {
            id: 7,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("tenant-a".to_string()),
            name: "worker-pod".to_string(),
            uid: "uid-worker-pod".to_string(),
            resource_version: 42,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "tenant-a",
                    "name": "worker-pod",
                    "uid": "uid-worker-pod",
                    "resourceVersion": "42"
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.50.8.9"
                }
            })),
        };

        klights_leader_api::NodeOutbox::record_pod_status_checkpoint(&outbox, &checkpoint, 1_234)
            .await
            .expect("record through focused outbox port");

        let stored = node_db
            .legacy_get_pod_status_checkpoint("uid-worker-pod")
            .await
            .expect("read checkpoint")
            .expect("checkpoint exists");
        assert_eq!(stored.pod_uid, "uid-worker-pod");
        assert_eq!(stored.namespace, "tenant-a");
        assert_eq!(stored.pod_name, "worker-pod");
        assert_eq!(stored.base_rv, 42);
        assert_eq!(stored.updated_ms, 1_234);
        assert_eq!(stored.status["phase"], "Running");
        assert_eq!(stored.status["podIP"], "10.50.8.9");
    }

    #[test]
    fn dispatcher_constructor_requires_only_the_focused_delivery_port() {
        let constructor: fn(NodeLocalStores, Arc<dyn LeaderOutboxDelivery>) -> OutboxDispatcher =
            OutboxDispatcher::for_tests;
        let _ = constructor;
    }

    #[derive(Default)]
    struct BlockingOutboxDelivery {
        started: Notify,
        release: Notify,
    }

    impl LeaderOutboxDelivery for BlockingOutboxDelivery {
        fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                OutboxApplyResult::try_applied(1)
            })
        }
    }

    #[tokio::test]
    async fn in_flight_delivery_renews_its_claim_lease_until_rpc_completion() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(BlockingOutboxDelivery::default());
        let lease_ms = 120;
        let dispatcher = Arc::new(OutboxDispatcher::for_tests_with_lease_renewal(
            node_db.clone(),
            client.clone(),
            supervisor(),
            lease_ms,
        ));
        let claimed_at = super::now_ms();
        outbox
            .enqueue_command(OutboxCommand::new(
                "lease-renewal",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/lease-renewal/uid-lease-renewal",
                    Some("default".to_string()),
                    "lease-renewal",
                    Some("uid-lease-renewal".to_string()),
                ),
                "uid-lease-renewal",
                pod_status_command("default", "lease-renewal", "uid-lease-renewal"),
                claimed_at,
            ))
            .await
            .expect("enqueue lease-renewal row");

        let dispatch = tokio::spawn({
            let dispatcher = dispatcher.clone();
            async move { dispatcher.dispatch_due_once(claimed_at).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), client.started.notified())
            .await
            .expect("delivery must start");

        tokio::time::sleep(std::time::Duration::from_millis(lease_ms as u64 * 2)).await;
        assert_eq!(
            node_db
                .legacy_requeue_expired_outbox_leases(super::now_ms())
                .await
                .expect("requeue expired leases while RPC is active"),
            0,
            "the active RPC must renew before its claim can expire"
        );

        client.release.notify_one();
        assert_eq!(
            dispatch
                .await
                .expect("join dispatch")
                .expect("dispatch row"),
            DispatchOutcome::Dispatched
        );
    }

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    async fn node_db() -> NodeLocalStores {
        selector::open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:outbox-test",
        )
        .await
        .expect("open node-local test db")
    }

    #[tokio::test]
    async fn outbox_runtime_observation_checkpoint_round_trips_by_uid() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db);

        outbox
            .record_runtime_observation_checkpoint(
                "uid-runtime-checkpoint",
                vec!["ctr-a".to_string(), "ctr-b".to_string()],
                2,
                1234,
            )
            .await
            .expect("record runtime observation checkpoint");

        let loaded = outbox
            .get_runtime_observation_checkpoint("uid-runtime-checkpoint")
            .await
            .expect("load runtime observation checkpoint")
            .expect("checkpoint exists");
        assert_eq!(loaded.pod_uid, "uid-runtime-checkpoint");
        assert_eq!(
            loaded.container_ids,
            vec!["ctr-a".to_string(), "ctr-b".to_string()]
        );
        assert_eq!(loaded.generation, 2);
        assert_eq!(loaded.updated_ms, 1234);

        outbox
            .delete_runtime_observation_checkpoint("uid-runtime-checkpoint")
            .await
            .expect("delete runtime observation checkpoint");
        assert!(
            outbox
                .get_runtime_observation_checkpoint("uid-runtime-checkpoint")
                .await
                .expect("load after delete")
                .is_none()
        );
    }

    /// A worker restart resets the in-memory stamp allocator, and the host
    /// wall clock can step backward across that restart (NTP correction / VM
    /// skew). The leader drops a status whose stamp regressed, so the stamp
    /// MUST stay strictly monotonic across restart regardless of the clock.
    /// The shared node-local handle plays the role of node.db surviving the
    /// restart; the second `Outbox` is the post-restart process.
    #[tokio::test]
    async fn status_stamp_stays_monotonic_across_restart_under_clock_regression() {
        let handle = node_db().await;

        let outbox1 = Outbox::with_notify(handle.clone(), Arc::new(tokio::sync::Notify::new()));
        let s1 = klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
            &outbox1, 1_000_000,
        )
        .await
        .unwrap();
        let s2 = klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
            &outbox1, 2_000_000,
        )
        .await
        .unwrap();
        assert!(s2 > s1, "stamps must increase while issuing: {s1} -> {s2}");

        // Restart: brand-new in-memory allocator over the SAME node-local store,
        // with a wall clock that has stepped backward below the last stamp.
        let outbox2 = Outbox::with_notify(handle.clone(), Arc::new(tokio::sync::Notify::new()));
        let s3 = klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
            &outbox2, 500_000,
        )
        .await
        .unwrap();
        assert!(
            s3 > s2,
            "stamp must stay strictly monotonic across restart even when the clock regresses: last={s2} after_restart={s3}"
        );

        // And it must keep advancing after the restart too.
        let s4 = klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
            &outbox2, 500_001,
        )
        .await
        .unwrap();
        assert!(
            s4 > s3,
            "post-restart stamps must keep increasing: {s3} -> {s4}"
        );
    }

    fn pod_status_command(namespace: &str, name: &str, uid: &str) -> StorageCommand {
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        }
    }

    fn lease_renew_command(node_name: &str, uid: &str) -> StorageCommand {
        StorageCommand::UpdateResource {
            api_version: "coordination.k8s.io/v1".to_string(),
            kind: "Lease".to_string(),
            namespace: Some("kube-node-lease".to_string()),
            name: node_name.to_string(),
            data: serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "namespace": "kube-node-lease",
                    "name": node_name,
                    "uid": uid
                },
                "spec": {
                    "holderIdentity": node_name,
                    "leaseDurationSeconds": 50,
                    "renewTime": "2026-05-25T13:15:21.000000Z"
                }
            }),
            expected_rv: 1,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: Some(1),
            },
        }
    }

    #[test]
    fn payload_round_trips_storage_command_as_protobuf() {
        let payload = OutboxPayload::from_command(pod_status_command("default", "web", "uid-1"));

        let bytes = payload.encode_protobuf().expect("encode payload");
        let decoded = OutboxPayload::decode_protobuf(&bytes).expect("decode payload");

        assert_eq!(decoded, payload);
    }

    #[derive(Default)]
    struct FakeApplyClient {
        calls: Mutex<Vec<String>>,
        responses: Mutex<Vec<Result<OutboxApplyResult, OutboxApplyError>>>,
    }

    impl FakeApplyClient {
        async fn push_response(&self, response: Result<OutboxApplyResult, OutboxApplyError>) {
            self.responses.lock().await.push(response);
        }

        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }
    }

    impl LeaderOutboxDelivery for FakeApplyClient {
        fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .await
                    .push(request.idempotency_key().to_string());
                self.responses
                    .lock()
                    .await
                    .pop()
                    .unwrap_or(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
            })
        }
    }

    struct IncompatibleCodecDelivery;

    impl LeaderOutboxDelivery for IncompatibleCodecDelivery {
        fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async {
                Err(OutboxApplyError::codec_incompatible(
                    klights_cluster_core::COMMAND_CODEC_VERSION - 1,
                    klights_cluster_core::COMMAND_CODEC_VERSION,
                ))
            })
        }
    }

    #[tokio::test]
    async fn successful_terminal_actor_delete_removes_uid_status_checkpoint() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        node_db
            .legacy_upsert_pod_status_checkpoint(
                "uid-terminal",
                "default",
                "terminal",
                7,
                serde_json::json!({"phase": "Failed"}),
                1_000,
            )
            .await
            .expect("seed UID status checkpoint");
        outbox
            .enqueue_command(OutboxCommand::new(
                "terminal-actor-delete",
                OutboxOperation::PodMetadata,
                OutboxSubject::new(
                    "v1/Pod/default/terminal/uid-terminal",
                    Some("default".to_string()),
                    "terminal",
                    Some("uid-terminal".to_string()),
                ),
                "uid-terminal",
                StorageCommand::FinalizeBoundPod {
                    namespace: "default".to_string(),
                    name: "terminal".to_string(),
                    pod_uid: "uid-terminal".to_string(),
                    node_name: "worker-a".to_string(),
                    observed_resource_version: 7,
                },
                1_000,
            ))
            .await
            .expect("enqueue terminal actor delete");

        let dispatcher =
            OutboxDispatcher::for_tests(node_db.clone(), Arc::new(FakeApplyClient::default()));
        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert!(
            node_db
                .legacy_get_pod_status_checkpoint("uid-terminal")
                .await
                .expect("read status checkpoint")
                .is_none(),
            "successful terminal actor delete must remove the UID status checkpoint"
        );
    }

    #[derive(Default)]
    struct IdempotentApplyClient {
        calls: Mutex<Vec<String>>,
        applied: Mutex<HashSet<String>>,
    }

    impl IdempotentApplyClient {
        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }

        async fn applied_keys(&self) -> HashSet<String> {
            self.applied.lock().await.clone()
        }
    }

    impl LeaderOutboxDelivery for IdempotentApplyClient {
        fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async move {
                let idempotency_key = request.idempotency_key().to_string();
                self.calls.lock().await.push(idempotency_key.clone());
                let mut applied = self.applied.lock().await;
                if applied.insert(idempotency_key) {
                    Ok(OutboxApplyResult::Applied {
                        applied_rv: applied.len() as i64,
                    })
                } else {
                    Ok(OutboxApplyResult::AlreadyApplied {
                        applied_rv: Some(applied.len() as i64),
                    })
                }
            })
        }
    }

    /// bug-grpc: records the maximum number of concurrently in-flight
    /// `apply_outbox` calls, sleeping briefly so overlapping calls are
    /// observable. Used to prove pipelined dispatch keeps > 1 RPC in
    /// flight (bounded by the batch window).
    #[derive(Default)]
    struct InFlightTrackingClient {
        current: std::sync::atomic::AtomicUsize,
        max: std::sync::atomic::AtomicUsize,
    }

    impl InFlightTrackingClient {
        fn max_in_flight(&self) -> usize {
            self.max.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl LeaderOutboxDelivery for InFlightTrackingClient {
        fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            Box::pin(async move {
                use std::sync::atomic::Ordering;
                let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
                self.max.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                self.current.fetch_sub(1, Ordering::SeqCst);
                Ok(OutboxApplyResult::Applied { applied_rv: 1 })
            })
        }
    }

    #[tokio::test]
    async fn dispatcher_survives_transient_apply_error() {
        // bug-grpc: a transient (Retryable) apply error must NOT propagate
        // out of `dispatch_due_once` (which would kill the run loop). The
        // row is backed off and redelivered on the next due pass.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        // Stack pops LIFO: this Retryable is returned first; the default
        // Ok is returned on the redispatch.
        client
            .push_response(Err(OutboxApplyError::Retryable("transient".to_string())))
            .await;
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "transient-key",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue");

        // First pass: apply fails Retryable -> must be reported as a
        // (non-fatal) Dispatched, not an Err that would crash run().
        assert_eq!(
            dispatcher
                .dispatch_due_once(20)
                .await
                .expect("transient apply error must not propagate"),
            DispatchOutcome::Dispatched
        );

        // The row is backed off with bounded jitter; advance to the actual wake and redeliver.
        let after_backoff = node_db
            .legacy_next_outbox_wake_ms(20)
            .await
            .expect("read next jittered wake")
            .expect("retry wake exists");
        let (backoff_lower, backoff_upper) =
            super::adaptive_backoff_bounds(0, klights_types::RTT_DEFAULT_MS);
        assert!(
            (20 + backoff_lower..=20 + backoff_upper).contains(&after_backoff),
            "retry wake must stay inside the first-attempt adaptive jitter window \
             [{},{}]: {after_backoff}",
            20 + backoff_lower,
            20 + backoff_upper
        );
        assert_eq!(
            dispatcher
                .dispatch_due_once(after_backoff)
                .await
                .expect("redispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await.len(), 2, "row must be retried once");
        assert!(
            node_db
                .legacy_claim_next_due_outbox(after_backoff + 1, 1_000, "check-empty")
                .await
                .expect("claim")
                .is_none(),
            "row must be completed after the successful retry"
        );
    }

    #[tokio::test]
    async fn dispatcher_survives_complete_outbox_race() {
        // bug-grpc: when a slow WAN apply outlives its claim lease, the
        // post-RPC complete races on a stale token (complete returns
        // false). This must be non-fatal — the row stays claimable and is
        // redelivered (the leader replies AlreadyApplied), then completes.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(IdempotentApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "race-key",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue");

        // Simulate an in-flight slow apply whose lease then expires:
        // claim with a short lease token, then requeue (clears the lease).
        let row = node_db
            .legacy_claim_next_due_outbox(20, 5, "stale-token")
            .await
            .expect("claim")
            .expect("a due row");
        node_db
            .legacy_requeue_expired_outbox_leases(100)
            .await
            .expect("requeue");
        // The stale-token complete now finds no matching lease — the race
        // is detected and is non-fatal (does not lose the row).
        assert!(
            !node_db
                .legacy_complete_outbox(row.id, "stale-token")
                .await
                .expect("complete"),
            "stale-token complete must report a lost lease race, not error"
        );

        // The dispatcher re-claims and completes the surviving row.
        assert_eq!(
            dispatcher
                .dispatch_due_once(200)
                .await
                .expect("redispatch after race"),
            DispatchOutcome::Dispatched
        );
        assert!(
            node_db
                .legacy_claim_next_due_outbox(300, 1_000, "check-empty")
                .await
                .expect("claim")
                .is_none(),
            "row must be completed after the lease race recovery"
        );
    }

    #[tokio::test]
    async fn pipelined_dispatch_keeps_multiple_in_flight() {
        // bug-grpc: batch dispatch must keep multiple `apply_outbox` RPCs
        // in flight concurrently (pipelined), bounded by the batch window.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(InFlightTrackingClient::default());
        let batch = 4usize;
        let dispatcher =
            OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), batch);

        // Enqueue one row each for distinct subjects so the batch claims
        // them all (per-subject FIFO claims at most one per subject).
        for i in 0..batch {
            let pod = format!("pod-{i}");
            let uid = format!("uid-{i}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    format!("inflight-key-{i}"),
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod}/{uid}"),
                        Some("default".to_string()),
                        pod.clone(),
                        Some(uid.clone()),
                    ),
                    &uid,
                    pod_status_command("default", &pod, &uid),
                    10,
                ))
                .await
                .expect("enqueue");
        }

        assert_eq!(
            dispatcher.dispatch_due_once(20).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );

        let max = client.max_in_flight();
        assert!(
            max > 1,
            "dispatch must pipeline multiple RPCs concurrently, saw max in-flight = {max}"
        );
        assert!(
            max <= batch,
            "in-flight window must not exceed the batch size, saw {max} > {batch}"
        );
    }

    #[tokio::test]
    async fn production_dispatcher_drains_multiple_status_subjects_per_tick() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::production_for_tests(
            node_db.clone(),
            client.clone(),
            Arc::new(Notify::new()),
        );

        for i in 0..8 {
            let pod = format!("production-pod-{i}");
            let uid = format!("production-uid-{i}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    format!("production-key-{i}"),
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod}/{uid}"),
                        Some("default".to_string()),
                        pod.clone(),
                        Some(uid.clone()),
                    ),
                    &uid,
                    pod_status_command("default", &pod, &uid),
                    10,
                ))
                .await
                .expect("enqueue");
        }

        assert_eq!(
            dispatcher.dispatch_due_once(20).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        let calls = client.calls().await;
        assert_eq!(
            calls.len(),
            8,
            "production dispatcher must drain independent due status rows in one tick"
        );
        assert!(
            node_db
                .legacy_claim_next_due_outbox(20, 1_000, "assert-empty")
                .await
                .expect("claim after production dispatch")
                .is_none()
        );
    }

    #[tokio::test]
    async fn dispatcher_delivers_due_rows_in_subject_fifo_order() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "key-1",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue first");
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-2",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue second");

        assert_eq!(
            dispatcher.dispatch_due_once(10).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(10).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["key-1", "key-2"]);
        assert!(
            node_db
                .legacy_claim_next_due_outbox(10, 1_000, "assert-empty")
                .await
                .expect("claim after drain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn dispatcher_prioritizes_lease_renew_over_older_pod_status_rows() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db, client.clone());

        for i in 0..32 {
            let key = format!("pod-status-{i:02}");
            let pod_name = format!("web-{i:02}");
            let pod_uid = format!("pod-uid-{i:02}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    &key,
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod_name}/{pod_uid}"),
                        Some("default".to_string()),
                        pod_name.clone(),
                        Some(pod_uid.clone()),
                    ),
                    &pod_uid,
                    pod_status_command("default", &pod_name, &pod_uid),
                    1_000 + i,
                ))
                .await
                .expect("enqueue pod status");
        }
        outbox
            .enqueue_command(OutboxCommand::new(
                "lease-renew",
                OutboxOperation::LeaseRenew,
                OutboxSubject::new(
                    "coordination.k8s.io/v1/Lease/kube-node-lease/mn-leader/lease-uid",
                    Some("kube-node-lease".to_string()),
                    "mn-leader",
                    Some("lease-uid".to_string()),
                ),
                "",
                lease_renew_command("mn-leader", "lease-uid"),
                1_100,
            ))
            .await
            .expect("enqueue lease renew");

        assert_eq!(
            dispatcher.dispatch_due_once(1_100).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert!(
            client.calls().await.is_empty(),
            "LeaseRenew uses the focused lease port and must not reach durable delivery"
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_100).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["pod-status-00"]);
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimed_after_restart() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "key-lease",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                100,
            ))
            .await
            .expect("enqueue");
        let claimed = node_db
            .legacy_claim_next_due_outbox(100, 50, "dead-dispatcher")
            .await
            .expect("initial claim")
            .expect("row claimed");
        assert_eq!(claimed.idempotency_key, "key-lease");

        assert_eq!(
            dispatcher.dispatch_due_once(120).await.expect("dispatch"),
            DispatchOutcome::Idle {
                next_wake_ms: Some(150)
            }
        );
        assert_eq!(
            dispatcher.dispatch_due_once(151).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["key-lease"]);
    }

    #[tokio::test]
    async fn crash_recovery_replays_expired_leases_without_duplicate_effects() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(IdempotentApplyClient::default());
        for i in 0..50 {
            let key = format!("pod-status-{i:02}");
            let pod_name = format!("web-{i:02}");
            let pod_uid = format!("uid-{i:02}");
            let subject_key = format!("v1/Pod/default/{pod_name}/{pod_uid}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    &key,
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        subject_key,
                        Some("default".to_string()),
                        pod_name.clone(),
                        Some(pod_uid.clone()),
                    ),
                    &pod_uid,
                    pod_status_command("default", &pod_name, &pod_uid),
                    1_000 + i,
                ))
                .await
                .expect("enqueue pod status");
        }

        for _ in 0..10 {
            let row = node_db
                .legacy_claim_next_due_outbox(2_000, 100, "crashed-dispatcher")
                .await
                .expect("claim")
                .expect("row");
            client
                .deliver_outbox(
                    OutboxDeliveryRequest::try_new(
                        row.idempotency_key,
                        OutboxOperation::try_from(row.operation.as_str())
                            .expect("operation")
                            .try_delivery_operation()
                            .expect("durable operation"),
                        Arc::<[u8]>::from(row.payload_proto),
                        row.client_id,
                        row.stream_id,
                        row.stream_seq,
                    )
                    .expect("valid delivery request"),
                )
                .await
                .expect("simulate leader effect before crash");
        }

        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());
        let mut now = 2_101;
        loop {
            match dispatcher.dispatch_due_once(now).await.expect("dispatch") {
                DispatchOutcome::Dispatched => {
                    now += 1;
                }
                DispatchOutcome::Idle { next_wake_ms: None } => break,
                DispatchOutcome::Idle {
                    next_wake_ms: Some(next),
                } => {
                    now = next.max(now + 1);
                }
            }
        }

        assert_eq!(client.applied_keys().await.len(), 50);
        assert_eq!(client.calls().await.len(), 60);
        assert!(
            node_db
                .legacy_claim_next_due_outbox(now, 1_000, "assert-empty")
                .await
                .expect("claim after drain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn retryable_error_requeues_with_backoff_and_terminal_uid_mismatch_completes() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        client
            .push_response(Err(OutboxApplyError::Retryable("leader down".into())))
            .await;
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-retry",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                1_000,
            ))
            .await
            .expect("enqueue retry row");
        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        let next_retry = match dispatcher.dispatch_due_once(1_100).await.expect("dispatch") {
            DispatchOutcome::Idle {
                next_wake_ms: Some(next),
            } => next,
            other => panic!("expected idle retry wake, got {other:?}"),
        };
        let (backoff_lower, backoff_upper) =
            super::adaptive_backoff_bounds(0, klights_types::RTT_DEFAULT_MS);
        assert!(
            (1_000 + backoff_lower..=1_000 + backoff_upper).contains(&next_retry),
            "retry wake must stay inside the first-attempt adaptive jitter window \
             [{},{}]: {next_retry}",
            1_000 + backoff_lower,
            1_000 + backoff_upper
        );

        client
            .push_response(Err(OutboxApplyError::UidMismatch {
                expected: "uid-1".into(),
                actual: "uid-2".into(),
            }))
            .await;
        assert_eq!(
            dispatcher
                .dispatch_due_once(next_retry)
                .await
                .expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert!(
            node_db
                .legacy_claim_next_due_outbox(next_retry, 1_000, "assert-empty")
                .await
                .expect("claim after terminal drop")
                .is_none()
        );
    }

    #[tokio::test]
    async fn incompatible_codec_never_consumes_actor_delete_at_shared_retry_cap() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let dispatcher =
            OutboxDispatcher::for_tests(node_db.clone(), Arc::new(IncompatibleCodecDelivery));
        outbox
            .enqueue_command(OutboxCommand::new(
                "codec-rejected-actor-delete",
                OutboxOperation::PodMetadata,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-codec",
                    Some("default".to_string()),
                    "web",
                    Some("uid-codec".to_string()),
                ),
                "uid-codec",
                StorageCommand::FinalizeBoundPod {
                    namespace: "default".to_string(),
                    name: "web".to_string(),
                    pod_uid: "uid-codec".to_string(),
                    node_name: "worker-a".to_string(),
                    observed_resource_version: 7,
                },
                1_000,
            ))
            .await
            .expect("enqueue actor delete");

        let mut now = 1_000;
        for _ in 0..=super::MAX_OUTBOX_ATTEMPTS {
            assert_eq!(
                dispatcher.dispatch_due_once(now).await.expect("dispatch"),
                DispatchOutcome::Dispatched
            );
            now = match dispatcher.dispatch_due_once(now).await.expect("next wake") {
                DispatchOutcome::Idle {
                    next_wake_ms: Some(next),
                } => next,
                other => panic!("expected retained row with a retry wake, got {other:?}"),
            };
        }

        assert!(
            node_db.legacy_list_dead_letter().await.unwrap().is_empty(),
            "codec rejection must never dead-letter the durable actor delete"
        );
        let retained = node_db
            .legacy_claim_next_due_outbox(now, 1_000, "retained-codec-row")
            .await
            .unwrap()
            .expect("codec-rejected actor delete must remain durable");
        assert_eq!(retained.idempotency_key, "codec-rejected-actor-delete");
        assert!(retained.attempt > super::MAX_OUTBOX_ATTEMPTS);
    }

    #[tokio::test]
    async fn retryable_errors_jitter_backoff_to_avoid_synchronized_retry_storm() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher =
            OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);
        let now = 1_000;

        for i in 0..8 {
            client
                .push_response(Err(OutboxApplyError::Retryable(
                    "raft proposal flow control saturated".into(),
                )))
                .await;
            let pod_name = format!("storm-{i}");
            let pod_uid = format!("uid-storm-{i}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    format!("retry-storm-{i}"),
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod_name}/{pod_uid}"),
                        Some("default".to_string()),
                        pod_name.clone(),
                        Some(pod_uid.clone()),
                    ),
                    &pod_uid,
                    pod_status_command("default", &pod_name, &pod_uid),
                    now,
                ))
                .await
                .expect("enqueue retry storm row");
        }

        assert_eq!(
            dispatcher.dispatch_due_once(now).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );

        let mut next_due_times = Vec::new();
        while let Some(row) = node_db
            .legacy_claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect-jitter")
            .await
            .expect("claim retry row")
        {
            next_due_times.push(row.next_due_ms);
            node_db
                .legacy_complete_outbox(row.id, row.lease_token.as_deref().expect("lease token"))
                .await
                .expect("complete inspected row");
        }

        assert_eq!(next_due_times.len(), 8);
        let unique: HashSet<i64> = next_due_times.iter().copied().collect();
        assert!(
            unique.len() > 1,
            "retryable rows must not all re-fire at the same millisecond: {next_due_times:?}"
        );
        let (backoff_lower, backoff_upper) =
            super::adaptive_backoff_bounds(0, klights_types::RTT_DEFAULT_MS);
        assert!(
            next_due_times
                .iter()
                .all(|due| *due >= now + backoff_lower && *due <= now + backoff_upper),
            "first retry jitter must stay inside the adaptive window \
             [{},{}]: {next_due_times:?}",
            now + backoff_lower,
            now + backoff_upper
        );
    }

    #[test]
    fn adaptive_backoff_first_retry_is_below_current_five_second_floor() {
        // RTT 200 ms (the default estimate), first retry (attempt 0) must land
        // well below the old 5 s linear floor so a transient apply error under
        // a ~200 ms RTT lossy link does not starve status propagation for a
        // full 5 s.
        for key in ["pod-status-a", "pod-status-b", "pod-status-c"] {
            let sleep = super::adaptive_jittered_backoff_ms(0, key, 200);
            assert!(
                (250..=1_000_i64).contains(&sleep),
                "first retry backoff must be in [250,1000] ms, got {sleep} for {key:?}"
            );
        }
    }

    #[test]
    fn adaptive_backoff_caps_at_sixty_seconds() {
        // High-RTT (3 s) path drives base*2^attempt up toward MAX_BACKOFF_MS;
        // it must never exceed the cap, even for very large attempt counts.
        for attempt in [6, 7, 8, 12, 50, 1_000] {
            let sleep = super::adaptive_jittered_backoff_ms(attempt, "cap-key", 3_000);
            assert!(
                sleep <= super::MAX_BACKOFF_MS,
                "backoff must cap at MAX_BACKOFF_MS ({}) for attempt {attempt}, got {sleep}",
                super::MAX_BACKOFF_MS
            );
        }
    }

    #[test]
    fn adaptive_backoff_desynchronizes_keys_for_same_attempt() {
        let times: HashSet<i64> = (0..8)
            .map(|i| super::adaptive_jittered_backoff_ms(0, &format!("pod-status-{i}"), 200))
            .collect();
        assert!(
            times.len() >= 4,
            "8 keys at attempt 0 must desynchronize to >=4 distinct backoffs, got {times:?}"
        );
    }

    #[test]
    fn adaptive_backoff_changes_for_same_key_across_attempts() {
        let times: HashSet<i64> = (0_i64..6)
            .map(|a| super::adaptive_jittered_backoff_ms(a, "pod-status-3", 200))
            .collect();
        assert!(
            times.len() >= 2,
            "same key across attempts must vary, got {times:?}"
        );
    }

    #[test]
    fn adaptive_backoff_is_deterministic_for_same_key_attempt_and_rtt() {
        for (attempt, key, rtt) in [(0_i64, "k1", 200_i64), (3, "k2", 800), (7, "k3", 3_000)] {
            let a = super::adaptive_jittered_backoff_ms(attempt, key, rtt);
            let b = super::adaptive_jittered_backoff_ms(attempt, key, rtt);
            assert_eq!(
                a, b,
                "backoff must be deterministic for key={key} attempt={attempt} rtt={rtt}"
            );
        }
    }

    /// T4/T7: the dispatcher must feed successful apply round-trips into its
    /// RTT estimator, and the retry backoff must then reflect the observed
    /// RTT rather than the fixed 200 ms default. Seeds a successful apply
    /// (records the round-trip), then asserts the estimator moved off the
    /// default before any retry backoff is computed.
    #[tokio::test]
    async fn dispatcher_feeds_apply_round_trips_into_rtt_estimator() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        client
            .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
            .await;
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "rtt-seed",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-rtt",
                    Some("default".to_string()),
                    "web",
                    Some("uid-rtt".to_string()),
                ),
                "uid-rtt",
                pod_status_command("default", "web", "uid-rtt"),
                10,
            ))
            .await
            .expect("enqueue");
        dispatcher
            .dispatch_due_once(20)
            .await
            .expect("dispatch the successful apply");
        // The successful round-trip must have been recorded: the estimate is
        // no longer the unsampled default (200 ms).
        let estimate = dispatcher.rtt_estimate_ms();
        assert_ne!(
            estimate,
            klights_types::RTT_DEFAULT_MS,
            "a successful apply round-trip must update the RTT estimator off the default"
        );
    }

    #[tokio::test]
    async fn retryable_backoff_uses_injected_raft_rtt_without_outbox_success_sample() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let raft_rtt = Arc::new(klights_types::RttEstimator::new());
        raft_rtt.record_sample(std::time::Duration::from_millis(800));
        let dispatcher = OutboxDispatcher::for_tests_with_rtt_estimator(
            node_db.clone(),
            client.clone(),
            raft_rtt,
        );

        client
            .push_response(Err(OutboxApplyError::Retryable("leader down".into())))
            .await;
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-retry-raft-rtt",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-rtt",
                    Some("default".to_string()),
                    "web",
                    Some("uid-rtt".to_string()),
                ),
                "uid-rtt",
                pod_status_command("default", "web", "uid-rtt"),
                1_000,
            ))
            .await
            .expect("enqueue retry row");

        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        let next_retry = match dispatcher.dispatch_due_once(1_100).await.expect("dispatch") {
            DispatchOutcome::Idle {
                next_wake_ms: Some(next),
            } => next,
            other => panic!("expected idle retry wake, got {other:?}"),
        };
        let (backoff_lower, backoff_upper) = super::adaptive_backoff_bounds(0, 800);
        assert!(
            (1_000 + backoff_lower..=1_000 + backoff_upper).contains(&next_retry),
            "retry wake must use injected raft RTT window [{},{}], got {next_retry}",
            1_000 + backoff_lower,
            1_000 + backoff_upper
        );
    }

    #[tokio::test]
    async fn applied_checkpoint_marker_does_not_drop_unmaterialized_pod_ip_status() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        node_db
            .legacy_upsert_pod_status_checkpoint(
                "uid-checkpoint-race",
                "default",
                "checkpoint-race",
                10,
                serde_json::json!({
                    "phase": "Pending",
                    "podIP": "10.50.5.2",
                    "podIPs": [{"ip": "10.50.5.2"}],
                    "hostIP": "10.99.0.15",
                    "hostIPs": [{"ip": "10.99.0.15"}]
                }),
                200,
            )
            .await
            .expect("record newer checkpoint");
        node_db
            .legacy_mark_pod_status_checkpoint_applied("uid-checkpoint-race", 12, 300)
            .await
            .expect("older outbox row marked applied");
        let live = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "checkpoint-race".to_string(),
            uid: "uid-checkpoint-race".to_string(),
            resource_version: 20,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "checkpoint-race",
                    "uid": "uid-checkpoint-race",
                    "resourceVersion": "20"
                },
                "spec": {
                    "nodeName": "mn-controlplane3",
                    "containers": [{"name": "e2e", "image": "registry.k8s.io/conformance:v1.34.6"}]
                },
                "status": {"phase": "Pending"}
            })),
        };

        let merged = outbox
            .merge_pod_status_checkpoint(live)
            .await
            .expect("merge checkpoint");

        assert_eq!(
            merged
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.50.5.2"),
            "checkpoint must survive until its status fields are visible in the live Pod"
        );
        assert!(
            node_db
                .legacy_get_pod_status_checkpoint("uid-checkpoint-race")
                .await
                .expect("read checkpoint")
                .is_some(),
            "unmaterialized checkpoint should remain for later local reads"
        );
    }

    #[tokio::test]
    async fn later_runtime_checkpoint_without_ip_preserves_prior_pod_ip_and_scheduled_condition() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let live = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "checkpoint-runtime-race".to_string(),
            uid: "uid-checkpoint-runtime-race".to_string(),
            resource_version: 20,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "checkpoint-runtime-race",
                    "uid": "uid-checkpoint-runtime-race",
                    "resourceVersion": "20"
                },
                "spec": {
                    "nodeName": "mn-replica",
                    "containers": [{"name": "main", "image": "busybox"}]
                },
                "status": {
                    "phase": "Pending",
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-06-30T05:07:27Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-30T05:07:26Z"}
                    ]
                }
            })),
        };

        outbox
            .record_pod_status_checkpoint(
                &live,
                serde_json::json!({
                    "phase": "Pending",
                    "podIP": "10.50.3.2",
                    "podIPs": [{"ip": "10.50.3.2"}],
                    "hostIP": "10.99.0.13",
                    "hostIPs": [{"ip": "10.99.0.13"}],
                    "conditions": [
                        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-06-30T05:07:27Z"},
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-30T05:07:26Z"}
                    ],
                    "containerStatuses": []
                }),
                100,
            )
            .await
            .expect("record podIP checkpoint");

        outbox
            .record_pod_status_checkpoint(
                &live,
                serde_json::json!({
                    "phase": "Pending",
                    "conditions": [
                        {
                            "type": "PodScheduled",
                            "status": "False",
                            "lastTransitionTime": "2026-06-30T05:07:26Z",
                            "reason": "SchedulingPending"
                        },
                        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-30T05:07:26Z"},
                        {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-06-30T05:07:29Z"},
                        {"type": "Ready", "status": "False", "lastTransitionTime": "2026-06-30T05:07:29Z"}
                    ],
                    "containerStatuses": [{
                        "name": "main",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-06-30T05:07:29Z"}}
                    }]
                }),
                200,
            )
            .await
            .expect("record runtime checkpoint");

        let merged = outbox
            .merge_pod_status_checkpoint(live)
            .await
            .expect("merge checkpoint");

        assert_eq!(
            merged
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.50.3.2"),
            "runtime checkpoint without network fields must not erase the prior CNI podIP checkpoint"
        );
        let scheduled = merged
            .data
            .pointer("/status/conditions")
            .and_then(|conditions| conditions.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|value| value.as_str()) == Some("PodScheduled")
                })
            })
            .expect("PodScheduled condition present");
        assert_eq!(
            scheduled.get("status").and_then(|value| value.as_str()),
            Some("True"),
            "a runtime checkpoint must not downgrade a bound Pod back to SchedulingPending"
        );
    }

    #[tokio::test]
    async fn stale_pod_status_outbox_does_not_block_actor_finalize_delete() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let created = cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-web",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-web",
                        "uid": "uid-deadline-web"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .expect("create pod");

        let mut terminating_data = std::sync::Arc::unwrap_or_clone(created.data);
        terminating_data["metadata"]["deletionTimestamp"] =
            serde_json::json!("2026-05-24T18:00:00Z");
        terminating_data["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(0);
        let terminating = cluster_db
            .update_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-web",
                terminating_data,
                created.resource_version,
            )
            .await
            .expect("mark pod terminating");

        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client);
        let subject = "v1/Pod/default/deadline-web/uid-deadline-web";
        let stale_status = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "deadline-web".to_string(),
            status: serde_json::json!({
                "phase": "Failed",
                "reason": "DeadlineExceeded",
                "message": "Pod was active on the node longer than the specified deadline (5s)"
            }),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some("uid-deadline-web".to_string()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        };
        outbox
            .enqueue_command(OutboxCommand::new(
                "deadline-web-stale-deadline",
                OutboxOperation::DeadlineExceeded,
                OutboxSubject::new(
                    subject,
                    Some("default".to_string()),
                    "deadline-web",
                    Some("uid-deadline-web".to_string()),
                ),
                "uid-deadline-web",
                stale_status,
                1_000,
            ))
            .await
            .expect("enqueue stale status");
        outbox
            .enqueue_command(OutboxCommand::new(
                "deadline-web-actor-finalize-delete",
                OutboxOperation::PodMetadata,
                OutboxSubject::new(
                    subject,
                    Some("default".to_string()),
                    "deadline-web",
                    Some("uid-deadline-web".to_string()),
                ),
                "uid-deadline-web",
                StorageCommand::FinalizeBoundPod {
                    namespace: "default".to_string(),
                    name: "deadline-web".to_string(),
                    pod_uid: "uid-deadline-web".to_string(),
                    node_name: "worker-a".to_string(),
                    observed_resource_version: terminating.resource_version,
                },
                1_001,
            ))
            .await
            .expect("enqueue actor finalize delete");

        assert_eq!(
            dispatcher.dispatch_due_once(1_001).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_002).await.expect("dispatch"),
            DispatchOutcome::Dispatched,
            "strict stream FIFO must consume the older status decision before actor delete"
        );
        assert!(
            matches!(
                dispatcher.dispatch_due_once(1_003).await.expect("dispatch"),
                DispatchOutcome::Idle { .. }
            ),
            "actor-finalize delete should complete superseded status rows"
        );
        let dead_letters = node_db
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter");
        assert!(
            dead_letters.is_empty(),
            "ordered actor-finalize delete must not dead-letter: {dead_letters:?}"
        );
        assert!(
            cluster_db
                .get_resource("v1", "Pod", Some("default"), "deadline-web")
                .await
                .expect("read pod")
                .is_none(),
            "stale status conflicts must not block the actor-owned delete row"
        );
    }

    #[tokio::test]
    async fn outbox_terminal_decision_unknown_operation_consumes_assigned_sequence() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let (node_db, sqlite_node_db) =
            crate::datastore::node_local::selector::open_node_local_with_sqlite(
                BackendKind::Sqlite,
                None,
                supervisor(),
                None,
                "sqlite:outbox-terminal-unknown-test",
            )
            .await
            .expect("open node-local test db");
        let sqlite_node_db = sqlite_node_db.expect("SQLite test backend");
        node_db
            .legacy_enqueue_outbox(crate::datastore::node_local::OutboxInsert {
                idempotency_key: "unknown-operation-seq-1".to_string(),
                enqueued_ms: 1_000,
                subject_key: "v1/Pod/default/after-unknown/uid-after-unknown".to_string(),
                subject_api_version: "v1".to_string(),
                subject_kind: "Pod".to_string(),
                subject_namespace: Some("default".to_string()),
                subject_name: "after-unknown".to_string(),
                subject_uid: Some("uid-after-unknown".to_string()),
                pod_uid: "uid-after-unknown".to_string(),
                operation: OutboxOperation::PodStatus.as_str().to_string(),
                payload_proto: vec![0xff],
                next_due_ms: 1_000,
                classification: pod_status_classification(),
            })
            .await
            .expect("enqueue legacy row before unknown operation migration");
        let position_before_corruption = sqlite_node_db
            .outbox_stream_position_for_test("unknown-operation-seq-1")
            .await
            .expect("read assigned position before corruption")
            .expect("assigned position exists");
        assert_eq!(
            sqlite_node_db
                .outbox_operation_for_test("unknown-operation-seq-1")
                .await
                .unwrap()
                .as_deref(),
            Some("PodStatus")
        );
        sqlite_node_db
            .set_outbox_operation_for_test("unknown-operation-seq-1", "FutureUnknownOperation")
            .await
            .expect("simulate an assigned legacy/corrupt operation value");
        assert_eq!(
            sqlite_node_db
                .outbox_operation_for_test("unknown-operation-seq-1")
                .await
                .unwrap()
                .as_deref(),
            Some("FutureUnknownOperation")
        );
        assert_eq!(
            sqlite_node_db
                .outbox_stream_position_for_test("unknown-operation-seq-1")
                .await
                .unwrap(),
            Some(position_before_corruption),
            "the test corruption must change only operation, not stream identity"
        );
        let outbox = Outbox::new(node_db.clone());
        outbox
            .enqueue_command(OutboxCommand::new(
                "known-operation-seq-2",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/after-unknown/uid-after-unknown",
                    Some("default".to_string()),
                    "after-unknown",
                    Some("uid-after-unknown".to_string()),
                ),
                "uid-after-unknown",
                pod_status_command("default", "after-unknown", "uid-after-unknown"),
                1_001,
            ))
            .await
            .expect("enqueue known successor row");
        let client_id = node_db
            .identity()
            .get_node_meta("outbox_client_id")
            .await
            .expect("read outbox client id")
            .expect("outbox client id exists");
        let first_position = sqlite_node_db
            .outbox_stream_position_for_test("unknown-operation-seq-1")
            .await
            .expect("read unknown row position")
            .expect("unknown row position exists");
        let second_position = sqlite_node_db
            .outbox_stream_position_for_test("known-operation-seq-2")
            .await
            .expect("read successor row position")
            .expect("successor row position exists");
        assert_eq!(first_position.1, 1);
        assert_eq!(second_position, (first_position.0, 2));
        let client = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client);
        let resource_version_before = cluster_db
            .get_current_resource_version()
            .await
            .expect("read public resourceVersion before terminal decision");
        let watch_position_before = cluster_db
            .current_watch_replay_position()
            .await
            .expect("read watch position before terminal decision");

        assert_eq!(
            dispatcher
                .dispatch_due_once(1_001)
                .await
                .expect("dispatch unknown"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            cluster_db.list_outbox_stream_watermarks().await.unwrap(),
            vec![klights_cluster_core::OutboxStreamWatermark {
                client_id: client_id.clone(),
                stream_id: first_position.0,
                stream_seq: 1,
            }],
            "an assigned unknown operation requires a leader terminal decision"
        );
        assert_eq!(
            cluster_db.get_current_resource_version().await.unwrap(),
            resource_version_before,
            "the terminal sentinel must not allocate a public resourceVersion"
        );
        assert_eq!(
            cluster_db.current_watch_replay_position().await.unwrap(),
            watch_position_before,
            "the terminal sentinel must not append watch history"
        );
        assert!(
            cluster_db
                .get_resource("v1", "Pod", Some("__klights-terminal-outbox__"), "decision",)
                .await
                .expect("read terminal sentinel target")
                .is_none(),
            "the terminal sentinel must not create or update a Pod"
        );
        assert_eq!(
            dispatcher
                .dispatch_due_once(1_002)
                .await
                .expect("dispatch successor"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            cluster_db.list_outbox_stream_watermarks().await.unwrap(),
            vec![klights_cluster_core::OutboxStreamWatermark {
                client_id,
                stream_id: first_position.0,
                stream_seq: 2,
            }],
            "the known successor must apply after the terminal decision"
        );
        assert!(
            node_db
                .legacy_claim_next_due_outbox(1_003, 1_000, "assert-empty")
                .await
                .expect("claim after drain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn assigned_empty_payload_uses_a_durable_terminal_sentinel() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let node_db = crate::datastore::node_local::selector::open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:assigned-empty-payload-test",
        )
        .await
        .unwrap();
        node_db
            .legacy_enqueue_outbox(crate::datastore::node_local::OutboxInsert {
                idempotency_key: "assigned-empty-payload".to_string(),
                enqueued_ms: 1,
                subject_key: "v1/Pod/default/web/pod-uid".to_string(),
                subject_api_version: "v1".to_string(),
                subject_kind: "Pod".to_string(),
                subject_namespace: Some("default".to_string()),
                subject_name: "web".to_string(),
                subject_uid: Some("pod-uid".to_string()),
                pod_uid: "pod-uid".to_string(),
                operation: OutboxOperation::PodStatus.as_str().to_string(),
                payload_proto: Vec::new(),
                next_due_ms: 1,
                classification: pod_status_classification(),
            })
            .await
            .unwrap();
        let client_id = node_db
            .identity()
            .get_node_meta("outbox_client_id")
            .await
            .unwrap()
            .unwrap();
        let client = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client);

        assert_eq!(
            dispatcher.dispatch_due_once(1).await.unwrap(),
            DispatchOutcome::Dispatched
        );
        let watermark = cluster_db.list_outbox_stream_watermarks().await.unwrap();
        assert_eq!(watermark.len(), 1);
        assert_eq!(watermark[0].client_id, client_id);
        assert_eq!(watermark[0].stream_seq, 1);
        assert!(node_db.legacy_list_dead_letter().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn outbox_terminal_decision_unknown_operation_transport_failure_requeues() {
        let (node_db, sqlite_node_db) =
            crate::datastore::node_local::selector::open_node_local_with_sqlite(
                BackendKind::Sqlite,
                None,
                supervisor(),
                None,
                "sqlite:outbox-terminal-unknown-retry-test",
            )
            .await
            .expect("open node-local test db");
        let sqlite_node_db = sqlite_node_db.expect("SQLite test backend");
        node_db
            .legacy_enqueue_outbox(crate::datastore::node_local::OutboxInsert {
                idempotency_key: "unknown-operation-retry".to_string(),
                enqueued_ms: 1_000,
                subject_key: "internal/unknown-operation-retry".to_string(),
                subject_api_version: "internal".to_string(),
                subject_kind: "Unknown".to_string(),
                subject_namespace: None,
                subject_name: "unknown-operation-retry".to_string(),
                subject_uid: None,
                pod_uid: String::new(),
                operation: OutboxOperation::PodStatus.as_str().to_string(),
                payload_proto: vec![0xff],
                next_due_ms: 1_000,
                classification: pod_status_classification(),
            })
            .await
            .expect("enqueue row before operation corruption");
        let position_before_corruption = sqlite_node_db
            .outbox_stream_position_for_test("unknown-operation-retry")
            .await
            .expect("read assigned position before corruption")
            .expect("assigned position exists");
        sqlite_node_db
            .set_outbox_operation_for_test("unknown-operation-retry", "FutureUnknownOperation")
            .await
            .expect("simulate assigned corrupt operation");
        assert_eq!(
            sqlite_node_db
                .outbox_operation_for_test("unknown-operation-retry")
                .await
                .unwrap()
                .as_deref(),
            Some("FutureUnknownOperation")
        );
        assert_eq!(
            sqlite_node_db
                .outbox_stream_position_for_test("unknown-operation-retry")
                .await
                .unwrap(),
            Some(position_before_corruption),
            "the test corruption must preserve the exact assigned watermark"
        );
        let client = Arc::new(FakeApplyClient::default());
        client
            .push_response(Err(OutboxApplyError::Retryable(
                "leader unavailable".to_string(),
            )))
            .await;
        let dispatcher = OutboxDispatcher::for_tests(node_db, client.clone());

        assert_eq!(
            dispatcher
                .dispatch_due_once(1_000)
                .await
                .expect("dispatch unknown"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["unknown-operation-retry"]);
        assert_eq!(
            sqlite_node_db.legacy_outbox_stats().await.unwrap().pending,
            1,
            "transport/follower failure must retain the assigned row for retry"
        );
    }

    #[tokio::test]
    async fn uid_mismatch_drops_event_no_retry() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        client
            .push_response(Err(OutboxApplyError::UidMismatch {
                expected: "uid-1".into(),
                actual: "uid-2".into(),
            }))
            .await;
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-uid-mismatch",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                1_000,
            ))
            .await
            .expect("enqueue");

        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Idle { next_wake_ms: None }
        );
        assert_eq!(client.calls().await, vec!["key-uid-mismatch"]);
    }

    #[tokio::test]
    async fn node_registration_and_event_writes_enqueue_outbox_rows() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .expect("open sqlite datastore"),
        );
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let existing_node = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "node-a"},
            "status": {}
        });
        db.create_resource("v1", "Node", None, "node-a", existing_node)
            .await
            .expect("seed existing node");

        let registration_profile = klights_kubelet::node_config::NodeRegistrationProfile::new(
            klights_network_api::NodePeerMode::Root,
            klights_kubelet::node_config::KubeletNodeRole::Worker,
            true,
            klights_types::BuildIdentity::new("v1.34.6+klights-test", "test-commit"),
        );
        crate::node_output_integration_tests::register_node_with_outbox(
            &crate::kubelet::file_blocking::test_file_process_executor(),
            db.as_ref(),
            &outbox,
            "node-a",
            &registration_profile,
            None,
            None,
        )
        .await
        .expect("enqueue node registration");
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "pod-uid-1"
            }
        });
        let query = crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter::new(db.as_ref());
        klights_kubelet::pod_events::emit_pod_event_with_outbox(
            &query,
            Some(&outbox),
            klights_kubelet::pod_events::PodEventRecord {
                pod: &pod,
                reason: "Started",
                message: "Started container app",
                event_type: "Normal",
                reporting_component: "klights-kubelet",
                reporting_instance: "node-a",
                operation_now: klights_supervisor::SystemWallClock::now_utc(),
            },
        )
        .await
        .expect("enqueue event");

        let mut operations = Vec::new();
        while let Some(row) = node_db
            .legacy_claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect")
            .await
            .expect("claim")
        {
            operations.push(row.operation);
            node_db
                .legacy_complete_outbox(row.id, row.lease_token.as_deref().expect("lease token"))
                .await
                .expect("complete");
        }

        assert_eq!(operations, vec!["NodeRegistration", "EventCreate"]);
        assert!(
            db.list_resources(
                "v1",
                "Event",
                Some("default"),
                crate::datastore::ResourceListQuery::all()
            )
            .await
            .expect("list events")
            .items
            .is_empty()
        );
    }
}
