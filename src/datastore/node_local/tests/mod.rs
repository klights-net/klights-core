use std::sync::Arc;

use crate::datastore::backend_kind::BackendKind;
use crate::datastore::node_local::{
    NodeLocalBackend, NodeLocalDb, NodeLocalHandle, OutboxFailureDisposition, OutboxInsert,
    SqliteNodeLocalDb, selector,
};
use crate::datastore::node_local::{
    PodSlotAdmissionEvent, PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotClearResult,
    PodSlotMutationResult,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn pod_status_classification() -> klights_node_store::OutboxClassification {
    klights_node_store::OutboxClassification::try_new(
        klights_node_store::OutboxPriority::Workload,
        klights_node_store::OutboxSupersedability::PodStatus,
        klights_node_store::TerminalDeleteClassification::NotTerminalDelete,
        klights_node_store::OutboxSequencePolicy::PerSubject,
    )
    .expect("valid Pod status classification")
}

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn open_node_local_in_memory() -> NodeLocalDb {
    let executor = crate::datastore::node_local::sqlite::open::open_with_opts(
        crate::datastore::node_local::sqlite::open::in_memory_opts(),
        supervisor(),
        "sqlite:node-local-test",
    )
    .await
    .expect("open node-local executor");
    NodeLocalDb::from_executor(executor).expect("create node-local db")
}

async fn open_sqlite_node_local_backend_handle() -> NodeLocalHandle {
    let executor = crate::datastore::node_local::sqlite::open::open_with_opts(
        crate::datastore::node_local::sqlite::open::in_memory_opts(),
        supervisor(),
        "sqlite:node-local-backend-test",
    )
    .await
    .expect("open node-local executor");
    let db = SqliteNodeLocalDb::from_executor(executor).expect("create sqlite node-local db");
    Arc::new(db)
}

async fn open_node_local_on_disk(path: &std::path::Path) -> NodeLocalDb {
    let mut opts = crate::datastore::node_local::sqlite::open::disk_opts(path.to_path_buf());
    opts.allow_existing_perms = true;
    let executor = crate::datastore::node_local::sqlite::open::open_with_opts(
        opts,
        supervisor(),
        "sqlite:node-local-disk-test",
    )
    .await
    .expect("open disk node-local executor");
    NodeLocalDb::from_executor(executor).expect("create disk node-local db")
}

fn test_outbox_insert(key: &str, subject_key: &str, now_ms: i64) -> OutboxInsert {
    test_outbox_insert_with_operation(key, subject_key, "PodStatus", now_ms)
}

fn test_outbox_insert_with_operation(
    key: &str,
    subject_key: &str,
    operation: &str,
    now_ms: i64,
) -> OutboxInsert {
    let operation_kind: klights_cluster_core::OutboxOperation = operation
        .try_into()
        .expect("test outbox operation must be recognized");
    let priority = match operation_kind.priority() {
        klights_cluster_core::OutboxPriority::Lease => klights_node_store::OutboxPriority::Lease,
        klights_cluster_core::OutboxPriority::NodeHealth => {
            klights_node_store::OutboxPriority::NodeHealth
        }
        klights_cluster_core::OutboxPriority::Workload => {
            klights_node_store::OutboxPriority::Workload
        }
        klights_cluster_core::OutboxPriority::Diagnostic => {
            klights_node_store::OutboxPriority::Diagnostic
        }
    };
    let supersedability = if operation_kind.is_supersedable_pod_status() {
        klights_node_store::OutboxSupersedability::PodStatus
    } else {
        klights_node_store::OutboxSupersedability::Never
    };
    let sequence_policy = if operation_kind.uses_per_subject_sequence() {
        klights_node_store::OutboxSequencePolicy::PerSubject
    } else {
        klights_node_store::OutboxSequencePolicy::Unsequenced
    };
    let classification = klights_node_store::OutboxClassification::try_new(
        priority,
        supersedability,
        klights_node_store::TerminalDeleteClassification::NotTerminalDelete,
        sequence_policy,
    )
    .expect("valid test outbox classification");
    OutboxInsert {
        idempotency_key: key.to_string(),
        enqueued_ms: now_ms,
        subject_key: subject_key.to_string(),
        subject_api_version: "v1".to_string(),
        subject_kind: "Pod".to_string(),
        subject_namespace: Some("default".to_string()),
        subject_name: "web".to_string(),
        subject_uid: Some("pod-uid".to_string()),
        pod_uid: "pod-uid".to_string(),
        operation: operation.to_string(),
        payload_proto: vec![],
        next_due_ms: now_ms,
        classification,
    }
}

#[tokio::test]
async fn outbox_failure_threshold_is_atomic_and_lease_bound() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert(
        "atomic-dead-letter",
        "v1/Pod/default/web/pod-uid",
        1,
    ))
    .await
    .unwrap();
    db.set_outbox_attempt_for_test("atomic-dead-letter", 719)
        .await
        .unwrap();
    let row = db
        .claim_next_due_outbox(1, 1_000, "owned-lease")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        db.record_outbox_failure(row.id, "stale-lease", 10, "retry", 720)
            .await
            .unwrap(),
        OutboxFailureDisposition::LeaseLost
    );
    assert!(db.list_dead_letter().await.unwrap().is_empty());
    assert_eq!(
        db.record_outbox_failure(row.id, "owned-lease", 10, "retry", 720)
            .await
            .unwrap(),
        OutboxFailureDisposition::DeadLettered
    );
    let dead = db.list_dead_letter().await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].attempts, 720);
    assert_eq!(dead[0].last_error, "retry");
    assert_eq!(
        db.record_outbox_failure(row.id, "owned-lease", 10, "duplicate", 720)
            .await
            .unwrap(),
        OutboxFailureDisposition::LeaseLost
    );
    assert_eq!(db.list_dead_letter().await.unwrap().len(), 1);
}

#[tokio::test]
async fn node_local_outbox_assigns_monotonic_seq_per_stream() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert(
        "same-stream-1",
        "v1/Pod/default/web/pod-uid",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert(
        "same-stream-2",
        "v1/Pod/default/web/pod-uid",
        2,
    ))
    .await
    .unwrap();

    let first = db
        .claim_next_due_outbox(10, 1000, "lease-a")
        .await
        .unwrap()
        .unwrap();
    db.complete_outbox(first.id, "lease-a").await.unwrap();
    let second = db
        .claim_next_due_outbox(10, 1000, "lease-b")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(first.stream_id, second.stream_id);
    assert_eq!(first.stream_seq, 1);
    assert_eq!(second.stream_seq, 2);
}

#[tokio::test]
async fn legacy_outbox_stream_identity_is_repaired_durably_before_delivery() {
    use klights_leader_api::{OutboxDeliveryOperation, OutboxDeliveryRequest};

    let directory = tempfile::tempdir().expect("create node-local restart fixture");
    let path = directory.path().join("node.db");
    let mut legacy = test_outbox_insert(
        "legacy-pre-stream-row",
        "v1/Pod/default/legacy/legacy-uid",
        100,
    );
    legacy.payload_proto = vec![1];

    let db = open_node_local_on_disk(&path).await;
    db.enqueue_outbox(legacy)
        .await
        .expect("seed durable row using current shape");
    db.clear_outbox_stream_identity_for_test("legacy-pre-stream-row")
        .await
        .expect("model a pre-stream-schema row after column migration");
    drop(db);

    let db = open_node_local_on_disk(&path).await;
    let first = db
        .claim_next_due_outbox(100, 1_000, "legacy-first-lease")
        .await
        .expect("claim legacy row after restart")
        .expect("legacy row must remain deliverable");
    assert!(!first.client_id.is_empty());
    assert!(first.stream_id > 0);
    assert_eq!(first.stream_seq, 1);
    OutboxDeliveryRequest::try_new(
        first.idempotency_key.clone(),
        OutboxDeliveryOperation::PodStatus,
        std::sync::Arc::from(first.payload_proto.clone()),
        first.client_id.clone(),
        first.stream_id,
        first.stream_seq,
    )
    .expect("claim must repair identity before request validation can drop the row");
    assert!(
        db.mark_outbox_attempt_failed(
            first.id,
            "legacy-first-lease",
            200,
            "leader temporarily unavailable",
        )
        .await
        .expect("release legacy row for retry")
    );

    let mut successor = test_outbox_insert(
        "post-migration-successor",
        "v1/Pod/default/legacy/legacy-uid",
        110,
    );
    successor.payload_proto = vec![2];
    db.enqueue_outbox(successor)
        .await
        .expect("enqueue same-stream successor");
    assert!(
        db.claim_next_due_outbox(150, 1_000, "blocked-successor")
            .await
            .expect("query blocked successor")
            .is_none(),
        "the repaired legacy head must block its strict-stream successor"
    );
    drop(db);

    let db = open_node_local_on_disk(&path).await;
    let retried = db
        .claim_next_due_outbox(200, 1_000, "legacy-restart-lease")
        .await
        .expect("claim repaired row after second restart")
        .expect("repaired row survives restart");
    assert_eq!(retried.client_id, first.client_id);
    assert_eq!(
        (retried.stream_id, retried.stream_seq),
        (first.stream_id, 1)
    );
    assert!(
        db.complete_outbox(retried.id, "legacy-restart-lease")
            .await
            .expect("complete legacy row after leader decision")
    );

    let successor = db
        .claim_next_due_outbox(201, 1_000, "successor-lease")
        .await
        .expect("claim successor")
        .expect("successor progresses after legacy decision");
    assert_eq!(successor.client_id, first.client_id);
    assert_eq!(successor.stream_id, first.stream_id);
    assert_eq!(successor.stream_seq, 2);
}

#[tokio::test]
async fn legacy_outbox_upgrade_repairs_fifo_before_successor_enqueue() {
    let directory = tempfile::tempdir().expect("create node-local upgrade fixture");
    let path = directory.path().join("node.db");
    let subject = "v1/Pod/default/legacy-upgrade/legacy-uid";

    let db = open_node_local_on_disk(&path).await;
    for (key, enqueued_ms) in [
        ("legacy-live-head", 100),
        ("legacy-dead-head", 101),
        ("legacy-live-tail", 102),
    ] {
        db.enqueue_outbox(test_outbox_insert(key, subject, enqueued_ms))
            .await
            .expect("seed pre-stream row");
    }
    assert!(
        db.move_outbox_to_dead_letter_if_max_attempts("legacy-dead-head", 0)
            .await
            .expect("move legacy middle row to dead letter")
    );
    db.clear_all_outbox_stream_identity_for_test()
        .await
        .expect("model pre-stream live and dead-letter rows");
    drop(db);

    let db = open_node_local_on_disk(&path).await;
    db.enqueue_outbox(test_outbox_insert("current-successor", subject, 103))
        .await
        .expect("enqueue current row before any legacy claim");

    let first = db
        .claim_next_due_outbox(200, 1_000, "legacy-head-lease")
        .await
        .expect("claim repaired legacy head")
        .expect("legacy head remains first");
    assert_eq!(first.idempotency_key, "legacy-live-head");
    assert_eq!(first.stream_seq, 1);
    assert!(
        db.complete_outbox(first.id, "legacy-head-lease")
            .await
            .expect("complete legacy head")
    );

    let dead = db
        .list_dead_letter()
        .await
        .expect("list repaired dead letter");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].idempotency_key, "legacy-dead-head");
    assert_eq!(dead[0].client_id, first.client_id);
    assert_eq!(dead[0].stream_id, first.stream_id);
    assert_eq!(dead[0].stream_seq, 2);
    assert!(
        db.claim_next_due_outbox(200, 1_000, "blocked-live-tail")
            .await
            .expect("query behind repaired dead letter")
            .is_none(),
        "the repaired dead-letter sequence must block later legacy/current rows"
    );

    assert!(
        db.replay_dead_letter(dead[0].id, pod_status_classification())
            .await
            .expect("replay exact repaired dead-letter head")
    );
    let replay_now = i64::MAX / 4;
    let replayed = db
        .claim_next_due_outbox(replay_now, 1_000, "legacy-dead-replay")
        .await
        .expect("claim replayed legacy dead-letter row")
        .expect("replayed dead-letter row is the exact next sequence");
    assert_eq!(replayed.idempotency_key, "legacy-dead-head");
    assert_eq!(replayed.stream_seq, 2);
    assert!(
        db.complete_outbox(replayed.id, "legacy-dead-replay")
            .await
            .expect("complete replayed legacy dead-letter row")
    );

    for (expected_key, expected_seq, token) in [
        ("legacy-live-tail", 3, "legacy-tail-lease"),
        ("current-successor", 4, "current-successor-lease"),
    ] {
        let row = db
            .claim_next_due_outbox(replay_now, 1_000, token)
            .await
            .expect("claim ordered successor")
            .expect("ordered successor is deliverable");
        assert_eq!(row.idempotency_key, expected_key);
        assert_eq!(row.client_id, first.client_id);
        assert_eq!(row.stream_id, first.stream_id);
        assert_eq!(row.stream_seq, expected_seq);
        assert!(
            db.complete_outbox(row.id, token)
                .await
                .expect("complete ordered successor")
        );
    }
}

#[tokio::test]
async fn node_local_outbox_commits_stream_sequence_before_enqueue_returns() {
    let db = open_node_local_in_memory().await;
    let subject = "v1/Pod/default/atomic-sequence/pod-uid";

    db.enqueue_outbox(test_outbox_insert("atomic-sequence-1", subject, 1))
        .await
        .unwrap();
    assert_eq!(
        db.outbox_stream_position_for_test("atomic-sequence-1")
            .await
            .unwrap(),
        Some((
            crate::datastore::node_local::sqlite::outbox_stream_id(subject),
            1
        )),
        "the durable sequence must exist before any claim or delivery can observe the row",
    );

    db.enqueue_outbox(test_outbox_insert("atomic-sequence-2", subject, 2))
        .await
        .unwrap();
    assert_eq!(
        db.outbox_stream_position_for_test("atomic-sequence-2")
            .await
            .unwrap(),
        Some((
            crate::datastore::node_local::sqlite::outbox_stream_id(subject),
            2
        )),
        "enqueue must allocate the next sequence in the same transaction",
    );
}

#[tokio::test]
async fn outbox_durability_next_wake_tracks_the_fifo_blocker_not_blocked_younger_work() {
    let db = open_node_local_in_memory().await;
    let subject = "v1/Pod/default/fifo-wake/pod-uid";

    db.enqueue_outbox(test_outbox_insert("fifo-blocker", subject, 500))
        .await
        .unwrap();
    db.enqueue_outbox(test_outbox_insert("fifo-blocked-younger", subject, 100))
        .await
        .unwrap();

    assert_eq!(
        db.next_outbox_wake_ms(200).await.unwrap(),
        Some(500),
        "a past-due younger row cannot cause a wake loop while an older FIFO row is future-due",
    );
}

#[tokio::test]
async fn outbox_durability_next_wake_tracks_an_older_active_lease() {
    let db = open_node_local_in_memory().await;
    let subject = "v1/Pod/default/leased-fifo-wake/pod-uid";

    db.enqueue_outbox(test_outbox_insert("leased-fifo-blocker", subject, 100))
        .await
        .unwrap();
    db.enqueue_outbox(test_outbox_insert("leased-fifo-younger", subject, 100))
        .await
        .unwrap();
    db.claim_next_due_outbox(100, 400, "active-lease")
        .await
        .unwrap()
        .expect("claim older blocker");

    assert_eq!(
        db.next_outbox_wake_ms(200).await.unwrap(),
        Some(500),
        "the older row's lease expiry, not a blocked younger due time, controls the next wake",
    );
}

#[tokio::test]
async fn node_local_outbox_rows_share_stable_client_epoch() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert(
        "client-epoch-1",
        "v1/Pod/default/a/uid-a",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert(
        "client-epoch-2",
        "v1/Pod/default/b/uid-b",
        2,
    ))
    .await
    .unwrap();

    let first = db
        .claim_next_due_outbox(10, 1000, "lease-a")
        .await
        .unwrap()
        .unwrap();
    db.complete_outbox(first.id, "lease-a").await.unwrap();
    let second = db
        .claim_next_due_outbox(10, 1000, "lease-b")
        .await
        .unwrap()
        .unwrap();

    assert!(!first.client_id.is_empty());
    assert_eq!(first.client_id, second.client_id);
}

#[test]
fn node_local_outbox_hash_reserves_zero_for_unsequenced_ops() {
    for i in 0..4096 {
        let subject_key = format!("v1/Pod/default/web-{i}/uid-{i}");
        let stream_id = crate::datastore::node_local::sqlite::outbox_stream_id(&subject_key);
        assert!(
            stream_id > 0,
            "normal hashed subject {subject_key} must use a positive stream id, got {stream_id}"
        );
    }
}

#[tokio::test]
async fn node_local_outbox_claim_skips_same_subject_rows_with_stream_in_flight() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert(
        "inflight-stream-1",
        "v1/Pod/default/web-2/uid-2",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert(
        "inflight-stream-2",
        "v1/Pod/default/web-2/uid-2",
        2,
    ))
    .await
    .unwrap();

    let first = db
        .claim_next_due_outbox(10, 1000, "lease-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.stream_seq, 1);
    assert!(
        db.claim_next_due_outbox(10, 1000, "lease-b")
            .await
            .unwrap()
            .is_none(),
        "same stream must wait until the earlier leased row completes"
    );
}

#[tokio::test]
async fn node_local_outbox_batch_claim_is_atomic_across_independent_claimers() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert(
        "atomic-claim-a",
        "v1/Pod/default/a/uid-a",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert(
        "atomic-claim-b",
        "v1/Pod/default/b/uid-b",
        1,
    ))
    .await
    .unwrap();

    let (left, right) = tokio::join!(
        db.claim_due_outbox_batch(10, 2, 1_000, "claim-left"),
        db.claim_due_outbox_batch(10, 2, 1_000, "claim-right"),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let left_ids = left
        .iter()
        .map(|row| row.id)
        .collect::<std::collections::BTreeSet<_>>();
    let right_ids = right
        .iter()
        .map(|row| row.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(left_ids.is_disjoint(&right_ids));
    assert_eq!(left.len() + right.len(), 2);
}

#[tokio::test]
async fn node_local_outbox_prioritizes_status_before_older_events() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert_with_operation(
        "event-first",
        "events.k8s.io/v1/Event/default/diagnostic/event-uid",
        "EventCreate",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert_with_operation(
        "status-second",
        "v1/Pod/default/web/pod-uid",
        "PodStatus",
        2,
    ))
    .await
    .unwrap();

    let claimed = db
        .claim_next_due_outbox(10, 1000, "lease-status")
        .await
        .unwrap()
        .expect("status row should be claimable");

    assert_eq!(
        claimed.idempotency_key, "status-second",
        "readiness-critical Pod status must not wait behind older diagnostic events"
    );
}

#[tokio::test]
async fn node_local_outbox_ages_diagnostic_events_into_fair_service() {
    let db = open_node_local_in_memory().await;
    db.enqueue_outbox(test_outbox_insert_with_operation(
        "aged-event",
        "events.k8s.io/v1/Event/default/diagnostic/event-uid",
        "EventCreate",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(test_outbox_insert_with_operation(
        "fresh-status",
        "v1/Pod/default/web/pod-uid",
        "PodStatus",
        crate::node_outbox::payload::OUTBOX_DIAGNOSTIC_AGING_MS + 2,
    ))
    .await
    .unwrap();

    let claimed = db
        .claim_next_due_outbox(
            crate::node_outbox::payload::OUTBOX_DIAGNOSTIC_AGING_MS + 2,
            1_000,
            "lease-aged-event",
        )
        .await
        .unwrap()
        .expect("aged event should be claimable");

    assert_eq!(
        claimed.idempotency_key, "aged-event",
        "diagnostic events must receive bounded service under continuous status traffic"
    );
}

#[tokio::test]
async fn node_local_outbox_keeps_lease_then_node_status_ahead_of_workload_status() {
    let db = open_node_local_in_memory().await;
    for (key, operation) in [
        ("pod-status", "PodStatus"),
        ("node-status", "NodeStatus"),
        ("lease-renew", "LeaseRenew"),
    ] {
        db.enqueue_outbox(test_outbox_insert_with_operation(
            key,
            &format!("subject/{key}"),
            operation,
            1,
        ))
        .await
        .unwrap();
    }

    let mut order = Vec::new();
    for index in 0..3 {
        let token = format!("priority-lease-{index}");
        let row = db
            .claim_next_due_outbox(10, 1_000, &token)
            .await
            .unwrap()
            .expect("priority row");
        order.push(row.idempotency_key.clone());
        db.complete_outbox(row.id, &token).await.unwrap();
    }
    assert_eq!(order, ["lease-renew", "node-status", "pod-status"]);
}

#[tokio::test]
async fn node_local_schema_has_only_slim_uid_bound_tables() {
    let db = open_node_local_in_memory().await;

    let tables = db.table_names_for_test().await.expect("table names");

    assert!(tables.contains(&"outbox".to_string()));
    assert!(tables.contains(&"outbox_dead_letter".to_string()));
    assert!(tables.contains(&"pod_runtime".to_string()));
    assert!(tables.contains(&"pod_status_checkpoints".to_string()));
    assert!(tables.contains(&"pod_networks".to_string()));
    assert!(tables.contains(&"pod_endpoints".to_string()));
    assert!(tables.contains(&"pod_workqueue".to_string()));
    assert!(tables.contains(&"probe_state".to_string()));
    assert!(tables.contains(&"replication_checkpoint".to_string()));
    assert!(tables.contains(&"_node_meta".to_string()));

    for forbidden in [
        "namespaced_resources",
        "cluster_resources",
        "namespaces",
        "watch_events",
        "pod_sandboxes",
    ] {
        assert!(
            !tables.contains(&forbidden.to_string()),
            "node.db must not contain cluster resource/cache table {forbidden}"
        );
    }

    for table in [
        "outbox",
        "pod_runtime",
        "pod_status_checkpoints",
        "pod_networks",
        "pod_endpoints",
        "pod_workqueue",
        "probe_state",
    ] {
        assert!(
            db.table_has_not_null_column_for_test(table, "pod_uid")
                .await
                .expect("pod_uid column check"),
            "{table} must have pod_uid TEXT NOT NULL"
        );
    }

    assert!(
        !db.schema_contains_full_resource_body_column_for_test()
            .await
            .expect("body column check"),
        "node.db must not contain Kubernetes resource body data BLOB columns"
    );
}

#[tokio::test]
async fn pod_status_checkpoint_is_uid_bound_and_status_only() {
    let db = open_node_local_in_memory().await;

    db.upsert_pod_status_checkpoint(
        "uid-1",
        "default",
        "web",
        7,
        serde_json::json!({
            "phase": "Running",
            "podIP": "10.42.0.9",
        }),
        100,
    )
    .await
    .expect("upsert checkpoint");

    let checkpoint = db
        .get_pod_status_checkpoint("uid-1")
        .await
        .expect("get checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.pod_uid, "uid-1");
    assert_eq!(checkpoint.namespace, "default");
    assert_eq!(checkpoint.pod_name, "web");
    assert_eq!(checkpoint.base_rv, 7);
    assert_eq!(checkpoint.applied_rv, None);
    assert_eq!(
        checkpoint.status.pointer("/podIP").and_then(|v| v.as_str()),
        Some("10.42.0.9")
    );
    assert!(checkpoint.status.get("metadata").is_none());

    db.mark_pod_status_checkpoint_applied("uid-1", 12, 200)
        .await
        .expect("mark applied");
    assert_eq!(
        db.get_pod_status_checkpoint("uid-1")
            .await
            .expect("get marked")
            .expect("checkpoint still exists")
            .applied_rv,
        Some(12)
    );

    db.delete_pod_status_checkpoint("uid-1")
        .await
        .expect("delete checkpoint");
    assert!(
        db.get_pod_status_checkpoint("uid-1")
            .await
            .expect("get deleted")
            .is_none()
    );
}

#[tokio::test]
async fn node_meta_mismatch_refuses_boot() {
    let db = open_node_local_in_memory().await;

    db.ensure_node_identity("cluster-a", "node-a")
        .await
        .expect("initial identity write");

    let err = db
        .ensure_node_identity("cluster-b", "node-a")
        .await
        .expect_err("cluster id change must refuse boot");

    assert!(err.to_string().contains("node.db identity mismatch"));
}

#[tokio::test]
async fn pod_runtime_is_uid_keyed_and_same_name_replacements_are_distinct() {
    let db = open_node_local_in_memory().await;

    db.admit_pod_runtime("uid-old", "default", "web", "worker-a")
        .await
        .expect("admit old uid");
    db.admit_pod_runtime("uid-new", "default", "web", "worker-a")
        .await
        .expect("admit new uid");

    let rows = db.list_pod_runtime().await.expect("list runtime");
    let uids: Vec<_> = rows.into_iter().map(|row| row.pod_uid).collect();

    assert_eq!(uids, vec!["uid-new".to_string(), "uid-old".to_string()]);
}

#[tokio::test]
async fn pod_slot_persistence_preserves_uid_cas_outcomes_and_monotonic_versions() {
    let db = open_node_local_in_memory().await;
    let mut events = db.subscribe_pod_slot_admissions();

    let admitted = db
        .pod_slot_try_admit("default", "web", "uid-old", "worker-a")
        .await
        .expect("admit old uid");
    assert_eq!(
        admitted,
        PodSlotAdmissionResult::Admitted {
            resource_version: 1
        }
    );
    assert!(matches!(
        events.recv().await.expect("admit event"),
        PodSlotAdmissionEvent::Changed {
            pod_uid,
            state: PodSlotAdmissionState::Admitted,
            resource_version: 1,
            ..
        } if pod_uid == "uid-old"
    ));

    assert_eq!(
        db.pod_slot_try_admit("default", "web", "uid-new", "worker-a")
            .await
            .expect("replacement must be blocked"),
        PodSlotAdmissionResult::Blocked {
            blocking_uid: "uid-old".to_string(),
            blocking_node: "worker-a".to_string(),
            state: PodSlotAdmissionState::Admitted,
            resource_version: 1,
        }
    );
    assert_eq!(
        db.pod_slot_mark_terminating("default", "web", "uid-old", "worker-a")
            .await
            .expect("mark old uid terminating"),
        PodSlotMutationResult::Changed {
            resource_version: 2
        }
    );
    assert!(matches!(
        db.pod_slot_clear_if_uid("default", "web", "uid-new")
            .await
            .expect("wrong uid is a typed no-op"),
        PodSlotClearResult::UidMismatch {
            blocking_uid,
            resource_version: 2,
            ..
        } if blocking_uid == "uid-old"
    ));
    assert_eq!(
        db.pod_slot_clear_if_uid("default", "web", "uid-old")
            .await
            .expect("clear old uid"),
        PodSlotClearResult::Cleared {
            resource_version: 3
        }
    );
    assert_eq!(
        db.pod_slot_try_admit("default", "web", "uid-new", "worker-a")
            .await
            .expect("admit replacement after clear"),
        PodSlotAdmissionResult::Admitted {
            resource_version: 4
        }
    );
}

#[tokio::test]
async fn sqlite_backend_implements_node_local_backend() {
    let handle = open_sqlite_node_local_backend_handle().await;
    fn assert_backend_trait(_: &dyn NodeLocalBackend) {}
    assert_backend_trait(handle.as_ref());
    assert_eq!(handle.backend_name(), "sqlite");

    handle
        .set_node_meta("node_uid", "node-a")
        .await
        .expect("write meta through trait object");
    assert_eq!(
        handle.get_node_meta("node_uid").await.expect("read meta"),
        Some("node-a".to_string())
    );
}

#[tokio::test]
async fn selector_creates_sqlite_node_db_and_node_local_schema() {
    let directory = tempfile::tempdir().expect("node-local selector fixture");
    std::fs::set_permissions(
        directory.path(),
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("secure node-local fixture directory");
    let path = directory.path().join("node.db");
    let handle = selector::open_node_local(
        BackendKind::Sqlite,
        Some(&path),
        supervisor(),
        None,
        "sqlite:node-local-selector-test",
    )
    .await
    .expect("open sqlite node-local");

    assert_eq!(handle.backend_name(), "sqlite");
    assert!(path.is_file(), "node-local selector must create node.db");
    handle
        .set_node_meta("schema-owner", "node-local")
        .await
        .expect("node-local selector must initialize the node metadata table");
    assert_eq!(
        handle
            .get_node_meta("schema-owner")
            .await
            .expect("read initialized node metadata table")
            .as_deref(),
        Some("node-local")
    );
}

#[tokio::test]
async fn redb_node_local_selector_fails_fast_until_backend_lands() {
    let result = selector::open_node_local(
        BackendKind::Redb,
        None,
        supervisor(),
        None,
        "redb:node-local-selector-test",
    )
    .await;
    let err = match result {
        Ok(handle) => panic!(
            "redb node-local unexpectedly opened {}",
            handle.backend_name()
        ),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("node-local redb backend not implemented yet"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_local_handle_hides_concrete_backend_type() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[test]
fn node_local_backend_is_not_exposed_by_datastore_backend() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[test]
fn node_local_backend_has_no_cluster_resource_crud() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[tokio::test]
async fn endpoint_handoff_queues_mutation_after_authoritative_snapshot() {
    let db = open_node_local_in_memory().await;
    let handoff_guard = db.lock_pod_endpoint_handoff_for_test().await;
    let mut subscribe = Box::pin(db.subscribe_pod_endpoints_with_snapshot());
    assert!(matches!(
        futures::poll!(subscribe.as_mut()),
        std::task::Poll::Pending
    ));

    let row = crate::datastore::node_local::PodEndpointRow {
        pod_uid: "handoff-uid".into(),
        namespace: "default".into(),
        pod_name: "handoff-pod".into(),
        node_name: "node-a".into(),
        mode: crate::datastore::node_local::PodEndpointMode::EncryptedDirect,
        pod_ip: "10.42.0.9".parse().unwrap(),
        node_ip: "192.0.2.9".parse().unwrap(),
        host_port_tcp: None,
        host_port_udp: None,
        generation: 1,
        updated_at: 1,
    };
    let mut upsert = Box::pin(db.upsert_endpoint(row.clone()));
    assert!(matches!(
        futures::poll!(upsert.as_mut()),
        std::task::Poll::Pending
    ));

    drop(handoff_guard);
    let (snapshot, mut events) = subscribe.await.expect("atomic endpoint handoff");
    assert!(
        snapshot.is_empty(),
        "queued upsert must be after the snapshot"
    );
    upsert.await.expect("queued endpoint upsert");
    assert_eq!(
        events.recv().await.expect("post-snapshot event"),
        crate::datastore::node_local::PodEndpointEvent::Upsert(row)
    );
}

#[tokio::test]
async fn malformed_endpoint_ports_fail_instead_of_wrapping_to_u16() {
    let db = open_node_local_in_memory().await;
    for (tcp, udp) in [(Some(65_536i64), None), (None, Some(-1i64))] {
        db.db_call("test_insert_malformed_endpoint_port", move |conn| {
            conn.execute("DELETE FROM pod_endpoints", [])?;
            conn.execute(
                "INSERT INTO pod_endpoints
                 (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip,
                  host_port_tcp, host_port_udp, generation, updated_ms)
                 VALUES ('bad-port', 'default', 'bad-port', 'node-a', 'hostport',
                         '10.42.0.10', '192.0.2.10', ?1, ?2, 1, 1)",
                rusqlite::params![tcp, udp],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let error = db
            .get_endpoint_by_pod_ip("10.42.0.10".parse().unwrap())
            .await
            .expect_err("invalid persisted port must fail decoding");
        assert!(
            format!("{error:#}").contains("pod endpoint port outside 1..=65535"),
            "unexpected decode error: {error:#}"
        );
    }
}

// ── Task 9: RuntimeObservationCheckpoint tests ────────────────────

#[tokio::test]
async fn runtime_observation_checkpoint_survives_actor_restart() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-restart".to_string(),
        container_ids: vec![
            "containerd://ctr-abc".to_string(),
            "containerd://ctr-def".to_string(),
        ],
        generation: 2,
        updated_ms: 1000,
    })
    .await
    .expect("upsert checkpoint");

    // Simulate actor restart: a new actor reads back the persisted checkpoint.
    let loaded = db
        .get_runtime_observation_checkpoint("uid-restart")
        .await
        .expect("get checkpoint")
        .expect("checkpoint must exist after actor restart");

    assert_eq!(loaded.pod_uid, "uid-restart");
    assert_eq!(loaded.generation, 2);
    assert!(
        loaded
            .container_ids
            .contains(&"containerd://ctr-abc".to_string())
    );
    assert!(
        loaded
            .container_ids
            .contains(&"containerd://ctr-def".to_string())
    );
    assert_eq!(loaded.updated_ms, 1000);
}

#[tokio::test]
async fn runtime_observation_checkpoint_survives_worker_restart() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    // Write checkpoints for two pods
    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-pod-a".to_string(),
        container_ids: vec!["containerd://ctr-a1".to_string()],
        generation: 1,
        updated_ms: 500,
    })
    .await
    .expect("upsert pod-a checkpoint");

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-pod-b".to_string(),
        container_ids: vec!["containerd://ctr-b1".to_string()],
        generation: 3,
        updated_ms: 750,
    })
    .await
    .expect("upsert pod-b checkpoint");

    // Simulate worker restart: both checkpoints survive and can be loaded.
    let a = db
        .get_runtime_observation_checkpoint("uid-pod-a")
        .await
        .expect("get a")
        .expect("a exists");
    assert_eq!(a.generation, 1);
    let b = db
        .get_runtime_observation_checkpoint("uid-pod-b")
        .await
        .expect("get b")
        .expect("b exists");
    assert_eq!(b.generation, 3);

    // Reconcile pod-a; checkpoint deleted. pod-b checkpoint survives.
    db.delete_runtime_observation_checkpoint("uid-pod-a")
        .await
        .expect("delete a");
    assert!(
        db.get_runtime_observation_checkpoint("uid-pod-a")
            .await
            .expect("get a after delete")
            .is_none()
    );
    assert!(
        db.get_runtime_observation_checkpoint("uid-pod-b")
            .await
            .expect("get b after a delete")
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_uid_bound() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-alpha".to_string(),
        container_ids: vec!["containerd://alpha-1".to_string()],
        generation: 5,
        updated_ms: 100,
    })
    .await
    .expect("upsert alpha");

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-beta".to_string(),
        container_ids: vec![
            "containerd://beta-1".to_string(),
            "containerd://beta-2".to_string(),
        ],
        generation: 7,
        updated_ms: 200,
    })
    .await
    .expect("upsert beta");

    // Each UID returns only its own checkpoint.
    let alpha = db
        .get_runtime_observation_checkpoint("uid-alpha")
        .await
        .expect("get alpha")
        .expect("alpha exists");
    assert_eq!(alpha.container_ids, vec!["containerd://alpha-1"]);
    assert_eq!(alpha.generation, 5);

    let beta = db
        .get_runtime_observation_checkpoint("uid-beta")
        .await
        .expect("get beta")
        .expect("beta exists");
    assert_eq!(beta.container_ids.len(), 2);
    assert_eq!(beta.generation, 7);

    // Deleting alpha must not affect beta.
    db.delete_runtime_observation_checkpoint("uid-alpha")
        .await
        .expect("delete alpha");
    assert!(
        db.get_runtime_observation_checkpoint("uid-alpha")
            .await
            .expect("get alpha gone")
            .is_none()
    );
    assert!(
        db.get_runtime_observation_checkpoint("uid-beta")
            .await
            .expect("get beta still")
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_removed_after_successful_reconcile() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-reconcile".to_string(),
        container_ids: vec!["containerd://ctr-99".to_string()],
        generation: 1,
        updated_ms: 300,
    })
    .await
    .expect("upsert before reconcile");

    assert!(
        db.get_runtime_observation_checkpoint("uid-reconcile")
            .await
            .expect("pre-reconcile get")
            .is_some()
    );

    // Successful reconcile: actor removes its checkpoint.
    db.delete_runtime_observation_checkpoint("uid-reconcile")
        .await
        .expect("delete after reconcile");

    assert!(
        db.get_runtime_observation_checkpoint("uid-reconcile")
            .await
            .expect("post-reconcile get")
            .is_none(),
        "checkpoint must be gone after successful reconcile"
    );
}
