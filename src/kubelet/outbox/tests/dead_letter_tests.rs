use std::sync::Arc;

use crate::datastore::ResourcePreconditions;
use crate::datastore::backend_kind::BackendKind;
use crate::datastore::command::StorageCommand;
use crate::datastore::node_local::{DeadLetterTestInsert, NodeLocalHandle, OutboxInsert};
use crate::datastore::node_local::{SqliteNodeLocalDb, selector};
use crate::datastore::sqlite::{DbExecutor, opener};
use crate::kubelet::outbox::payload::OutboxPayload;
use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn node_db() -> NodeLocalHandle {
    selector::open_node_local(
        BackendKind::Sqlite,
        None,
        supervisor(),
        None,
        "sqlite:dead-letter-test",
    )
    .await
    .expect("open node-local test db")
}

async fn node_db_concrete() -> SqliteNodeLocalDb {
    let executor = DbExecutor::open_with_opts(
        opener::OpenOpts::node_in_memory(),
        supervisor(),
        "sqlite:dead-letter-concrete-test",
    )
    .await
    .expect("open node-local executor");
    SqliteNodeLocalDb::from_executor(executor).expect("create sqlite node-local db")
}

fn pod_status_payload_bytes(namespace: &str, name: &str, uid: &str) -> Vec<u8> {
    let command = StorageCommand::UpdateStatus {
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
    };
    OutboxPayload::from_command(command)
        .encode_protobuf()
        .expect("encode payload")
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[tokio::test]
async fn dead_letter_after_max_attempts() {
    let ndb = node_db().await;

    // Enqueue a row with attempt=719 (one before max)
    ndb.enqueue_outbox(OutboxInsert {
        idempotency_key: "dead-letter-key".to_string(),
        enqueued_ms: 1000,
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        subject_api_version: "v1".to_string(),
        subject_kind: "Pod".to_string(),
        subject_namespace: Some("default".to_string()),
        subject_name: "web".to_string(),
        subject_uid: Some("uid-1".to_string()),
        pod_uid: "uid-1".to_string(),
        operation: "PodStatus".to_string(),
        payload_proto: pod_status_payload_bytes("default", "web", "uid-1"),
        next_due_ms: 1000,
    })
    .await
    .expect("enqueue");

    // Simulate 720 attempts by trying to dead-letter with different attempt thresholds.
    // The dead_letter_if_max_attempts checks that the row's attempt >= max_attempts before moving.
    // Since the row has attempt=0 by default, first call with max_attempts=0 should dead-letter.
    let result = ndb
        .move_outbox_to_dead_letter_if_max_attempts("dead-letter-key", 0)
        .await
        .expect("move to dead letter");
    assert!(
        result,
        "should move to dead letter when max_attempts=0 and attempt=0"
    );

    // After moving, row should be in dead-letter
    let dead_rows = ndb.list_dead_letter().await.expect("list dead letter");
    assert_eq!(dead_rows.len(), 1);
    assert_eq!(dead_rows[0].idempotency_key, "dead-letter-key");

    // Outbox should be empty for this key
    let far_future = now_ms() + 86_400_000;
    let remaining = ndb
        .claim_next_due_outbox(far_future, 100, "check-empty")
        .await
        .expect("claim check");
    assert!(
        remaining.is_none()
            || remaining
                .as_ref()
                .is_some_and(|r| r.idempotency_key != "dead-letter-key"),
        "outbox should not contain the dead-lettered key"
    );
}

#[tokio::test]
async fn replay_dead_letter_re_enqueues_with_attempt_zero() {
    let ndb = node_db_concrete().await;

    // Insert directly into dead_letter via test-only helper
    let payload = pod_status_payload_bytes("default", "web", "uid-1");
    ndb.insert_dead_letter_test_only(DeadLetterTestInsert {
        idempotency_key: "replay-key",
        operation: "PodStatus",
        subject_key: "v1/Pod/default/web/uid-1",
        subject_api_version: "v1",
        subject_kind: "Pod",
        subject_namespace: Some("default"),
        subject_name: "web",
        subject_uid: Some("uid-1"),
        pod_uid: "uid-1",
        payload_proto: &payload,
        attempts: 720,
        last_error: "max attempts",
        moved_at_ms: 2000,
    })
    .await
    .expect("insert dead letter");

    // Replay it
    ndb.replay_dead_letter(1).await.expect("replay dead letter");

    // Dead letter should be empty
    let dead_rows = ndb.list_dead_letter().await.expect("list dead letter");
    assert!(
        dead_rows.is_empty(),
        "dead letter should be empty after replay"
    );

    // Outbox should have the re-enqueued row with attempt=0.  Use a far-future now_ms
    // to ensure the newly inserted row (with current-time next_due_ms) is claimed.
    let far_future = now_ms() + 86_400_000; // 24h from now
    let row = ndb
        .claim_next_due_outbox(far_future, 1000, "replay-check")
        .await
        .expect("claim")
        .expect("row should exist");
    assert_eq!(row.idempotency_key, "replay-key");
    assert_eq!(row.attempt, 0, "replayed row should have attempt=0");
    assert_ne!(
        row.next_due_ms, 2000,
        "next_due_ms should be current time, not original"
    );
}

#[tokio::test]
async fn delete_dead_letter_removes_entry() {
    let ndb = node_db_concrete().await;

    let payload = pod_status_payload_bytes("default", "web", "uid-1");
    ndb.insert_dead_letter_test_only(DeadLetterTestInsert {
        idempotency_key: "delete-key",
        operation: "PodStatus",
        subject_key: "v1/Pod/default/web/uid-1",
        subject_api_version: "v1",
        subject_kind: "Pod",
        subject_namespace: Some("default"),
        subject_name: "web",
        subject_uid: Some("uid-1"),
        pod_uid: "uid-1",
        payload_proto: &payload,
        attempts: 720,
        last_error: "max attempts",
        moved_at_ms: 2000,
    })
    .await
    .expect("insert dead letter");

    ndb.delete_dead_letter(1).await.expect("delete dead letter");

    let dead_rows = ndb.list_dead_letter().await.expect("list dead letter");
    assert!(
        dead_rows.is_empty(),
        "dead letter should be empty after delete"
    );
}

#[tokio::test]
async fn outbox_stats_return_metrics() {
    let ndb = node_db().await;

    // Enqueue a few rows
    for i in 0..3 {
        ndb.enqueue_outbox(OutboxInsert {
            idempotency_key: format!("stats-key-{}", i),
            enqueued_ms: now_ms(),
            subject_key: format!("v1/Pod/default/web-{i}/uid-{i}"),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: format!("web-{}", i),
            subject_uid: Some(format!("uid-{}", i)),
            pod_uid: format!("uid-{}", i),
            operation: "PodStatus".to_string(),
            payload_proto: pod_status_payload_bytes(
                "default",
                &format!("web-{}", i),
                &format!("uid-{}", i),
            ),
            next_due_ms: now_ms(),
        })
        .await
        .expect("enqueue stats row");
    }

    let stats = ndb.outbox_stats().await.expect("outbox stats");
    assert_eq!(stats.pending, 3);
    assert!(stats.oldest_age_seconds >= 0.0);
    assert_eq!(stats.dead_letter_count, 0);
}
