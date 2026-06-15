//! Leader-side lost-update guard for pipelined Pod status dispatch.
//!
//! The status outbox drops the live-RV precondition so a slow status apply no
//! longer stalls behind a newer resourceVersion. That reopened the classic
//! "an older status snapshot, retried after a newer one already applied,
//! clobbers it" race (pipelined in-flight rows + per-subject FIFO only at
//! claim time). The fix: each worker stamps its status snapshots with a
//! strictly-increasing value; the leader records the highest stamp applied
//! per Pod subject and no-ops any snapshot whose stamp is older-or-equal.

use super::*;
use crate::datastore::ResourcePreconditions;
use crate::datastore::command::StorageCommand;
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
use serde_json::json;

const STATUS_OPS: &[OutboxOperation] = &[
    OutboxOperation::PodStatus,
    OutboxOperation::RuntimeReconcile,
    OutboxOperation::ProbeReadiness,
    OutboxOperation::DeadlineExceeded,
    OutboxOperation::ContainerStatusSnapshot,
    OutboxOperation::EphemeralContainerStatuses,
];

async fn create_running_pod(db: &Datastore, uid: &str) {
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "web", "uid": uid},
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");
}

fn status_payload(status: serde_json::Value, uid: &str, stamp: Option<i64>) -> Vec<u8> {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status,
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: stamp,
    };
    OutboxPayload::from_command(command)
        .encode_protobuf()
        .expect("encode payload")
}

async fn live_status_message(db: &Datastore) -> Option<String> {
    db.get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("read pod")
        .expect("pod exists")
        .data
        .pointer("/status/message")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

#[tokio::test]
async fn stale_status_retry_does_not_clobber_newer_status_for_all_ops() {
    for op in STATUS_OPS {
        let db = Datastore::new_in_memory().await.expect("db");
        create_running_pod(&db, "uid-1").await;

        // Newer snapshot (stamp 200) applies first.
        db.apply_outbox_transactionally(
            "key-newer",
            op.as_str(),
            &status_payload(
                json!({"phase": "Running", "message": "newer"}),
                "uid-1",
                Some(200),
            ),
            "worker-a",
        )
        .await
        .unwrap_or_else(|e| panic!("apply newer for {op}: {e}"));
        assert_eq!(live_status_message(&db).await.as_deref(), Some("newer"));

        // Stale snapshot (stamp 100) retried afterwards must be a no-op,
        // not an error: the worker row must complete instead of retrying.
        db.apply_outbox_transactionally(
            "key-stale",
            op.as_str(),
            &status_payload(
                json!({"phase": "Running", "message": "stale"}),
                "uid-1",
                Some(100),
            ),
            "worker-a",
        )
        .await
        .unwrap_or_else(|e| panic!("apply stale for {op}: {e}"));

        assert_eq!(
            live_status_message(&db).await.as_deref(),
            Some("newer"),
            "stale status retry must not clobber the newer status for {op}"
        );
    }
}

#[tokio::test]
async fn in_order_status_snapshots_apply_for_all_ops() {
    for op in STATUS_OPS {
        let db = Datastore::new_in_memory().await.expect("db");
        create_running_pod(&db, "uid-1").await;

        db.apply_outbox_transactionally(
            "key-first",
            op.as_str(),
            &status_payload(
                json!({"phase": "Running", "message": "first"}),
                "uid-1",
                Some(100),
            ),
            "worker-a",
        )
        .await
        .unwrap_or_else(|e| panic!("apply first for {op}: {e}"));

        db.apply_outbox_transactionally(
            "key-second",
            op.as_str(),
            &status_payload(
                json!({"phase": "Running", "message": "second"}),
                "uid-1",
                Some(200),
            ),
            "worker-a",
        )
        .await
        .unwrap_or_else(|e| panic!("apply second for {op}: {e}"));

        assert_eq!(
            live_status_message(&db).await.as_deref(),
            Some("second"),
            "newer snapshot in normal order must apply for {op}"
        );
    }
}

#[tokio::test]
async fn equal_stamp_snapshot_is_treated_as_already_applied() {
    let db = Datastore::new_in_memory().await.expect("db");
    create_running_pod(&db, "uid-1").await;

    db.apply_outbox_transactionally(
        "key-a",
        OutboxOperation::PodStatus.as_str(),
        &status_payload(
            json!({"phase": "Running", "message": "a"}),
            "uid-1",
            Some(200),
        ),
        "worker-a",
    )
    .await
    .expect("apply a");

    // Same stamp, different snapshot: older-or-equal → no-op (defensive; the
    // producer guarantees strict monotonicity so this only fires on a true
    // duplicate-grade resend).
    db.apply_outbox_transactionally(
        "key-b",
        OutboxOperation::PodStatus.as_str(),
        &status_payload(
            json!({"phase": "Running", "message": "b"}),
            "uid-1",
            Some(200),
        ),
        "worker-a",
    )
    .await
    .expect("apply b");

    assert_eq!(live_status_message(&db).await.as_deref(), Some("a"));
}

#[tokio::test]
async fn uid_mismatch_remains_terminal_under_gate() {
    let db = Datastore::new_in_memory().await.expect("db");
    create_running_pod(&db, "uid-1").await;

    let result = db
        .apply_outbox_transactionally(
            "key-mismatch",
            OutboxOperation::PodStatus.as_str(),
            &status_payload(
                json!({"phase": "Running", "message": "wrong-uid"}),
                "uid-OTHER",
                Some(500),
            ),
            "worker-a",
        )
        .await;

    assert!(
        result.is_err(),
        "a status snapshot for a different UID must be a terminal error"
    );
    assert_eq!(
        live_status_message(&db).await,
        None,
        "same-name replacement protection: a mismatched UID must not write status"
    );
}

#[tokio::test]
async fn unstamped_status_keeps_last_writer_wins() {
    // Backward compatibility: a command without a stamp (direct API/status
    // writers, older payloads) is not gated and keeps last-writer-wins.
    let db = Datastore::new_in_memory().await.expect("db");
    create_running_pod(&db, "uid-1").await;

    db.apply_outbox_transactionally(
        "key-1",
        OutboxOperation::PodStatus.as_str(),
        &status_payload(json!({"phase": "Running", "message": "one"}), "uid-1", None),
        "worker-a",
    )
    .await
    .expect("apply one");
    db.apply_outbox_transactionally(
        "key-2",
        OutboxOperation::PodStatus.as_str(),
        &status_payload(json!({"phase": "Running", "message": "two"}), "uid-1", None),
        "worker-a",
    )
    .await
    .expect("apply two");

    assert_eq!(live_status_message(&db).await.as_deref(), Some("two"));
}
