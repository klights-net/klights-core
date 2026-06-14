use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::datastore::ResourcePreconditions;
use crate::datastore::backend_kind::BackendKind;
use crate::datastore::command::StorageCommand;
use crate::datastore::node_local::{NodeLocalHandle, selector};
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
use crate::kubelet::outbox::{
    DispatchOutcome, Outbox, OutboxApplyClient, OutboxApplyError, OutboxApplyResult, OutboxCommand,
    OutboxDispatcher, OutboxSubject,
};
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
        "sqlite:batch-test",
    )
    .await
    .expect("open node-local test db")
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
    }
}

/// A fake apply client that records calls. Responses must be pre-loaded
/// via `push_responses`: each call pops the next response from the stack.
#[derive(Default)]
struct StackApplyClient {
    calls: Mutex<Vec<String>>,
    responses: Mutex<Vec<Result<OutboxApplyResult, OutboxApplyError>>>,
}

impl StackApplyClient {
    async fn push_response(&self, response: Result<OutboxApplyResult, OutboxApplyError>) {
        self.responses.lock().await.push(response);
    }

    async fn calls(&self) -> Vec<String> {
        self.calls.lock().await.clone()
    }
}

#[async_trait]
impl OutboxApplyClient for StackApplyClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        _operation: OutboxOperation,
        _payload: Bytes,
    ) -> Result<OutboxApplyResult, OutboxApplyError> {
        self.calls.lock().await.push(idempotency_key.to_string());
        self.responses
            .lock()
            .await
            .pop()
            .unwrap_or(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
    }
}

#[tokio::test]
async fn leader_dispatcher_uses_consolidated_apply() {
    // Leader dispatcher with batch_mode sends multiple rows in one
    // claim/complete cycle (consolidated via node.db batch ops).
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

    // Enqueue 10 rows for different subjects (different pods).
    let now = 1_000i64;
    for i in 0..10 {
        let pod_name = format!("pod-{}", i);
        let pod_uid = format!("uid-{}", i);
        let subject_key = format!("v1/Pod/default/{}/{}", pod_name, pod_uid);
        outbox
            .enqueue_command(OutboxCommand::new(
                format!("batch-key-{}", i),
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    subject_key,
                    Some("default".to_string()),
                    pod_name.clone(),
                    Some(pod_uid.clone()),
                ),
                &pod_uid,
                pod_status_command("default", &pod_name, &pod_uid),
                now + i,
            ))
            .await
            .expect("enqueue");
    }

    // Pre-load success responses for all 10 rows.
    for _ in 0..10 {
        client
            .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
            .await;
    }

    // Single dispatch call should claim all due rows and apply them.
    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 10)
            .await
            .expect("dispatch"),
        DispatchOutcome::Dispatched
    );

    let calls = client.calls().await;
    assert_eq!(calls.len(), 10, "all 10 rows should be dispatched");

    // All rows should be completed (no more due).
    assert!(
        node_db
            .claim_next_due_outbox(now + 100, 1_000, "check-empty")
            .await
            .expect("claim after batch")
            .is_none(),
        "all rows should be completed after batch dispatch"
    );
}

#[tokio::test]
async fn leader_batch_respects_subject_fifo() {
    // When multiple rows exist for the same subject, the batch claim must
    // respect per-subject_key FIFO: only the oldest due row per subject
    // is claimed per batch.
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

    let now = 2_000i64;
    // Three rows for the SAME subject ("v1/Pod/default/web/uid-web").
    for i in 0..3 {
        outbox
            .enqueue_command(OutboxCommand::new(
                format!("fifo-key-{}", i),
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-web",
                    Some("default".to_string()),
                    "web",
                    Some("uid-web".to_string()),
                ),
                "uid-web",
                pod_status_command("default", "web", "uid-web"),
                now + i,
            ))
            .await
            .expect("enqueue");
    }

    // Pre-load one response.
    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
        .await;

    // First batch should claim only the oldest row (fifo-key-0).
    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 10)
            .await
            .expect("dispatch"),
        DispatchOutcome::Dispatched
    );

    let calls = client.calls().await;
    assert_eq!(
        calls.len(),
        1,
        "only the oldest row per subject should be claimed"
    );
    assert_eq!(calls[0], "fifo-key-0", "oldest row dispatched first");

    // After completing fifo-key-0, the next batch should claim fifo-key-1.
    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 2 }))
        .await;
    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 20)
            .await
            .expect("dispatch"),
        DispatchOutcome::Dispatched
    );
    let calls2 = client.calls().await;
    assert_eq!(calls2.len(), 2);
    assert_eq!(
        calls2[1], "fifo-key-1",
        "second row dispatched after first completes"
    );

    // Last row.
    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 3 }))
        .await;
    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 30)
            .await
            .expect("dispatch"),
        DispatchOutcome::Dispatched
    );
}

/// Client that simulates a crash after cluster apply but before
/// node-db complete. First call succeeds (simulates cluster apply),
/// second call for the same key returns AlreadyApplied (ledger replay).
#[derive(Default)]
struct CrashRecoveryApplyClient {
    calls: Mutex<Vec<String>>,
    applied: Mutex<HashSet<String>>,
}

impl CrashRecoveryApplyClient {
    async fn calls(&self) -> Vec<String> {
        self.calls.lock().await.clone()
    }
}

#[async_trait]
impl OutboxApplyClient for CrashRecoveryApplyClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        _operation: OutboxOperation,
        _payload: Bytes,
    ) -> Result<OutboxApplyResult, OutboxApplyError> {
        self.calls.lock().await.push(idempotency_key.to_string());
        let mut applied = self.applied.lock().await;
        if applied.insert(idempotency_key.to_string()) {
            Ok(OutboxApplyResult::Applied {
                applied_rv: applied.len() as i64,
            })
        } else {
            Ok(OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(applied.len() as i64),
            })
        }
    }
}

#[tokio::test]
async fn crash_after_cluster_apply_before_node_complete_replays_from_ledger() {
    // Simulate: dispatcher applies to leader (cluster.db), then crashes
    // before completing the node.db row. On restart, the row is re-claimed
    // (expired lease) and re-dispatched. The leader returns AlreadyApplied
    // from its ledger, and the dispatcher completes the row without
    // duplicating the mutation.
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());

    // First dispatcher "crashes" after cluster apply but before node-db complete.
    {
        let client = Arc::new(CrashRecoveryApplyClient::default());
        let _dispatcher =
            OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

        let now = 3_000i64;
        outbox
            .enqueue_command(OutboxCommand::new(
                "crash-key",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-web",
                    Some("default".to_string()),
                    "web",
                    Some("uid-web".to_string()),
                ),
                "uid-web",
                pod_status_command("default", "web", "uid-web"),
                now,
            ))
            .await
            .expect("enqueue");

        // Simulate successful cluster apply (ledger recorded).
        client
            .apply_outbox(
                "crash-key",
                OutboxOperation::PodStatus,
                Bytes::from(
                    OutboxPayload::from_command(pod_status_command("default", "web", "uid-web"))
                        .encode_protobuf()
                        .unwrap(),
                ),
            )
            .await
            .expect("cluster apply");

        // Now claim the row with a short lease and let it "expire"
        // (don't complete it, simulating a crash).
        let row = node_db
            .claim_next_due_outbox(now, 10, "crashed-dispatcher")
            .await
            .expect("claim")
            .expect("row");
        assert_eq!(row.idempotency_key, "crash-key");
        // Don't complete — crash!
    }

    // Second dispatcher starts after crash recovery.
    {
        let client = Arc::new(CrashRecoveryApplyClient::default());
        let dispatcher =
            OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

        let now = 4_000i64;
        // The row's lease has expired. dispatcher should re-claim and
        // re-dispatch. Leader returns AlreadyApplied, so dispatcher
        // completes the row without double-applying.

        // Pre-seed the leader with the same key to simulate ledger replay.
        client
            .apply_outbox("crash-key", OutboxOperation::PodStatus, Bytes::new())
            .await
            .expect("pre-seed ledger");

        assert_eq!(
            dispatcher.dispatch_due_once(now).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );

        let calls = client.calls().await;
        // 2 calls: 1 for pre-seed (simulating ledger record), 1 for dispatch
        assert_eq!(
            calls.len(),
            2,
            "row should be pre-seeded then re-dispatched"
        );
        assert_eq!(calls[0], "crash-key");
        assert_eq!(calls[1], "crash-key");

        // Row should be completed.
        assert!(
            node_db
                .claim_next_due_outbox(now + 100, 1_000, "check-empty")
                .await
                .expect("claim after recovery")
                .is_none(),
            "row should be completed after crash recovery"
        );
    }
}

#[tokio::test]
async fn idempotency_survives_gc_replay() {
    // When the leader's applied_outbox record is pruned by GC, a replayed
    // status outbox must succeed as a fresh apply without causing inconsistency.
    // This test verifies the dispatcher side: if the leader returns
    // Applied (not AlreadyApplied) after a GC, the dispatcher handles it correctly.
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

    let now = 5_000i64;
    outbox
        .enqueue_command(OutboxCommand::new(
            "gc-replay-key",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                "v1/Pod/default/web/uid-web",
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            now,
        ))
        .await
        .expect("enqueue");

    // First apply succeeds (creates ledger).
    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
        .await;
    assert_eq!(
        dispatcher.dispatch_due_once(now).await.expect("dispatch"),
        DispatchOutcome::Dispatched
    );

    // Re-enqueue the same key (simulating a retry after GC pruned the ledger).
    // The leader treats it as a fresh apply because the ledger is gone.
    outbox
        .enqueue_command(OutboxCommand::new(
            "gc-replay-key",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                "v1/Pod/default/web/uid-web",
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            now + 100,
        ))
        .await
        .expect("re-enqueue after gc");

    // Leader applies as fresh (ledger was pruned).
    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 2 }))
        .await;
    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 100)
            .await
            .expect("dispatch"),
        DispatchOutcome::Dispatched
    );

    let calls = client.calls().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], "gc-replay-key");
    assert_eq!(calls[1], "gc-replay-key");

    // Both dispatches completed successfully.
    assert!(
        node_db
            .claim_next_due_outbox(now + 200, 1_000, "check-empty")
            .await
            .expect("claim after test")
            .is_none(),
        "all rows completed"
    );
}
