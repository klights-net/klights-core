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
        observed_status_stamp: None,
    }
}

fn pod_delete_command(namespace: &str, name: &str, uid: &str) -> StorageCommand {
    StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some(namespace.to_string()),
        name: name.to_string(),
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
async fn outbox_lost_response_remains_retryable_and_claimable_after_backoff() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

    let now = 10_000i64;
    outbox
        .enqueue_command(OutboxCommand::new(
            "lost-response-key",
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

    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 7 }))
        .await;
    client
        .push_response(Err(OutboxApplyError::Retryable(
            "simulated lost response".to_string(),
        )))
        .await;

    assert_eq!(
        dispatcher
            .dispatch_due_once(now)
            .await
            .expect("first dispatch"),
        DispatchOutcome::Dispatched
    );

    assert!(
        node_db
            .claim_next_due_outbox(now, 1_000, "too-soon")
            .await
            .expect("claim too soon")
            .is_none(),
        "retryable row must respect existing backoff"
    );

    assert_eq!(
        dispatcher
            .dispatch_due_once(now + 60_000)
            .await
            .expect("second dispatch"),
        DispatchOutcome::Dispatched
    );

    assert!(
        node_db
            .claim_next_due_outbox(now + 120_000, 1_000, "done")
            .await
            .expect("claim after success")
            .is_none(),
        "row should be completed after retry succeeds"
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

#[tokio::test]
async fn batch_claim_blocks_younger_same_subject_while_older_leased() {
    // Lost-update regression (D1/A1/C1): the batch claim must enforce the SAME
    // strict per-subject single-in-flight invariant as the single-row claim.
    // While an older same-subject row is leased (in-flight on a slow WAN apply),
    // a younger same-subject row must NOT be claimable — otherwise two snapshots
    // for one Pod go in flight concurrently and can apply in raft order != stamp
    // order, clobbering the newer status.
    use crate::datastore::node_local::OutboxInsert;

    let node_db = node_db().await;
    let subject = "v1/Pod/default/web/uid-web";
    let insert = |key: &str, enqueued: i64| OutboxInsert {
        idempotency_key: key.to_string(),
        enqueued_ms: enqueued,
        subject_key: subject.to_string(),
        subject_api_version: "v1".to_string(),
        subject_kind: "Pod".to_string(),
        subject_namespace: Some("default".to_string()),
        subject_name: "web".to_string(),
        subject_uid: Some("uid-web".to_string()),
        pod_uid: "uid-web".to_string(),
        operation: OutboxOperation::PodStatus.as_str().to_string(),
        payload_proto: Vec::new(),
        next_due_ms: enqueued,
    };
    // Older row A, then younger row B, same subject.
    node_db
        .enqueue_outbox(insert("older-A", 100))
        .await
        .unwrap();
    node_db
        .enqueue_outbox(insert("younger-B", 101))
        .await
        .unwrap();

    let now = 200i64;
    // First batch claims only the oldest row A and leases it for a long time
    // (simulating an in-flight apply that hasn't completed/acked yet).
    let first = node_db
        .claim_due_outbox_batch(now, 16, 10_000, "dispatcher-1")
        .await
        .expect("first batch claim");
    assert_eq!(first.len(), 1, "only oldest per subject claimed");
    assert_eq!(first[0].idempotency_key, "older-A");

    // Second batch while A is still leased: B must remain blocked, because an
    // older same-subject row still exists. Current buggy SQL claims B here.
    let second = node_db
        .claim_due_outbox_batch(now + 1, 16, 10_000, "dispatcher-2")
        .await
        .expect("second batch claim");
    assert!(
        second.is_empty(),
        "younger same-subject row must not be claimed while older is leased/in-flight, got {:?}",
        second
            .iter()
            .map(|r| &r.idempotency_key)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn single_claim_allows_actor_finalize_delete_to_leapfrog_backed_off_pod_status() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let subject = OutboxSubject::new(
        "v1/Pod/default/web/uid-web",
        Some("default".to_string()),
        "web",
        Some("uid-web".to_string()),
    );

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-status-backoff",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                subject.key.clone(),
                subject.namespace.clone(),
                subject.name.clone(),
                subject.uid.clone(),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            1_000,
        ))
        .await
        .expect("enqueue status");
    let status = node_db
        .claim_next_due_outbox(1_000, 100, "status-lease")
        .await
        .expect("claim status")
        .expect("status row due");
    assert_eq!(status.idempotency_key, "web-status-backoff");
    node_db
        .mark_outbox_attempt_failed(status.id, "status-lease", 30_000, "transport down")
        .await
        .expect("back off status");

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            subject,
            "uid-web",
            pod_delete_command("default", "web", "uid-web"),
            1_001,
        ))
        .await
        .expect("enqueue actor finalize delete");

    let claimed = node_db
        .claim_next_due_outbox(1_001, 100, "delete-lease")
        .await
        .expect("claim delete")
        .expect("actor-finalize delete must be due despite older status backoff");
    assert_eq!(claimed.idempotency_key, "web-actor-finalize-delete");
}

#[tokio::test]
async fn batch_claim_allows_actor_finalize_delete_to_leapfrog_backed_off_pod_status() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let subject = "v1/Pod/default/web/uid-web";

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-status-backoff-batch",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            1_000,
        ))
        .await
        .expect("enqueue status");
    let status = node_db
        .claim_due_outbox_batch(1_000, 16, 100, "status-batch-lease")
        .await
        .expect("claim status batch")
        .pop()
        .expect("status row due");
    assert_eq!(status.idempotency_key, "web-status-backoff-batch");
    node_db
        .mark_outbox_attempt_failed(status.id, "status-batch-lease", 30_000, "transport down")
        .await
        .expect("back off status");

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-actor-finalize-delete-batch",
            OutboxOperation::PodMetadata,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_delete_command("default", "web", "uid-web"),
            1_001,
        ))
        .await
        .expect("enqueue actor finalize delete");

    let claimed = node_db
        .claim_due_outbox_batch(1_001, 16, 100, "delete-batch-lease")
        .await
        .expect("claim delete batch");
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0].idempotency_key,
        "web-actor-finalize-delete-batch"
    );
}

#[tokio::test]
async fn batch_claim_leases_only_terminal_delete_when_older_status_is_also_due() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let subject = "v1/Pod/default/web/uid-web";

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-status-due-with-delete",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            1_000,
        ))
        .await
        .expect("enqueue status");
    outbox
        .enqueue_command(OutboxCommand::new(
            "web-actor-finalize-delete-due",
            OutboxOperation::PodMetadata,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_delete_command("default", "web", "uid-web"),
            1_001,
        ))
        .await
        .expect("enqueue actor finalize delete");

    let claimed = node_db
        .claim_due_outbox_batch(1_001, 16, 100, "delete-only-batch-lease")
        .await
        .expect("claim delete batch");
    assert_eq!(
        claimed
            .iter()
            .map(|row| row.idempotency_key.as_str())
            .collect::<Vec<_>>(),
        vec!["web-actor-finalize-delete-due"],
        "terminal delete must be the only leased row for this subject"
    );
}

#[tokio::test]
async fn terminal_delete_apply_completes_older_superseded_status_rows() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);
    let subject = "v1/Pod/default/web/uid-web";

    outbox
        .enqueue_command(OutboxCommand::new(
            "web-status-superseded",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_status_command("default", "web", "uid-web"),
            1_000,
        ))
        .await
        .expect("enqueue status");
    outbox
        .enqueue_command(OutboxCommand::new(
            "web-actor-finalize-delete-cleans-status",
            OutboxOperation::PodMetadata,
            OutboxSubject::new(
                subject,
                Some("default".to_string()),
                "web",
                Some("uid-web".to_string()),
            ),
            "uid-web",
            pod_delete_command("default", "web", "uid-web"),
            1_001,
        ))
        .await
        .expect("enqueue actor finalize delete");

    client
        .push_response(Ok(OutboxApplyResult::Applied { applied_rv: 10 }))
        .await;
    assert_eq!(
        dispatcher
            .dispatch_due_once(1_001)
            .await
            .expect("dispatch terminal delete"),
        DispatchOutcome::Dispatched
    );

    assert!(
        node_db
            .claim_next_due_outbox(30_000, 100, "after-delete")
            .await
            .expect("claim after terminal delete")
            .is_none(),
        "older superseded status rows must be completed after terminal delete applies"
    );
}

#[tokio::test]
async fn nonterminal_status_remains_fifo_blocked_by_older_backed_off_status() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let subject = "v1/Pod/default/web/uid-web";

    for (key, at) in [("older-status", 1_000), ("younger-status", 1_001)] {
        outbox
            .enqueue_command(OutboxCommand::new(
                key,
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    subject,
                    Some("default".to_string()),
                    "web",
                    Some("uid-web".to_string()),
                ),
                "uid-web",
                pod_status_command("default", "web", "uid-web"),
                at,
            ))
            .await
            .expect("enqueue status");
    }

    let older = node_db
        .claim_next_due_outbox(1_000, 100, "older-lease")
        .await
        .expect("claim older status")
        .expect("older status due");
    node_db
        .mark_outbox_attempt_failed(older.id, "older-lease", 30_000, "transport down")
        .await
        .expect("back off older status");

    assert!(
        node_db
            .claim_next_due_outbox(1_001, 100, "younger-lease")
            .await
            .expect("claim younger status")
            .is_none(),
        "a younger non-terminal status row must remain blocked by the older status backoff"
    );
}

#[tokio::test]
async fn actor_finalize_delete_for_replacement_uid_is_not_blocked_by_old_uid_status() {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());

    outbox
        .enqueue_command(OutboxCommand::new(
            "old-web-status-backoff",
            OutboxOperation::PodStatus,
            OutboxSubject::new(
                "v1/Pod/default/web/uid-old",
                Some("default".to_string()),
                "web",
                Some("uid-old".to_string()),
            ),
            "uid-old",
            pod_status_command("default", "web", "uid-old"),
            1_000,
        ))
        .await
        .expect("enqueue old status");
    let old_status = node_db
        .claim_next_due_outbox(1_000, 100, "old-status-lease")
        .await
        .expect("claim old status")
        .expect("old status due");
    node_db
        .mark_outbox_attempt_failed(old_status.id, "old-status-lease", 30_000, "transport down")
        .await
        .expect("back off old status");

    outbox
        .enqueue_command(OutboxCommand::new(
            "replacement-web-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            OutboxSubject::new(
                "v1/Pod/default/web/uid-new",
                Some("default".to_string()),
                "web",
                Some("uid-new".to_string()),
            ),
            "uid-new",
            pod_delete_command("default", "web", "uid-new"),
            1_001,
        ))
        .await
        .expect("enqueue replacement delete");

    let claimed = node_db
        .claim_next_due_outbox(1_001, 100, "replacement-delete-lease")
        .await
        .expect("claim replacement delete")
        .expect("replacement UID delete must not share FIFO with old UID status");
    assert_eq!(
        claimed.idempotency_key,
        "replacement-web-actor-finalize-delete"
    );
}

#[tokio::test]
async fn actor_owned_delete_uid_mismatch_is_not_silently_dropped() {
    actor_owned_delete_terminal_case_is_handled(
        "delete-uid-mismatch",
        Err(OutboxApplyError::UidMismatch {
            expected: "uid-old".to_string(),
            actual: "uid-new".to_string(),
        }),
        true,
    )
    .await;
}

#[tokio::test]
async fn actor_owned_delete_conflict_terminal_is_not_silently_dropped() {
    actor_owned_delete_terminal_case_is_handled(
        "delete-conflict-terminal",
        Err(OutboxApplyError::ConflictTerminal(
            "resourceVersion precondition failed".to_string(),
        )),
        true,
    )
    .await;
}

#[tokio::test]
async fn actor_owned_delete_not_found_completes_without_dead_letter() {
    actor_owned_delete_terminal_case_is_handled(
        "delete-not-found",
        Err(OutboxApplyError::NotFound("pod already gone".to_string())),
        false,
    )
    .await;
}

async fn actor_owned_delete_terminal_case_is_handled(
    key: &str,
    response: Result<OutboxApplyResult, OutboxApplyError>,
    should_dead_letter: bool,
) {
    let node_db = node_db().await;
    let outbox = Outbox::new(node_db.clone());
    let client = Arc::new(StackApplyClient::default());
    let dispatcher = OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), 16);

    let now = 7_000i64;
    outbox
        .enqueue_command(OutboxCommand::new(
            key,
            OutboxOperation::RuntimeReconcile,
            OutboxSubject::new(
                "v1/Pod/default/web/uid-old",
                Some("default".to_string()),
                "web",
                Some("uid-old".to_string()),
            ),
            "uid-old",
            pod_delete_command("default", "web", "uid-old"),
            now,
        ))
        .await
        .expect("enqueue");

    client.push_response(response).await;

    assert_eq!(
        dispatcher.dispatch_due_once(now).await.expect("dispatch"),
        DispatchOutcome::Dispatched
    );

    let dead = node_db.list_dead_letter().await.expect("list dead letter");
    if should_dead_letter {
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].idempotency_key, key);
    } else {
        assert!(
            dead.is_empty(),
            "not-found actor-owned Pod delete is success and must not require operator replay"
        );
    }
    assert!(
        node_db
            .claim_next_due_outbox(now + 100, 1_000, "check-empty")
            .await
            .expect("claim")
            .is_none(),
        "terminal actor-owned delete should not remain pending for retry"
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
