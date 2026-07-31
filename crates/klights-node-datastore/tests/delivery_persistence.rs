use std::sync::Arc;

use klights_node_datastore::{delivery::SqliteDeliveryStore, open};
use klights_node_store::{
    DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest, DeadLetterStore,
    OUTBOX_DIAGNOSTIC_AGING_MS, OutboxAttemptFailure, OutboxAttemptFailureRecord,
    OutboxBatchClaimRequest, OutboxClaimRequest, OutboxClassification, OutboxCompletion,
    OutboxDispatcherStore, OutboxEnqueue, OutboxFailureDisposition, OutboxNow, OutboxPriority,
    OutboxProducerStore, OutboxSequencePolicy, OutboxSubject, OutboxSupersedability,
    TerminalDeleteClassification,
};
use klights_supervisor::{DbExecutor, SystemWallClock, TaskCategoryConfig, TaskSupervisor};
use klights_types::ResourceKey;

struct DeliveryDb {
    executor: DbExecutor,
    store: SqliteDeliveryStore,
}

impl std::ops::Deref for DeliveryDb {
    type Target = SqliteDeliveryStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

fn classification(priority: OutboxPriority) -> OutboxClassification {
    OutboxClassification::try_new(
        priority,
        OutboxSupersedability::Never,
        TerminalDeleteClassification::NotTerminalDelete,
        OutboxSequencePolicy::Unsequenced,
    )
    .unwrap()
}

async fn fresh() -> DeliveryDb {
    let executor = open::open_with_opts(
        open::in_memory_opts(),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:delivery-persistence-test",
    )
    .await
    .unwrap();
    DeliveryDb {
        store: SqliteDeliveryStore::new(executor.clone(), Arc::new(SystemWallClock)),
        executor,
    }
}

async fn on_disk(path: &std::path::Path) -> DeliveryDb {
    let mut opts = open::disk_opts(path.to_path_buf());
    opts.allow_existing_perms = true;
    let executor = open::open_with_opts(
        opts,
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:delivery-persistence-disk-test",
    )
    .await
    .unwrap();
    DeliveryDb {
        store: SqliteDeliveryStore::new(executor.clone(), Arc::new(SystemWallClock)),
        executor,
    }
}

fn operation_classification(operation: &str) -> OutboxClassification {
    let (priority, supersedability, sequence_policy) = match operation {
        "LeaseRenew" => (
            OutboxPriority::Lease,
            OutboxSupersedability::Never,
            OutboxSequencePolicy::Unsequenced,
        ),
        "NodeStatus" => (
            OutboxPriority::NodeHealth,
            OutboxSupersedability::Never,
            OutboxSequencePolicy::PerSubject,
        ),
        "EventCreate" => (
            OutboxPriority::Diagnostic,
            OutboxSupersedability::Never,
            OutboxSequencePolicy::PerSubject,
        ),
        _ => (
            OutboxPriority::Workload,
            OutboxSupersedability::PodStatus,
            OutboxSequencePolicy::PerSubject,
        ),
    };
    OutboxClassification::try_new(
        priority,
        supersedability,
        TerminalDeleteClassification::NotTerminalDelete,
        sequence_policy,
    )
    .unwrap()
}

fn outbox(key: &str, subject_key: &str, operation: &str, now_ms: i64) -> OutboxEnqueue {
    OutboxEnqueue::try_new(
        key,
        now_ms,
        OutboxSubject::new(
            subject_key,
            ResourceKey::new("v1", "Pod", Some("default".to_string()), "web"),
            Some("pod-uid".to_string()),
            "pod-uid",
        ),
        operation,
        operation_classification(operation),
        Vec::new(),
        now_ms,
    )
    .unwrap()
}

async fn clear_stream_identity(db: &DeliveryDb, all: bool) {
    db.executor
        .call_raw("test:clear_stream_identity", move |conn| {
            let tx = conn.transaction()?;
            if all {
                tx.execute(
                    "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0",
                    [],
                )?;
                tx.execute(
                    "UPDATE outbox_dead_letter SET client_id = '', stream_id = 0, stream_seq = 0",
                    [],
                )?;
            } else {
                tx.execute(
                    "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0 \
                     WHERE idempotency_key = 'legacy-pre-stream-row'",
                    [],
                )?;
            }
            tx.execute("DELETE FROM outbox_stream_sequences", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn claim(
    db: &DeliveryDb,
    now_ms: i64,
    lease_ms: i64,
    token: &str,
) -> Option<klights_node_store::OutboxRecord> {
    db.claim_next_due_outbox(OutboxClaimRequest::try_new(now_ms, lease_ms, token).unwrap())
        .await
        .unwrap()
}

async fn complete(db: &DeliveryDb, id: i64, token: &str) -> bool {
    db.complete_outbox(OutboxCompletion::try_new(id, token).unwrap())
        .await
        .unwrap()
}

async fn dead_lettered(store: &SqliteDeliveryStore) -> (i64, Vec<u8>) {
    let payload = vec![0, 255, 1, 0, 128];
    store
        .enqueue_outbox(
            OutboxEnqueue::try_new(
                "dead-letter/exact",
                101,
                OutboxSubject::new(
                    "v1/Pod/default/raw/uid/raw",
                    ResourceKey::new("v1", "Pod", Some("default".to_string()), "raw"),
                    Some("uid/raw".to_string()),
                    "uid/raw",
                ),
                "OpaqueOperation",
                classification(OutboxPriority::Workload),
                payload.clone(),
                101,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let claimed = store
        .claim_next_due_outbox(OutboxClaimRequest::try_new(101, 100, "lease/exact").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .record_outbox_failure(
                OutboxAttemptFailureRecord::try_new(
                    claimed.id(),
                    "lease/exact",
                    102,
                    "terminal/exact",
                    1,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        OutboxFailureDisposition::DeadLettered
    );
    (claimed.id(), payload)
}

#[tokio::test]
async fn list_and_get_round_trip_exact_persisted_dead_letter_facts() {
    let store = fresh().await;
    let (original_id, payload) = dead_lettered(&store).await;

    let listed = store.list_dead_letter().await.unwrap();
    assert_eq!(listed.len(), 1);
    let entry = &listed[0];
    let fetched = store
        .get_dead_letter(DeadLetterKey::try_new(entry.id()).unwrap())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&fetched, entry);
    assert_eq!(entry.original_id(), original_id);
    assert!(!entry.client_id().is_empty());
    assert_eq!(entry.idempotency_key(), "dead-letter/exact");
    assert_eq!(entry.enqueued_ms(), 101);
    assert_eq!(entry.subject().subject_key(), "v1/Pod/default/raw/uid/raw");
    assert_eq!(entry.subject().subject_uid(), Some("uid/raw"));
    assert_eq!(entry.subject().pod_uid(), "uid/raw");
    assert_eq!(entry.operation(), "OpaqueOperation");
    assert_eq!(entry.payload(), payload);
    assert_eq!(entry.attempts(), 1);
    assert_eq!(entry.last_error(), "terminal/exact");
}

#[tokio::test]
async fn replay_uses_caller_supplied_classification_without_decoding_payload() {
    let store = fresh().await;
    let (_, payload) = dead_lettered(&store).await;
    let entry = store.list_dead_letter().await.unwrap().remove(0);
    let replay_classification = classification(OutboxPriority::Diagnostic);

    assert!(
        store
            .replay_dead_letter(DeadLetterReplayRequest::new(
                DeadLetterKey::try_new(entry.id()).unwrap(),
                replay_classification,
            ))
            .await
            .unwrap()
    );

    let replayed = store
        .claim_next_due_outbox(
            OutboxClaimRequest::try_new(i64::MAX - 100, 100, "replay/lease").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed.classification(), replay_classification);
    assert_eq!(replayed.payload(), payload);
}

#[tokio::test]
async fn outbox_failure_threshold_is_atomic_and_lease_bound() {
    let db = fresh().await;
    db.enqueue_outbox(outbox(
        "atomic-dead-letter",
        "v1/Pod/default/web/pod-uid",
        "PodStatus",
        1,
    ))
    .await
    .unwrap();
    db.executor
        .call_raw("test:set_outbox_attempt", |conn| {
            conn.execute(
                "UPDATE outbox SET attempt = 719 WHERE idempotency_key = 'atomic-dead-letter'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let row = claim(&db, 1, 1_000, "owned-lease").await.unwrap();

    assert_eq!(
        db.record_outbox_failure(
            OutboxAttemptFailureRecord::try_new(row.id(), "stale-lease", 10, "retry", 720,)
                .unwrap(),
        )
        .await
        .unwrap(),
        OutboxFailureDisposition::LeaseLost
    );
    assert!(db.list_dead_letter().await.unwrap().is_empty());
    assert_eq!(
        db.record_outbox_failure(
            OutboxAttemptFailureRecord::try_new(row.id(), "owned-lease", 10, "retry", 720,)
                .unwrap(),
        )
        .await
        .unwrap(),
        OutboxFailureDisposition::DeadLettered
    );
    let dead = db.list_dead_letter().await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].attempts(), 720);
    assert_eq!(dead[0].last_error(), "retry");
    assert_eq!(
        db.record_outbox_failure(
            OutboxAttemptFailureRecord::try_new(row.id(), "owned-lease", 10, "duplicate", 720,)
                .unwrap(),
        )
        .await
        .unwrap(),
        OutboxFailureDisposition::LeaseLost
    );
    assert_eq!(db.list_dead_letter().await.unwrap().len(), 1);
}

#[tokio::test]
async fn node_local_outbox_assigns_monotonic_seq_per_stream() {
    let db = fresh().await;
    for (key, now) in [("same-stream-1", 1), ("same-stream-2", 2)] {
        db.enqueue_outbox(outbox(key, "v1/Pod/default/web/pod-uid", "PodStatus", now))
            .await
            .unwrap();
    }
    let first = claim(&db, 10, 1_000, "lease-a").await.unwrap();
    assert!(complete(&db, first.id(), "lease-a").await);
    let second = claim(&db, 10, 1_000, "lease-b").await.unwrap();
    assert_eq!(first.sequence().stream_id(), second.sequence().stream_id());
    assert_eq!(first.sequence().stream_seq(), 1);
    assert_eq!(second.sequence().stream_seq(), 2);
}

#[tokio::test]
async fn legacy_outbox_stream_identity_is_repaired_durably_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("node.db");
    let db = on_disk(&path).await;
    db.enqueue_outbox(outbox(
        "legacy-pre-stream-row",
        "v1/Pod/default/legacy/legacy-uid",
        "PodStatus",
        100,
    ))
    .await
    .unwrap();
    clear_stream_identity(&db, false).await;
    drop(db);

    let db = on_disk(&path).await;
    let first = claim(&db, 100, 1_000, "legacy-first-lease").await.unwrap();
    assert!(!first.client_id().is_empty());
    assert!(first.sequence().stream_id() > 0);
    assert_eq!(first.sequence().stream_seq(), 1);
    assert!(
        db.mark_outbox_attempt_failed(
            OutboxAttemptFailure::try_new(
                first.id(),
                "legacy-first-lease",
                200,
                "leader temporarily unavailable",
            )
            .unwrap(),
        )
        .await
        .unwrap()
    );
    db.enqueue_outbox(outbox(
        "post-migration-successor",
        "v1/Pod/default/legacy/legacy-uid",
        "PodStatus",
        110,
    ))
    .await
    .unwrap();
    assert!(claim(&db, 150, 1_000, "blocked-successor").await.is_none());
    drop(db);

    let db = on_disk(&path).await;
    let retried = claim(&db, 200, 1_000, "legacy-restart-lease")
        .await
        .unwrap();
    assert_eq!(retried.client_id(), first.client_id());
    assert_eq!(retried.sequence(), first.sequence());
    assert!(complete(&db, retried.id(), "legacy-restart-lease").await);
    let successor = claim(&db, 201, 1_000, "successor-lease").await.unwrap();
    assert_eq!(successor.client_id(), first.client_id());
    assert_eq!(
        successor.sequence().stream_id(),
        first.sequence().stream_id()
    );
    assert_eq!(successor.sequence().stream_seq(), 2);
}

#[tokio::test]
async fn legacy_outbox_upgrade_repairs_fifo_before_successor_enqueue() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("node.db");
    let subject = "v1/Pod/default/legacy-upgrade/legacy-uid";
    let db = on_disk(&path).await;
    for (key, now) in [
        ("legacy-live-head", 100),
        ("legacy-dead-head", 101),
        ("legacy-live-tail", 102),
    ] {
        db.enqueue_outbox(outbox(key, subject, "PodStatus", now))
            .await
            .unwrap();
    }
    assert!(
        db.move_outbox_to_dead_letter_if_max_attempts(
            DeadLetterMoveRequest::try_new("legacy-dead-head", 0).unwrap(),
        )
        .await
        .unwrap()
    );
    clear_stream_identity(&db, true).await;
    drop(db);

    let db = on_disk(&path).await;
    db.enqueue_outbox(outbox("current-successor", subject, "PodStatus", 103))
        .await
        .unwrap();
    let first = claim(&db, 200, 1_000, "legacy-head-lease").await.unwrap();
    assert_eq!(first.idempotency_key(), "legacy-live-head");
    assert_eq!(first.sequence().stream_seq(), 1);
    assert!(complete(&db, first.id(), "legacy-head-lease").await);
    let dead = db.list_dead_letter().await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].idempotency_key(), "legacy-dead-head");
    assert_eq!(dead[0].sequence().stream_seq(), 2);
    assert!(claim(&db, 200, 1_000, "blocked-live-tail").await.is_none());
    assert!(
        db.replay_dead_letter(DeadLetterReplayRequest::new(
            DeadLetterKey::try_new(dead[0].id()).unwrap(),
            operation_classification("PodStatus"),
        ))
        .await
        .unwrap()
    );
    let replay_now = i64::MAX / 4;
    let replayed = claim(&db, replay_now, 1_000, "legacy-dead-replay")
        .await
        .unwrap();
    assert_eq!(replayed.idempotency_key(), "legacy-dead-head");
    assert_eq!(replayed.sequence().stream_seq(), 2);
    assert!(complete(&db, replayed.id(), "legacy-dead-replay").await);
    for (key, sequence, token) in [
        ("legacy-live-tail", 3, "legacy-tail-lease"),
        ("current-successor", 4, "current-successor-lease"),
    ] {
        let row = claim(&db, replay_now, 1_000, token).await.unwrap();
        assert_eq!(row.idempotency_key(), key);
        assert_eq!(row.sequence().stream_seq(), sequence);
        assert!(complete(&db, row.id(), token).await);
    }
}

#[tokio::test]
async fn node_local_outbox_commits_stream_sequence_before_enqueue_returns() {
    let db = fresh().await;
    let subject = "v1/Pod/default/atomic-sequence/pod-uid";
    for (key, now, sequence) in [("atomic-sequence-1", 1, 1), ("atomic-sequence-2", 2, 2)] {
        db.enqueue_outbox(outbox(key, subject, "PodStatus", now))
            .await
            .unwrap();
        let key = key.to_string();
        let observed = db
            .executor
            .call_raw("test:stream_position", move |conn| {
                conn.query_row(
                    "SELECT stream_id, stream_seq FROM outbox WHERE idempotency_key = ?1",
                    [key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert!(observed.0 > 0);
        assert_eq!(observed.1, sequence);
    }
}

#[tokio::test]
async fn outbox_durability_next_wake_tracks_the_fifo_blocker_not_blocked_younger_work() {
    let db = fresh().await;
    let subject = "v1/Pod/default/fifo-wake/pod-uid";
    db.enqueue_outbox(outbox("fifo-blocker", subject, "PodStatus", 500))
        .await
        .unwrap();
    db.enqueue_outbox(outbox("fifo-blocked-younger", subject, "PodStatus", 100))
        .await
        .unwrap();
    assert_eq!(
        db.next_outbox_wake_ms(OutboxNow::try_new(200).unwrap())
            .await
            .unwrap(),
        Some(500)
    );
}

#[tokio::test]
async fn outbox_durability_next_wake_tracks_an_older_active_lease() {
    let db = fresh().await;
    let subject = "v1/Pod/default/leased-fifo-wake/pod-uid";
    for key in ["leased-fifo-blocker", "leased-fifo-younger"] {
        db.enqueue_outbox(outbox(key, subject, "PodStatus", 100))
            .await
            .unwrap();
    }
    claim(&db, 100, 400, "active-lease").await.unwrap();
    assert_eq!(
        db.next_outbox_wake_ms(OutboxNow::try_new(200).unwrap())
            .await
            .unwrap(),
        Some(500)
    );
}

#[tokio::test]
async fn node_local_outbox_rows_share_stable_client_epoch() {
    let db = fresh().await;
    for (key, subject, now) in [
        ("client-epoch-1", "v1/Pod/default/a/uid-a", 1),
        ("client-epoch-2", "v1/Pod/default/b/uid-b", 2),
    ] {
        db.enqueue_outbox(outbox(key, subject, "PodStatus", now))
            .await
            .unwrap();
    }
    let first = claim(&db, 10, 1_000, "lease-a").await.unwrap();
    assert!(complete(&db, first.id(), "lease-a").await);
    let second = claim(&db, 10, 1_000, "lease-b").await.unwrap();
    assert!(!first.client_id().is_empty());
    assert_eq!(first.client_id(), second.client_id());
}

#[tokio::test]
async fn node_local_outbox_claim_skips_same_subject_rows_with_stream_in_flight() {
    let db = fresh().await;
    for (key, now) in [("inflight-stream-1", 1), ("inflight-stream-2", 2)] {
        db.enqueue_outbox(outbox(key, "v1/Pod/default/web-2/uid-2", "PodStatus", now))
            .await
            .unwrap();
    }
    let first = claim(&db, 10, 1_000, "lease-a").await.unwrap();
    assert_eq!(first.sequence().stream_seq(), 1);
    assert!(claim(&db, 10, 1_000, "lease-b").await.is_none());
}

#[tokio::test]
async fn node_local_outbox_batch_claim_is_atomic_across_independent_claimers() {
    let db = fresh().await;
    for (key, subject) in [
        ("atomic-claim-a", "v1/Pod/default/a/uid-a"),
        ("atomic-claim-b", "v1/Pod/default/b/uid-b"),
    ] {
        db.enqueue_outbox(outbox(key, subject, "PodStatus", 1))
            .await
            .unwrap();
    }
    let (left, right) = tokio::join!(
        db.claim_due_outbox_batch(
            OutboxBatchClaimRequest::try_new(10, 2, 1_000, "claim-left").unwrap()
        ),
        db.claim_due_outbox_batch(
            OutboxBatchClaimRequest::try_new(10, 2, 1_000, "claim-right").unwrap()
        ),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    let left_ids = left
        .iter()
        .map(|row| row.id())
        .collect::<std::collections::BTreeSet<_>>();
    let right_ids = right
        .iter()
        .map(|row| row.id())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(left_ids.is_disjoint(&right_ids));
    assert_eq!(left.len() + right.len(), 2);
}

#[tokio::test]
async fn node_local_outbox_prioritizes_status_before_older_events() {
    let db = fresh().await;
    db.enqueue_outbox(outbox(
        "event-first",
        "events.k8s.io/v1/Event/default/diagnostic/event-uid",
        "EventCreate",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(outbox(
        "status-second",
        "v1/Pod/default/web/pod-uid",
        "PodStatus",
        2,
    ))
    .await
    .unwrap();
    assert_eq!(
        claim(&db, 10, 1_000, "lease-status")
            .await
            .unwrap()
            .idempotency_key(),
        "status-second"
    );
}

#[tokio::test]
async fn node_local_outbox_ages_diagnostic_events_into_fair_service() {
    let db = fresh().await;
    db.enqueue_outbox(outbox(
        "aged-event",
        "events.k8s.io/v1/Event/default/diagnostic/event-uid",
        "EventCreate",
        1,
    ))
    .await
    .unwrap();
    db.enqueue_outbox(outbox(
        "fresh-status",
        "v1/Pod/default/web/pod-uid",
        "PodStatus",
        OUTBOX_DIAGNOSTIC_AGING_MS + 2,
    ))
    .await
    .unwrap();
    assert_eq!(
        claim(
            &db,
            OUTBOX_DIAGNOSTIC_AGING_MS + 2,
            1_000,
            "lease-aged-event",
        )
        .await
        .unwrap()
        .idempotency_key(),
        "aged-event"
    );
}

#[tokio::test]
async fn node_local_outbox_keeps_lease_then_node_status_ahead_of_workload_status() {
    let db = fresh().await;
    for (key, operation) in [
        ("pod-status", "PodStatus"),
        ("node-status", "NodeStatus"),
        ("lease-renew", "LeaseRenew"),
    ] {
        db.enqueue_outbox(outbox(key, &format!("subject/{key}"), operation, 1))
            .await
            .unwrap();
    }
    let mut order = Vec::new();
    for index in 0..3 {
        let token = format!("priority-lease-{index}");
        let row = claim(&db, 10, 1_000, &token).await.unwrap();
        order.push(row.idempotency_key().to_string());
        assert!(complete(&db, row.id(), &token).await);
    }
    assert_eq!(order, ["lease-renew", "node-status", "pod-status"]);
}
