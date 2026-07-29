use std::sync::Arc;

use klights_node_datastore::{delivery::SqliteDeliveryStore, open};
use klights_node_store::{
    DeadLetterKey, DeadLetterReplayRequest, DeadLetterStore, OutboxAttemptFailureRecord,
    OutboxClaimRequest, OutboxClassification, OutboxDispatcherStore, OutboxEnqueue,
    OutboxFailureDisposition, OutboxPriority, OutboxProducerStore, OutboxSequencePolicy,
    OutboxSubject, OutboxSupersedability, TerminalDeleteClassification,
};
use klights_supervisor::{SystemWallClock, TaskCategoryConfig, TaskSupervisor};
use klights_types::ResourceKey;

fn classification(priority: OutboxPriority) -> OutboxClassification {
    OutboxClassification::try_new(
        priority,
        OutboxSupersedability::Never,
        TerminalDeleteClassification::NotTerminalDelete,
        OutboxSequencePolicy::Unsequenced,
    )
    .unwrap()
}

async fn fresh() -> SqliteDeliveryStore {
    let executor = open::open_with_opts(
        open::in_memory_opts(),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:delivery-persistence-test",
    )
    .await
    .unwrap();
    SqliteDeliveryStore::new(executor, Arc::new(SystemWallClock))
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
