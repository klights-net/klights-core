use super::*;
use crate::pod_identity::PodIdentity;
use serde_json::json;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[tokio::test]
async fn pod_workqueue_enqueue_peek_claim_round_trip() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = PodIdentity::new("default", "p1", "uid-1");
    db.pod_workqueue_enqueue(PodWorkqueueKind::Pod, &pod, json!({"a":1}), 1, 0, None)
        .await
        .unwrap();

    let next_due = db.pod_workqueue_peek_next_due().await.unwrap();
    assert!(next_due.is_some(), "expected queued row");

    let row = db
        .pod_workqueue_claim_due(now_ms() + 10_000)
        .await
        .unwrap()
        .expect("row should be due");
    assert_eq!(row.kind, PodWorkqueueKind::Pod);
    assert_eq!(row.namespace, "default");
    assert_eq!(row.name, "p1");
    assert_eq!(row.uid, "uid-1");
    assert_eq!(row.payload, json!({"a":1}));
    assert_eq!(row.attempt_count, 1);

    let none = db.pod_workqueue_peek_next_due().await.unwrap();
    assert!(none.is_none(), "queue should be empty after claim");
}

#[tokio::test]
async fn pod_workqueue_claim_orders_by_next_due_then_id() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod_a = PodIdentity::new("default", "a", "uid-a");
    db.pod_workqueue_enqueue(PodWorkqueueKind::Pod, &pod_a, json!({}), 1, 0, None)
        .await
        .unwrap();
    let pod_b = PodIdentity::new("default", "b", "uid-b");
    db.pod_workqueue_enqueue(PodWorkqueueKind::Pod, &pod_b, json!({}), 1, 0, None)
        .await
        .unwrap();

    let first = db
        .pod_workqueue_claim_due(now_ms() + 10_000)
        .await
        .unwrap()
        .expect("first row");
    let second = db
        .pod_workqueue_claim_due(now_ms() + 10_000)
        .await
        .unwrap()
        .expect("second row");

    assert_eq!(first.name, "a");
    assert_eq!(second.name, "b");
    assert!(first.next_attempt_at_ms <= second.next_attempt_at_ms);
}

#[tokio::test]
async fn pod_workqueue_record_failure_requeues_with_incremented_attempt() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = PodIdentity::new("ns-x", "ns-x", "uid-ns");
    db.pod_workqueue_enqueue(
        PodWorkqueueKind::Namespace,
        &pod,
        json!({"k":"v"}),
        1,
        0,
        None,
    )
    .await
    .unwrap();

    let claimed = db
        .pod_workqueue_claim_due(now_ms() + 10_000)
        .await
        .unwrap()
        .expect("claimed");
    db.pod_workqueue_record_failure(claimed, 0, "boom")
        .await
        .unwrap();

    let retried = db
        .pod_workqueue_claim_due(now_ms() + 10_000)
        .await
        .unwrap()
        .expect("requeued");
    assert_eq!(retried.kind, PodWorkqueueKind::Namespace);
    assert_eq!(retried.attempt_count, 2);
    assert_eq!(retried.payload, json!({"k":"v"}));
}
