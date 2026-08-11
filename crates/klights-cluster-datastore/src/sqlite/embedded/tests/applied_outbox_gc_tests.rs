#![cfg(test)]

use super::*;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

async fn insert_gc_row(
    db: &Datastore,
    idempotency_key: &str,
    subject_key: &str,
    operation: &str,
    first_seen_ms: i64,
    applied_rv: i64,
) {
    db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
        idempotency_key: idempotency_key.to_string(),
        subject_key: subject_key.to_string(),
        operation: operation.to_string(),
        first_seen_ms,
        applied_rv: Some(applied_rv),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert applied-outbox row");
}

#[tokio::test]
async fn gc_applied_outbox_prunes_ttl_expired() {
    let db = Datastore::new_in_memory()
        .await
        .expect("in-memory datastore");
    let now = now_ms();
    insert_gc_row(
        &db,
        "old-key",
        "v1/Pod/default/web/uid-1",
        "PodStatus",
        now - 13 * 60 * 60 * 1000,
        1,
    )
    .await;
    insert_gc_row(
        &db,
        "recent-key",
        "v1/Pod/default/web/uid-1",
        "PodStatus",
        now - 60 * 60 * 1000,
        2,
    )
    .await;

    assert_eq!(
        db.gc_applied_outbox(now, 12 * 60 * 60 * 1000)
            .await
            .unwrap(),
        1
    );
    assert!(db.get_applied_outbox("old-key").await.unwrap().is_none());
    assert!(db.get_applied_outbox("recent-key").await.unwrap().is_some());
}

#[tokio::test]
async fn gc_applied_outbox_does_not_touch_recent() {
    let db = Datastore::new_in_memory()
        .await
        .expect("in-memory datastore");
    let now = now_ms();
    for index in 0..10 {
        insert_gc_row(
            &db,
            &format!("recent-{index}"),
            &format!("v1/Pod/default/web-{index}/uid-{index}"),
            "PodStatus",
            now - 11 * 60 * 60 * 1000,
            index,
        )
        .await;
    }

    assert_eq!(
        db.gc_applied_outbox(now, 12 * 60 * 60 * 1000)
            .await
            .unwrap(),
        0
    );
    for index in 0..10 {
        assert!(
            db.get_applied_outbox(&format!("recent-{index}"))
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn gc_applied_outbox_prunes_event_create_and_unknown_operations() {
    let db = Datastore::new_in_memory()
        .await
        .expect("in-memory datastore");
    let now = now_ms();
    let old = now - 13 * 60 * 60 * 1000;
    insert_gc_row(
        &db,
        "event-key",
        "events.k8s.io/v1/Event/default/web.1/uid-event",
        "EventCreate",
        old,
        1,
    )
    .await;
    insert_gc_row(
        &db,
        "future-key",
        "example.io/v1/Future/default/name/uid-future",
        "FutureOperation",
        old,
        2,
    )
    .await;

    assert_eq!(
        db.gc_applied_outbox(now, 12 * 60 * 60 * 1000)
            .await
            .unwrap(),
        2
    );
    assert!(db.get_applied_outbox("event-key").await.unwrap().is_none());
    assert!(db.get_applied_outbox("future-key").await.unwrap().is_none());
}
