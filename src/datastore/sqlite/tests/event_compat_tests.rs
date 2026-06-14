use super::*;
use crate::datastore::sqlite::crud::helpers::{event_read_api_versions, needs_event_v1_compat};
use serde_json::json;

// ---- Unit tests for the compat helpers ---------------------------------

#[test]
fn event_read_api_versions_expands_for_core_v1_event() {
    let v = event_read_api_versions("v1", "Event");
    assert_eq!(v, vec!["v1", "events.k8s.io/v1"]);
    assert!(needs_event_v1_compat("v1", "Event"));
}

#[test]
fn event_read_api_versions_expands_for_events_k8s_io_v1_event() {
    let v = event_read_api_versions("events.k8s.io/v1", "Event");
    assert_eq!(v, vec!["v1", "events.k8s.io/v1"]);
    assert!(needs_event_v1_compat("events.k8s.io/v1", "Event"));
}

#[test]
fn event_read_api_versions_does_not_expand_for_non_event_resource() {
    assert!(event_read_api_versions("v1", "Pod").is_empty());
    assert!(event_read_api_versions("v1", "ConfigMap").is_empty());
    assert!(event_read_api_versions("apps/v1", "Deployment").is_empty());
    assert!(!needs_event_v1_compat("v1", "Pod"));
}

#[test]
fn event_read_api_versions_does_not_expand_for_event_in_unrelated_group() {
    // A custom kind that happens to be named "Event" but lives outside the
    // K8s Events compat envelope (e.g., a CRD called Event in some.group/v1)
    // must not pick up the cross-version compat behavior — that would
    // randomly merge unrelated rows.
    assert!(event_read_api_versions("some.group/v1", "Event").is_empty());
    assert!(!needs_event_v1_compat("some.group/v1", "Event"));
}

// ---- Behavior tests through the Datastore ------------------------------

async fn seed_default_namespace(db: &Datastore) {
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();
}

/// Bug B regression: when replication is at-least-once (follower
/// disconnects + reconnects + receives a snapshot covering an RV it
/// already applied), `apply_replicated_create_resource` may try to
/// insert a watch_events row whose RV already exists. Without
/// idempotent insert handling the entire apply transaction rolled
/// back with `UNIQUE constraint failed: watch_events.resource_version`
/// and the worker's reconcile loop spammed
/// "failed to apply replicated entry" until the worker fell behind.
/// This test forces that exact at-least-once condition (insert, delete
/// the row, then replay the same Create at the same RV) and asserts
/// the apply succeeds.
#[tokio::test]
async fn apply_replicated_create_is_idempotent_when_watch_events_row_already_exists() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "replay-cm",
            "namespace": "default",
            "uid": "11111111-1111-1111-1111-111111111111"
        },
        "data": {"k": "v"}
    });
    // First apply — establishes the watch_events row at RV=42.
    db.apply_replicated_create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "replay-cm",
        cm.clone(),
        crate::datastore::ReplicatedCreateOptions::new(
            42,
            Some("11111111-1111-1111-1111-111111111111".to_string()),
        ),
    )
    .await
    .expect("first apply must succeed");

    // Delete the resource row but leave watch_events history intact —
    // simulates the state on the worker after a later Delete event.
    db.delete_resource("v1", "ConfigMap", Some("default"), "replay-cm")
        .await
        .expect("delete must succeed");

    // Replay the same Create at the same RV — exercises the
    // "watch_events already has RV=42" path inside
    // insert_watch_event_in_conn. Pre-fix: UNIQUE constraint rolled
    // the whole tx back. Post-fix: idempotent, returns Ok.
    db.apply_replicated_create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "replay-cm",
        cm.clone(),
        crate::datastore::ReplicatedCreateOptions::new(
            42,
            Some("11111111-1111-1111-1111-111111111111".to_string()),
        ),
    )
    .await
    .expect("replay of an already-applied RV must be idempotent, not error");
}

#[tokio::test]
async fn event_posted_via_events_k8s_io_v1_is_readable_via_core_v1_get() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    db.create_resource(
        "events.k8s.io/v1",
        "Event",
        Some("default"),
        "evt-bridge",
        json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "metadata": {"name": "evt-bridge", "namespace": "default"},
            "reportingController": "test-controller",
            "type": "Normal"
        }),
    )
    .await
    .unwrap();

    // GET via core/v1 should find it (compat shim).
    let via_core = db
        .get_resource("v1", "Event", Some("default"), "evt-bridge")
        .await
        .unwrap();
    assert!(
        via_core.is_some(),
        "core/v1 GET must read events.k8s.io/v1 row via compat shim"
    );
    let row = via_core.unwrap();
    assert_eq!(row.api_version, "events.k8s.io/v1");
    assert_eq!(row.name, "evt-bridge");
}

#[tokio::test]
async fn event_posted_via_core_v1_is_readable_via_events_k8s_io_v1_get() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    db.create_resource(
        "v1",
        "Event",
        Some("default"),
        "evt-reverse",
        json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"name": "evt-reverse", "namespace": "default"},
            "type": "Normal"
        }),
    )
    .await
    .unwrap();

    // Reverse direction: GET via events.k8s.io/v1 should find the core/v1 row.
    let via_events_v1 = db
        .get_resource("events.k8s.io/v1", "Event", Some("default"), "evt-reverse")
        .await
        .unwrap();
    assert!(
        via_events_v1.is_some(),
        "events.k8s.io/v1 GET must read core/v1 row via compat shim"
    );
    assert_eq!(via_events_v1.unwrap().api_version, "v1");
}

#[tokio::test]
async fn event_listed_via_core_v1_includes_both_api_versions() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    db.create_resource(
        "v1",
        "Event",
        Some("default"),
        "core-evt",
        json!({"apiVersion":"v1","kind":"Event","metadata":{"name":"core-evt","namespace":"default"},"type":"Normal"}),
    ).await.unwrap();
    db.create_resource(
        "events.k8s.io/v1",
        "Event",
        Some("default"),
        "new-evt",
        json!({"apiVersion":"events.k8s.io/v1","kind":"Event","metadata":{"name":"new-evt","namespace":"default"},"type":"Normal"}),
    ).await.unwrap();

    let list = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let names: Vec<&str> = list.items.iter().map(|r| r.name.as_str()).collect();
    assert!(
        names.contains(&"core-evt") && names.contains(&"new-evt"),
        "core/v1 LIST must surface both api_versions, got: {names:?}"
    );
}

#[tokio::test]
async fn non_event_resource_get_does_not_apply_compat_shim() {
    // Same name in two unrelated CRDs — must NOT cross-read.
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    db.create_resource(
        "alpha.example.com/v1",
        "Widget",
        Some("default"),
        "shared",
        json!({"apiVersion":"alpha.example.com/v1","kind":"Widget","metadata":{"name":"shared","namespace":"default"}}),
    ).await.unwrap();
    db.create_resource(
        "beta.example.com/v1",
        "Widget",
        Some("default"),
        "shared",
        json!({"apiVersion":"beta.example.com/v1","kind":"Widget","metadata":{"name":"shared","namespace":"default"}}),
    ).await.unwrap();

    let alpha = db
        .get_resource("alpha.example.com/v1", "Widget", Some("default"), "shared")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        alpha.api_version, "alpha.example.com/v1",
        "non-Event resources must respect strict api_version identity"
    );

    let beta = db
        .get_resource("beta.example.com/v1", "Widget", Some("default"), "shared")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(beta.api_version, "beta.example.com/v1");
}

#[tokio::test]
async fn event_get_with_both_api_versions_present_returns_one_row() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_default_namespace(&db).await;

    db.create_resource(
        "v1",
        "Event",
        Some("default"),
        "dup",
        json!({"apiVersion":"v1","kind":"Event","metadata":{"name":"dup","namespace":"default"},"type":"Normal"}),
    ).await.unwrap();
    db.create_resource(
        "events.k8s.io/v1",
        "Event",
        Some("default"),
        "dup",
        json!({"apiVersion":"events.k8s.io/v1","kind":"Event","metadata":{"name":"dup","namespace":"default"},"type":"Normal"}),
    ).await.unwrap();

    // Both rows exist (different api_versions, distinct unique key). GET via
    // either api_version returns exactly one row (LIMIT 1 in the compat
    // query). Which row is implementation-defined; what matters is that GET
    // does not error and does not return both stitched together.
    let row = db
        .get_resource("v1", "Event", Some("default"), "dup")
        .await
        .unwrap();
    assert!(
        row.is_some(),
        "GET with both versions present must return one row, not None"
    );
}
