//! DSB-R-07 — Cross-backend parametrized tests.
//!
//! Uses `parametrize_backends!` macro to run each test against both
//! SQLite and redb without duplication. Backend-specific tests (PRAGMA,
//! fingerprint, table-definition) stay in their own module.

use serde_json::{Value, json};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::redb::RedbDatastore;
use crate::datastore::sqlite::Datastore as SqliteDs;
use crate::datastore::types::*;
use klights_cluster_core::{PatchKind, ResourcePreconditions, WatchReplayPosition};

async fn sqlite_db() -> SqliteDs {
    SqliteDs::new_in_memory().await.unwrap()
}

async fn redb_db() -> RedbDatastore {
    RedbDatastore::new_in_memory().await.unwrap()
}

#[tokio::test]
async fn redb_snapshot_fence_coordinates_capture_and_mutation() {
    let db = redb_db().await;
    let exclusive = DatastoreBackend::acquire_snapshot_exclusive_fence(&db)
        .await
        .unwrap()
        .expect("redb must provide an exclusive snapshot fence");

    let mutation = DatastoreBackend::acquire_snapshot_mutation_fence(&db);
    tokio::pin!(mutation);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut mutation)
            .await
            .is_err(),
        "mutation fence must wait while capture owns the exclusive fence"
    );

    drop(exclusive);
    tokio::time::timeout(std::time::Duration::from_secs(2), &mut mutation)
        .await
        .expect("mutation fence must become available after capture releases it")
        .unwrap()
        .expect("redb must provide a mutation fence");
}

#[tokio::test]
async fn redb_applied_outbox_snapshot_page_rejects_unbounded_requests() {
    let db = redb_db().await;
    let oversized =
        std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1).unwrap();
    assert!(
        db.list_applied_outbox_paged(None, oversized).await.is_err(),
        "redb must implement the bounded native page contract instead of the full-list fallback"
    );
}

/// Run the same async test body against both backends.
/// Generates `<name>_sqlite` and `<name>_redb` test functions.
/// Uses concat_idents! internally to produce the names.
macro_rules! parametrize_backends {
    (
        $(#[$meta:meta])*
        $name:ident, |$db:ident| $body:expr_2021
    ) => {
        mod $name {
            use super::*;
            $(#[$meta])*
            #[tokio::test]
            async fn sqlite() {
                let $db = super::sqlite_db().await;
                let $db: &dyn DatastoreBackend = &$db;
                $body
            }
            $(#[$meta])*
            #[tokio::test]
            async fn redb() {
                let $db = super::redb_db().await;
                let $db: &dyn DatastoreBackend = &$db;
                $body
            }
        }
    };
}

// ---- Parametrized cross-backend tests ----

parametrize_backends!(
    applied_outbox_snapshot_pages_are_exclusive_and_lossless,
    |db| {
        let row_count = klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1;
        for index in 0..row_count {
            db.insert_applied_outbox(AppliedOutboxRecord {
                idempotency_key: format!("snapshot-page-{index:03}"),
                subject_key: format!("subject-{index:03}"),
                operation: "Update".to_string(),
                first_seen_ms: index as i64,
                applied_rv: Some(index as i64 + 1),
                result_proto: vec![index as u8],
                status_stamp: None,
            })
            .await
            .unwrap();
        }

        let page_limit =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE).unwrap();
        let mut after_key = None;
        let mut keys = Vec::new();
        let mut page_lengths = Vec::new();
        loop {
            let page = db
                .list_applied_outbox_paged(after_key.as_deref(), page_limit)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= page_limit.get());
            page_lengths.push(page.len());
            after_key = page.last().map(|row| row.idempotency_key.clone());
            keys.extend(page.into_iter().map(|row| row.idempotency_key));
        }
        assert_eq!(
            page_lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
        assert_eq!(
            keys,
            (0..row_count)
                .map(|index| format!("snapshot-page-{index:03}"))
                .collect::<Vec<_>>()
        );
    }
);

parametrize_backends!(create_and_get, |db| {
    let pod =
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"nginx","namespace":"default"}});
    db.create_resource("v1", "Pod", Some("default"), "nginx", pod.clone())
        .await
        .unwrap();
    let got = db
        .get_resource("v1", "Pod", Some("default"), "nginx")
        .await
        .unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().name, "nginx");
});

parametrize_backends!(
    delete_resource_without_watch_with_tombstone_marks_and_deletes_with_default_backend_fallback,
    |db| {
        let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "tombstone-mark-fallback",
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "tombstone-mark-fallback",
                        "namespace": "default",
                        "uid": "tombstone-mark-fallback-uid"
                    },
                }),
            )
            .await
            .unwrap();

        let deleted = db
            .delete_resource_without_watch_with_tombstone(
                "v1",
                "ConfigMap",
                Some("default"),
                "tombstone-mark-fallback",
                ResourcePreconditions::uid_and_resource_version(
                    created.uid.clone(),
                    created.resource_version,
                ),
                15,
            )
            .await
            .unwrap();

        assert_eq!(deleted.uid, created.uid);
        assert_eq!(deleted.name, "tombstone-mark-fallback");
        assert!(
            deleted
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .is_some_and(|ts| !ts.is_empty())
        );
        assert_eq!(deleted.data["metadata"]["deletionGracePeriodSeconds"], 15);

        let after = db
            .get_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "tombstone-mark-fallback",
            )
            .await
            .unwrap();
        assert!(
            after.is_none(),
            "delete_resource_without_watch_with_tombstone must remove the row"
        );
    }
);

parametrize_backends!(status_noop_update_does_not_advance_resource_version, |db| {
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "status-noop",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "status-noop", "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "x"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let unchanged = db
        .update_status_only(
            "v1",
            "Pod",
            Some("default"),
            "status-noop",
            json!({"phase": "Pending"}),
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(
        unchanged.resource_version, created.resource_version,
        "unchanged status must not advance resourceVersion"
    );
    assert_eq!(unchanged.data, created.data);
});

parametrize_backends!(
    resource_noop_update_does_not_advance_resource_version,
    |db| {
        let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "resource-noop",
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "resource-noop", "namespace": "default"},
                    "data": {"k": "v"}
                }),
            )
            .await
            .unwrap();

        let mut incoming = (*created.data).clone();
        incoming["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());
        let unchanged = db
            .update_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "resource-noop",
                incoming,
                created.resource_version,
            )
            .await
            .unwrap();

        assert_eq!(
            unchanged.resource_version, created.resource_version,
            "unchanged object update must not advance resourceVersion"
        );
        assert_eq!(unchanged.data, created.data);
    }
);

parametrize_backends!(patch_noop_update_does_not_advance_resource_version, |db| {
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "patch-noop",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "patch-noop", "namespace": "default"},
                "data": {"k": "v"}
            }),
        )
        .await
        .unwrap();

    let unchanged = db
        .patch_resource_latest(
            "v1",
            "ConfigMap",
            Some("default"),
            "patch-noop",
            PatchKind::Merge,
            json!({"data": {"k": "v"}}),
        )
        .await
        .unwrap()
        .expect("resource must exist");

    assert_eq!(
        unchanged.resource_version, created.resource_version,
        "unchanged patch must not advance resourceVersion"
    );
    assert_eq!(unchanged.data, created.data);
});

parametrize_backends!(applied_outbox_gc_prunes_all_expired_records, |db| {
    let now_ms = 1_700_000_000_000i64;
    let ttl_ms = 12 * 60 * 60 * 1000i64;
    let expired_ms = now_ms - ttl_ms - 1;
    let recent_ms = now_ms - 60_000;

    db.insert_applied_outbox(AppliedOutboxRecord {
        idempotency_key: "expired-pod-status".to_string(),
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: expired_ms,
        applied_rv: Some(10),
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .unwrap();
    db.insert_applied_outbox(AppliedOutboxRecord {
        idempotency_key: "recent-pod-status".to_string(),
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: recent_ms,
        applied_rv: Some(11),
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .unwrap();
    db.insert_applied_outbox(AppliedOutboxRecord {
        idempotency_key: "expired-event-create".to_string(),
        subject_key: "v1/Event/default/web.1/uid-event".to_string(),
        operation: "EventCreate".to_string(),
        first_seen_ms: expired_ms,
        applied_rv: Some(12),
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .unwrap();
    db.insert_applied_outbox(AppliedOutboxRecord {
        idempotency_key: "expired-future-operation".to_string(),
        subject_key: "example.io/v1/Future/default/name/uid-future".to_string(),
        operation: "FutureOperation".to_string(),
        first_seen_ms: expired_ms,
        applied_rv: Some(13),
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .unwrap();

    let pruned = db.gc_applied_outbox(now_ms, ttl_ms).await.unwrap();
    assert_eq!(pruned, 3);
    assert!(
        db.get_applied_outbox("expired-pod-status")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_applied_outbox("recent-pod-status")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db.get_applied_outbox("expired-event-create")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_applied_outbox("expired-future-operation")
            .await
            .unwrap()
            .is_none()
    );
});

parametrize_backends!(delete_resource, |db| {
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "p",
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"default"}}),
    )
    .await
    .unwrap();
    db.delete_resource("v1", "Pod", Some("default"), "p")
        .await
        .unwrap();
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "p")
            .await
            .unwrap()
            .is_none()
    );
});

parametrize_backends!(create_duplicate_returns_error, |db| {
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "dup",
        json!({"metadata":{"name":"dup"}}),
    )
    .await
    .unwrap();
    assert!(
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "dup",
            json!({"metadata":{"name":"dup"}})
        )
        .await
        .is_err()
    );
});

parametrize_backends!(get_missing_returns_none, |db| {
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "nope")
            .await
            .unwrap()
            .is_none()
    );
});

#[tokio::test]
async fn update_with_wrong_rv_conflict_sqlite() {
    let db = sqlite_db().await;
    let db: &dyn DatastoreBackend = &db;
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm",
        json!({"metadata":{"name":"cm"}}),
    )
    .await
    .unwrap();
    assert!(
        db.update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            json!({"metadata":{"name":"cm"}}),
            999
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn update_with_wrong_rv_conflict_redb() {
    let db = redb_db().await;
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"},"data":{"k":"v1"}}),
        )
        .await
        .unwrap();
    let err = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"},"data":{"k":"v2"}}),
            created.resource_version + 999,
        )
        .await
        .expect_err("redb must enforce resourceVersion preconditions");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
}

parametrize_backends!(namespace_crud, |db| {
    db.create_namespace(
        "test-ns",
        json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"test-ns"}}),
    )
    .await
    .unwrap();
    assert!(db.get_namespace("test-ns").await.unwrap().is_some());
    let list = db.list_namespaces(None, None).await.unwrap();
    assert!(list.items.iter().any(|ns| ns.name == "test-ns"));
    // update_namespace with expected_rv=0 may conflict if the backend
    // assigns a non-zero RV on creation. Use the value from the created namespace.
    let created = db.get_namespace("test-ns").await.unwrap().unwrap();
    let rv = created.resource_version;
    db.update_namespace(
        "test-ns",
        json!({"metadata":{"name":"test-ns","labels":{"env":"test"}}}),
        rv,
    )
    .await
    .unwrap();
    db.delete_namespace("test-ns").await.unwrap();
    assert!(db.get_namespace("test-ns").await.unwrap().is_none());
});

parametrize_backends!(namespace_contents_and_count, |db| {
    db.create_namespace("countns", json!({"metadata":{"name":"countns"}}))
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("countns"),
        "pod1",
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod1","namespace":"countns"}}),
    )
    .await
    .unwrap();
    db.create_resource("v1", "ConfigMap", Some("countns"), "cm1",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"countns"}})
        ).await.unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("countns"),
        "sec1",
        json!({"apiVersion":"v1","kind":"Secret","metadata":{"name":"sec1","namespace":"countns"}}),
    )
    .await
    .unwrap();
    assert_eq!(db.count_namespace_resources("countns").await.unwrap(), 3);
    assert_eq!(
        db.list_namespace_resources("countns").await.unwrap().len(),
        3
    );
    assert_eq!(
        db.list_namespace_resources_of_kind("countns", "ConfigMap")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.list_namespace_resources_excluding_kind("countns", "ConfigMap")
            .await
            .unwrap()
            .len(),
        2
    );
    db.delete_namespace_contents("countns").await.unwrap();
    assert_eq!(db.count_namespace_resources("countns").await.unwrap(), 1);
    assert!(
        db.get_resource("v1", "Pod", Some("countns"), "pod1")
            .await
            .unwrap()
            .is_some(),
        "namespace content cleanup must not remove Pod rows; actor finalization owns Pod datastore deletion"
    );
});

parametrize_backends!(owner_ref_crud, |db| {
    db.create_resource("apps/v1", "ReplicaSet", Some("default"), "rs",
            json!({"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"rs","namespace":"default","uid":"rs-uid-123"}})
        ).await.unwrap();
    db.create_resource("v1", "Pod", Some("default"), "pod1",
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"pod1","namespace":"default","uid":"pod-uid-456","ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rs-uid-123"}]}})
        ).await.unwrap();
    let owned = db
        .find_owned_resources("rs-uid-123", Some("default"))
        .await
        .unwrap();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].name, "pod1");
    let by_uid = db
        .list_resources_by_owner_uid("v1", "Pod", Some("default"), "rs-uid-123")
        .await
        .unwrap();
    assert_eq!(by_uid.len(), 1);
    db.delete_resource("v1", "Pod", Some("default"), "pod1")
        .await
        .unwrap();
    assert!(
        db.find_owned_resources("rs-uid-123", Some("default"))
            .await
            .unwrap()
            .is_empty()
    );
});

parametrize_backends!(watch_events, |db| {
    db.create_namespace("watchns", json!({"metadata":{"name":"watchns"}}))
        .await
        .unwrap();
    let rv = db.get_current_resource_version().await.unwrap();
    db.create_resource("v1", "ConfigMap", Some("watchns"), "cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"watchns"}})
        ).await.unwrap();
    let targets = vec![WatchTarget::namespaced("v1", "ConfigMap")];
    let events = db.list_watch_events_since(&targets, rv).await.unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0].resource.name, "cm");
});

parametrize_backends!(resource_version_advances, |db| {
    let rv0 = db.get_current_resource_version().await.unwrap();
    db.create_namespace("rvns", json!({"metadata":{"name":"rvns"}}))
        .await
        .unwrap();
    let rv1 = db.get_current_resource_version().await.unwrap();
    assert!(rv1 > rv0);
    let advanced = db.advance_resource_version_after(rv1).await.unwrap();
    assert!(advanced > rv1);
});

parametrize_backends!(list_limit_zero_returns_all_items_without_continue, |db| {
    for name in ["cm-1", "cm-2", "cm-3"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({"metadata":{"name": name, "namespace": "default"}}),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(0), None),
        )
        .await
        .unwrap();
    let names: Vec<_> = list.items.iter().map(|item| item.name.as_str()).collect();

    assert_eq!(names, vec!["cm-1", "cm-2", "cm-3"]);
    assert_eq!(list.continue_token, None);
    assert_eq!(list.remaining_item_count, None);
});

parametrize_backends!(
    field_selector_upstream_semantics_match_across_backends,
    |db| {
        for name in ["comma,name", "equals=name", "ordinary"] {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": name, "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        }

        for (selector, expected) in [
            (r"metadata.name=comma\,name", vec!["comma,name"]),
            (r"metadata.name=equals\=name", vec!["equals=name"]),
            (
                "status.phase=",
                vec!["comma,name", "equals=name", "ordinary"],
            ),
            (
                "status.phase!=Running",
                vec!["comma,name", "equals=name", "ordinary"],
            ),
        ] {
            let list = db
                .list_resources(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    ResourceListQuery::new(None, Some(selector), None, None),
                )
                .await
                .unwrap();
            let mut names = list
                .items
                .iter()
                .map(|resource| resource.name.as_str())
                .collect::<Vec<_>>();
            let mut expected = expected;
            names.sort_unstable();
            expected.sort_unstable();
            assert_eq!(names, expected, "selector={selector}");
        }

        db.create_resource(
            "events.k8s.io/v1",
            "Event",
            Some("default"),
            "scheduled",
            json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {"name": "scheduled", "namespace": "default"},
                "regarding": {"apiVersion": "v1", "kind": "Pod", "name": "pod-a"}
            }),
        )
        .await
        .unwrap();
        let events = db
            .list_resources(
                "events.k8s.io/v1",
                "Event",
                Some("default"),
                ResourceListQuery::new(
                    None,
                    Some("involvedObject.kind=Pod,involvedObject.name=pod-a"),
                    None,
                    None,
                ),
            )
            .await
            .unwrap();
        assert_eq!(events.items.len(), 1);
        assert_eq!(events.items[0].name, "scheduled");
    }
);

parametrize_backends!(list_carries_atomic_durable_watch_position, |db| {
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "positioned-list",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "positioned-list", "namespace": "default"}
        }),
    )
    .await
    .unwrap();

    let resources = db
        .list_resources("v1", "ConfigMap", Some("default"), ResourceListQuery::all())
        .await
        .unwrap();
    let resource_position = resources
        .watch_replay_position
        .expect("resource LIST must expose its durable replay position");
    assert!(resource_position.event_id > 0);
    assert_eq!(
        resource_position.resource_version,
        resources.resource_version
    );
    assert_eq!(
        resource_position.event_id,
        db.current_watch_replay_position().await.unwrap().event_id
    );

    db.create_namespace(
        "positioned-list-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "positioned-list-ns"}
        }),
    )
    .await
    .unwrap();
    let namespaces = db.list_namespaces(None, None).await.unwrap();
    let namespace_position = namespaces
        .watch_replay_position
        .expect("Namespace LIST must expose its durable replay position");
    assert!(namespace_position.event_id > resource_position.event_id);
    assert_eq!(
        namespace_position.resource_version,
        namespaces.resource_version
    );
    assert_eq!(
        namespace_position.event_id,
        db.current_watch_replay_position().await.unwrap().event_id
    );
});

parametrize_backends!(multi_version_watch_baseline_is_one_atomic_snapshot, |db| {
    for (api_version, name) in [
        ("widgets.example.test/v1", "widget-v1"),
        ("widgets.example.test/v2", "widget-v2"),
    ] {
        db.create_resource(
            api_version,
            "Widget",
            Some("default"),
            name,
            json!({
                "apiVersion": api_version,
                "kind": "Widget",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "labels": {"baseline": "atomic"}
                }
            }),
        )
        .await
        .unwrap();
    }
    db.create_resource(
        "widgets.example.test/v1",
        "Widget",
        Some("other"),
        "widget-v1-other",
        json!({
            "apiVersion": "widgets.example.test/v1",
            "kind": "Widget",
            "metadata": {
                "name": "widget-v1-other",
                "namespace": "other",
                "labels": {"baseline": "atomic"}
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "clusterwidgets.example.test/v1",
        "ClusterWidget",
        None,
        "cluster-widget",
        json!({
            "apiVersion": "clusterwidgets.example.test/v1",
            "kind": "ClusterWidget",
            "metadata": {
                "name": "cluster-widget",
                "labels": {"baseline": "atomic"}
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "widgets.example.test/v2",
        "Widget",
        Some("default"),
        "widget-filtered-out",
        json!({
            "apiVersion": "widgets.example.test/v2",
            "kind": "Widget",
            "metadata": {
                "name": "widget-filtered-out",
                "namespace": "default",
                "labels": {"baseline": "other"}
            }
        }),
    )
    .await
    .unwrap();

    let list = db
        .list_resources_for_watch_targets(
            &[
                WatchTarget::namespaced("widgets.example.test/v1", "Widget"),
                WatchTarget::namespaced_in_namespace(
                    "widgets.example.test/v2",
                    "Widget",
                    "default",
                ),
                WatchTarget::cluster("clusterwidgets.example.test/v1", "ClusterWidget"),
            ],
            Some("baseline=atomic"),
        )
        .await
        .unwrap();

    assert_eq!(
        list.items
            .iter()
            .map(|item| (item.api_version.as_str(), item.name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("widgets.example.test/v1", "widget-v1"),
            ("widgets.example.test/v1", "widget-v1-other"),
            ("widgets.example.test/v2", "widget-v2"),
            ("clusterwidgets.example.test/v1", "cluster-widget")
        ]
    );
    let position = list
        .watch_replay_position
        .expect("multi-version baseline must carry one durable replay position");
    assert_eq!(position.resource_version, list.resource_version);
    assert_eq!(
        position.event_id,
        db.current_watch_replay_position().await.unwrap().event_id
    );
});

parametrize_backends!(
    multi_target_watch_list_supports_kubernetes_label_operators,
    |db| {
        for (name, labels) in [
            ("prod", json!({"env": "prod", "tier": "web"})),
            ("dev", json!({"env": "dev"})),
            ("unlabelled", json!({"tier": "batch"})),
        ] {
            db.create_resource(
                "widgets.test/v1",
                "Widget",
                Some("default"),
                name,
                json!({
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "labels": labels
                    }
                }),
            )
            .await
            .unwrap();
        }
        let targets = [WatchTarget::namespaced_in_namespace(
            "widgets.test/v1",
            "Widget",
            "default",
        )];
        for (selector, expected) in [
            ("env", vec!["dev", "prod"]),
            ("!env", vec!["unlabelled"]),
            ("env in (prod,stage)", vec!["prod"]),
            ("env notin (prod)", vec!["dev", "unlabelled"]),
            ("env!=prod", vec!["dev", "unlabelled"]),
        ] {
            let list = db
                .list_resources_for_watch_targets(&targets, Some(selector))
                .await
                .unwrap();
            assert_eq!(
                list.items
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>(),
                expected,
                "selector {selector}"
            );
        }
    }
);

parametrize_backends!(positive_rv_handoff_keeps_late_lower_rv_events, |db| {
    async fn apply(db: &dyn DatastoreBackend, name: &str, resource_version: i64) {
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": format!("uid-{name}"),
                    "resourceVersion": resource_version.to_string()
                }
            }),
            ReplicatedCreateOptions {
                resource_version,
                meta_uid: Some(format!("uid-{name}")),
            },
        )
        .await
        .unwrap();
    }

    apply(db, "at-floor", 10).await;
    apply(db, "pre-anchor", 12).await;
    let anchor = db.current_watch_replay_position().await.unwrap();
    apply(db, "late-lower", 11).await;
    apply(db, "post-anchor", 13).await;

    let target = WatchTarget::namespaced_in_namespace("v1", "ConfigMap", "default");
    let mut position =
        WatchReplayPosition::from_resource_version_through_event_id(10, anchor.event_id);
    let mut names = Vec::new();
    loop {
        let replay = db
            .list_watch_events_after_position_checked_bounded(
                std::slice::from_ref(&target),
                position,
                std::num::NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap();
        let PositionedWatchReplayRead::Events(replay) = replay else {
            panic!("fresh composite handoff must be replayable");
        };
        names.extend(
            replay
                .events
                .iter()
                .map(|event| event.event.resource.name.clone()),
        );
        position = replay.next_position;
        if replay.events.is_empty() {
            break;
        }
    }
    assert_eq!(names, vec!["pre-anchor", "late-lower", "post-anchor"]);
    assert_eq!(
        position.resource_version_filter_through_event_id, 0,
        "RV filtering must be released after replay crosses the handoff anchor"
    );
});

parametrize_backends!(
    snapshot_at_exact_watch_position_reverses_later_change,
    |db| {
        let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "selected",
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "selected",
                        "namespace": "default",
                        "labels": {"track": "yes"}
                    }
                }),
            )
            .await
            .unwrap();
        let position = db.current_watch_replay_position().await.unwrap();

        let mut changed = (*created.data).clone();
        changed["metadata"]["labels"]["track"] = json!("no");
        db.update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "selected",
            changed,
            created.resource_version,
        )
        .await
        .unwrap();

        let snapshot = db
            .snapshot_resources_at_position(
                &[WatchTarget::namespaced_in_namespace(
                    "v1",
                    "ConfigMap",
                    "default",
                )],
                Some("track=yes"),
                None,
                position,
            )
            .await
            .unwrap();
        let SnapshotAtRv::List(snapshot) = snapshot else {
            panic!("an earlier exact event position must be reconstructed");
        };
        assert_eq!(snapshot.items.len(), 1);
        assert_eq!(snapshot.items[0].name, "selected");
        assert_eq!(snapshot.watch_replay_position, Some(position));
    }
);

parametrize_backends!(
    snapshot_at_composite_position_is_atomic_across_targets,
    |db| {
        db.create_resource(
            "widgets.test/v1",
            "Widget",
            Some("default"),
            "v1",
            json!({
                "apiVersion": "widgets.test/v1",
                "kind": "Widget",
                "metadata": {"name": "v1", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "widgets.test/v2",
            "Widget",
            Some("default"),
            "v2-existing",
            json!({
                "apiVersion": "widgets.test/v2",
                "kind": "Widget",
                "metadata": {"name": "v2-existing", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
        let rv_boundary = db.get_current_resource_version().await.unwrap();
        let anchor = db.current_watch_replay_position().await.unwrap();
        db.create_resource(
            "widgets.test/v2",
            "Widget",
            Some("default"),
            "v2-late",
            json!({
                "apiVersion": "widgets.test/v2",
                "kind": "Widget",
                "metadata": {"name": "v2-late", "namespace": "default"}
            }),
        )
        .await
        .unwrap();

        let position = WatchReplayPosition::from_resource_version_through_event_id(
            rv_boundary,
            anchor.event_id,
        );
        let snapshot = db
            .snapshot_resources_at_position(
                &[
                    WatchTarget::namespaced("widgets.test/v2", "Widget"),
                    WatchTarget::namespaced("widgets.test/v1", "Widget"),
                ],
                None,
                None,
                position,
            )
            .await
            .unwrap();
        let SnapshotAtRv::List(snapshot) = snapshot else {
            panic!("composite position before the current event high-water must reconstruct");
        };
        assert_eq!(
            snapshot
                .items
                .iter()
                .map(|resource| resource.name.as_str())
                .collect::<Vec<_>>(),
            vec!["v2-existing", "v1"],
            "reconstructed snapshot must preserve caller/storage-version order"
        );
        assert_eq!(snapshot.watch_replay_position, Some(position));
    }
);

parametrize_backends!(list_page_request_drives_resource_pagination, |db| {
    for name in ["cm-1", "cm-2", "cm-3"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({"metadata":{"name": name, "namespace": "default"}}),
        )
        .await
        .unwrap();
    }

    let page1 = db
        .list_resources_page(
            "v1",
            "ConfigMap",
            Some("default"),
            None,
            None,
            crate::datastore::ListPageRequest::try_new(Some(2), None).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        page1
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["cm-1", "cm-2"]
    );
    assert_eq!(page1.continue_token.as_deref(), Some("cm-2"));
    assert_eq!(page1.remaining_item_count, Some(1));

    let page2 = db
        .list_resources_page(
            "v1",
            "ConfigMap",
            Some("default"),
            None,
            None,
            crate::datastore::ListPageRequest::try_new(Some(2), page1.continue_token.clone())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        page2
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["cm-3"]
    );
    assert_eq!(page2.continue_token, None);
    assert_eq!(page2.remaining_item_count, None);
});

parametrize_backends!(
    selector_pagination_remaining_count_matches_filtered_items,
    |db| {
        for name in ["web-1", "web-2", "web-3", "web-4"] {
            db.create_resource(
                "v1",
                "Pod",
                Some("default"),
                name,
                json!({
                    "metadata":{
                        "name": name,
                        "namespace": "default",
                        "labels": {"app": "web"}
                    }
                }),
            )
            .await
            .unwrap();
        }
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "api-1",
            json!({
                "metadata":{
                    "name": "api-1",
                    "namespace": "default",
                    "labels": {"app": "api"}
                }
            }),
        )
        .await
        .unwrap();

        let page1 = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(Some("app=web"), None, Some(2), None),
            )
            .await
            .unwrap();
        assert_eq!(
            page1
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["web-1", "web-2"]
        );
        assert_eq!(page1.continue_token.as_deref(), Some("web-2"));

        let page2 = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("app=web"),
                    None,
                    Some(2),
                    page1.continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            page2
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["web-3", "web-4"]
        );
        assert_eq!(page2.continue_token, None);
    }
);

parametrize_backends!(gc_watch_events, |db| {
    db.create_namespace("gcns", json!({"metadata":{"name":"gcns"}}))
        .await
        .unwrap();
    for i in 0..5 {
        db.create_resource("v1", "ConfigMap", Some("gcns"), &format!("cm{i}"),
                json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":format!("cm{i}"),"namespace":"gcns"}})
            ).await.unwrap();
    }
    let removed = db.gc_watch_events(3, 1000).await.unwrap();
    assert!(removed >= 2);
});

parametrize_backends!(watch_position_survives_empty_retained_history, |db| {
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "before-gc",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "before-gc", "namespace": "default"}
        }),
    )
    .await
    .unwrap();
    let before_gc = db.current_watch_replay_position().await.unwrap();
    assert!(before_gc.event_id > 0);
    assert!(db.gc_watch_events(0, -1).await.unwrap() > 0);

    let list = db
        .list_resources("v1", "ConfigMap", Some("default"), ResourceListQuery::all())
        .await
        .unwrap();
    let after_gc = list
        .watch_replay_position
        .expect("LIST must retain the allocator high-water after watch GC");
    assert_eq!(after_gc.event_id, before_gc.event_id);

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "after-gc",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "after-gc", "namespace": "default"}
        }),
    )
    .await
    .unwrap();
    let replay = db
        .list_watch_events_after_position_checked_bounded(
            &[WatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            after_gc,
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap();
    let PositionedWatchReplayRead::Events(replay) = replay else {
        panic!("a current allocator position must not expire after retention GC");
    };
    assert_eq!(
        replay
            .events
            .iter()
            .map(|event| event.event.resource.name.as_str())
            .collect::<Vec<_>>(),
        vec!["after-gc"]
    );
});

parametrize_backends!(watch_position_ahead_of_allocator_expires, |db| {
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "anchor",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "anchor", "namespace": "default"}
        }),
    )
    .await
    .unwrap();
    let current = db.current_watch_replay_position().await.unwrap();
    let ahead = WatchReplayPosition {
        event_id: current.event_id.saturating_add(1),
        ..current
    };

    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &[WatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            ahead,
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap(),
        PositionedWatchReplayRead::Expired
    ));

    let legacy_ahead =
        WatchReplayPosition::from_resource_version(current.resource_version.saturating_add(1));
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &[WatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            legacy_ahead,
            std::num::NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap(),
        PositionedWatchReplayRead::Expired
    ));
});

parametrize_backends!(
    scoped_replay_floor_allows_retained_in_scope_event_after_unrelated_gc,
    |db| {
        for i in 0..20 {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("noise"),
                &format!("cm-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "noise", "name": format!("cm-{i}")}
                }),
            )
            .await
            .unwrap();
        }

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("app"),
                "frontend",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "app", "name": "frontend"},
                    "spec": {"containers": [{"name": "app", "image": "pause"}]}
                }),
            )
            .await
            .unwrap();

        db.gc_watch_events(1, 1000).await.unwrap();
        let since_rv = pod.resource_version - 10;

        let replay = db
            .list_watch_events_since_checked(
                &[WatchTarget::namespaced_in_namespace("v1", "Pod", "app")],
                since_rv,
            )
            .await
            .unwrap();

        match replay {
            WatchReplayRead::Events(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].resource.name, "frontend");
            }
            WatchReplayRead::Expired => {
                panic!("unrelated lower-RV churn must not expire app/Pod replay");
            }
        }
    }
);

parametrize_backends!(
    scoped_replay_floor_allows_retained_in_scope_event_before_unrelated_newer_gc,
    |db| {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("app"),
            "baseline",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "app", "name": "baseline"}
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("app"),
                "frontend",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "app", "name": "frontend"},
                    "spec": {"containers": [{"name": "app", "image": "pause"}]}
                }),
            )
            .await
            .unwrap();

        for i in 0..20 {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("noise"),
                &format!("cm-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "noise", "name": format!("cm-{i}")}
                }),
            )
            .await
            .unwrap();
        }

        db.gc_watch_events(1, 1000).await.unwrap();
        let since_rv = pod.resource_version - 1;

        let replay = db
            .list_watch_events_since_checked(
                &[WatchTarget::namespaced_in_namespace("v1", "Pod", "app")],
                since_rv,
            )
            .await
            .unwrap();

        match replay {
            WatchReplayRead::Events(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].resource.name, "frontend");
            }
            WatchReplayRead::Expired => {
                panic!("unrelated higher-RV churn must not expire app/Pod replay");
            }
        }
    }
);

parametrize_backends!(
    scoped_replay_floor_expires_when_in_scope_event_was_gc_collected,
    |db| {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("app"),
            "baseline",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "app", "name": "baseline"}
            }),
        )
        .await
        .unwrap();

        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("app"),
                "frontend",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "app", "name": "frontend"},
                    "spec": {"containers": [{"name": "app", "image": "pause"}]}
                }),
            )
            .await
            .unwrap();

        db.create_resource(
            "v1",
            "Pod",
            Some("app"),
            "backend",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "app", "name": "backend"},
                "spec": {"containers": [{"name": "app", "image": "pause"}]}
            }),
        )
        .await
        .unwrap();

        for i in 0..20 {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("noise"),
                &format!("cm-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "noise", "name": format!("cm-{i}")}
                }),
            )
            .await
            .unwrap();
        }

        db.gc_watch_events(1, 1000).await.unwrap();
        let since_rv = pod.resource_version - 1;

        let replay = db
            .list_watch_events_since_checked(
                &[WatchTarget::namespaced_in_namespace("v1", "Pod", "app")],
                since_rv,
            )
            .await
            .unwrap();

        assert!(
            matches!(replay, WatchReplayRead::Expired),
            "missing in-scope event must expire checked replay"
        );
    }
);

parametrize_backends!(list_resource_keys_for_scope, |db| {
    db.create_resource(
        "v1",
        "Node",
        None,
        "n1",
        json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "p1",
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p1","namespace":"default"}}),
    )
    .await
    .unwrap();
    let cluster_keys = db
        .list_resource_keys_for_scope("v1".into(), "Node".into(), false)
        .await
        .unwrap();
    assert_eq!(cluster_keys.len(), 1);
    assert!(cluster_keys[0].0.is_none());
    let ns_keys = db
        .list_resource_keys_for_scope("v1".into(), "Pod".into(), true)
        .await
        .unwrap();
    assert_eq!(ns_keys.len(), 1);
    assert_eq!(ns_keys[0].0.as_deref(), Some("default"));
});

// ---- Redb-only tests (exercise redb-specific codepaths) ----

#[tokio::test]
async fn redb_update_resource() {
    let db = redb_db().await;
    let created = db.create_resource("v1", "ConfigMap", Some("default"), "cm",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"},"data":{"key":"val1"}})
    ).await.unwrap();
    let updated = db.update_resource("v1", "ConfigMap", Some("default"), "cm",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"},"data":{"key":"val2"}}),
        created.resource_version
    ).await.unwrap();
    assert!(updated.resource_version > created.resource_version);
}

#[tokio::test]
async fn redb_update_status_only() {
    let db = redb_db().await;
    db.create_resource("v1", "Pod", Some("default"), "mypod",
        json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"mypod","namespace":"default"},"spec":{"containers":[{"name":"main"}]}})
    ).await.unwrap();
    let updated = db
        .update_status_only(
            "v1",
            "Pod",
            Some("default"),
            "mypod",
            json!({"phase":"Running","podIP":"10.42.0.5"}),
            None,
        )
        .await
        .unwrap();
    assert_eq!(updated.data["status"]["phase"], "Running");
    assert_eq!(updated.data["spec"]["containers"][0]["name"], "main");
}

#[tokio::test]
async fn redb_patch_resource() {
    let db = redb_db().await;
    db.create_resource("v1", "ConfigMap", Some("default"), "cm",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default"},"data":{"key":"val"}})
    ).await.unwrap();
    let patched = db
        .patch_resource_latest(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            PatchKind::Merge,
            json!({"data":{"key":"patched"}}),
        )
        .await
        .unwrap();
    assert!(patched.is_some());
    assert_eq!(patched.unwrap().data["data"]["key"], "patched");
}

#[tokio::test]
async fn redb_node_subnet() {
    let db = redb_db().await;
    let ns = db
        .allocate_node_subnet("node1", "10.42.0.0/16", "192.168.1.10")
        .await
        .unwrap();
    assert_eq!(ns.node_name.as_ref(), "node1");
    let ns2 = db
        .allocate_node_subnet("node1", "10.42.0.0/16", "192.168.1.10")
        .await
        .unwrap();
    assert_eq!(ns.subnet_base_int, ns2.subnet_base_int);
    assert!(db.get_node_subnet("node1").await.unwrap().is_some());
    let ns3 = db
        .allocate_node_subnet("node2", "10.42.0.0/16", "192.168.1.11")
        .await
        .unwrap();
    assert_ne!(ns3.subnet_base_int, ns.subnet_base_int);
    let peers = db
        .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::excluding("node1").unwrap())
        .await
        .unwrap();
    assert_eq!(peers.len(), 1);
    let all = db
        .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::all())
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    db.delete_node_subnet("node2").await.unwrap();
    assert!(db.get_node_subnet("node2").await.unwrap().is_none());
}

parametrize_backends!(node_dataplane_metadata_round_trip, |db| {
    let metadata = klights_cluster_store::DataplanePeerMetadata::try_new(
        "node1".to_string(),
        klights_cluster_store::DataplaneMode::Root,
        klights_cluster_store::DataplaneEncryption::Enabled,
        Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        Some("192.0.2.10".to_string()),
        Some(51_820),
    )
    .unwrap();
    db.update_node_dataplane(metadata.clone()).await.unwrap();
    assert_eq!(
        db.get_node_dataplane("node1").await.unwrap(),
        Some(metadata)
    );
});

#[tokio::test]
async fn redb_find_owned_by_name_kind_empty_uid() {
    let db = redb_db().await;
    db.create_resource("apps/v1", "Deployment", Some("default"), "mydep",
        json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"mydep","namespace":"default"}})
    ).await.unwrap();
    db.create_resource("apps/v1", "ReplicaSet", Some("default"), "mydep-abc",
        json!({"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"mydep-abc","namespace":"default","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"mydep","uid":""}]}})
    ).await.unwrap();
    db.create_resource("v1", "ConfigMap", Some("default"), "cm1",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"default","ownerReferences":[{"apiVersion":"v1","kind":"SomeOther","name":"other","uid":""}]}})
    ).await.unwrap();
    let owned = db
        .find_owned_by_name_kind_empty_uid("apps/v1", "mydep", "Deployment", Some("default"))
        .await
        .unwrap();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].name, "mydep-abc");
}

#[tokio::test]
async fn redb_delete_resource_with_tombstone_command_stamps_and_watches_deleted_row() {
    use crate::bootstrap::sequenced_datastore::DatastoreApplier;
    use klights_cluster_core::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };

    let db = redb_db().await;
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "delete-cmd-tombstone",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "delete-cmd-tombstone", "namespace": "default", "uid": "delete-cmd-tombstone-uid"},
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::DeleteResourceWithTombstone {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "delete-cmd-tombstone".to_string(),
        preconditions: ResourcePreconditions::uid_and_resource_version(
            created.uid.clone(),
            created.resource_version,
        ),
        grace_seconds: 20,
    };

    db.apply_command(
        command,
        CommandMeta {
            command_id: CommandId("redb-delete-tombstone-command".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 0,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "test-node".into(),
        },
    )
    .await
    .unwrap();

    let key_events: Vec<_> = db
        .list_all_watch_events_since(0)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.resource.name == "delete-cmd-tombstone")
        .collect();

    assert_eq!(
        key_events.len(),
        2,
        "tombstone command should emit ADDED + DELETED watch entries only"
    );

    let deleted = key_events
        .iter()
        .find(|event| event.event_type.as_ref() == "DELETED")
        .expect("delete command should emit exactly one DELETED watch event");
    assert_eq!(
        deleted.resource.data["metadata"]["deletionGracePeriodSeconds"],
        json!(20)
    );
    assert!(
        deleted
            .resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str)
            .is_some_and(|ts| !ts.is_empty()),
        "DELETED watch payload must include deletionTimestamp"
    );

    assert!(
        !key_events
            .iter()
            .any(|event| event.event_type.as_ref() == "MODIFIED"),
        "tombstone command must not emit an intermediate MODIFIED event"
    );

    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "delete-cmd-tombstone")
            .await
            .unwrap()
            .is_none(),
        "resource row should be removed after tombstone delete"
    );
}
