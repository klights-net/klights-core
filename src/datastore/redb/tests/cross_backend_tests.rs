//! DSB-R-07 — Cross-backend parametrized tests.
//!
//! Uses `parametrize_backends!` macro to run each test against both
//! SQLite and redb without duplication. Backend-specific tests (PRAGMA,
//! fingerprint, table-definition) stay in their own module.

use serde_json::json;

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::redb::RedbDatastore;
use crate::datastore::sqlite::Datastore as SqliteDs;
use crate::datastore::types::*;
use crate::pod_identity::PodIdentity;

async fn sqlite_db() -> SqliteDs {
    SqliteDs::new_in_memory().await.unwrap()
}

async fn redb_db() -> RedbDatastore {
    RedbDatastore::new_in_memory().await.unwrap()
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

parametrize_backends!(pod_slot_try_admit_inserts_empty_slot, |db| {
    let result = db
        .pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
        .await
        .unwrap();

    assert!(matches!(
        result,
        PodSlotAdmissionResult::Admitted { resource_version } if resource_version > 0
    ));
});

parametrize_backends!(pod_slot_try_admit_same_uid_idempotent, |db| {
    let first = db
        .pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
        .await
        .unwrap();
    let second = db
        .pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
        .await
        .unwrap();

    assert_eq!(first, second);
});

parametrize_backends!(
    pod_slot_try_admit_different_uid_blocked_without_write,
    |db| {
        db.pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
            .await
            .unwrap();
        let blocked = db
            .pod_slot_try_admit("default", "slot-pod", "uid-b", "node-b")
            .await
            .unwrap();

        assert!(matches!(
            blocked,
            PodSlotAdmissionResult::Blocked {
                ref blocking_uid,
                ref blocking_node,
                state: PodSlotAdmissionState::Admitted,
                ..
            } if blocking_uid.as_str() == "uid-a" && blocking_node.as_str() == "node-a"
        ));

        let still_blocked = db
            .pod_slot_try_admit("default", "slot-pod", "uid-b", "node-b")
            .await
            .unwrap();
        assert_eq!(blocked, still_blocked);
    }
);

parametrize_backends!(pod_slot_mark_terminating_different_uid_conflicts, |db| {
    db.pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
        .await
        .unwrap();
    let err = db
        .pod_slot_mark_terminating("default", "slot-pod", "uid-b", "node-b")
        .await
        .expect_err("different UID must not overwrite admission row");
    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
});

parametrize_backends!(pod_slot_clear_if_uid_does_not_clear_replacement, |db| {
    db.pod_slot_try_admit("default", "slot-pod", "uid-a", "node-a")
        .await
        .unwrap();
    db.pod_slot_clear_if_uid("default", "slot-pod", "uid-b", "node-b")
        .await
        .unwrap();
    let blocked = db
        .pod_slot_try_admit("default", "slot-pod", "uid-b", "node-b")
        .await
        .unwrap();
    assert!(matches!(
        blocked,
        PodSlotAdmissionResult::Blocked { blocking_uid, .. } if blocking_uid == "uid-a"
    ));
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
        crate::datastore::errors::is_conflict_error(&err),
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

parametrize_backends!(pod_endpoint_empty, |db| {
    assert!(
        db.pod_endpoint_get_by_pod_ip(std::net::Ipv4Addr::new(10, 42, 0, 5))
            .await
            .unwrap()
            .is_none()
    );
});

parametrize_backends!(subscribe_pod_endpoints, |db| {
    let _rx = db.subscribe_pod_endpoints();
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
async fn redb_sandbox_lifecycle() {
    let db = redb_db().await;
    db.record_sandbox("default", "mypod", "pod-uid-1", "sid-123")
        .await
        .unwrap();
    assert_eq!(
        db.get_sandbox("default", "mypod").await.unwrap(),
        Some("sid-123".to_string())
    );
    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "pod-uid-1")
            .await
            .unwrap(),
        Some("sid-123".to_string())
    );
    assert!(
        db.get_sandbox_for_uid("default", "mypod", "wrong-uid")
            .await
            .unwrap()
            .is_none()
    );
    let sandboxes = db.list_sandboxes().await.unwrap();
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].pod_uid, "pod-uid-1");
    db.delete_sandbox_for_uid("default", "mypod", "wrong-uid", "sid-123")
        .await
        .unwrap();
    assert_eq!(
        db.get_sandbox("default", "mypod").await.unwrap(),
        Some("sid-123".to_string()),
        "UID-qualified delete must not remove a replacement/mismatched row"
    );
    db.delete_sandbox_for_uid("default", "mypod", "pod-uid-1", "sid-123")
        .await
        .unwrap();
    assert!(db.get_sandbox("default", "mypod").await.unwrap().is_none());
}

#[tokio::test]
async fn redb_concurrent_sandbox_insert_different_uids_both_survive() {
    let db = redb_db().await;
    db.record_sandbox("default", "mypod", "uid-a", "sid-a")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-b", "sid-b")
        .await
        .unwrap();

    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-a")
            .await
            .unwrap(),
        Some("sid-a".to_string())
    );
    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-b")
            .await
            .unwrap(),
        Some("sid-b".to_string())
    );

    let sandboxes = db.list_sandboxes().await.unwrap();
    assert_eq!(sandboxes.len(), 2);
}

#[tokio::test]
async fn redb_record_sandbox_same_uid_replaces_same_uid_only() {
    let db = redb_db().await;
    db.record_sandbox("default", "mypod", "uid-a", "sid-old")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-b", "sid-b")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-a", "sid-new")
        .await
        .unwrap();

    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-a")
            .await
            .unwrap(),
        Some("sid-new".to_string())
    );
    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-b")
            .await
            .unwrap(),
        Some("sid-b".to_string())
    );
}

#[tokio::test]
async fn redb_ipam() {
    let db = redb_db().await;
    db.record_sandbox("default", "pod1", "uid1", "sid1")
        .await
        .unwrap();
    let subnet_base: u32 = 0x0A2A0100;
    let pod = PodIdentity::new("default", "pod1", "uid1");
    let (ip, ip_int) = db
        .ipam_allocate_and_record_pod_network(
            "sid1",
            &pod,
            subnet_base,
            256,
            "veth0",
            "/var/run/netns/sid1",
        )
        .await
        .unwrap();
    assert!(!ip.is_empty());
    let (ip2, ip2_int) = db
        .ipam_allocate_and_record_pod_network(
            "sid1",
            &pod,
            subnet_base,
            256,
            "veth0",
            "/var/run/netns/sid1",
        )
        .await
        .unwrap();
    assert_eq!(ip, ip2);
    assert_eq!(ip_int, ip2_int);
    assert!(db.get_pod_network("sid1").await.unwrap().is_some());
    assert!(
        db.get_pod_network_for_pod("default", "pod1", "uid1")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db.list_pod_network_sandbox_ids()
            .await
            .unwrap()
            .contains(&"sid1".to_string())
    );
    db.delete_pod_network("sid1").await.unwrap();
    assert!(db.get_pod_network("sid1").await.unwrap().is_none());
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
    let peers = db.list_peer_subnets("node1").await.unwrap();
    assert_eq!(peers.len(), 1);
    db.delete_node_subnet("node2").await.unwrap();
    assert!(db.get_node_subnet("node2").await.unwrap().is_none());
}

parametrize_backends!(node_dataplane_metadata_round_trip, |db| {
    let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
        "node1".to_string(),
        crate::networking::wireguard::DataplaneMode::Root,
        crate::networking::wireguard::DataplaneEncryption::Enabled,
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
async fn redb_workqueue() {
    let db = redb_db().await;
    let pod = crate::pod_identity::PodIdentity::new("default", "mypod", "uid1");
    db.pod_workqueue_enqueue(
        PodWorkqueueKind::Pod,
        &pod,
        json!({"key":"val"}),
        0,
        0,
        None,
    )
    .await
    .unwrap();
    assert!(db.pod_workqueue_peek_next_due().await.unwrap().is_some());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let entry = db.pod_workqueue_claim_due(now).await.unwrap().unwrap();
    assert_eq!(entry.name, "mypod");
    assert!(db.pod_workqueue_claim_due(now).await.unwrap().is_none());
    db.pod_workqueue_record_failure(entry, 100, "test error")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let e = db.pod_workqueue_claim_due(future).await.unwrap().unwrap();
    db.pod_workqueue_complete(e.id).await.unwrap();
    let ns_pod = crate::pod_identity::PodIdentity::new("", "myns", "uid2");
    db.pod_workqueue_enqueue(PodWorkqueueKind::Namespace, &ns_pod, json!({}), 0, 0, None)
        .await
        .unwrap();
    let far = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;
    let e2 = db.pod_workqueue_claim_due(far).await.unwrap().unwrap();
    db.pod_workqueue_dead_letter(e2.id, "permanent failure")
        .await
        .unwrap();
    assert!(db.pod_workqueue_peek_next_due().await.unwrap().is_none());
}

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
