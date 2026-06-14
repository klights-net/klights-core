use super::*;
use serde_json::json;
#[tokio::test]
async fn test_mixed_watch_replay_orders_events_across_kinds() {
    let db = Datastore::new_in_memory().await.unwrap();
    let start_rv = db.get_current_resource_version().await.unwrap();

    let created_pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "pod-a",
            json!({"metadata":{"name":"pod-a","namespace":"default"}}),
        )
        .await
        .unwrap();
    let secret = db
        .create_resource(
            "v1",
            "Secret",
            Some("default"),
            "sec-a",
            json!({"metadata":{"name":"sec-a","namespace":"default"}}),
        )
        .await
        .unwrap();

    let replay = db
        .list_watch_events_since(
            &[
                WatchTarget::namespaced("v1", "Pod"),
                WatchTarget::namespaced("v1", "Secret"),
            ],
            start_rv,
        )
        .await
        .unwrap();

    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].event_type, "ADDED");
    assert_eq!(replay[0].resource.kind, "Pod");
    assert_eq!(
        replay[0].resource.resource_version,
        created_pod.resource_version
    );
    assert_eq!(replay[1].event_type, "ADDED");
    assert_eq!(replay[1].resource.kind, "Secret");
    assert_eq!(replay[1].resource.resource_version, secret.resource_version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_live_watch_events_remain_monotonic_under_concurrent_creates() {
    use tokio::time::{Duration, timeout};

    // Regression guard: create_resource previously allocated resourceVersion
    // in one DB call and wrote row/watch-event in a later DB call.
    // Under concurrency, that allowed higher RV to be committed/broadcast
    // before lower RV, which can be dropped by rv<=last_rv watch cursors.
    let db = Datastore::new_in_memory().await.unwrap();

    let db_slow = db.clone();
    let slow_data = json!({
        "metadata": {"name": "cm-slow", "namespace": "default"},
        // Large payload makes this task spend longer between RV reservation and write.
        "data": {"blob": "x".repeat(2 * 1024 * 1024)}
    });
    let slow = tokio::spawn(async move {
        db_slow
            .create_resource("v1", "ConfigMap", Some("default"), "cm-slow", slow_data)
            .await
            .unwrap()
    });

    // Give the slow task a head start so it is likely to reserve RV first.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let db_fast = db.clone();
    let fast = tokio::spawn(async move {
        db_fast
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "cm-fast",
                json!({
                    "metadata": {"name": "cm-fast", "namespace": "default"},
                    "data": {"k": "v"}
                }),
            )
            .await
            .unwrap()
    });

    timeout(Duration::from_secs(15), slow)
        .await
        .expect("slow create task timeout")
        .expect("slow create task join failed");
    timeout(Duration::from_secs(15), fast)
        .await
        .expect("fast create task timeout")
        .expect("fast create task join failed");

    let ordered_rvs: Vec<i64> = db
        .db_call("test_live_watch_events_rv_scan", |conn| {
            let mut stmt = conn.prepare(
                "SELECT resource_version FROM watch_events
                     WHERE api_version = 'v1' AND kind = 'ConfigMap'
                       AND namespace = 'default'
                       AND name IN ('cm-slow', 'cm-fast')
                     ORDER BY id ASC",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?;
            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }
            Ok(values)
        })
        .await
        .unwrap();

    assert_eq!(
        ordered_rvs.len(),
        2,
        "expected exactly two ConfigMap watch events for this test"
    );
    let first_rv = ordered_rvs[0];
    let second_rv = ordered_rvs[1];

    assert!(
        second_rv > first_rv,
        "live watch event RVs must be strictly increasing, got [{}, {}]",
        first_rv,
        second_rv
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_resources_response_rv_covers_items_when_mutation_races_with_selector_page() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        json!({
            "metadata": {
                "name": "pod-a",
                "namespace": "default",
                "labels": {"race": "snapshot"}
            }
        }),
    )
    .await
    .unwrap();

    let pause = Datastore::install_list_resources_snapshot_pause_for_test(
        "v1",
        "Pod",
        Some("default"),
        Some("race=snapshot"),
        None,
        Some(500),
        None,
    );
    let list_db = db.clone();
    let list_task = tokio::spawn(async move {
        list_db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                ResourceListQuery::new(Some("race=snapshot"), None, Some(500), None),
            )
            .await
            .unwrap()
    });

    pause.wait_for_hit().await;
    let raced = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "pod-b",
            json!({
                "metadata": {
                    "name": "pod-b",
                    "namespace": "default",
                    "labels": {"race": "snapshot"}
                }
            }),
        )
        .await
        .unwrap();
    pause.resume();

    let list = list_task.await.expect("list task panicked");
    assert!(
        list.items.iter().any(|item| item.name == raced.name),
        "test setup must include the pod created while the list was paused; got {:?}",
        list.items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        list.resource_version >= raced.resource_version,
        "list resourceVersion must be at least as new as every returned item; list rv={}, raced pod rv={}",
        list.resource_version,
        raced.resource_version
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_resources_response_rv_does_not_advance_past_concurrent_delete_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_root = dir.path().join("state");
    let cluster_db_path = db_root.join("sqlite").join("cluster.db");
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let db = Datastore::new_persistent(&db_root, supervisor.clone(), None)
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-a",
        json!({
            "metadata": {
                "name": "cm-a",
                "namespace": "default",
                "labels": {"race": "snapshot-delete"}
            },
            "data": {"k": "v"}
        }),
    )
    .await
    .unwrap();

    let pause = Datastore::install_list_resources_snapshot_after_rows_pause_for_test(
        "v1",
        "ConfigMap",
        Some("default"),
        Some("race=snapshot-delete"),
        None,
        Some(500),
        None,
    );
    let list_db = db.clone();
    let list_task = tokio::spawn(async move {
        list_db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::new(Some("race=snapshot-delete"), None, Some(500), None),
            )
            .await
            .unwrap()
    });

    tokio::time::timeout(std::time::Duration::from_secs(10), pause.wait_for_hit())
        .await
        .expect("list_resources after-rows pause was not reached");
    let delete_result = std::thread::spawn(move || -> rusqlite::Result<(i64, usize)> {
        let mut conn = rusqlite::Connection::open(cluster_db_path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE metadata SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) \
             WHERE key = 'resource_version'",
            [],
        )?;
        let delete_rv: i64 = tx.query_row(
            "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'resource_version'",
            [],
            |row| row.get(0),
        )?;
        let rows = tx.execute(
            "DELETE FROM namespaced_resources \
             WHERE api_version = 'v1' AND kind = 'ConfigMap' \
               AND namespace = 'default' AND name = 'cm-a'",
            [],
        )?;
        tx.commit()?;
        Ok((delete_rv, rows))
    })
    .join()
    .expect("raw sqlite delete thread panicked");
    pause.resume();
    let (delete_rv, deleted_rows) = delete_result.expect("raw sqlite delete failed");
    assert_eq!(deleted_rows, 1, "raw sqlite delete must remove cm-a");

    let list = list_task.await.expect("list task panicked");
    assert!(
        list.items.iter().any(|item| item.name == "cm-a"),
        "test setup must include cm-a from the row snapshot; got {:?}",
        list.items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        list.resource_version < delete_rv,
        "list contains cm-a, so its resourceVersion must precede cm-a deletion; list rv={}, delete rv={}",
        list.resource_version,
        delete_rv
    );
}

#[tokio::test]
async fn list_resources_response_rv_allows_catch_up_for_post_list_delete() {
    // A delete applied *after* a complete list must be visible to a watch that
    // resumes from the list's resourceVersion. With the list anchored to the
    // global snapshot rv, the only delete raft can produce is one stamped at a
    // strictly higher rv (raft applies committed entries in order, and rv
    // stamping is monotonic via `next_resource_version_in_tx` /
    // `advance_metadata_rv_to_at_least`). A delete stamped *below* the global
    // counter is unreachable under the raft-only cluster.db model, so this test
    // exercises the only real scenario: delete at current_rv + 1.
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-post-delete",
            json!({
                "metadata": {
                    "name": "cm-post-delete",
                    "namespace": "default",
                    "labels": {"race": "post-delete"}
                },
                "data": {"k": "v"}
            }),
        )
        .await
        .unwrap();

    // Unrelated mutations advance the global counter past the row's RV, so the
    // list rv is strictly greater than max(item rv) — the regression scenario.
    db.advance_resource_version_after(created.resource_version + 10)
        .await
        .unwrap();

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            ResourceListQuery::new(Some("race=post-delete"), None, Some(500), None),
        )
        .await
        .unwrap();
    assert!(
        list.items.iter().any(|item| item.name == created.name),
        "test setup must list cm-post-delete before the delete"
    );
    assert!(
        list.resource_version > created.resource_version,
        "list rv ({}) must exceed the row's rv ({}) for this regression scenario",
        list.resource_version,
        created.resource_version
    );

    // Raft stamps the next sequential rv: the delete lands above the list rv.
    let delete_rv = list.resource_version + 1;
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        delete_rv,
        vec![crate::log_apply::LogApplyMutation::DeleteResource(
            crate::log_apply::LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm-post-delete".to_string(),
                uid: created.uid.clone(),
                precondition_resource_version: None,
            },
        )],
    ))
    .await
    .unwrap();

    let catch_up = db
        .list_resources_modified_since("v1", "ConfigMap", Some("default"), list.resource_version)
        .await
        .unwrap();
    assert!(
        catch_up.iter().any(|event| {
            event.resource.name == "cm-post-delete" && event.event_type.as_ref() == "DELETED"
        }),
        "watch catch-up from list rv={} must include post-list delete rv={}",
        list.resource_version,
        delete_rv
    );
}

#[tokio::test]
async fn list_resources_response_rv_is_global_snapshot_not_max_item() {
    // A complete (non-paginated) list must report the global snapshot
    // resourceVersion, not max(item RV). Reporting max(item RV) under-anchors a
    // follow-up `?watch=true&resourceVersion=<list rv>`, which then replays the
    // durable event history of long-gone objects (the kubectl `-w` phantom-pod
    // artifact). Real K8s returns the collection's snapshot revision here.
    let db = Datastore::new_in_memory().await.unwrap();

    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "pod-a",
            json!({"metadata": {"name": "pod-a", "namespace": "default"}}),
        )
        .await
        .unwrap();

    // Unrelated mutations advance the global resourceVersion past the pod's RV.
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-1",
        json!({"metadata": {"name": "cm-1", "namespace": "default"}, "data": {}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-2",
        json!({"metadata": {"name": "cm-2", "namespace": "default"}, "data": {}}),
    )
    .await
    .unwrap();

    let global_rv = db.get_current_resource_version().await.unwrap();
    assert!(
        global_rv > pod.resource_version,
        "test setup must advance global rv past the pod's rv; global={global_rv}, pod={}",
        pod.resource_version
    );

    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            ResourceListQuery::new(None, None, Some(500), None),
        )
        .await
        .unwrap();
    assert!(
        list.items.iter().any(|item| item.name == "pod-a"),
        "list must contain pod-a"
    );
    assert_eq!(
        list.resource_version, global_rv,
        "complete list resourceVersion must be the global snapshot rv ({global_rv}), \
         not max(item rv) ({})",
        pod.resource_version
    );
}

#[tokio::test]
async fn test_update_hook_emits_snapshot_event_data_for_fast_create_then_update() {
    use crate::watch::EventType;
    let db = Datastore::new_in_memory().await.unwrap();

    let create_data = json!({
        "metadata": {"name": "cm-race", "namespace": "default", "uid": "uid-cm-race"},
        "data": {}
    });
    let update_data = json!({
        "metadata": {"name": "cm-race", "namespace": "default", "uid": "uid-cm-race"},
        "data": {"mutation": "1"}
    });
    let create_bytes = serde_json::to_vec(&create_data).unwrap();
    let update_bytes = serde_json::to_vec(&update_data).unwrap();

    let (created_rv, updated_rv) = db
            .db_call("test_update_hook_seed_watch_rows", move |conn| {
                let created_rv = Datastore::next_resource_version_in_conn(conn)?;
                conn.execute(
                    "INSERT INTO namespaced_resources
                     (api_version, kind, namespace, name, uid, resource_version, created_rv, data)
                     VALUES ('v1', 'ConfigMap', 'default', 'cm-race', 'uid-cm-race', ?1, ?1, ?2)",
                    rusqlite::params![created_rv, &create_bytes],
                )?;
                conn.execute(
                    "INSERT INTO watch_events
                     (api_version, kind, namespace, name, resource_version, event_type, data)
                     VALUES ('v1', 'ConfigMap', 'default', 'cm-race', ?1, 'ADDED', ?2)",
                    rusqlite::params![created_rv, &create_bytes],
                )?;

                let updated_rv = Datastore::next_resource_version_in_conn(conn)?;
                conn.execute(
                    "UPDATE namespaced_resources
                     SET resource_version = ?1, uid = 'uid-cm-race', data = ?2
                     WHERE api_version = 'v1' AND kind = 'ConfigMap' AND namespace = 'default' AND name = 'cm-race'",
                    rusqlite::params![updated_rv, &update_bytes],
                )?;
                conn.execute(
                    "INSERT INTO watch_events
                     (api_version, kind, namespace, name, resource_version, event_type, data)
                     VALUES ('v1', 'ConfigMap', 'default', 'cm-race', ?1, 'MODIFIED', ?2)",
                    rusqlite::params![updated_rv, &update_bytes],
                )?;
                Ok((created_rv, updated_rv))
            })
            .await
            .unwrap();

    let replay = db
        .list_watch_events_since(&[WatchTarget::namespaced("v1", "ConfigMap")], 0)
        .await
        .unwrap();
    assert_eq!(replay.len(), 2);

    let first = replay[0].clone().into_watch_event();
    let second = replay[1].clone().into_watch_event();

    assert_eq!(first.event_type, EventType::Added);
    assert_eq!(first.resource_version(), Some(created_rv));
    assert!(
        first
            .object
            .pointer("/data/mutation")
            .and_then(|v| v.as_str())
            .is_none(),
        "ADDED event must preserve create snapshot, not post-update data"
    );

    assert_eq!(second.event_type, EventType::Modified);
    assert_eq!(second.resource_version(), Some(updated_rv));
    assert_eq!(
        second
            .object
            .pointer("/data/mutation")
            .and_then(|v| v.as_str()),
        Some("1")
    );
}

// ========================
// Normalize/denormalize tests
// ========================

#[test]
fn test_normalize_namespace_none_becomes_empty_string() {
    assert_eq!(Datastore::normalize_namespace(&None), "");
}

#[test]
fn test_normalize_namespace_some_preserved() {
    assert_eq!(
        Datastore::normalize_namespace(&Some("kube-system".to_string())),
        "kube-system"
    );
}

#[test]
fn test_denormalize_namespace_empty_becomes_none() {
    assert_eq!(Datastore::denormalize_namespace("".to_string()), None);
}

#[test]
fn test_denormalize_namespace_nonempty_becomes_some() {
    assert_eq!(
        Datastore::denormalize_namespace("default".to_string()),
        Some("default".to_string())
    );
}

// ========================
// Resource version monotonic increase
// ========================

#[tokio::test]
async fn test_resource_versions_monotonically_increase() {
    let db = Datastore::new_in_memory().await.unwrap();

    let r1 = db
        .create_resource("v1", "Pod", None, "p1", json!({}))
        .await
        .unwrap();
    let r2 = db
        .create_resource("v1", "Pod", None, "p2", json!({}))
        .await
        .unwrap();
    let r3 = db
        .create_resource("v1", "ConfigMap", None, "cm1", json!({}))
        .await
        .unwrap();

    assert!(r2.resource_version > r1.resource_version);
    assert!(r3.resource_version > r2.resource_version);
}

// ========================
// Delete idempotency
// ========================

#[tokio::test]
async fn test_double_delete_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource("v1", "Pod", None, "pod", json!({}))
        .await
        .unwrap();

    db.delete_resource("v1", "Pod", None, "pod").await.unwrap();

    // Second delete should fail (already deleted)
    let result = db.delete_resource("v1", "Pod", None, "pod").await;
    assert!(result.is_err());
}

// ========================
// find_owned_resources across kinds
// ========================

#[tokio::test]
async fn test_find_owned_resources_across_kinds() {
    let db = Datastore::new_in_memory().await.unwrap();
    let owner_uid = "rs-uid-cross";

    // Pod owned by ReplicaSet
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod1",
        json!({
            "metadata": {
                "name": "pod1",
                "ownerReferences": [{"uid": owner_uid, "kind": "ReplicaSet"}]
            }
        }),
    )
    .await
    .unwrap();

    // ConfigMap (not owned)
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm1",
        json!({"metadata": {"name": "cm1"}}),
    )
    .await
    .unwrap();

    // Service (also owned by same UID — unusual but valid)
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "svc1",
        json!({
            "metadata": {
                "name": "svc1",
                "ownerReferences": [{"uid": owner_uid, "kind": "ReplicaSet"}]
            }
        }),
    )
    .await
    .unwrap();

    let owned = db
        .find_owned_resources(owner_uid, Some("default"))
        .await
        .unwrap();
    assert_eq!(owned.len(), 2, "Should find pod and service, not configmap");
}

#[tokio::test]
async fn test_find_owned_resources_cluster_scoped() {
    let db = Datastore::new_in_memory().await.unwrap();
    let owner_uid = "owner-cluster";

    // Cluster-scoped resource with ownerReferences
    db.create_resource(
        "v1",
        "Node",
        None,
        "node1",
        json!({
            "metadata": {
                "name": "node1",
                "ownerReferences": [{"uid": owner_uid, "kind": "Cluster"}]
            }
        }),
    )
    .await
    .unwrap();

    // Namespaced resource (should not appear for cluster-scoped query)
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod1",
        json!({
            "metadata": {
                "name": "pod1",
                "ownerReferences": [{"uid": owner_uid, "kind": "Cluster"}]
            }
        }),
    )
    .await
    .unwrap();

    // Query without namespace filter finds all
    let owned = db.find_owned_resources(owner_uid, None).await.unwrap();
    assert_eq!(owned.len(), 2);
}

#[tokio::test]
async fn test_namespace_delete_cascades() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create namespace
    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    // Create a pod in the namespace
    let created_pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            "test-pod",
            json!({"metadata": {"name": "test-pod", "namespace": "test-ns"}}),
        )
        .await
        .unwrap();

    // Verify pod exists
    let pod = db
        .get_resource("v1", "Pod", Some("test-ns"), "test-pod")
        .await
        .unwrap();
    assert!(pod.is_some());

    // Namespace hard-delete must not cascade to Pod rows. The actor-owned
    // UID finalization path removes Pods after runtime/cache cleanup.
    assert!(db.delete_namespace("test-ns").await.is_err());

    // Verify pod is preserved
    let preserved_pod = db
        .get_resource("v1", "Pod", Some("test-ns"), "test-pod")
        .await
        .unwrap();
    assert!(preserved_pod.is_some());

    db.delete_resource_with_preconditions(
        "v1",
        "Pod",
        Some("test-ns"),
        "test-pod",
        crate::datastore::ResourcePreconditions::uid(&created_pod.uid),
    )
    .await
    .unwrap();
    db.delete_namespace("test-ns").await.unwrap();
    assert!(db.get_namespace("test-ns").await.unwrap().is_none());
}

// ========================
// split_selector tests
// ========================

#[test]
fn test_split_selector_simple_equality() {
    let parts = split_selector("app=nginx");
    assert_eq!(parts, vec!["app=nginx"]);
}

#[test]
fn test_split_selector_multiple_requirements() {
    let parts = split_selector("app=nginx,version=v1");
    assert_eq!(parts, vec!["app=nginx", "version=v1"]);
}

#[test]
fn test_split_selector_preserves_parenthesized_commas() {
    // "in (a,b)" should NOT be split at the comma inside parens
    let parts = split_selector("env in (prod,staging),app=web");
    assert_eq!(parts, vec!["env in (prod,staging)", "app=web"]);
}

#[test]
fn test_split_selector_empty_string() {
    let parts = split_selector("");
    assert!(parts.is_empty());
}

#[test]
fn test_split_selector_single_exists() {
    let parts = split_selector("has-gpu");
    assert_eq!(parts, vec!["has-gpu"]);
}

#[test]
fn test_split_selector_notin_with_multiple_values() {
    let parts = split_selector("env notin (dev,test,staging)");
    assert_eq!(parts, vec!["env notin (dev,test,staging)"]);
}

// ========================
// parse_label_selector tests
// ========================

#[test]
fn test_parse_label_selector_equality() {
    let reqs = parse_label_selector("app=nginx").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        matches!(&reqs[0], LabelRequirement::Equality { key, value } if key == "app" && value == "nginx")
    );
}

#[test]
fn test_parse_label_selector_inequality() {
    let reqs = parse_label_selector("env!=prod").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        matches!(&reqs[0], LabelRequirement::Inequality { key, value } if key == "env" && value == "prod")
    );
}

#[test]
fn test_parse_label_selector_exists() {
    let reqs = parse_label_selector("has-gpu").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(matches!(&reqs[0], LabelRequirement::Exists { key } if key == "has-gpu"));
}

#[test]
fn test_parse_label_selector_not_exists() {
    let reqs = parse_label_selector("!deprecated").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(matches!(&reqs[0], LabelRequirement::NotExists { key } if key == "deprecated"));
}

#[test]
fn test_parse_label_selector_in_operator() {
    let reqs = parse_label_selector("env in (prod,staging)").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        matches!(&reqs[0], LabelRequirement::In { key, values } if key == "env" && values == &["prod", "staging"])
    );
}

#[test]
fn test_parse_label_selector_notin_operator() {
    let reqs = parse_label_selector("env notin (dev,test)").unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        matches!(&reqs[0], LabelRequirement::NotIn { key, values } if key == "env" && values == &["dev", "test"])
    );
}

#[test]
fn test_parse_label_selector_combined() {
    let reqs = parse_label_selector("app=nginx,env!=dev,!deprecated").unwrap();
    assert_eq!(reqs.len(), 3);
    assert!(
        matches!(&reqs[0], LabelRequirement::Equality { key, value } if key == "app" && value == "nginx")
    );
    assert!(
        matches!(&reqs[1], LabelRequirement::Inequality { key, value } if key == "env" && value == "dev")
    );
    assert!(matches!(&reqs[2], LabelRequirement::NotExists { key } if key == "deprecated"));
}

#[test]
fn test_parse_label_selector_empty_string() {
    let reqs = parse_label_selector("").unwrap();
    assert!(reqs.is_empty());
}

// ========================
// LabelRequirement::matches tests
// ========================

#[test]
fn test_label_requirement_equality_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx", "env": "prod"})).unwrap();
    let req = LabelRequirement::Equality {
        key: "app".to_string(),
        value: "nginx".to_string(),
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_equality_no_match() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "redis"})).unwrap();
    let req = LabelRequirement::Equality {
        key: "app".to_string(),
        value: "nginx".to_string(),
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_equality_missing_key() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "prod"})).unwrap();
    let req = LabelRequirement::Equality {
        key: "app".to_string(),
        value: "nginx".to_string(),
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_equality_no_labels() {
    let req = LabelRequirement::Equality {
        key: "app".to_string(),
        value: "nginx".to_string(),
    };
    assert!(!req.matches(None));
}

#[test]
fn test_label_requirement_inequality_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "staging"})).unwrap();
    let req = LabelRequirement::Inequality {
        key: "env".to_string(),
        value: "prod".to_string(),
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_inequality_no_match() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "prod"})).unwrap();
    let req = LabelRequirement::Inequality {
        key: "env".to_string(),
        value: "prod".to_string(),
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_inequality_missing_key_returns_true() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx"})).unwrap();
    let req = LabelRequirement::Inequality {
        key: "env".to_string(),
        value: "prod".to_string(),
    };
    // K8s spec: inequality with missing key returns true (label doesn't equal value)
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_exists_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"has-gpu": "true"})).unwrap();
    let req = LabelRequirement::Exists {
        key: "has-gpu".to_string(),
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_exists_missing_key() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx"})).unwrap();
    let req = LabelRequirement::Exists {
        key: "has-gpu".to_string(),
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_exists_no_labels() {
    let req = LabelRequirement::Exists {
        key: "app".to_string(),
    };
    assert!(!req.matches(None));
}

#[test]
fn test_label_requirement_not_exists_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx"})).unwrap();
    let req = LabelRequirement::NotExists {
        key: "deprecated".to_string(),
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_not_exists_key_present() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"deprecated": "true"})).unwrap();
    let req = LabelRequirement::NotExists {
        key: "deprecated".to_string(),
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_not_exists_no_labels() {
    let req = LabelRequirement::NotExists {
        key: "deprecated".to_string(),
    };
    // K8s spec: NotExists with no labels returns true
    assert!(req.matches(None));
}

#[test]
fn test_label_requirement_in_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "prod"})).unwrap();
    let req = LabelRequirement::In {
        key: "env".to_string(),
        values: vec!["prod".to_string(), "staging".to_string()],
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_in_no_match() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "dev"})).unwrap();
    let req = LabelRequirement::In {
        key: "env".to_string(),
        values: vec!["prod".to_string(), "staging".to_string()],
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_in_missing_key() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx"})).unwrap();
    let req = LabelRequirement::In {
        key: "env".to_string(),
        values: vec!["prod".to_string()],
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_notin_matches() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "prod"})).unwrap();
    let req = LabelRequirement::NotIn {
        key: "env".to_string(),
        values: vec!["dev".to_string(), "test".to_string()],
    };
    assert!(req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_notin_no_match() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"env": "dev"})).unwrap();
    let req = LabelRequirement::NotIn {
        key: "env".to_string(),
        values: vec!["dev".to_string(), "test".to_string()],
    };
    assert!(!req.matches(Some(&labels)));
}

#[test]
fn test_label_requirement_notin_missing_key_returns_true() {
    let labels: serde_json::Map<String, Value> =
        serde_json::from_value(json!({"app": "nginx"})).unwrap();
    let req = LabelRequirement::NotIn {
        key: "env".to_string(),
        values: vec!["dev".to_string()],
    };
    // K8s spec: NotIn with missing key returns true (label value is not in set)
    assert!(req.matches(Some(&labels)));
}

#[tokio::test]
async fn test_db_create_resource_sets_creation_timestamp() {
    let db = Datastore::new_in_memory().await.unwrap();
    // Create resource without creationTimestamp
    let data = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {}
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();

    // Verify creationTimestamp was set
    let timestamp = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("creationTimestamp"))
        .and_then(|t| t.as_str());
    assert!(timestamp.is_some(), "creationTimestamp should be set");
    assert!(
        !timestamp.unwrap().is_empty(),
        "creationTimestamp should not be empty"
    );
}

#[tokio::test]
async fn list_with_metadata_name_selector_returns_exact_resource() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();

    // 20 pods is enough to prove the metadata.name= selector picks 1 instead
    // of returning everything; the SQL pushdown logic doesn't care about N.
    for i in 0..20 {
        let name = format!("pod-{i}");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "default"},
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("metadata.name=pod-19"),
                Some(1),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(
        list.items.len(),
        1,
        "selector must return exactly one match"
    );
    assert_eq!(list.items[0].name, "pod-19");
    // Page exactly fits the single match — no continuation token, no remaining count.
    assert!(list.continue_token.is_none());
    assert_eq!(list.remaining_item_count, None);
}

#[tokio::test]
async fn list_with_metadata_name_and_residual_selector_returns_match() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();

    for i in 0..10 {
        let name = format!("pod-{i}");
        let phase = if i == 7 { "Pending" } else { "Running" };
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": name, "namespace": "default"},
                "status": {"phase": phase}
            }),
        )
        .await
        .unwrap();
    }

    // metadata.name pushed to SQL; status.phase=Running stays as residual,
    // matched in Rust. pod-7 (Pending) should be filtered out.
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("metadata.name=pod-7,status.phase=Running"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        list.items.len(),
        0,
        "residual filter must reject Pending pod"
    );

    let list_match = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("metadata.name=pod-3,status.phase=Running"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(list_match.items.len(), 1);
    assert_eq!(list_match.items[0].name, "pod-3");
}

// ========================
// Selector + limit page-bounded decoding tests
// ========================

/// Helper: create N pods with a given label in "default" namespace.
async fn seed_pods_with_label(db: &Datastore, count: usize, label_key: &str, label_val: &str) {
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();
    for i in 0..count {
        let name = format!("pod-{i:04}");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": &name,
                    "namespace": "default",
                    "labels": { label_key: label_val }
                },
                "spec": { "nodeName": "node-a" },
                "status": { "phase": "Running" }
            }),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn test_label_selector_limit_returns_correct_page() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 10, "app", "nginx").await;

    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(2), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 2, "page size must equal limit");
    assert!(
        list.continue_token.is_some(),
        "continue_token must be set when more items exist"
    );
    // Selector-limited queries omit exact remainingItemCount
    assert_eq!(
        list.remaining_item_count, None,
        "remaining_item_count must be None for selector queries"
    );
    // Items must be sorted by name
    assert_eq!(list.items[0].name, "pod-0000");
    assert_eq!(list.items[1].name, "pod-0001");
}

#[tokio::test]
async fn test_label_selector_limit_pagination_with_continue_token() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 10, "app", "nginx").await;

    // Page 1
    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(2), None),
        )
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    let token1 = page1.continue_token.clone().unwrap();

    // Page 2
    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("app=nginx"),
                None,
                Some(2),
                Some(&token1),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].name, "pod-0002");
    assert_eq!(page2.items[1].name, "pod-0003");
    let token2 = page2.continue_token.clone().unwrap();

    // Continue until exhausted
    let mut remaining = 10 - 4; // 6 left
    let mut token = token2;
    while remaining > 0 {
        let page = db
            .list_resources(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("app=nginx"),
                    None,
                    Some(2),
                    Some(&token),
                ),
            )
            .await
            .unwrap();
        let expected_len = remaining.min(2) as usize;
        assert_eq!(page.items.len(), expected_len);
        remaining -= page.items.len() as i64;
        if remaining > 0 {
            assert!(page.continue_token.is_some());
            token = page.continue_token.unwrap();
        } else {
            assert!(page.continue_token.is_none());
        }
    }
    assert_eq!(remaining, 0, "all items must be consumed");
}

#[tokio::test]
async fn test_field_selector_limit_returns_correct_page() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 10, "app", "nginx").await;

    // spec.nodeName is an indexed field — should be pushed to SQL
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.nodeName=node-a"),
                Some(2),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 2, "page size must equal limit");
    assert!(list.continue_token.is_some());
    assert_eq!(list.remaining_item_count, None);
    assert_eq!(list.items[0].name, "pod-0000");
    assert_eq!(list.items[1].name, "pod-0001");
}

#[tokio::test]
async fn test_field_selector_limit_pagination_with_continue_token() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 10, "app", "nginx").await;

    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.nodeName=node-a"),
                Some(2),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    let token = page1.continue_token.clone().unwrap();

    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.nodeName=node-a"),
                Some(2),
                Some(&token),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert_eq!(page2.items[0].name, "pod-0002");
    assert_eq!(page2.items[1].name, "pod-0003");
}

#[tokio::test]
async fn test_label_selector_limit_exact_fit_no_continue() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 3, "app", "nginx").await;

    // limit=3 with 3 matching items → no continue token
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(3), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(
        list.continue_token.is_none(),
        "no continue token when all items fit in one page"
    );
    assert_eq!(list.remaining_item_count, None);
}

#[tokio::test]
async fn test_label_selector_limit_exceeds_total() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_pods_with_label(&db, 3, "app", "nginx").await;

    // limit=10 with only 3 matching items
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(10), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(list.continue_token.is_none());
}

// ========================
// Residual selector cursor batching tests (Task 1)
// ========================

/// Helper: create N ConfigMaps where only the last one has `data.match=yes`.
/// ConfigMap has no indexed fields, so any field selector is residual.
async fn seed_configmaps_with_residual_match(db: &Datastore, count: usize) {
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();
    for i in 0..count {
        let name = format!("cm-{i:04}");
        let data = if i == count - 1 {
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": &name, "namespace": "default" },
                "data": { "match": "yes" }
            })
        } else {
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": &name, "namespace": "default" },
                "data": { "match": "no" }
            })
        };
        db.create_resource("v1", "ConfigMap", Some("default"), &name, data)
            .await
            .unwrap();
    }
}

/// Regression: residual selector pagination must not silently drop matches
/// beyond the first candidate window.
///
/// Seeds 100 ConfigMaps where only the last one (cm-0099) matches
/// `fieldSelector=data.match=yes`. With `limit=1`, the first candidate
/// batch is only 64 rows; the matching item sits beyond that window.
/// Before the cursor-batching fix, the list returns empty with no
/// continue token.
#[tokio::test]
async fn residual_selector_limit_does_not_drop_match_after_candidate_window() {
    let db = Datastore::new_in_memory().await.unwrap();
    let total = 100;
    seed_configmaps_with_residual_match(&db, total).await;

    // Walk pages until we find the matching item or exhaust the list.
    let mut found = false;
    let mut token: Option<String> = None;
    for _ in 0..total {
        let page = db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    None,
                    Some("data.match=yes"),
                    Some(1),
                    token.as_deref(),
                ),
            )
            .await
            .unwrap();

        if !page.items.is_empty() {
            assert_eq!(
                page.items[0].name, "cm-0099",
                "only the last configmap should match"
            );
            found = true;
            break;
        }
        // Empty page but continue_token means more candidates may exist.
        assert!(
            page.continue_token.is_some(),
            "empty page must carry a continue token when candidates remain"
        );
        token = page.continue_token;
    }
    assert!(
        found,
        "the late-matching configmap must be reachable via pagination"
    );
}

/// Selector list queries must always set remainingItemCount to None
/// because computing exact counts would require scanning all candidates.
#[tokio::test]
async fn residual_selector_pagination_omits_remaining_item_count() {
    let db = Datastore::new_in_memory().await.unwrap();
    seed_configmaps_with_residual_match(&db, 10).await;

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, Some("data.match=yes"), Some(5), None),
        )
        .await
        .unwrap();

    assert_eq!(
        list.remaining_item_count, None,
        "selector queries must not set remainingItemCount"
    );
}

/// Residual selector scanning must stop as soon as limit+1 matches are
/// collected — no full-table scan should happen when results are found
/// early in the candidate stream.
#[tokio::test]
async fn residual_selector_stops_after_limit_plus_one_matches() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("default", json!({"metadata": {"name": "default"}}))
        .await
        .unwrap();

    // Create 20 ConfigMaps all matching the residual field selector.
    for i in 0..20 {
        let name = format!("cm-{i:04}");
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": &name, "namespace": "default" },
                "data": { "match": "yes" }
            }),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, Some("data.match=yes"), Some(3), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(
        list.continue_token.is_some(),
        "more matches exist beyond limit, continue token must be set"
    );
    // Verify correct items
    assert_eq!(list.items[0].name, "cm-0000");
    assert_eq!(list.items[1].name, "cm-0001");
    assert_eq!(list.items[2].name, "cm-0002");
    // remainingItemCount must be None for selector queries
    assert_eq!(list.remaining_item_count, None);
}
