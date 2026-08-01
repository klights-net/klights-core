#[cfg(test)]
mod tests {
    use crate::sqlite::embedded::Datastore;
    use klights_cluster_store::{
        ResourceCollectionScope, ResourceListRead, ResourceListRequest, ResourceVersionMatch,
    };
    use klights_cluster_store::{ResourceList, ResourceListOptions, SnapshotAtRv};
    use serde_json::json;

    async fn put(db: &Datastore, name: &str, val: &str) -> i64 {
        let r = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": name, "namespace": "default"},
                    "data": {"k": val}
                }),
            )
            .await
            .unwrap();
        r.resource_version
    }

    fn sorted_names(list: &ResourceList) -> Vec<String> {
        let mut v: Vec<String> = list.items.iter().map(|r| r.name.clone()).collect();
        v.sort();
        v
    }

    async fn put_in_namespace(db: &Datastore, namespace: &str, name: &str) -> i64 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": namespace}
            }),
        )
        .await
        .unwrap()
        .resource_version
    }

    fn lower_identities(read: &ResourceListRead) -> Vec<(Option<String>, String)> {
        read.items()
            .iter()
            .map(|resource| (resource.namespace.clone(), resource.name.clone()))
            .collect()
    }

    #[tokio::test]
    async fn all_namespace_current_and_exact_use_composite_namespace_name_pages() {
        let db = Datastore::new_in_memory().await.unwrap();
        put_in_namespace(&db, "ns-a", "a").await;
        put_in_namespace(&db, "ns-a", "same").await;
        put_in_namespace(&db, "ns-b", "same").await;
        let exact_rv = put_in_namespace(&db, "ns-z", "z").await;

        // Force Exact down the historical reconstruction path without changing
        // the target collection.
        db.create_resource(
            "v1",
            "Secret",
            Some("ns-a"),
            "later",
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "later", "namespace": "ns-a"}
            }),
        )
        .await
        .unwrap();

        let expected = vec![
            (Some("ns-a".to_string()), "a".to_string()),
            (Some("ns-a".to_string()), "same".to_string()),
            (Some("ns-b".to_string()), "same".to_string()),
            (Some("ns-z".to_string()), "z".to_string()),
        ];

        let store = db.focused_read_store();
        for mode in [
            ResourceVersionMatch::Any,
            ResourceVersionMatch::Exact(exact_rv),
        ] {
            let first = klights_cluster_store::ClusterResourceRead::list_resources(
                store.as_ref(),
                ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::AllNamespaces,
                    klights_cluster_store::ResourceListQuery::try_new(
                        None,
                        None,
                        Some(2),
                        None,
                        mode,
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(lower_identities(&first), expected[..2]);
            let continuation = first
                .continuation()
                .cloned()
                .expect("first page has a composite continuation");
            assert_eq!(continuation.after().namespace(), Some("ns-a"));
            assert_eq!(continuation.after().name(), "same");
            let second = klights_cluster_store::ClusterResourceRead::list_resources(
                store.as_ref(),
                ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::AllNamespaces,
                    klights_cluster_store::ResourceListQuery::try_new(
                        None,
                        None,
                        Some(2),
                        Some(continuation),
                        mode,
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(lower_identities(&second), expected[2..]);
        }
    }

    #[tokio::test]
    async fn snapshot_reconstructs_state_at_past_rv() {
        let db = Datastore::new_in_memory().await.unwrap();
        put(&db, "a", "old").await;
        let rb = put(&db, "b", "bee").await; // snapshot point: {a:old, b:bee}

        // Mutations after the snapshot point must not leak into the snapshot.
        let cur_a = db
            .get_resource("v1", "ConfigMap", Some("default"), "a")
            .await
            .unwrap()
            .unwrap();
        db.update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "a",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "a", "namespace": "default"},
                "data": {"k": "new"}
            }),
            cur_a.resource_version,
        )
        .await
        .unwrap();
        db.delete_resource("v1", "ConfigMap", Some("default"), "b")
            .await
            .unwrap();
        put(&db, "c", "see").await;

        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListOptions::all(),
                rb,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string(), "b".to_string()],
            "snapshot at rb must contain a (deleted-after view) and b, not c"
        );
        assert_eq!(list.resource_version, rb);
        let a = list.items.iter().find(|r| r.name == "a").unwrap();
        assert_eq!(
            a.data.pointer("/data/k").and_then(|v| v.as_str()),
            Some("old"),
            "a must show its pre-update value at the snapshot rv"
        );
    }

    #[tokio::test]
    async fn snapshot_at_or_after_current_defers_to_live() {
        let db = Datastore::new_in_memory().await.unwrap();
        put(&db, "a", "x").await;
        let cur = db.get_current_resource_version().await.unwrap();
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListOptions::all(),
                cur,
            )
            .await
            .unwrap();
        assert!(matches!(snap, SnapshotAtRv::Current));
    }

    #[tokio::test]
    async fn snapshot_below_retained_window_is_expired() {
        let db = Datastore::new_in_memory().await.unwrap();
        let ra = put(&db, "a", "x").await;
        for i in 0..5 {
            put(&db, &format!("p{i}"), "y").await;
        }
        // Prune the window to the single most recent event so `ra` drops out.
        db.gc_watch_events(1, 1000).await.unwrap();
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListOptions::all(),
                ra,
            )
            .await
            .unwrap();
        assert!(
            matches!(snap, SnapshotAtRv::Expired),
            "an rv below the retained window must be Expired"
        );
    }

    #[tokio::test]
    async fn snapshot_applies_selectors_and_pagination() {
        let db = Datastore::new_in_memory().await.unwrap();
        for name in ["a", "b", "c"] {
            put(&db, name, "v").await;
        }
        let rv = put(&db, "d", "v").await; // snapshot over {a,b,c,d}
        put(&db, "e", "v").await; // after snapshot — excluded

        // Page 1: limit 2 over the historical set.
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListOptions::new(None, None, Some(2), None),
                rv,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(sorted_names(&list), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(list.continue_token.as_deref(), Some("b"));
        assert_eq!(list.remaining_item_count, Some(2));

        // Page 2: continue after "b".
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListOptions::new(None, None, Some(2), Some("b")),
                rv,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["c".to_string(), "d".to_string()],
            "page 2 must contain c,d from the historical set (not the later e)"
        );
        assert_eq!(list.continue_token, None);
    }

    async fn put_ns(db: &Datastore, name: &str, label: &str) -> i64 {
        let r = db
            .create_namespace(
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": name, "labels": {"k": label}}
                }),
            )
            .await
            .unwrap();
        r.resource_version
    }

    /// Namespaces persist in their own table (no created_rv column), so their
    /// snapshot reconstruction must read that table for live rows and derive
    /// existence-at-N from watch_events history. This mirrors
    /// `snapshot_reconstructs_state_at_past_rv` for the Namespace kind.
    #[tokio::test]
    async fn snapshot_reconstructs_namespace_state_at_past_rv() {
        let db = Datastore::new_in_memory().await.unwrap();
        put_ns(&db, "a", "old").await;
        let rb = put_ns(&db, "b", "bee").await; // snapshot point: {a:old, b}

        // Mutations after the snapshot point must not leak into the snapshot.
        let cur_a = db.get_namespace("a").await.unwrap().unwrap();
        db.update_namespace(
            "a",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "a", "labels": {"k": "new"}}
            }),
            cur_a.resource_version,
        )
        .await
        .unwrap();
        db.delete_namespace("b").await.unwrap();
        put_ns(&db, "c", "see").await;

        let snap = db
            .snapshot_resources_at_rv("v1", "Namespace", None, ResourceListOptions::all(), rb)
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string(), "b".to_string()],
            "namespace snapshot at rb must contain a and b, not the later c"
        );
        assert_eq!(list.resource_version, rb);
        let a = list.items.iter().find(|r| r.name == "a").unwrap();
        assert_eq!(
            a.data
                .pointer("/metadata/labels/k")
                .and_then(|v| v.as_str()),
            Some("old"),
            "namespace a must show its pre-update value at the snapshot rv"
        );
    }

    /// A namespace created entirely after the snapshot rv must be absent (not
    /// erroneously treated as expired) even though the namespaces table has no
    /// created_rv column — the earliest-retained ADDED event proves it was born
    /// after N.
    #[tokio::test]
    async fn snapshot_namespace_created_after_rv_is_absent() {
        let db = Datastore::new_in_memory().await.unwrap();
        let rb = put_ns(&db, "a", "old").await; // snapshot point: {a}
        put_ns(&db, "z", "new").await; // created after the snapshot

        let snap = db
            .snapshot_resources_at_rv("v1", "Namespace", None, ResourceListOptions::all(), rb)
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string()],
            "namespace z created after N must be absent from the snapshot, not expired"
        );
    }
}
