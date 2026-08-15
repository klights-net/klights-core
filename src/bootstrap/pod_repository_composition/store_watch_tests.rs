#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn pod_store_round_trips_create_get_list_update_delete() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;

        // create
        let created = store
            .persistence
            .seed_pod(
                "default",
                "p1",
                make_pod("p1", Some("owner-a"), Some(("app", "x"))),
            )
            .await
            .unwrap();
        assert_eq!(created.name, "p1");
        assert_eq!(created.kind, "Pod");

        // get
        let fetched = store
            .query
            .get_pod_by_name("default", "p1")
            .await
            .unwrap()
            .expect("p1 present");
        assert_eq!(fetched.name, "p1");
        assert_eq!(fetched.namespace.as_deref(), Some("default"));

        // additional pods to make list/list_by_owner non-trivial
        store
            .persistence
            .seed_pod(
                "default",
                "p2",
                make_pod("p2", Some("owner-a"), Some(("app", "y"))),
            )
            .await
            .unwrap();
        store
            .persistence
            .seed_pod(
                "default",
                "p3",
                make_pod("p3", Some("owner-b"), Some(("app", "x"))),
            )
            .await
            .unwrap();

        // list (namespaced, no selector)
        let all = store
            .query
            .list_pods_filtered(Some("default"), None)
            .await
            .unwrap();
        assert_eq!(all.items().len(), 3);

        // list (label selector) — must match exactly the pods carrying app=x
        let by_label = store
            .query
            .list_pods_filtered(Some("default"), Some("app=x"))
            .await
            .unwrap();
        let mut names: Vec<String> = by_label.items().iter().map(|r| r.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["p1".to_string(), "p3".to_string()]);

        // list_by_owner
        let owned = store
            .query
            .list_pods_by_owner("default", "owner-a")
            .await
            .unwrap();
        let mut owned_names: Vec<String> = owned.iter().map(|r| r.name.clone()).collect();
        owned_names.sort();
        assert_eq!(owned_names, vec!["p1".to_string(), "p2".to_string()]);

        // update: full-object update with CAS pass
        let mut body: serde_json::Value = (*fetched.data).clone();
        body["metadata"]["labels"] = json!({"app": "x", "tier": "frontend"});
        let updated = store
            .persistence
            .replace_pod("default", "p1", body, fetched.resource_version)
            .await
            .unwrap();
        assert!(updated.resource_version > fetched.resource_version);
        assert_eq!(
            updated.data["metadata"]["labels"]["tier"],
            json!("frontend")
        );

        // update: CAS fail (using the now-stale resource_version with mutated
        // data so the dedupe-on-identical-data fast path doesn't swallow it).
        let mut stale_body: serde_json::Value = (*updated.data).clone();
        stale_body["metadata"]["labels"] = json!({"app": "x", "tier": "stale"});
        let conflict = store
            .persistence
            .replace_pod("default", "p1", stale_body, fetched.resource_version)
            .await;
        let err = conflict.expect_err("stale rv must produce a conflict");
        assert!(
            err.to_string().contains("409"),
            "expected 409 Conflict, got {err:?}"
        );

        // update_status: stale RV returns Conflict
        let stale_status_conflict = store
            .update_pod_status(
                "default",
                "p1",
                json!({"phase": "Running"}),
                Some(fetched.resource_version),
            )
            .await;
        let err = stale_status_conflict.expect_err("stale rv on status must conflict");
        assert!(
            err.to_string().contains("409"),
            "expected 409 Conflict on status update, got {err:?}"
        );

        // update_status: CAS pass with the live RV — read-modify-write
        let current = store
            .query
            .get_pod_by_name("default", "p1")
            .await
            .unwrap()
            .unwrap();
        let after_status = store
            .update_pod_status(
                "default",
                "p1",
                json!({"phase": "Running"}),
                Some(current.resource_version),
            )
            .await
            .unwrap();
        assert_eq!(after_status.data["status"]["phase"], json!("Running"));
    }
    #[tokio::test]
    async fn delete_unscheduled_removes_terminating_unscheduled_pod() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;
        let created = store
            .persistence
            .seed_pod("default", "u1", make_pod("u1", None, None))
            .await
            .unwrap();
        let marked = store
            .mark_pod_deleting_for_uid("default", "u1", &created.uid, &delete_mark_body())
            .await
            .unwrap();

        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default",
                "u1",
                &created.uid,
                marked.resource_version,
            )
            .await
            .unwrap();

        assert_eq!(outcome, UnscheduledPodDeletionOutcome::Removed);
        assert!(
            store
                .query
                .get_pod_by_name("default", "u1")
                .await
                .unwrap()
                .is_none(),
            "unscheduled terminating Pod row must be removed"
        );
    }
    #[tokio::test]
    async fn delete_unscheduled_defers_when_kubelet_picked_pod_up() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;
        let mut pod = make_pod("s1", None, None);
        pod["spec"]["nodeName"] = json!("node-a");
        let created = store
            .persistence
            .seed_pod("default", "s1", pod)
            .await
            .unwrap();
        let marked = store
            .mark_pod_deleting_for_uid("default", "s1", &created.uid, &delete_mark_body())
            .await
            .unwrap();

        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default",
                "s1",
                &created.uid,
                marked.resource_version,
            )
            .await
            .unwrap();

        assert_eq!(outcome, UnscheduledPodDeletionOutcome::DeferToActor);
        assert!(
            store
                .query
                .get_pod_by_name("default", "s1")
                .await
                .unwrap()
                .is_some(),
            "a Pod bound to a node must only be removed by the actor"
        );
    }
    #[tokio::test]
    async fn delete_unscheduled_waits_for_finalizers() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;
        let mut pod = make_pod("f1", None, None);
        pod["metadata"]["finalizers"] = json!(["example.com/hold"]);
        let created = store
            .persistence
            .seed_pod("default", "f1", pod)
            .await
            .unwrap();
        let marked = store
            .mark_pod_deleting_for_uid("default", "f1", &created.uid, &delete_mark_body())
            .await
            .unwrap();

        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default",
                "f1",
                &created.uid,
                marked.resource_version,
            )
            .await
            .unwrap();

        assert_eq!(outcome, UnscheduledPodDeletionOutcome::FinalizersPending);
        assert!(
            store
                .query
                .get_pod_by_name("default", "f1")
                .await
                .unwrap()
                .is_some()
        );
    }
    #[tokio::test]
    async fn delete_unscheduled_refuses_non_terminating_pod() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;
        let created = store
            .persistence
            .seed_pod("default", "live1", make_pod("live1", None, None))
            .await
            .unwrap();

        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default",
                "live1",
                &created.uid,
                created.resource_version,
            )
            .await
            .unwrap();

        assert_eq!(outcome, UnscheduledPodDeletionOutcome::Retry);
        assert!(
            store
                .query
                .get_pod_by_name("default", "live1")
                .await
                .unwrap()
                .is_some(),
            "a non-terminating Pod must never be hard-deleted"
        );
    }
    #[tokio::test]
    async fn delete_unscheduled_is_idempotent_for_missing_or_replaced_uid() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;

        // Missing Pod — nothing to remove.
        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default", "ghost", "uid-x", 1,
            )
            .await
            .unwrap();
        assert_eq!(outcome, UnscheduledPodDeletionOutcome::Removed);

        // A same-name replacement Pod owns the slot: our (old) UID is already gone.
        let created = store
            .persistence
            .seed_pod("default", "r1", make_pod("r1", None, None))
            .await
            .unwrap();
        let marked = store
            .mark_pod_deleting_for_uid("default", "r1", &created.uid, &delete_mark_body())
            .await
            .unwrap();
        let outcome = store
            .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                "default",
                "r1",
                "stale-uid",
                marked.resource_version,
            )
            .await
            .unwrap();
        assert_eq!(outcome, UnscheduledPodDeletionOutcome::Removed);
        assert!(
            store
                .query
                .get_pod_by_name("default", "r1")
                .await
                .unwrap()
                .is_some(),
            "the live replacement Pod must be preserved"
        );
    }
    #[tokio::test]
    async fn pod_reader_get_pod_returns_existing_pod() {
        let repo = build_store_watch_repo().await;
        repo.persistence
            .seed_pod("default", "p1", make_pod("p1", None, None))
            .await
            .unwrap();
        let got = repo.query.get_pod_by_name("default", "p1").await.unwrap();
        let pod = got.expect("pod present");
        assert_eq!(pod.name, "p1");
        assert_eq!(pod.namespace.as_deref(), Some("default"));
        assert!(
            repo.query
                .get_pod_by_name("default", "missing")
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn pod_reader_list_pods_paginates_via_limit_and_continue_token() {
        let repo = build_store_watch_repo().await;
        for i in 0..3 {
            repo.persistence
                .seed_pod(
                    "default",
                    &format!("p{i}"),
                    make_pod(&format!("p{i}"), None, None),
                )
                .await
                .unwrap();
        }
        let page1 = repo
            .query
            .list_pods_exact(Some("default"), None, None, Some(1), None)
            .await
            .unwrap();
        assert_eq!(page1.items().len(), 1);
        let cont = page1
            .continue_token()
            .expect("continue token must be set when more pages remain");
        let page2 = repo
            .query
            .list_pods_exact(Some("default"), None, None, Some(1), Some(cont))
            .await
            .unwrap();
        assert_eq!(page2.items().len(), 1);
        assert_ne!(page1.items()[0].name, page2.items()[0].name);
    }
    #[tokio::test]
    async fn cluster_backed_pod_reader_list_pods_uses_fresh_leader_list() {
        let pod = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("refresh-ns".to_string()),
            name: "mounted-pod".to_string(),
            uid: "mounted-pod-uid".to_string(),
            resource_version: 22,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "refresh-ns",
                    "name": "mounted-pod",
                    "uid": "mounted-pod-uid",
                    "resourceVersion": "22"
                },
                "spec": {
                    "nodeName": "node-a",
                    "containers": [{"name": "app", "image": "busybox"}]
                },
                "status": {"phase": "Running"}
            })),
        };
        let repo = IntegrationPodStoreWatchFixture::new_cluster_backed(Arc::new(
            FakeLeaderApiClient::new(pod.clone())
                .with_cached_list_items(Vec::new())
                .with_fresh_list_items(vec![pod.clone()]),
        ))
        .await;

        let listed = repo
            .query
            .list_pods_exact(Some("refresh-ns"), None, None, None, None)
            .await
            .expect("cluster-backed pod list should succeed");

        assert_eq!(
            listed
                .items()
                .iter()
                .map(|pod| pod.name.as_str())
                .collect::<Vec<_>>(),
            vec!["mounted-pod"],
            "volume refresh and lifecycle decisions must not use a stale ready pod-list cache"
        );
    }
    #[tokio::test]
    async fn pod_reader_list_pods_by_owner_uid_filters_by_controller_owner() {
        let repo = build_store_watch_repo().await;
        repo.persistence
            .seed_pod("default", "a1", make_pod("a1", Some("owner-a"), None))
            .await
            .unwrap();
        repo.persistence
            .seed_pod("default", "a2", make_pod("a2", Some("owner-a"), None))
            .await
            .unwrap();
        repo.persistence
            .seed_pod("default", "b1", make_pod("b1", Some("owner-b"), None))
            .await
            .unwrap();
        let owned_a = repo
            .query
            .list_pods_by_owner("default", "owner-a")
            .await
            .unwrap();
        let mut names: Vec<String> = owned_a.iter().map(|r| r.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["a1".to_string(), "a2".to_string()]);
    }
    #[tokio::test]
    async fn pod_watch_source_receives_added_on_create() {
        let repo = IntegrationPodStoreWatchFixture::new_inline().await;

        let mut rx = repo.watch.subscribe();
        repo.persistence
            .seed_pod("default", "watched", make_pod("watched", None, None))
            .await
            .unwrap();
        let evt = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("watch event must arrive within 2s")
            .expect("watch channel must not lag/close");
        assert_eq!(evt.event_type, klights_watch::EventType::Added);
        let object = evt.object.as_ref();
        assert_eq!(object["kind"], serde_json::json!("Pod"));
        assert_eq!(object["metadata"]["name"], serde_json::json!("watched"));
    }
}
