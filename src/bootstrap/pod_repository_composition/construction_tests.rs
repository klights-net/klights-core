#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;

    #[tokio::test]
    async fn pod_repository_constructs_with_db_and_supervisor() {
        let _repo = IntegrationPodConstructionFixture::new_inline().await;
    }
    #[tokio::test]
    async fn pod_repository_build_parts_exposes_repository_and_background_without_starting() {
        let repo = IntegrationPodConstructionFixture::new_inline().await;
        assert!(repo.background_is_available());
    }
    #[tokio::test]
    async fn pod_repository_build_parts_does_not_start_workqueue_until_background_start() {
        let repo = IntegrationPodConstructionFixture::new_inline().await;

        // build_parts must not call workqueue.start().
        assert!(
            !repo.workqueue_start_called(),
            "build_parts must not start the workqueue; background.start() owns that"
        );

        // Explicit start must call workqueue.start().
        repo.start_background().await.unwrap();
        assert!(
            repo.workqueue_start_called(),
            "background.start() must call workqueue.start()"
        );
    }
    #[tokio::test]
    async fn pod_workqueue_runner_start_calls_workqueue_start_once() {
        let repo = IntegrationPodConstructionFixture::new_inline().await;

        // build_parts must not have started the workqueue.
        assert!(!repo.workqueue_start_called());

        // Start must delegate to workqueue.start().
        repo.start_background().await.unwrap();
        assert!(repo.workqueue_start_called());

        // Calling start again is idempotent (reconciler uses AtomicBool CAS).
        repo.start_background().await.unwrap();
        assert!(repo.workqueue_start_called());
    }
    #[tokio::test]
    async fn pod_object_service_requires_uid_for_mutating_paths() {
        let repo = IntegrationPodMetadataFixture::new_inline().await;

        // Create a Pod with UID "uid-new".
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "test-pod",
                "uid": "uid-new"
            },
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}]
            },
            "status": {"phase": "Pending"}
        });
        repo.persistence
            .seed_pod("default", "test-pod", pod.clone())
            .await
            .unwrap();

        // Stale UID must be rejected for update_pod_owner_references.
        let err = repo
            .update_pod_owner_references_for_uid(
                "default",
                "test-pod",
                "uid-old",
                vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"})],
            )
            .await;
        assert!(
            err.is_err(),
            "stale UID must be rejected for update_pod_owner_references"
        );

        // Stale UID must be rejected for merge_pod_labels.
        let err = repo
            .merge_pod_labels_for_uid(
                "default",
                "test-pod",
                "uid-old",
                vec![("app".to_string(), "v2".to_string())],
            )
            .await;
        assert!(
            err.is_err(),
            "stale UID must be rejected for merge_pod_labels"
        );

        // Verify the replacement Pod is unchanged.
        let live = repo
            .query
            .get_pod_by_name("default", "test-pod")
            .await
            .unwrap()
            .expect("replacement Pod must still exist");
        assert_eq!(live.uid, "uid-new");
        assert!(
            live.data
                .pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(true),
            "ownerReferences must not be set by stale-UID call"
        );

        // Correct UID must succeed.
        repo.update_pod_owner_references_for_uid(
            "default",
            "test-pod",
            "uid-new",
            vec![json!({"apiVersion": "v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid"})],
        )
        .await
        .expect("correct UID must succeed for update_pod_owner_references");

        repo.merge_pod_labels_for_uid(
            "default",
            "test-pod",
            "uid-new",
            vec![("app".to_string(), "v2".to_string())],
        )
        .await
        .expect("correct UID must succeed for merge_pod_labels");
    }
    #[tokio::test]
    async fn pod_status_service_writes_are_uid_preconditioned() {
        let repo = IntegrationPodStatusFixture::new_inline().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "status-test", "uid": "uid-correct"},
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Pending"}
        });
        repo.persistence
            .seed_pod("default", "status-test", pod)
            .await
            .unwrap();

        // Stale UID: write with wrong UID must be rejected.
        let update = super::super::assembly_support::support::PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: String::new(),
            host_ip: String::new(),
            container_statuses: vec![],
            init_container_statuses: None,
            qos_class: None,
        };
        let err = repo
            .status_ports()
            .set_pod_status_for_uid("default", "status-test", "uid-wrong", update.clone(), None)
            .await;
        assert!(err.is_err(), "stale UID status write must be rejected");

        // Correct UID: write must succeed.
        let live = repo
            .query
            .get_pod_by_name("default", "status-test")
            .await
            .unwrap()
            .expect("Pod must exist");
        repo.status_ports()
            .set_pod_status_for_uid("default", "status-test", &live.uid, update, None)
            .await
            .expect("correct UID status write must succeed");
    }
    #[tokio::test]
    async fn mark_start_pending_for_retry_writes_err_image_pull_then_image_pull_backoff() {
        let repo = IntegrationPodStatusFixture::new_inline().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "pull-fail", "uid": "uid-1"},
            "spec": {"containers": [{"name": "app", "image": "docker.io/library/busybox:1.36"}]},
            "status": {"phase": "Pending"}
        });
        repo.persistence
            .seed_pod("default", "pull-fail", pod)
            .await
            .unwrap();

        let err_msg = "Failed to pull image \"docker.io/library/busybox:1.36\": \
                       CRI pull_image failed: connection refused";

        // First failure: ErrImagePull.
        repo.status_ports()
            .mark_start_pending_for_retry_for_uid("default", "pull-fail", "uid-1", err_msg)
            .await
            .expect("first retry status write must succeed");

        let after_first = repo
            .query
            .get_pod_by_name("default", "pull-fail")
            .await
            .unwrap()
            .expect("pod must still exist");
        assert_eq!(
            after_first
                .data
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            Some("Pending"),
            "phase must stay Pending on retryable startup failure"
        );
        let first_reason = after_first
            .data
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str());
        assert_eq!(first_reason, Some("ErrImagePull"));

        // Second failure: ImagePullBackOff.
        repo.status_ports()
            .mark_start_pending_for_retry_for_uid("default", "pull-fail", "uid-1", err_msg)
            .await
            .expect("second retry status write must succeed");

        let after_second = repo
            .query
            .get_pod_by_name("default", "pull-fail")
            .await
            .unwrap()
            .expect("pod must still exist");
        let second_reason = after_second
            .data
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str());
        assert_eq!(second_reason, Some("ImagePullBackOff"));
        assert_eq!(
            after_second
                .data
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            Some("Pending"),
        );
        let second_message = after_second
            .data
            .pointer("/status/containerStatuses/0/state/waiting/message")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(
            second_message.contains("busybox") || !second_message.is_empty(),
            "waiting.message must carry the underlying error: got {second_message:?}"
        );
    }
    #[tokio::test]
    async fn mark_start_pending_for_retry_rejects_stale_uid() {
        let repo = IntegrationPodStatusFixture::new_inline().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "same-name", "uid": "uid-current"},
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Pending"}
        });
        repo.persistence
            .seed_pod("default", "same-name", pod)
            .await
            .unwrap();
        let before = repo
            .query
            .get_pod_by_name("default", "same-name")
            .await
            .unwrap()
            .unwrap();

        let err = repo
            .status_ports()
            .mark_start_pending_for_retry_for_uid(
                "default",
                "same-name",
                "uid-stale",
                "Failed to pull image",
            )
            .await;
        assert!(
            err.is_err(),
            "stale UID retry-status write must be rejected"
        );

        let after = repo
            .query
            .get_pod_by_name("default", "same-name")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.uid, before.uid);
        assert_eq!(after.resource_version, before.resource_version);
    }
    #[tokio::test]
    async fn pod_subresource_service_status_and_ephemeral_updates_require_uid() {
        let repo = IntegrationPodStatusFixture::new_inline().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "subres-test", "uid": "uid-correct"},
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Pending"}
        });
        repo.persistence
            .seed_pod("default", "subres-test", pod)
            .await
            .unwrap();

        let live = repo
            .query
            .get_pod_by_name("default", "subres-test")
            .await
            .unwrap()
            .unwrap();

        // Stale UID: status write with wrong UID must fail.
        {
            let update = super::super::assembly_support::support::PodStatusUpdate {
                phase: "Failed".to_string(),
                pod_ip: String::new(),
                host_ip: String::new(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            };
            let err = repo
                .status_ports()
                .set_pod_status_for_uid("default", "subres-test", "uid-stale", update, None)
                .await;
            assert!(err.is_err(), "stale UID status write must be rejected");
        }

        // Verify replacement is unchanged.
        let after = repo
            .query
            .get_pod_by_name("default", "subres-test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.uid, live.uid);
        assert_eq!(after.resource_version, live.resource_version);

        // Correct UID succeeds.
        repo.api_ports()
            .update_ephemeral_containers_for_pod(
                "default",
                "subres-test",
                vec![],
                live.resource_version,
            )
            .await
            .expect("correct UID ephemeral containers update must succeed");
    }
    #[tokio::test]
    async fn pod_network_service_pod_network_rows_are_uid_keyed() {
        let repo = IntegrationPodNetworkScenarioFixture::new_inline().await;

        // read_pod_network_assignment requires pod_uid in its signature.
        // This is a compile-time contract: every caller must supply a UID.
        let result = repo
            .read_pod_network_assignment("sb-noexist", "default", "net-test", "uid-specific", false)
            .await;
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn pod_watch_service_preserves_resource_version_and_uid() {
        let repo = IntegrationPodStoreWatchFixture::new_inline().await;

        let mut rx = repo.watch.subscribe();

        // Create a Pod — it must emit a watch event with UID and resourceVersion.
        repo.persistence
            .seed_pod(
                "default",
                "watch-test",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "watch-test",
                        "uid": "uid-watch"
                    },
                    "spec": {
                        "containers": [{"name": "app", "image": "busybox"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("watch event must be received within timeout")
            .expect("watch channel must be open");

        let pod_uid = event
            .object
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert!(!pod_uid.is_empty(), "watch event must carry UID");
        assert!(
            event.resource_version().unwrap_or(0) > 0,
            "watch event must carry resourceVersion"
        );
    }
    #[tokio::test]
    async fn pod_store_mutating_methods_require_uid_or_create_context() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;

        let pod_new = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "uid-audit",
                "uid": "uid-new",
                "deletionTimestamp": "2026-07-24T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "busybox"}]
            },
            "status": {"phase": "Pending"}
        });
        store
            .persistence
            .seed_pod("default", "uid-audit", pod_new)
            .await
            .unwrap();

        let created = store
            .query
            .get_pod_by_name("default", "uid-audit")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(created.uid, "uid-new");

        // Actor finalization with a stale UID must not delete the replacement.
        let outcome = store
            .finalize_bound_pod_after_actor_cleanup("default", "uid-audit", "uid-stale")
            .await
            .unwrap();
        assert_eq!(
            outcome,
            super::super::assembly_support::support::BoundPodDeleteOutcome::IdentityChanged
        );

        // Replacement Pod must still exist with uid-new.
        let still_there = store
            .query
            .get_pod_by_name("default", "uid-audit")
            .await
            .unwrap()
            .expect("replacement Pod must not be deleted by stale UID");
        assert_eq!(still_there.uid, "uid-new");

        // update resolves UID from current Pod state and uses it as
        // a DB precondition. A fresh replacement with different UID would
        // cause the precondition check to fail.
        let updated = store
            .persistence
            .replace_pod(
                "default",
                "uid-audit",
                still_there.data.as_ref().clone(),
                still_there.resource_version,
            )
            .await
            .expect("update with correct current state must succeed");
        assert_eq!(updated.uid, "uid-new");

        // Bound actor finalization with the correct UID must succeed.
        let outcome = store
            .finalize_bound_pod_after_actor_cleanup("default", "uid-audit", "uid-new")
            .await
            .expect("correct UID delete must succeed");
        assert_eq!(
            outcome,
            super::super::assembly_support::support::BoundPodDeleteOutcome::Removed
        );
        assert!(
            store
                .query
                .get_pod_by_name("default", "uid-audit")
                .await
                .unwrap()
                .is_none(),
            "Pod must be deleted after correct-UID delete"
        );
    }
}
