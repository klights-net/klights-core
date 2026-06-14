//! Controller common tests.

mod cases {
    use super::super::*;
    use crate::datastore::sqlite::Datastore;

    use serde_json::json;

    // --- build_owner_ref tests ---

    #[test]
    fn test_build_owner_ref_sets_correct_fields() {
        let owner_ref = build_owner_ref("apps/v1", "ReplicaSet", "my-rs", "uid-abc");

        assert_eq!(owner_ref["apiVersion"], "apps/v1");
        assert_eq!(owner_ref["kind"], "ReplicaSet");
        assert_eq!(owner_ref["name"], "my-rs");
        assert_eq!(owner_ref["uid"], "uid-abc");
        assert_eq!(owner_ref["controller"], true);
        assert_eq!(owner_ref["blockOwnerDeletion"], true);
    }

    #[test]
    fn test_build_owner_ref_different_kinds() {
        let job_ref = build_owner_ref("batch/v1", "Job", "my-job", "job-uid");
        assert_eq!(job_ref["apiVersion"], "batch/v1");
        assert_eq!(job_ref["kind"], "Job");

        let ds_ref = build_owner_ref("apps/v1", "DaemonSet", "my-ds", "ds-uid");
        assert_eq!(ds_ref["kind"], "DaemonSet");

        let sts_ref = build_owner_ref("apps/v1", "StatefulSet", "my-sts", "sts-uid");
        assert_eq!(sts_ref["kind"], "StatefulSet");

        let deploy_ref = build_owner_ref("apps/v1", "Deployment", "my-deploy", "deploy-uid");
        assert_eq!(deploy_ref["kind"], "Deployment");
    }

    // --- is_owned_by tests ---

    #[test]
    fn test_is_owned_by_returns_true_when_uid_matches() {
        let resource = json!({
            "metadata": {
                "ownerReferences": [
                    {"uid": "owner-uid-123", "kind": "ReplicaSet"}
                ]
            }
        });
        assert!(is_owned_by(&resource, "owner-uid-123"));
    }

    #[test]
    fn test_is_owned_by_returns_false_when_uid_does_not_match() {
        let resource = json!({
            "metadata": {
                "ownerReferences": [
                    {"uid": "other-uid", "kind": "ReplicaSet"}
                ]
            }
        });
        assert!(!is_owned_by(&resource, "owner-uid-123"));
    }

    #[test]
    fn test_is_owned_by_returns_false_when_no_owner_references() {
        let resource = json!({
            "metadata": {
                "name": "no-owner-resource"
            }
        });
        assert!(!is_owned_by(&resource, "any-uid"));
    }

    #[test]
    fn test_is_owned_by_returns_false_when_no_metadata() {
        let resource = json!({
            "spec": {}
        });
        assert!(!is_owned_by(&resource, "any-uid"));
    }

    #[test]
    fn test_is_owned_by_matches_any_ref_in_multiple_refs() {
        let resource = json!({
            "metadata": {
                "ownerReferences": [
                    {"uid": "first-uid", "kind": "A"},
                    {"uid": "target-uid", "kind": "B"},
                    {"uid": "third-uid", "kind": "C"}
                ]
            }
        });
        assert!(is_owned_by(&resource, "target-uid"));
        assert!(!is_owned_by(&resource, "missing-uid"));
    }

    // --- is_pod_ready_value tests ---

    #[test]
    fn test_is_pod_ready_value_returns_true_when_ready_condition_true() {
        let pod = json!({
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True"}
                ]
            }
        });
        assert!(is_pod_ready_value(&pod));
    }

    #[test]
    fn test_is_pod_ready_value_returns_false_when_ready_condition_false() {
        let pod = json!({
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False"}
                ]
            }
        });
        assert!(!is_pod_ready_value(&pod));
    }

    #[test]
    fn test_is_pod_ready_value_returns_false_when_no_conditions() {
        let pod = json!({
            "status": {}
        });
        assert!(!is_pod_ready_value(&pod));
    }

    #[test]
    fn test_is_pod_ready_value_returns_false_when_no_status() {
        let pod = json!({
            "metadata": {"name": "my-pod"}
        });
        assert!(!is_pod_ready_value(&pod));
    }

    #[test]
    fn test_is_pod_ready_value_returns_true_when_ready_among_multiple_conditions() {
        let pod = json!({
            "status": {
                "conditions": [
                    {"type": "Initialized", "status": "True"},
                    {"type": "Ready", "status": "True"},
                    {"type": "ContainersReady", "status": "True"}
                ]
            }
        });
        assert!(is_pod_ready_value(&pod));
    }

    // --- count_ready_pods tests ---

    #[tokio::test]
    async fn test_count_ready_pods_counts_only_ready_pods() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create 3 pods: 2 ready, 1 not ready
        let ready_pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "ready-pod", "namespace": "default", "uid": "p1"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });
        let not_ready_pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "not-ready-pod", "namespace": "default", "uid": "p2"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        });
        let ready_pod2 = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "ready-pod-2", "namespace": "default", "uid": "p3"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });

        let r1 = db
            .create_resource("v1", "Pod", Some("default"), "ready-pod", ready_pod)
            .await
            .unwrap();
        let r2 = db
            .create_resource("v1", "Pod", Some("default"), "not-ready-pod", not_ready_pod)
            .await
            .unwrap();
        let r3 = db
            .create_resource("v1", "Pod", Some("default"), "ready-pod-2", ready_pod2)
            .await
            .unwrap();

        let pods = vec![r1, r2, r3];
        assert_eq!(count_ready_pods(&pods), 2);
    }

    #[test]
    fn test_count_ready_pods_returns_zero_for_empty_slice() {
        let pods: Vec<Resource> = vec![];
        assert_eq!(count_ready_pods(&pods), 0);
    }

    #[test]
    fn test_owner_ref_manager_trait_builds_owner_ref() {
        let common = DefaultControllerCommon;
        let owner_ref = common.build_owner_ref("apps/v1", "Deployment", "demo", "uid-demo");
        assert_eq!(owner_ref["kind"], "Deployment");
        assert_eq!(owner_ref["uid"], "uid-demo");
    }

    #[test]
    fn test_condition_builder_trait_builds_ready_condition() {
        let common = DefaultControllerCommon;
        let condition = common.build_condition("Ready", "True", "Reconciled", "ok");
        assert_eq!(condition["type"], "Ready");
        assert_eq!(condition["status"], "True");
        assert_eq!(condition["reason"], "Reconciled");
        assert_eq!(condition["message"], "ok");
    }

    #[test]
    fn test_owner_ref_manager_trait_checks_ownership() {
        let common = DefaultControllerCommon;
        let owned = json!({
            "metadata": {
                "ownerReferences": [{"uid": "owner-123"}]
            }
        });
        assert!(common.is_owned_by(&owned, "owner-123"));
        assert!(!common.is_owned_by(&owned, "owner-999"));
    }

    #[test]
    fn test_pod_counter_trait_methods_work() {
        let common = DefaultControllerCommon;
        let ready_pod = json!({
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });
        let not_ready_pod = json!({
            "status": {"conditions": [{"type": "Ready", "status": "False"}]}
        });

        assert!(common.is_pod_ready(&ready_pod));
        assert!(!common.is_pod_ready(&not_ready_pod));

        let pods = vec![
            Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                uid: "uid-p1".to_string(),
                data: std::sync::Arc::new(ready_pod),
                resource_version: 1,
            },
            Resource {
                id: 2,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p2".to_string(),
                uid: "uid-p2".to_string(),
                data: std::sync::Arc::new(not_ready_pod),
                resource_version: 2,
            },
        ];
        assert_eq!(common.count_ready_pods(&pods), 1);
    }

    #[test]
    fn test_is_pod_ready_value_falls_back_to_running_ready_container_statuses() {
        let pod = json!({
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"name": "c1", "ready": true},
                    {"name": "c2", "ready": true}
                ]
            }
        });
        assert!(
            is_pod_ready_value(&pod),
            "running pod with all containers ready should count as ready even if Ready condition is absent"
        );
    }

    #[test]
    fn build_child_pod_stamps_canonical_metadata() {
        let template = json!({
            "metadata": {"labels": {"app": "demo", "tier": "backend"}},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        });
        let pod = build_child_pod(
            &template,
            "demo-abc12",
            "default",
            "node-1",
            OwnerInfo {
                api_version: "apps/v1",
                kind: "ReplicaSet",
                name: "demo",
                uid: "rs-uid-1",
            },
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(pod["apiVersion"], "v1");
        assert_eq!(pod["kind"], "Pod");
        assert_eq!(pod["status"]["phase"], "Pending");
        assert_eq!(pod["metadata"]["name"], "demo-abc12");
        assert_eq!(pod["metadata"]["namespace"], "default");
        // Template labels survive verbatim.
        assert_eq!(pod["metadata"]["labels"]["app"], "demo");
        assert_eq!(pod["metadata"]["labels"]["tier"], "backend");
        // Owner ref is stamped with controller=true / blockOwnerDeletion=true.
        assert_eq!(pod["metadata"]["ownerReferences"][0]["uid"], "rs-uid-1");
        assert_eq!(pod["metadata"]["ownerReferences"][0]["controller"], true);
        // nodeName falls back to the supplied value when absent in the template.
        assert_eq!(pod["spec"]["nodeName"], "node-1");
        // Container spec from the template is preserved.
        assert_eq!(pod["spec"]["containers"][0]["image"], "nginx");
    }

    #[test]
    fn build_child_pod_merges_extra_labels_and_annotations() {
        let template = json!({
            "metadata": {"labels": {"app": "sts"}},
            "spec": {}
        });
        let pod = build_child_pod(
            &template,
            "sts-0",
            "default",
            "node-1",
            OwnerInfo {
                api_version: "apps/v1",
                kind: "StatefulSet",
                name: "sts",
                uid: "sts-uid",
            },
            &[("controller-revision-hash", "rev-9")],
            &[("klights.dev/template-hash", "h0bcde")],
        )
        .unwrap();

        // Template label survives, extra label is layered on.
        assert_eq!(pod["metadata"]["labels"]["app"], "sts");
        assert_eq!(
            pod["metadata"]["labels"]["controller-revision-hash"],
            "rev-9"
        );
        assert_eq!(
            pod["metadata"]["annotations"]["klights.dev/template-hash"],
            "h0bcde"
        );
    }

    #[test]
    fn build_child_pod_rejects_non_object_template() {
        // A template that isn't a JSON object cannot become a valid Pod;
        // the helper must surface the error so the controller retries
        // instead of submitting an empty Pod that K8s admission rejects.
        for bad in [
            json!("not an object"),
            json!(42),
            json!([1, 2, 3]),
            json!(null),
        ] {
            let result = build_child_pod(
                &bad,
                "p-bad",
                "default",
                "node-1",
                OwnerInfo {
                    api_version: "apps/v1",
                    kind: "ReplicaSet",
                    name: "rs",
                    uid: "rs-uid",
                },
                &[],
                &[],
            );
            assert!(
                result.is_err(),
                "build_child_pod must reject non-object template: {bad:?}"
            );
        }
    }

    #[test]
    fn build_child_pod_preserves_explicit_node_name_in_template() {
        let template = json!({
            "metadata": {"labels": {}},
            "spec": {"nodeName": "preset-node"}
        });
        let pod = build_child_pod(
            &template,
            "p-1",
            "default",
            "fallback-node",
            OwnerInfo {
                api_version: "apps/v1",
                kind: "DaemonSet",
                name: "ds",
                uid: "ds-uid",
            },
            &[],
            &[],
        )
        .unwrap();

        // Explicit nodeName from the template wins; the helper does not
        // stomp it with the fallback parameter.
        assert_eq!(pod["spec"]["nodeName"], "preset-node");
    }

    #[test]
    fn build_child_pod_replaces_empty_template_node_name_with_supplied_node() {
        let template = json!({
            "metadata": {"labels": {}},
            "spec": {"nodeName": ""}
        });
        let pod = build_child_pod(
            &template,
            "p-1",
            "default",
            "node-1",
            OwnerInfo {
                api_version: "apps/v1",
                kind: "DaemonSet",
                name: "ds",
                uid: "ds-uid",
            },
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(pod["spec"]["nodeName"], "node-1");
    }

    #[tokio::test]
    async fn test_write_status_preserves_spec_and_emits_status_only_update() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "rs-x",
                json!({
                    "metadata": {"name": "rs-x", "namespace": "default"},
                    "spec": {"replicas": 5},
                    "status": {"replicas": 0}
                }),
            )
            .await
            .unwrap();

        // Hand the controller the resource it just read (with apiVersion/kind/RV stamped).
        let mut resource: Value = (*created.data).clone();
        resource["apiVersion"] = json!("apps/v1");
        resource["kind"] = json!("ReplicaSet");
        resource["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

        let result = write_status(
            &db as &dyn DatastoreBackend,
            &resource,
            &json!({"replicas": 5, "readyReplicas": 5}),
        )
        .await
        .unwrap();

        assert_eq!(result.data["spec"]["replicas"], 5);
        assert_eq!(result.data["status"]["replicas"], 5);
        assert_eq!(result.data["status"]["readyReplicas"], 5);
    }

    #[tokio::test]
    async fn test_write_status_skips_unchanged_status() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "DaemonSet",
                Some("default"),
                "ds-noop",
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "DaemonSet",
                    "metadata": {"name": "ds-noop", "namespace": "default"},
                    "spec": {},
                    "status": {"desiredNumberScheduled": 1, "numberReady": 1}
                }),
            )
            .await
            .unwrap();

        let mut snapshot: Value = (*created.data).clone();
        snapshot["apiVersion"] = json!("apps/v1");
        snapshot["kind"] = json!("DaemonSet");
        snapshot["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

        let result = write_status(
            &db as &dyn DatastoreBackend,
            &snapshot,
            &json!({"desiredNumberScheduled": 1, "numberReady": 1}),
        )
        .await
        .unwrap();

        assert_eq!(
            result.resource_version, created.resource_version,
            "unchanged controller status must not churn resourceVersion"
        );
    }

    #[tokio::test]
    async fn test_write_status_for_resource_skips_unchanged_status() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "rc-noop",
                json!({
                    "metadata": {"name": "rc-noop", "namespace": "default"},
                    "spec": {"replicas": 1},
                    "status": {"replicas": 1, "readyReplicas": 1}
                }),
            )
            .await
            .unwrap();

        let result = write_status_for_resource(
            &db as &dyn DatastoreBackend,
            &created,
            &json!({"replicas": 1, "readyReplicas": 1}),
        )
        .await
        .unwrap();

        assert_eq!(
            result.resource_version, created.resource_version,
            "unchanged controller status must not churn resourceVersion or race e2e status writers"
        );
        assert_eq!(result.data["status"]["readyReplicas"], json!(1));
    }

    #[tokio::test]
    async fn test_write_status_skips_cas_when_resource_version_missing() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "dep-y",
            json!({
                "metadata": {"name": "dep-y", "namespace": "default"},
                "spec": {"replicas": 2},
                "status": {}
            }),
        )
        .await
        .unwrap();

        // Resource without metadata.resourceVersion — write_status must skip CAS,
        // not parse "" or default to 0 (which would mismatch).
        let resource = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "dep-y", "namespace": "default"},
        });
        write_status(
            &db as &dyn DatastoreBackend,
            &resource,
            &json!({"replicas": 2, "updatedReplicas": 2}),
        )
        .await
        .unwrap();

        let after = db
            .get_resource("apps/v1", "Deployment", Some("default"), "dep-y")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.data["status"]["updatedReplicas"], 2);
        assert_eq!(after.data["spec"]["replicas"], 2);
    }

    #[tokio::test]
    async fn test_write_status_retries_stale_snapshot_after_status_only_overlap() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "dep-stale",
                json!({
                    "metadata": {"name": "dep-stale", "namespace": "default"},
                    "spec": {"replicas": 1},
                    "status": {"availableReplicas": 0}
                }),
            )
            .await
            .unwrap();

        let mut stale_snapshot: Value = (*created.data).clone();
        stale_snapshot["apiVersion"] = json!("apps/v1");
        stale_snapshot["kind"] = json!("Deployment");
        stale_snapshot["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

        db.update_status_only(
            "apps/v1",
            "Deployment",
            Some("default"),
            "dep-stale",
            json!({"availableReplicas": 1}),
            Some(created.resource_version),
        )
        .await
        .expect("fresh writer must win first");

        let stale_write = write_status(
            &db as &dyn DatastoreBackend,
            &stale_snapshot,
            &json!({"availableReplicas": 0, "observedGeneration": 2}),
        )
        .await;
        let updated = stale_write.expect("stale status-only overlap must retry");
        assert_eq!(updated.data["status"]["availableReplicas"], 0);
        assert_eq!(updated.data["status"]["observedGeneration"], 2);
    }

    #[tokio::test]
    async fn test_write_status_for_resource_retries_stale_snapshot_after_status_only_overlap() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "rs-stale",
                json!({
                    "metadata": {"name": "rs-stale", "namespace": "default"},
                    "spec": {"replicas": 1},
                    "status": {"readyReplicas": 0}
                }),
            )
            .await
            .unwrap();

        db.update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-stale",
            json!({"readyReplicas": 1}),
            Some(created.resource_version),
        )
        .await
        .expect("fresh writer must win first");

        let stale_write = write_status_for_resource(
            &db as &dyn DatastoreBackend,
            &created,
            &json!({"readyReplicas": 0, "observedGeneration": 2}),
        )
        .await;
        let updated = stale_write.expect("stale status-only overlap must retry");
        assert_eq!(updated.data["status"]["readyReplicas"], 0);
        assert_eq!(updated.data["status"]["observedGeneration"], 2);
    }

    #[tokio::test]
    async fn test_write_status_for_resource_rejects_recreated_same_name_uid() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "rs-recreated",
                json!({
                    "metadata": {
                        "name": "rs-recreated",
                        "namespace": "default",
                        "uid": "uid-old",
                        "generation": 1
                    },
                    "spec": {"replicas": 1},
                    "status": {"readyReplicas": 0}
                }),
            )
            .await
            .unwrap();

        db.delete_resource("apps/v1", "ReplicaSet", Some("default"), "rs-recreated")
            .await
            .unwrap();
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-recreated",
            json!({
                "metadata": {
                    "name": "rs-recreated",
                    "namespace": "default",
                    "uid": "uid-new",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"readyReplicas": 0}
            }),
        )
        .await
        .unwrap();

        let stale_write = write_status_for_resource(
            &db as &dyn DatastoreBackend,
            &created,
            &json!({"readyReplicas": 1}),
        )
        .await;
        let err = stale_write.expect_err("old UID status writer must not mutate replacement");
        assert!(
            crate::datastore::errors::is_conflict_error(&err),
            "expected conflict from stale UID write, got {err:#}"
        );

        let replacement = db
            .get_resource("apps/v1", "ReplicaSet", Some("default"), "rs-recreated")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.uid, "uid-new");
        assert_eq!(replacement.data["status"]["readyReplicas"], 0);
    }

    #[tokio::test]
    async fn test_write_status_updates_live_status_when_desired_matches_stale_snapshot() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "dep-stale-unchanged",
                json!({
                    "metadata": {"name": "dep-stale-unchanged", "namespace": "default"},
                    "spec": {"replicas": 1},
                    "status": {"availableReplicas": 0}
                }),
            )
            .await
            .unwrap();

        let mut stale_snapshot: Value = (*created.data).clone();
        stale_snapshot["apiVersion"] = json!("apps/v1");
        stale_snapshot["kind"] = json!("Deployment");
        stale_snapshot["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

        db.update_status_only(
            "apps/v1",
            "Deployment",
            Some("default"),
            "dep-stale-unchanged",
            json!({"availableReplicas": 1}),
            Some(created.resource_version),
        )
        .await
        .expect("concurrent status writer must advance live status");

        let updated = write_status(
            &db as &dyn DatastoreBackend,
            &stale_snapshot,
            &json!({"availableReplicas": 0}),
        )
        .await
        .expect("stale unchanged snapshot status must still correct live status");

        assert_eq!(updated.data["status"]["availableReplicas"], 0);
    }

    #[tokio::test]
    async fn test_write_status_for_resource_updates_live_status_when_desired_matches_stale_snapshot()
     {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "rs-stale-unchanged",
                json!({
                    "metadata": {"name": "rs-stale-unchanged", "namespace": "default"},
                    "spec": {"replicas": 1},
                    "status": {"readyReplicas": 0}
                }),
            )
            .await
            .unwrap();

        db.update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-stale-unchanged",
            json!({"readyReplicas": 1}),
            Some(created.resource_version),
        )
        .await
        .expect("concurrent status writer must advance live status");

        let updated = write_status_for_resource(
            &db as &dyn DatastoreBackend,
            &created,
            &json!({"readyReplicas": 0}),
        )
        .await
        .expect("stale unchanged resource status must still correct live status");

        assert_eq!(updated.data["status"]["readyReplicas"], 0);
    }

    #[tokio::test]
    async fn test_write_status_stale_snapshot_does_not_retry_after_spec_change() {
        let db = Datastore::new_in_memory().await.unwrap();
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "dep-spec-changed",
                json!({
                    "metadata": {"name": "dep-spec-changed", "namespace": "default", "generation": 1},
                    "spec": {"replicas": 1},
                    "status": {"availableReplicas": 0}
                }),
            )
            .await
            .unwrap();

        let mut stale_snapshot: Value = (*created.data).clone();
        stale_snapshot["apiVersion"] = json!("apps/v1");
        stale_snapshot["kind"] = json!("Deployment");
        stale_snapshot["metadata"]["resourceVersion"] = json!(created.resource_version.to_string());

        db.update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "dep-spec-changed",
            json!({
                "metadata": {
                    "name": "dep-spec-changed",
                    "namespace": "default",
                    "generation": 2
                },
                "spec": {"replicas": 2},
                "status": {"availableReplicas": 0}
            }),
            created.resource_version,
        )
        .await
        .expect("spec writer must advance resourceVersion");

        let stale_write = write_status(
            &db as &dyn DatastoreBackend,
            &stale_snapshot,
            &json!({"availableReplicas": 1, "observedGeneration": 1}),
        )
        .await;
        let err = stale_write.expect_err("stale spec snapshot must not retry");
        assert!(
            format!("{err:#}").contains("409"),
            "expected 409, got {err:#}"
        );
    }

    // --- append_owner_reference / remove_owner_reference_by_uid tests ---

    #[test]
    #[allow(clippy::type_complexity)]
    fn owner_ref_append_and_remove_table_driven() {
        let cases: &[(Vec<Value>, Option<(String, String)>, Vec<Value>, &str)] = &[
            // (initial_refs, remove_spec, expected_refs, description)
            // No-op when uid absent
            (
                vec![json!({"kind": "ReplicaSet", "uid": "rs-1"})],
                Some(("ReplicaSet".to_string(), "rs-other".to_string())),
                vec![json!({"kind": "ReplicaSet", "uid": "rs-1"})],
                "no-op when uid absent",
            ),
            // Removal preserves order
            (
                vec![
                    json!({"kind": "ReplicaSet", "uid": "rs-1"}),
                    json!({"kind": "Job", "uid": "job-1"}),
                    json!({"kind": "ReplicaSet", "uid": "rs-2"}),
                ],
                Some(("Job".to_string(), "job-1".to_string())),
                vec![
                    json!({"kind": "ReplicaSet", "uid": "rs-1"}),
                    json!({"kind": "ReplicaSet", "uid": "rs-2"}),
                ],
                "removal preserves order",
            ),
            // Removal of only owner produces empty list
            (
                vec![json!({"kind": "ReplicaSet", "uid": "rs-1"})],
                Some(("ReplicaSet".to_string(), "rs-1".to_string())),
                vec![],
                "removal of only owner produces empty list",
            ),
            // No-op when there are no initial refs and no removal
            (vec![], None, vec![], "empty list stays empty"),
        ];

        for (initial_refs, remove_spec, expected_refs, desc) in cases {
            let mut resource = json!({
                "metadata": {
                    "ownerReferences": initial_refs.clone()
                }
            });

            // Apply removal if specified
            if let Some((kind, uid)) = remove_spec {
                let removed = remove_owner_reference_by_uid(&mut resource, kind, uid);
                if initial_refs.iter().any(|r| {
                    r["kind"].as_str() == Some(kind.as_str())
                        && r["uid"].as_str() == Some(uid.as_str())
                }) {
                    assert!(removed, "{desc}: expected removal to return true");
                } else {
                    assert!(!removed, "{desc}: expected removal to return false");
                }
            }

            let result_refs = resource["metadata"]["ownerReferences"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert_eq!(result_refs, *expected_refs, "{desc}");
        }
    }

    #[test]
    fn append_owner_reference_adds_to_existing_list() {
        let mut resource = json!({
            "metadata": {
                "ownerReferences": [{"kind": "ReplicaSet", "uid": "rs-1"}]
            }
        });
        append_owner_reference(&mut resource, json!({"kind": "Job", "uid": "job-1"}));
        let refs = resource["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1]["uid"], "job-1");
    }

    #[test]
    fn append_owner_reference_creates_array_when_missing() {
        let mut resource = json!({"metadata": {"name": "pod-1"}});
        append_owner_reference(&mut resource, json!({"kind": "Job", "uid": "job-1"}));
        let refs = resource["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["uid"], "job-1");
    }

    #[test]
    fn remove_owner_reference_returns_false_when_no_metadata() {
        let mut resource = json!({"spec": {}});
        assert!(!remove_owner_reference_by_uid(
            &mut resource,
            "Job",
            "uid-1"
        ));
    }

    // --- OwnerReferenceList tests (F4-04) ---

    #[test]
    fn owner_reference_list_append_sets_controller_ref_fields() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "my-rs",
            "uid-123",
        ));
        let refs = list.as_slice();

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].api_version, "apps/v1");
        assert_eq!(refs[0].kind, "ReplicaSet");
        assert_eq!(refs[0].name, "my-rs");
        assert_eq!(refs[0].uid, "uid-123");
        assert!(refs[0].controller);
        assert!(refs[0].block_owner_deletion);
    }

    #[test]
    fn owner_reference_list_remove_by_uid_preserves_non_matching_refs() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));
        list.append(OwnerReference::controller(
            "batch/v1", "Job", "job-1", "uid-2",
        ));
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-2",
            "uid-3",
        ));

        let removed = list.remove_by_uid("Job", "uid-2");
        assert!(removed);
        assert_eq!(list.len(), 2);
        assert_eq!(list.as_slice()[0].kind, "ReplicaSet");
        assert_eq!(list.as_slice()[1].kind, "ReplicaSet");
    }

    #[test]
    fn owner_reference_list_ignores_other_controller_uid() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));
        list.append(OwnerReference::controller(
            "batch/v1", "Job", "job-1", "uid-2",
        ));

        let removed = list.remove_by_uid("ReplicaSet", "uid-other");
        assert!(!removed);
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn owner_reference_list_find_controller_returns_controller_ref() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::new(
            "apps/v1".to_string(),
            "ReplicaSet".to_string(),
            "rs-1".to_string(),
            "uid-1".to_string(),
            true,
            true,
        ));
        list.append(OwnerReference::new(
            "v1".to_string(),
            "Pod".to_string(),
            "pod-1".to_string(),
            "uid-2".to_string(),
            false,
            false,
        ));

        let controller = list.find_controller();
        assert!(controller.is_some());
        assert_eq!(controller.unwrap().kind, "ReplicaSet");
        assert!(controller.unwrap().controller);
    }

    #[test]
    fn owner_reference_list_from_json_handles_missing_or_malformed() {
        let resource = json!({
            "metadata": {}
        });
        let list = OwnerReferenceList::from_json(&resource);
        assert!(list.is_empty());

        let resource_with_bad_ref = json!({
            "metadata": {
                "ownerReferences": [
                    {"kind": "BadRef"}, // missing required fields
                    {"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs-1", "uid": "uid-1"}
                ]
            }
        });
        let list = OwnerReferenceList::from_json(&resource_with_bad_ref);
        assert_eq!(list.len(), 1);
        assert_eq!(list.as_slice()[0].kind, "ReplicaSet");
    }

    #[test]
    fn owner_reference_list_write_to_resource_creates_metadata() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));

        let mut resource = json!({"spec": {}});
        list.write_to_resource(&mut resource);

        let refs = resource["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["uid"], "uid-1");
    }

    #[test]
    fn owner_reference_list_contains_uid_checks_membership() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));
        list.append(OwnerReference::controller(
            "batch/v1", "Job", "job-1", "uid-2",
        ));

        assert!(list.contains_uid("uid-1"));
        assert!(list.contains_uid("uid-2"));
        assert!(!list.contains_uid("uid-missing"));
    }

    #[test]
    fn owner_reference_list_to_json_array_produces_valid_json() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));

        let json_array = list.to_json_array();
        let arr = json_array.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["apiVersion"], "apps/v1");
        assert_eq!(arr[0]["controller"], true);
    }

    #[test]
    fn owner_reference_from_k8s_conversion() {
        let k8s_ref = K8sOwnerReference {
            api_version: "apps/v1".to_string(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: "ReplicaSet".to_string(),
            name: "rs-1".to_string(),
            uid: "uid-1".to_string(),
        };
        let owner = OwnerReference::from(k8s_ref);

        assert_eq!(owner.api_version, "apps/v1");
        assert!(owner.controller);
        assert!(owner.block_owner_deletion);
    }

    #[test]
    fn owner_reference_to_k8s_conversion() {
        let owner = OwnerReference::controller("apps/v1", "ReplicaSet", "rs-1", "uid-1");
        let k8s_ref = K8sOwnerReference::from(owner);

        assert_eq!(k8s_ref.api_version, "apps/v1");
        assert_eq!(k8s_ref.controller, Some(true));
        assert_eq!(k8s_ref.block_owner_deletion, Some(true));
    }

    #[test]
    fn owner_reference_list_from_json_roundtrip() {
        let mut list = OwnerReferenceList::new();
        list.append(OwnerReference::controller(
            "apps/v1",
            "ReplicaSet",
            "rs-1",
            "uid-1",
        ));

        let mut resource = json!({"spec": {}});
        list.write_to_resource(&mut resource);

        let list2 = OwnerReferenceList::from_json(&resource);
        assert_eq!(list2.len(), 1);
        assert_eq!(list2.as_slice()[0].uid, "uid-1");
    }
}
