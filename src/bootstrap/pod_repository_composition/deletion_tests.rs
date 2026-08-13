#[cfg(test)]
mod tests {
    use super::super::assembly_support::support::*;
    use klights_types::PodIdentity;

    #[tokio::test]
    async fn api_delete_pod_sets_deletion_timestamp_and_default_grace_30s() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "del-default").await;
        let outcome = repo
            .deletion
            .delete_pod(
                "default",
                "del-default",
                k8s_native_service::DeleteOptions::default(),
                false,
            )
            .await
            .unwrap();
        let r = match outcome {
            super::super::assembly_support::support::PodApiDeleteOutcome::GracefulSet(r) => r,
            _ => panic!("expected GracefulSet"),
        };
        assert!(r.data["metadata"]["deletionTimestamp"].is_string());
        assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(30));
    }
    #[tokio::test]
    async fn api_delete_pod_zero_grace_marks_terminating_pod_unready() {
        let repo = build_repo().await;
        repo.persistence.seed_pod(
                "default",
                "del-ready",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "del-ready",
                        "namespace": "default",
                        "uid": "uid-del-ready"
                    },
                    "spec": {
                        "nodeName": "test-node",
                        "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [
                            {"type": "Initialized", "status": "True"},
                            {"type": "PodScheduled", "status": "True"},
                            {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                            {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
                        ],
                        "containerStatuses": [{
                            "name": "app",
                            "ready": true,
                            "restartCount": 0,
                            "state": {"running": {"startedAt": "2026-04-30T00:00:00Z"}}
                        }]
                    }
                }),
            )
            .await
            .unwrap();

        let outcome = repo
            .deletion
            .delete_pod(
                "default",
                "del-ready",
                k8s_native_service::DeleteOptions {
                    propagation_policy: None,
                    orphan_dependents: None,
                    _grace_period_seconds: Some(0),
                    preconditions: None,
                },
                false,
            )
            .await
            .unwrap();
        let returned = match outcome {
            super::super::assembly_support::support::PodApiDeleteOutcome::GracefulSet(resource) => {
                resource
            }
            _ => panic!("expected GracefulSet"),
        };

        for pod in [
            returned,
            repo.query
                .get_pod_by_name("default", "del-ready")
                .await
                .unwrap()
                .expect("pod remains until actor-owned cleanup"),
        ] {
            assert!(pod.data.pointer("/metadata/deletionTimestamp").is_some());
            assert_eq!(
                pod.data
                    .pointer("/metadata/deletionGracePeriodSeconds")
                    .and_then(|value| value.as_i64()),
                Some(0)
            );
            let conditions = pod
                .data
                .pointer("/status/conditions")
                .and_then(|value| value.as_array())
                .expect("conditions must remain an array");
            for condition_type in ["Ready", "ContainersReady"] {
                let condition = conditions
                    .iter()
                    .find(|condition| {
                        condition.pointer("/type").and_then(|value| value.as_str())
                            == Some(condition_type)
                    })
                    .unwrap_or_else(|| panic!("missing {condition_type} condition"));
                assert_eq!(
                    condition
                        .pointer("/status")
                        .and_then(|value| value.as_str()),
                    Some("False"),
                    "terminating pod must not stay {condition_type}=True"
                );
                assert_eq!(
                    condition
                        .pointer("/reason")
                        .and_then(|value| value.as_str()),
                    Some("PodTerminating")
                );
            }
            let container_ready = pod
                .data
                .pointer("/status/containerStatuses/0/ready")
                .and_then(|value| value.as_bool());
            assert_eq!(container_ready, Some(false));
        }
    }
    #[tokio::test]
    async fn api_delete_pod_cascades_pod_owner_cycle_without_reentrant_stack_growth() {
        let repo = build_repo().await;
        for (name, uid, owner_name, owner_uid) in [
            ("pod1", "pod-1-uid", "pod3", "pod-3-uid"),
            ("pod2", "pod-2-uid", "pod1", "pod-1-uid"),
            ("pod3", "pod-3-uid", "pod2", "pod-2-uid"),
        ] {
            repo.persistence.seed_pod(
                "default",
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": name,
                        "namespace": "default",
                        "uid": uid,
                        "ownerReferences": [{
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "name": owner_name,
                            "uid": owner_uid,
                            "controller": true
                        }]
                    },
                    "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]},
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();
        }

        repo.deletion
            .delete(klights_pod_api::PodApiDeleteRequest {
                namespace: "default".to_string(),
                name: "pod1".to_string(),
                options: k8s_native_service::DeleteOptions::default().into(),
                dry_run: false,
            })
            .await
            .unwrap();

        for name in ["pod1", "pod2", "pod3"] {
            let pod = repo
                .query
                .get_pod_by_name("default", name)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{name} must remain until actor-owned finalization"));
            assert!(
                pod.data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|value| value.as_str())
                    .is_some(),
                "{name} must be marked terminating after cascade: {:?}",
                pod.data
            );
        }
    }
    #[tokio::test]
    async fn api_delete_pod_replaces_null_deletion_timestamp_with_real_timestamp() {
        let repo = build_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "del-null",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "del-null",
                        "namespace": "default",
                        "deletionTimestamp": null
                    },
                    "spec": {
                        "containers": [{ "name": "c", "image": "busybox" }]
                    }
                }),
            )
            .await
            .unwrap();

        let outcome = repo
            .deletion
            .delete_pod(
                "default",
                "del-null",
                k8s_native_service::DeleteOptions::default(),
                false,
            )
            .await
            .unwrap();
        let r = match outcome {
            super::super::assembly_support::support::PodApiDeleteOutcome::GracefulSet(r) => r,
            _ => panic!("expected GracefulSet"),
        };

        assert!(
            r.data["metadata"]["deletionTimestamp"].is_string(),
            "DELETE must convert null deletionTimestamp into a real timestamp"
        );
        assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(30));

        let persisted = repo
            .query
            .get_pod_by_name("default", "del-null")
            .await
            .unwrap()
            .expect("pod remains while actor cleanup owns final delete");
        assert!(
            persisted.data["metadata"]["deletionTimestamp"].is_string(),
            "persisted Pod must be visibly terminating immediately after DELETE"
        );
    }
    #[tokio::test]
    async fn api_delete_pod_uses_options_grace_period_when_provided() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "del-60").await;
        let opts = k8s_native_service::DeleteOptions {
            propagation_policy: None,
            orphan_dependents: None,
            _grace_period_seconds: Some(60),
            preconditions: None,
        };
        let outcome = repo
            .deletion
            .delete_pod("default", "del-60", opts, false)
            .await
            .unwrap();
        let r = match outcome {
            super::super::assembly_support::support::PodApiDeleteOutcome::GracefulSet(r) => r,
            _ => panic!("expected GracefulSet"),
        };
        assert_eq!(r.data["metadata"]["deletionGracePeriodSeconds"], json!(60));
    }
    #[tokio::test]
    async fn api_delete_pod_does_not_hard_delete_before_requested_grace_period() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "del-grace-five").await;
        let opts = k8s_native_service::DeleteOptions {
            propagation_policy: None,
            orphan_dependents: None,
            _grace_period_seconds: Some(5),
            preconditions: None,
        };

        repo.deletion
            .delete(klights_pod_api::PodApiDeleteRequest {
                namespace: "default".to_string(),
                name: "del-grace-five".to_string(),
                options: opts.into(),
                dry_run: false,
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(2300)).await;

        let after = repo
            .query
            .get_pod_by_name("default", "del-grace-five")
            .await
            .unwrap();
        assert!(
            after.is_some(),
            "Pod API delete must not hard-delete the object before its requested grace period"
        );
    }
    #[tokio::test]
    async fn api_delete_pod_dry_run_does_not_persist() {
        let repo = build_repo().await;
        let created = create_basic_pod_via_api(&repo, "del-dry").await;
        let outcome = repo
            .deletion
            .delete_pod(
                "default",
                "del-dry",
                k8s_native_service::DeleteOptions::default(),
                true,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            super::super::assembly_support::support::PodApiDeleteOutcome::DryRun(_)
        ));
        let after = repo
            .query
            .get_pod_by_name("default", "del-dry")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.resource_version, created.resource_version);
        assert!(after.data["metadata"].get("deletionTimestamp").is_none());
    }
    #[tokio::test]
    async fn api_delete_pod_retries_when_status_update_advances_resource_version_during_admission()
    {
        let repo = build_repo().await;
        let created = create_basic_pod_via_api(repo.as_ref(), "del-status-race").await;
        let webhook_handled =
            install_delete_admission_status_race_webhook(Arc::clone(&repo), "del-status-race")
                .await;

        let outcome = repo
            .deletion
            .delete_pod(
                "default",
                "del-status-race",
                k8s_native_service::DeleteOptions::default(),
                false,
            )
            .await
            .expect("DELETE must retry after an admission-time status conflict");

        tokio::time::timeout(std::time::Duration::from_secs(1), webhook_handled)
            .await
            .expect("delete admission webhook was called")
            .expect("delete admission webhook completed");
        let deleted = match outcome {
            super::super::assembly_support::support::PodApiDeleteOutcome::GracefulSet(resource) => {
                resource
            }
            other => panic!("expected GracefulSet, got {other:?}"),
        };
        assert!(deleted.resource_version > created.resource_version);
        assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
        assert_eq!(
            deleted.data["metadata"]["deletionGracePeriodSeconds"],
            json!(30)
        );
        assert_eq!(deleted.data["status"]["phase"], json!("Running"));

        let persisted = repo
            .query
            .get_pod_by_name("default", "del-status-race")
            .await
            .unwrap()
            .expect("pod remains until graceful delete completes");
        assert_eq!(
            persisted.data["metadata"]["deletionTimestamp"],
            deleted.data["metadata"]["deletionTimestamp"]
        );
        assert_eq!(persisted.data["status"]["phase"], json!("Running"));
    }
    #[tokio::test]
    async fn api_delete_pod_without_resource_version_precondition_survives_raft_status_race() {
        let outcome = super::super::assembly_support::support::run_raft_delete_mark_status_race(
            "del-raft-status-race",
            None,
        )
        .await
        .expect("DELETE without an RV precondition must apply to the latest Pod object");

        assert!(
            outcome.status_bumps > 0,
            "test proposer must advance status before the delete mark"
        );
        let created = outcome.created;
        let deleted = outcome.deleted;
        assert!(deleted.resource_version > created.resource_version);
        assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
        assert_eq!(
            deleted.data["metadata"]["deletionGracePeriodSeconds"],
            json!(30)
        );
        assert_eq!(deleted.data["status"]["phase"], json!("Running"));
        assert_eq!(deleted.data["status"]["raceBump"], json!(1));

        let persisted = outcome.persisted;
        assert_eq!(
            persisted.data["metadata"]["deletionTimestamp"],
            deleted.data["metadata"]["deletionTimestamp"]
        );
        assert_eq!(persisted.data["status"]["raceBump"], json!(1));
    }
    #[tokio::test]
    async fn api_delete_pod_zero_grace_without_resource_version_precondition_survives_raft_status_race()
     {
        let outcome = super::super::assembly_support::support::run_raft_delete_mark_status_race(
            "del-zero-grace-raft-status-race",
            Some(0),
        )
        .await
        .expect("zero-grace DELETE without an RV precondition must apply to the latest Pod object");

        assert!(
            outcome.status_bumps > 0,
            "test proposer must advance status before the delete mark"
        );
        let created = outcome.created;
        let deleted = outcome.deleted;
        assert!(deleted.resource_version > created.resource_version);
        assert!(deleted.data["metadata"]["deletionTimestamp"].is_string());
        assert_eq!(
            deleted.data["metadata"]["deletionGracePeriodSeconds"],
            json!(0)
        );
        assert_eq!(deleted.data["status"]["phase"], json!("Running"));
        assert_eq!(deleted.data["status"]["raceBump"], json!(1));
        for condition_type in ["Ready", "ContainersReady"] {
            let condition = deleted.data["status"]["conditions"]
                .as_array()
                .and_then(|conditions| {
                    conditions
                        .iter()
                        .find(|condition| condition.get("type") == Some(&json!(condition_type)))
                })
                .expect("terminating zero-grace pod must carry readiness conditions");
            assert_eq!(condition["status"], json!("False"));
            assert_eq!(condition["reason"], json!("PodTerminating"));
        }

        let persisted = outcome.persisted;
        assert_eq!(
            persisted.data["metadata"]["deletionTimestamp"],
            deleted.data["metadata"]["deletionTimestamp"]
        );
        assert_eq!(persisted.data["status"]["raceBump"], json!(1));
    }
    #[tokio::test]
    async fn api_delete_collection_pods_processes_all_matching_label_selector() {
        let repo = build_repo().await;
        // Three pods, two with app=x.
        use super::super::assembly_support::support::PodApiCreateRequest;
        for (n, label_app) in [("c1", "x"), ("c2", "x"), ("c3", "y")] {
            repo.api_mutations
                .create(PodApiCreateRequest {
                    namespace: "default".to_string(),
                    body: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": { "name": n, "labels": {"app": label_app} },
                        "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
                    }),
                    dry_run: false,
                })
                .await
                .unwrap();
        }
        repo.deletion
            .delete_collection(klights_pod_api::PodApiDeleteCollectionRequest {
                namespace: "default".to_string(),
                label_selector: Some("app=x".to_string()),
                field_selector: None,
                dry_run: false,
            })
            .await
            .unwrap();
        for n in ["c1", "c2"] {
            let pod = repo
                .query
                .get_pod_by_name("default", n)
                .await
                .unwrap()
                .expect("collection delete must leave Pod row until actor finalization");
            assert!(
                pod.data["metadata"]["deletionTimestamp"].is_string(),
                "pod {n} must be marked terminating after collection delete"
            );
        }
        // Pod c3 (app=y) must be untouched.
        let c3 = repo
            .query
            .get_pod_by_name("default", "c3")
            .await
            .unwrap()
            .unwrap();
        assert!(c3.data["metadata"].get("deletionTimestamp").is_none());
    }
    #[tokio::test]
    async fn create_controller_pod_persists_via_api_pipeline_with_admission() {
        let repo = build_repo().await;
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "ctrl-pod" },
            "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
        });
        let resource = repo
            .create_controller_pod("default", "ctrl-pod", "test-node", pod)
            .await
            .unwrap();
        assert_eq!(resource.name, "ctrl-pod");
        assert!(
            resource.data.pointer("/spec/nodeName").is_none(),
            "controller-facing create must not inject a node assignment unless the pod spec already has one"
        );
        assert_eq!(
            resource.data["spec"]["serviceAccountName"],
            json!("default")
        );
        assert!(
            repo.query
                .get_pod_by_name("default", "ctrl-pod")
                .await
                .unwrap()
                .is_some()
        );
    }
    #[tokio::test]
    async fn create_controller_pod_rejects_terminating_namespace() {
        let repo = build_repo().await;
        repo.seed_namespace(
            "terminating-ns",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-ns",
                    "deletionTimestamp": "2026-05-02T18:40:38Z"
                },
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .unwrap();

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": "late-pod" },
            "spec": { "containers": [{ "name": "c", "image": "busybox" }] }
        });

        let err = repo
            .create_controller_pod("terminating-ns", "late-pod", "test-node", pod)
            .await
            .expect_err("controller pod create must reject terminating namespaces");
        assert!(
            err.to_string()
                .contains("namespace terminating-ns is being terminated"),
            "unexpected error: {err:#}"
        );
        assert!(
            repo.query
                .get_pod_by_name("terminating-ns", "late-pod")
                .await
                .unwrap()
                .is_none(),
            "rejected controller-created pod must not be persisted"
        );
    }
    #[tokio::test]
    async fn delete_pod_marks_resource_terminating() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "rm-pod").await;
        repo.delete_pod("default", "rm-pod").await.unwrap();
        let pod = repo
            .query
            .get_pod_by_name("default", "rm-pod")
            .await
            .unwrap()
            .unwrap();
        assert!(pod.data["metadata"]["deletionTimestamp"].is_string());
    }
    #[tokio::test]
    async fn ordinary_pod_ports_preserve_query_update_and_graceful_mark_semantics() {
        use klights_pod_api::{
            PodGetRequest, PodLabel, PodListRequest, PodMarkTerminatingRequest, PodMutationTarget,
            PodOwnerListRequest, PodOwnerReference, PodRepositoryError, PodUpdateRequest,
        };

        let repo = build_repo().await;
        let created = create_basic_pod_via_api(&repo, "ordinary-port-pod").await;
        let identity = PodIdentity::new("default", "ordinary-port-pod", &created.uid);

        let queried = repo
            .query
            .get_pod(PodGetRequest::try_by_identity(identity.clone()).unwrap())
            .await
            .unwrap()
            .expect("UID-qualified query must find the Pod");
        assert_eq!(queried.uid, created.uid);

        let listed = repo
            .query
            .list_pods(
                PodListRequest::try_new(Some("default".to_string()), None, None, None, None)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(listed.items().iter().any(|pod| pod.uid == created.uid));

        let updated = repo
            .update
            .update_pod(PodUpdateRequest::merge_labels(
                PodMutationTarget::try_by_identity(identity.clone()).unwrap(),
                vec![PodLabel::try_new("ordinary-port", "true").unwrap()],
            ))
            .await
            .unwrap();
        assert_eq!(
            updated.data.pointer("/metadata/labels/ordinary-port"),
            Some(&json!("true"))
        );

        let updated = repo
            .update
            .update_pod(PodUpdateRequest::replace_owner_references(
                PodMutationTarget::try_by_identity(identity.clone()).unwrap(),
                vec![
                    PodOwnerReference::try_new(
                        "apps/v1",
                        "ReplicaSet",
                        "ordinary-owner",
                        "ordinary-owner-uid",
                        Some(true),
                        Some(true),
                    )
                    .unwrap(),
                ],
            ))
            .await
            .unwrap();
        assert_eq!(
            updated.data.pointer("/metadata/ownerReferences/0/uid"),
            Some(&json!("ordinary-owner-uid"))
        );

        let updated = repo
            .update
            .update_pod(
                PodUpdateRequest::try_record_sandbox_id(
                    PodMutationTarget::try_by_identity(identity.clone()).unwrap(),
                    "ordinary-sandbox",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            updated
                .data
                .pointer("/metadata/annotations/klights.dev~1sandbox-id"),
            Some(&json!("ordinary-sandbox"))
        );

        let owned = repo
            .query
            .list_pods_by_owner_uid(
                PodOwnerListRequest::try_new("default", "ordinary-owner-uid").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].uid, created.uid);
        let unowned = repo
            .query
            .list_pods_by_owner_uid(
                PodOwnerListRequest::try_new("default", "missing-owner-uid").unwrap(),
            )
            .await
            .unwrap();
        assert!(unowned.is_empty());

        let stale_identity = PodIdentity::new("default", "ordinary-port-pod", "stale-uid");
        let stale_updates = vec![
            PodUpdateRequest::merge_labels(
                PodMutationTarget::try_by_identity(stale_identity.clone()).unwrap(),
                vec![PodLabel::try_new("stale", "label").unwrap()],
            ),
            PodUpdateRequest::replace_owner_references(
                PodMutationTarget::try_by_identity(stale_identity.clone()).unwrap(),
                Vec::new(),
            ),
            PodUpdateRequest::try_record_sandbox_id(
                PodMutationTarget::try_by_identity(stale_identity.clone()).unwrap(),
                "stale-sandbox",
            )
            .unwrap(),
        ];
        for stale_update in stale_updates {
            let error = repo
                .update
                .update_pod(stale_update)
                .await
                .expect_err("stale UID metadata update must reject a same-name replacement");
            assert!(matches!(
                error,
                PodRepositoryError::UidMismatch {
                    ref expected,
                    ref actual,
                } if expected == "stale-uid" && actual == &created.uid
            ));
        }

        let mismatch = repo
            .deletion
            .ordinary_mark_pod_terminating(PodMarkTerminatingRequest::new(
                PodMutationTarget::try_by_identity(PodIdentity::new(
                    "default",
                    "ordinary-port-pod",
                    "replacement-uid",
                ))
                .unwrap(),
            ))
            .await
            .expect_err("a stale UID must not mark a same-name Pod terminating");
        assert!(matches!(mismatch, PodRepositoryError::Conflict { .. }));

        let terminating = repo
            .deletion
            .ordinary_mark_pod_terminating(PodMarkTerminatingRequest::new(
                PodMutationTarget::try_by_identity(identity).unwrap(),
            ))
            .await
            .unwrap();
        assert!(terminating.data["metadata"]["deletionTimestamp"].is_string());
        assert!(
            repo.query
                .get_pod_by_name("default", "ordinary-port-pod")
                .await
                .unwrap()
                .is_some(),
            "ordinary deletion may mark terminating but must not remove the Pod row"
        );
    }
    #[test]
    fn ordinary_pod_error_mapping_preserves_kubernetes_error_categories() {
        use klights_pod_api::PodRepositoryError;

        assert!(matches!(
            k8s_native_service::AppError::from(PodRepositoryError::not_found("default", "web")),
            k8s_native_service::AppError::NotFound(_)
        ));
        assert!(matches!(
            k8s_native_service::AppError::from(PodRepositoryError::uid_mismatch("old", "new")),
            k8s_native_service::AppError::Conflict(_)
        ));
        assert!(matches!(
            k8s_native_service::AppError::from(PodRepositoryError::conflict("resource changed")),
            k8s_native_service::AppError::Conflict(_)
        ));
        assert!(matches!(
            k8s_native_service::AppError::from(PodRepositoryError::unavailable(
                "leader unavailable"
            )),
            k8s_native_service::AppError::ServiceUnavailable(_)
        ));
    }
    #[tokio::test]
    async fn delete_pod_runs_side_effects_after_marking_terminating_with_original_pod() {
        let repo = IntegrationPodDeletionFixture::new_with_delete_side_effect_observation().await;
        let observed = repo.run_delete_side_effect_order_case().await.unwrap();

        assert_eq!(
            observed,
            Some((true, true)),
            "controller Pod delete must run Pod side effects after marking the row terminating and pass the original Pod object with ownerReferences"
        );
    }
    #[tokio::test]
    async fn delete_pod_owned_by_replicaset_enqueues_parent_deployment() {
        let repo = build_deletion_repo_with_dispatcher().await;

        repo.seed_scheduling_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web-recreate",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web-recreate",
                    "namespace": "default",
                    "uid": "deploy-recreate-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "web"}},
                    "strategy": {"type": "Recreate"},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();
        repo.seed_scheduling_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-x",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "rs-x",
                    "namespace": "default",
                    "uid": "rs-x-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "web-recreate",
                        "uid": "deploy-recreate-uid",
                        "controller": true
                    }]
                },
                "spec": {
                    "replicas": 0,
                    "selector": {"matchLabels": {"app": "web"}},
                    "template": {
                        "metadata": {"labels": {"app": "web"}},
                        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();
        repo.persistence
            .seed_pod(
                "default",
                "owned-pod",
                make_pod("owned-pod", Some("rs-x-uid"), Some(("app", "web"))),
            )
            .await
            .unwrap();

        repo.delete_pod("default", "owned-pod").await.unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "ReplicaSet"
                    && key.namespace() == Some("default")
                    && key.name() == "rs-x"
            }),
            "pod delete must still enqueue the owning ReplicaSet"
        );
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "Deployment"
                    && key.namespace() == Some("default")
                    && key.name() == "web-recreate"
            }),
            "pod delete under a ReplicaSet must enqueue the parent Deployment so Recreate rollouts continue after old pods are gone"
        );
    }
    #[tokio::test]
    async fn finalize_pod_deletion_after_actor_cleanup_removes_matching_terminating_pod_by_uid() {
        let repo = build_repo().await;
        let mut pod = make_pod("terminating", None, None);
        pod["metadata"]["uid"] = json!("uid-terminating");
        pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
        pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        pod["spec"]["nodeName"] = json!("worker-a");
        repo.persistence
            .seed_pod("default", "terminating", pod)
            .await
            .unwrap();

        repo.finalize_pod_deletion_after_actor_cleanup("default", "terminating", "uid-terminating")
            .await
            .unwrap();

        assert!(
            repo.query
                .get_pod_by_name("default", "terminating")
                .await
                .unwrap()
                .is_none(),
            "actor finalization should remove matching terminating Pod by UID"
        );
    }
    #[tokio::test]
    async fn finalize_pod_deletion_after_actor_cleanup_deletes_ready_foreground_owner() {
        let repo = build_repo().await;
        repo.seed_scheduling_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "foreground-owner",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "foreground-owner",
                    "namespace": "default",
                    "uid": "foreground-owner-uid",
                    "deletionTimestamp": "2026-05-13T00:00:00Z",
                    "finalizers": ["foregroundDeletion"]
                },
                "spec": {"replicas": 1, "selector": {"app": "foreground-owner"}}
            }),
        )
        .await
        .unwrap();
        let mut pod = make_pod("foreground-child", None, None);
        pod["metadata"]["uid"] = json!("foreground-child-uid");
        pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
        pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        pod["spec"]["nodeName"] = json!("worker-a");
        pod["metadata"]["ownerReferences"] = json!([{
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "foreground-owner",
            "uid": "foreground-owner-uid",
            "controller": true,
            "blockOwnerDeletion": true
        }]);
        repo.persistence
            .seed_pod("default", "foreground-child", pod)
            .await
            .unwrap();

        repo.finalize_pod_deletion_after_actor_cleanup(
            "default",
            "foreground-child",
            "foreground-child-uid",
        )
        .await
        .unwrap();

        assert!(
            repo.query
                .get_pod_by_name("default", "foreground-child")
                .await
                .unwrap()
                .is_none(),
            "actor finalization should remove matching terminating Pod by UID"
        );
        assert!(
            repo.read_non_pod_resource(
                "v1",
                "ReplicationController",
                "default",
                "foreground-owner"
            )
            .await
            .unwrap()
            .is_none(),
            "foreground owner must be removed after its final dependent Pod row is actor-finalized"
        );
    }
    #[tokio::test]
    async fn finalize_pod_deletion_after_actor_cleanup_preserves_finalizer_held_pod() {
        let repo = build_repo().await;
        let mut pod = make_pod("finalized", None, None);
        pod["metadata"]["uid"] = json!("uid-finalized");
        pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
        pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        pod["metadata"]["finalizers"] = json!(["example.com/test-finalizer"]);
        pod["spec"]["nodeName"] = json!("worker-a");
        repo.persistence
            .seed_pod("default", "finalized", pod)
            .await
            .unwrap();

        repo.finalize_pod_deletion_after_actor_cleanup("default", "finalized", "uid-finalized")
            .await
            .unwrap();

        let after = repo
            .query
            .get_pod_by_name("default", "finalized")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.data.pointer("/metadata/finalizers/0"),
            Some(&json!("example.com/test-finalizer"))
        );
    }
    #[tokio::test]
    async fn finalize_pod_deletion_after_actor_cleanup_preserves_replacement_pod() {
        let repo = build_repo().await;
        let mut replacement = make_pod("same-name", None, None);
        replacement["metadata"]["uid"] = json!("uid-new");
        repo.persistence
            .seed_pod("default", "same-name", replacement)
            .await
            .unwrap();

        repo.finalize_pod_deletion_after_actor_cleanup("default", "same-name", "uid-old")
            .await
            .unwrap();

        let after = repo
            .query
            .get_pod_by_name("default", "same-name")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.data.pointer("/metadata/uid"), Some(&json!("uid-new")));
    }
    #[tokio::test]
    async fn pod_delete_enqueues_service_reconcile_for_stale_endpoint_targetref() {
        let repo = build_deletion_repo_with_dispatcher().await;

        repo.seed_scheduling_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "web"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
        repo.seed_scheduling_resource(
            "v1",
            "Endpoints",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "web", "namespace": "default"},
                "subsets": [{
                    "addresses": [{
                        "ip": "10.42.0.50",
                        "targetRef": {
                            "kind": "Pod",
                            "namespace": "default",
                            "name": "stale-ep-pod",
                            "uid": "stale-ep-uid"
                        }
                    }],
                    "ports": [{"port": 80}]
                }]
            }),
        )
        .await
        .unwrap();

        let mut pod = make_pod("stale-ep-pod", None, Some(("app", "web")));
        pod["metadata"]["uid"] = json!("stale-ep-uid");
        pod["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.50",
            "podIPs": [{"ip": "10.42.0.50"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ]
        });
        repo.persistence
            .seed_pod("default", "stale-ep-pod", pod)
            .await
            .unwrap();

        repo.delete_pod("default", "stale-ep-pod").await.unwrap();

        let keys = repo.pending_reconcile_keys().await;
        assert_eq!(
            keys.iter()
                .filter(|key| {
                    key.api_version() == "v1"
                        && key.kind() == "Service"
                        && key.namespace() == Some("default")
                        && key.name() == "web"
                })
                .count(),
            1,
            "stale Endpoints targetRefs should enqueue the owning Service when a Pod is marked terminating"
        );
    }
    #[tokio::test]
    async fn update_pod_owner_references_replaces_list_with_cas() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "or-pod").await;
        let owners = vec![json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "name": "rs-x",
            "uid": "owner-x",
            "controller": true,
        })];
        let updated = repo
            .update_pod_owner_references("default", "or-pod", owners)
            .await
            .unwrap();
        let refs = updated.data["metadata"]["ownerReferences"]
            .as_array()
            .expect("ownerReferences present");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["uid"], json!("owner-x"));
    }
    #[tokio::test]
    async fn merge_pod_labels_preserves_existing_metadata_and_status() {
        let repo = build_repo().await;
        let _ = repo
            .api_mutations
            .create(api_create_request(
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "label-merge", "labels": {"app": "x"}},
                    "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                }),
                false,
            ))
            .await
            .unwrap();
        repo.merge_pod_labels(
            "default",
            "label-merge",
            vec![("pod-template-hash".to_string(), "abc123".to_string())],
        )
        .await
        .unwrap();

        let updated = repo
            .query
            .get_pod_by_name("default", "label-merge")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.data["metadata"]["labels"]["app"], json!("x"));
        assert_eq!(
            updated.data["metadata"]["labels"]["pod-template-hash"],
            json!("abc123")
        );
        assert_eq!(updated.data["metadata"]["name"], json!("label-merge"));
        assert!(updated.data.get("status").is_some());
    }
    #[tokio::test]
    async fn pod_label_change_enqueues_old_and_new_matching_services_once() {
        let repo = build_deletion_repo_with_dispatcher().await;

        repo.seed_scheduling_resource(
            "v1",
            "Service",
            Some("default"),
            "legacy",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "legacy", "namespace": "default"},
                "spec": {
                    "selector": {"app": "legacy"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
        repo.seed_scheduling_resource(
            "v1",
            "Service",
            Some("default"),
            "current",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "current", "namespace": "default"},
                "spec": {
                    "selector": {"app": "current"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();

        let mut pod = make_pod("label-transition", None, Some(("app", "legacy")));
        pod["status"] = json!({
            "phase": "Running",
            "podIP": "10.42.0.60",
            "podIPs": [{"ip": "10.42.0.60"}],
            "hostIP": "10.0.0.10",
            "hostIPs": [{"ip": "10.0.0.10"}],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ]
        });
        repo.persistence
            .seed_pod("default", "label-transition", pod)
            .await
            .unwrap();

        repo.merge_pod_labels(
            "default",
            "label-transition",
            vec![("app".to_string(), "current".to_string())],
        )
        .await
        .unwrap();

        let keys = repo.pending_reconcile_keys().await;
        let legacy_count = keys
            .iter()
            .filter(|key| {
                key.api_version() == "v1"
                    && key.kind() == "Service"
                    && key.namespace() == Some("default")
                    && key.name() == "legacy"
            })
            .count();
        let current_count = keys
            .iter()
            .filter(|key| {
                key.api_version() == "v1"
                    && key.kind() == "Service"
                    && key.namespace() == Some("default")
                    && key.name() == "current"
            })
            .count();
        assert_eq!(legacy_count, 1, "old selector match should enqueue once");
        assert_eq!(current_count, 1, "new selector match should enqueue once");
        assert_eq!(
            keys.iter()
                .filter(|key| key.api_version() == "v1" && key.kind() == "Service")
                .count(),
            2,
            "only the old and new matching Services should be enqueued"
        );
    }
    #[tokio::test]
    async fn update_pod_owner_references_returns_conflict_on_stale_rv() {
        let repo = build_repo().await;
        let _ = create_basic_pod_via_api(&repo, "or-race").await;

        // Two writers see the same RV. First writer wins; second loses CAS.
        let owners1 = vec![
            json!({"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs1","uid":"u1","controller":true}),
        ];
        repo.update_pod_owner_references("default", "or-race", owners1.clone())
            .await
            .expect("first writer wins");

        // The second update_pod_owner_references reads the live RV, so it
        // succeeds — the trait method does its own read-modify-write. To
        // observe a real CAS conflict, drive the store directly with a stale RV.
        let stale = repo
            .query
            .get_pod_by_name("default", "or-race")
            .await
            .unwrap()
            .unwrap();
        let mut tampered: serde_json::Value = (*stale.data).clone();
        tampered["metadata"]["labels"] = json!({"app": "tamper"});
        let mut stale_request = stale.clone();
        stale_request.resource_version = 1;
        let conflict = repo
            .api_mutations
            .update_pod("default", "or-race", tampered, stale_request, false)
            .await;
        assert!(conflict.unwrap_err().to_string().contains("409"));
    }
    #[tokio::test]
    async fn pod_store_update_status_with_concurrent_writer_returns_conflict() {
        // Two readers see the same resource_version. The first writer wins.
        // The second writer must observe a 409 Conflict.
        let store = IntegrationPodDeletionFixture::new_inline().await;

        let created = store
            .persistence
            .seed_pod("default", "racer", make_pod("racer", None, None))
            .await
            .unwrap();
        let snapshot_rv = created.resource_version;

        // Reader 1 → writer 1 (succeeds)
        store
            .api_ports()
            .replace_status_from_api("default", "racer", json!({"phase": "Running"}), snapshot_rv)
            .await
            .expect("first writer succeeds with the snapshot rv");

        // Reader 2 → writer 2 (still using the old snapshot rv)
        let conflict = store
            .api_ports()
            .replace_status_from_api("default", "racer", json!({"phase": "Failed"}), snapshot_rv)
            .await;
        let err = conflict.expect_err("second writer must lose CAS");
        assert!(
            err.to_string().contains("409"),
            "expected 409 Conflict, got {err:?}"
        );
    }
    #[tokio::test]
    async fn deletion_finalizer_without_outbox_retries_later() {
        let resource_version = 13;
        let pod_resource = klights::datastore::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "leader-finalize-no-outbox".to_string(),
            uid: "uid-leader-finalize-no-outbox".to_string(),
            resource_version,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "leader-finalize-no-outbox",
                    "uid": "uid-leader-finalize-no-outbox",
                    "resourceVersion": resource_version.to_string(),
                    "deletionTimestamp": "2026-05-13T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-no-outbox", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Running"}
            })),
        };
        let finalizer = IntegrationPodDeletionFixture::new_cluster_backed(Arc::new(
            FakeLeaderApiClient::new(pod_resource),
        ))
        .await;

        let result = finalizer
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "leader-finalize-no-outbox",
                "uid-leader-finalize-no-outbox",
            )
            .await;

        let result = result.expect_err("finalization must not succeed without outbox");
        assert!(
            result.to_string().contains("outbox"),
            "finalization should return outbox-retry error when outbox is unavailable"
        );

        let pod_row = finalizer
            .query
            .get_pod_by_name("default", "leader-finalize-no-outbox")
            .await
            .unwrap();
        assert!(
            pod_row.is_some(),
            "pod must remain when non-leader finalization is rejected"
        );
    }
    #[tokio::test]
    async fn deletion_finalizer_reissues_missing_delete_mark_through_outbox() {
        let pod_resource = klights::datastore::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "missing-delete-mark".to_string(),
            uid: "uid-missing-delete-mark".to_string(),
            resource_version: 21,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "missing-delete-mark",
                    "uid": "uid-missing-delete-mark",
                    "resourceVersion": "21"
                },
                "spec": {
                    "nodeName": "worker-1",
                    "terminationGracePeriodSeconds": 7,
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            })),
        };
        let finalizer =
            IntegrationPodWorkerFixture::new(Arc::new(FakeLeaderApiClient::new(pod_resource)))
                .await;

        let result = finalizer
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "missing-delete-mark",
                "uid-missing-delete-mark",
            )
            .await
            .expect("missing delete mark should enqueue a leader-routed delete mark");

        assert_eq!(
            result,
            super::super::assembly_support::support::PodFinalizationOutcome::FinalizersPending
        );
        let row = finalizer
            .claim_next_due_outbox(i64::MAX / 4, 1_000, "assert")
            .await
            .expect("claim outbox")
            .expect("delete-mark row enqueued");
        assert_eq!(row.operation, "PodMetadata");
        assert_eq!(row.pod_uid, "uid-missing-delete-mark");
        match row.command {
            super::super::assembly_support::support::PodOutboxCommand::DeleteMarkPatch {
                api_version,
                kind,
                namespace,
                name,
                patch_kind,
                pod_uid,
                resource_version,
                strict_resource_version,
                grace_period_seconds,
                has_deletion_timestamp,
            } => {
                assert_eq!(api_version, "v1");
                assert_eq!(kind, "Pod");
                assert_eq!(namespace.as_deref(), Some("default"));
                assert_eq!(name, "missing-delete-mark");
                assert_eq!(patch_kind, klights_cluster_core::PatchKind::Merge);
                assert_eq!(pod_uid, "uid-missing-delete-mark");
                assert_eq!(resource_version, None);
                assert!(!strict_resource_version);
                assert_eq!(grace_period_seconds, 7);
                assert!(has_deletion_timestamp);
            }
            other => panic!("expected Pod PatchResource outbox command, got {other:?}"),
        }
    }
    #[tokio::test]
    async fn deletion_finalizer_preserves_replacement_pod() {
        let finalizer = build_repo().await;

        // Create a replacement pod with a different UID.
        let replacement = make_terminating_pod("same-name", "uid-new");
        finalizer
            .persistence
            .seed_pod("default", "same-name", replacement)
            .await
            .unwrap();

        let result = finalizer
            .finalize_pod_deletion_after_actor_cleanup("default", "same-name", "uid-old")
            .await
            .unwrap();

        // Old UID is gone → DeletedOrAlreadyGone. Replacement pod must survive.
        assert_eq!(
            result,
            super::super::assembly_support::support::PodFinalizationOutcome::DeletedOrAlreadyGone
        );

        let after = finalizer
            .query
            .get_pod_by_name("default", "same-name")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.uid, "uid-new", "replacement pod must be preserved");
    }
    #[tokio::test]
    async fn deletion_finalizer_waits_for_finalizers() {
        let finalizer = build_repo().await;

        let mut pod = make_terminating_pod("finalizer-held", "uid-held");
        pod["metadata"]["finalizers"] = json!(["example.com/test-finalizer"]);
        pod["spec"]["nodeName"] = json!("worker-a");
        finalizer
            .persistence
            .seed_pod("default", "finalizer-held", pod)
            .await
            .unwrap();

        let result = finalizer
            .finalize_pod_deletion_after_actor_cleanup("default", "finalizer-held", "uid-held")
            .await
            .unwrap();

        // Finalizers still present → FinalizersPending.
        assert_eq!(
            result,
            super::super::assembly_support::support::PodFinalizationOutcome::FinalizersPending
        );

        let after = finalizer
            .query
            .get_pod_by_name("default", "finalizer-held")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.uid, "uid-held",
            "pod with finalizers must not be deleted"
        );
    }
    #[tokio::test]
    async fn deletion_finalizer_reissues_uid_delete_when_same_uid_lacks_delete_mark() {
        let repo = build_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "remark-delete",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "remark-delete",
                        "namespace": "default",
                        "uid": "uid-remark-delete"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx:latest"}]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .unwrap();

        let finalized = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "remark-delete",
                "uid-remark-delete",
            )
            .await
            .unwrap();

        assert_eq!(
            finalized,
            super::super::assembly_support::support::PodFinalizationOutcome::FinalizersPending,
            "same-UID non-terminating row must be retried, not treated as finalized"
        );
        let marked = repo
            .query
            .get_pod_by_name("default", "remark-delete")
            .await
            .unwrap()
            .expect("pod should remain after delete mark retry");
        assert_eq!(marked.uid, "uid-remark-delete");
        assert!(
            marked
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some(),
            "finalizer retry must restore a visible deletionTimestamp"
        );

        let finalized_after_mark = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "remark-delete",
                "uid-remark-delete",
            )
            .await
            .unwrap();
        assert_eq!(
            finalized_after_mark,
            super::super::assembly_support::support::PodFinalizationOutcome::DeletedOrAlreadyGone,
            "retry after the delete mark should complete actor-owned row removal"
        );
        assert!(
            repo.query
                .get_pod_by_name("default", "remark-delete")
                .await
                .unwrap()
                .is_none(),
            "actor-owned finalization should remove the same UID after the mark is visible"
        );
    }
    #[tokio::test]
    async fn deletion_finalizer_deletes_node_lost_terminal_with_uid_after_actor_cleanup() {
        let repo = build_repo().await;
        repo.persistence
            .seed_pod(
                "default",
                "node-lost-local-cleanup",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "node-lost-local-cleanup",
                        "namespace": "default",
                        "uid": "uid-node-lost-local-cleanup"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx:latest"}]
                    },
                    "status": {"phase": "Failed", "reason": "NodeLost"}
                }),
            )
            .await
            .unwrap();

        let finalized = repo
            .finalize_pod_deletion_after_actor_cleanup(
                "default",
                "node-lost-local-cleanup",
                "uid-node-lost-local-cleanup",
            )
            .await
            .unwrap();

        assert_eq!(
            finalized,
            super::super::assembly_support::support::PodFinalizationOutcome::DeletedOrAlreadyGone,
            "NodeLost terminal cleanup without deletionTimestamp should complete actor-owned finalization"
        );
        assert!(
            repo.query
                .get_pod_by_name("default", "node-lost-local-cleanup")
                .await
                .unwrap()
                .is_none(),
            "actor-owned NodeLost finalization must remove the same UID row"
        );
    }
    #[tokio::test]
    async fn emptydir_survivor_diagnosis_records_mark_workqueue_and_actor_state() {
        let repo = IntegrationPodDeletionFixture::new_with_gc_workqueue().await;

        let ns = "emptydir-diag";
        repo.seed_namespace(ns, json!({"metadata": {"name": ns}}))
            .await
            .unwrap();

        // RC owner.
        let rc_uid = "rc-uid-diag";
        repo.seed_scheduling_resource(
            "v1",
            "ReplicationController",
            Some(ns),
            "rc",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {"name": "rc", "namespace": ns, "uid": rc_uid},
                "spec": {"replicas": 5},
            }),
        )
        .await
        .unwrap();

        // Five Running, picked-up (spec.nodeName set) child Pods owned by the RC.
        const CHILDREN: usize = 5;
        for i in 0..CHILDREN {
            let name = format!("rc-pod-{i}");
            let uid = format!("pod-uid-{i}");
            repo.persistence
                .seed_pod(
                    ns,
                    &name,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": name,
                            "namespace": ns,
                            "uid": uid,
                            "ownerReferences": [{
                                "apiVersion": "v1",
                                "kind": "ReplicationController",
                                "name": "rc",
                                "uid": rc_uid,
                                "controller": true,
                            }],
                        },
                        "spec": {"nodeName": "node-a", "containers": [{"name": "c", "image": "x"}]},
                        "status": {"phase": "Running"},
                    }),
                )
                .await
                .unwrap();
        }

        // Drive the real RC background-delete cascade (one-shot inline call, the
        // path inners.rs runs after hard-deleting the RC row).
        repo.run_gc_cascade(rc_uid, "v1", "rc", "ReplicationController", ns)
            .await
            .expect("cascade must not error");

        // Leg (1): did every child receive metadata.deletionTimestamp?
        let mut marked = 0usize;
        for i in 0..CHILDREN {
            let name = format!("rc-pod-{i}");
            let pod = repo
                .query
                .get_pod_by_name(ns, &name)
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!(
                        "child {name} must still have a datastore row (only the actor may remove it)"
                    )
                });
            if pod
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
            {
                marked += 1;
            }
        }

        // Leg (2): drain pod_workqueue and record which children got a UID-bound
        // Pod-kind row (claim with a far-future clock so grace-delayed rows count).
        let mut enqueued_uids = std::collections::HashSet::new();
        for _ in 0..(CHILDREN * 4) {
            match repo.claim_uid_bound_pod_work().await.unwrap() {
                Some(entry) if entry.namespace == ns => {
                    enqueued_uids.insert(entry.uid);
                }
                Some(_) => continue,
                None => break,
            }
        }

        // Diagnosis assertions — these LOCK the leader-side contract. If a future
        // change regresses cascade enumeration, marking, or the UID-bound enqueue,
        // this fails and re-opens leg (1)/(2). A green run proves the deterministic
        // prod survivor is NOT a leader-side mark/enqueue gap, so the C fix must
        // target leg (3): workqueue -> actor / remote-worker finalization
        // convergence.
        assert_eq!(
            marked, CHILDREN,
            "cascade-did-not-mark: only {marked}/{CHILDREN} children got deletionTimestamp"
        );
        for i in 0..CHILDREN {
            let uid = format!("pod-uid-{i}");
            assert!(
                enqueued_uids.contains(&uid),
                "mark-without-workqueue: child {uid} marked terminating but no UID-bound pod_workqueue row"
            );
        }
    }
    #[tokio::test]
    async fn gc_marked_pod_enqueues_uid_bound_workqueue_entry() {
        let repo = IntegrationPodDeletionFixture::new_with_gc_workqueue().await;

        let ns = "gc-mark-enqueue";
        repo.seed_namespace(ns, json!({"metadata": {"name": ns}}))
            .await
            .unwrap();
        repo.persistence
            .seed_pod(
                ns,
                "picked-up",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "picked-up", "namespace": ns, "uid": "uid-gc"},
                    "spec": {"nodeName": "node-a", "containers": [{"name": "c", "image": "x"}]},
                    "status": {"phase": "Running"},
                }),
            )
            .await
            .unwrap();

        repo.request_gc_pod_delete(ns, "picked-up", "uid-gc")
            .await
            .unwrap();

        let pod = repo
            .query
            .get_pod_by_name(ns, "picked-up")
            .await
            .unwrap()
            .expect("GC must only mark a picked-up Pod, not hard-delete it");
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty()),
            "GC delete must mark the Pod terminating"
        );

        let row = repo
            .claim_uid_bound_pod_work()
            .await
            .unwrap()
            .expect("GC mark must create a UID-bound pod_workqueue row");
        assert_eq!(row.namespace, ns);
        assert_eq!(row.name, "picked-up");
        assert_eq!(row.uid, "uid-gc");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&row.payload).unwrap()["target_node"]
                .as_str(),
            Some("node-a"),
            "workqueue row must target the owning kubelet actor"
        );
    }
    #[tokio::test]
    async fn old_uid_operations_do_not_mutate_replacement() {
        let repo = build_repo().await;
        let ns = "default";
        let stale_uid = "uid-old";

        // --- update_pod_owner_references_for_uid ---
        {
            let name = "owner-refs";
            let before = create_replacement_pod(&repo, ns, name).await;
            let err = repo
                .update_pod_owner_references_for_uid(
                    ns,
                    name,
                    stale_uid,
                    vec![json!({
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "rs-attacker",
                        "uid": "rs-uid-attacker",
                        "controller": true
                    })],
                )
                .await;
            assert!(
                err.is_err(),
                "stale UID update_pod_owner_references_for_uid must be rejected"
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);
        }

        // --- merge_pod_labels_for_uid ---
        {
            let name = "labels";
            let before = create_replacement_pod(&repo, ns, name).await;
            let err = repo
                .merge_pod_labels_for_uid(
                    ns,
                    name,
                    stale_uid,
                    vec![
                        ("app".to_string(), "attacker".to_string()),
                        ("env".to_string(), "staging".to_string()),
                    ],
                )
                .await;
            assert!(
                err.is_err(),
                "stale UID merge_pod_labels_for_uid must be rejected"
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);
        }

        // --- set_pod_status_for_uid ---
        {
            let name = "status";
            let before = create_replacement_pod(&repo, ns, name).await;
            let update = super::super::assembly_support::support::PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.42.0.99".to_string(),
                host_ip: "192.168.1.99".to_string(),
                container_statuses: vec![],
                init_container_statuses: None,
                qos_class: None,
            };
            let err = repo
                .status_ports()
                .set_pod_status_for_uid(ns, name, stale_uid, update, None)
                .await;
            assert!(
                err.is_err(),
                "stale UID set_pod_status_for_uid must be rejected"
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);
        }

        // --- mark_start_pending_for_retry_for_uid ---
        {
            let name = "retry-status";
            let before = create_replacement_pod(&repo, ns, name).await;
            let err = repo
                .status_ports()
                .mark_start_pending_for_retry_for_uid(
                    ns,
                    name,
                    stale_uid,
                    "Failed to pull image \"nginx:1.27\": connection refused",
                )
                .await;
            assert!(
                err.is_err(),
                "stale UID mark_start_pending_for_retry_for_uid must be rejected"
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);
        }

        // --- set_probe_readiness_for_uid ---
        {
            let name = "probe-readiness";
            let before = create_replacement_pod(&repo, ns, name).await;
            let err = repo
                .status_ports()
                .set_probe_readiness_for_uid(ns, name, stale_uid, "app", false, None)
                .await;
            assert!(
                err.is_err(),
                "stale UID set_probe_readiness_for_uid must be rejected"
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);

            let updated = repo
                .status_ports()
                .set_probe_readiness_for_uid(ns, name, "uid-new", "app", false, None)
                .await
                .expect("matching UID readiness update must succeed");
            assert_eq!(updated.uid, "uid-new");
            assert!(updated.resource_version > before.resource_version);
            assert_eq!(
                updated.data.pointer("/status/conditions/0/status"),
                Some(&json!("False")),
                "matching UID readiness command must update the owning pod"
            );
        }

        // --- finalize_bound_with_uid ---
        {
            let name = "delete";
            let before = create_replacement_pod(&repo, ns, name).await;
            let outcome = repo
                .finalize_bound_pod_after_actor_cleanup(ns, name, stale_uid)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                super::super::assembly_support::support::BoundPodDeleteOutcome::IdentityChanged
            );
            let live = repo
                .query
                .get_pod_by_name(ns, name)
                .await
                .unwrap()
                .expect("pod exists");
            assert_replacement_unchanged(&live, &before);
        }
    }
    #[tokio::test]
    async fn deferred_delete_preserves_same_name_replacement() {
        let store =
            super::super::assembly_support::support::IntegrationPodStoreFixture::new().await;

        // (1) Picked-up terminating Pod: nodeName is set → DeferToActor.
        {
            let mut pod = make_terminating_pod("picked-up", "uid-picked");
            pod["spec"]["nodeName"] = json!("node-a");
            let created = store
                .persistence
                .seed_pod("default", "picked-up", pod)
                .await
                .unwrap();

            let outcome = store
                .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                    "default",
                    "picked-up",
                    "uid-picked",
                    created.resource_version,
                )
                .await
                .unwrap();
            assert_eq!(
                outcome,
                UnscheduledPodDeletionOutcome::DeferToActor,
                "a Pod bound to a kubelet must only be removed by its lifecycle actor"
            );
            let live = store
                .query
                .get_pod_by_name("default", "picked-up")
                .await
                .unwrap()
                .expect("picked-up Pod row must survive deferred delete");
            assert_eq!(live.uid, "uid-picked");
        }

        // (2) Same-name replacement: old-UID deferred entry must not touch the
        //     live replacement Pod. delete_unscheduled_with_uid returns Removed
        //     (the old UID is already gone) without deleting the replacement.
        {
            let replacement = make_terminating_pod("replaced", "uid-new");
            let created = store
                .persistence
                .seed_pod("default", "replaced", replacement)
                .await
                .unwrap();

            let outcome = store
                .delete_unscheduled_pod_with_uid_and_observed_resource_version(
                    "default",
                    "replaced",
                    "uid-old",
                    created.resource_version,
                )
                .await
                .unwrap();
            assert_eq!(
                outcome,
                UnscheduledPodDeletionOutcome::Removed,
                "stale-UID deferred delete must report Removed without touching the replacement"
            );
            let live = store
                .query
                .get_pod_by_name("default", "replaced")
                .await
                .unwrap()
                .expect("replacement Pod must survive stale-UID deferred delete");
            assert_eq!(live.uid, "uid-new", "replacement UID must be preserved");
        }
    }
    #[tokio::test]
    async fn unscheduled_delete_compare_and_swap_rejects_node_bind_race() {
        let outcome = super::super::assembly_support::support::run_unscheduled_pod_delete_cas_race(
            "bind-race",
            "uid-bind",
            super::super::assembly_support::support::PodDeleteCasRaceKind::SchedulerBind,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.disposition,
            UnscheduledPodDeletionOutcome::Retry,
            "node-bind race must lose the CAS and retry from the newly bound observation"
        );
        assert!(
            outcome.raced,
            "test proposer must have raced a nodeName bind before the CAS delete"
        );

        let live = outcome.live;
        assert_eq!(live.uid, "uid-bind");
        assert_eq!(
            live.data.pointer("/spec/nodeName").and_then(|v| v.as_str()),
            Some("node-bound-by-scheduler"),
            "the racing scheduler bind must be visible on the surviving Pod"
        );
    }
    #[tokio::test]
    async fn unscheduled_delete_compare_and_swap_rejects_resource_version_race() {
        let outcome = super::super::assembly_support::support::run_unscheduled_pod_delete_cas_race(
            "rv-race",
            "uid-rv",
            super::super::assembly_support::support::PodDeleteCasRaceKind::StatusUpdate,
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.disposition,
            UnscheduledPodDeletionOutcome::Retry,
            "resourceVersion race must lose the CAS and retry from a fresh observation"
        );
        assert!(
            outcome.raced,
            "test proposer must have advanced resourceVersion before the CAS delete"
        );

        let created_resource_version = outcome.created_resource_version;
        let live = outcome.live;
        assert_eq!(live.uid, "uid-rv");
        assert!(
            live.resource_version > created_resource_version,
            "the racing status write must have advanced the resourceVersion"
        );
    }
    #[tokio::test]
    async fn bound_actor_finalization_compare_and_swap_rejects_resource_version_race() {
        let outcome = super::super::assembly_support::support::run_bound_pod_delete_cas_race(
            "bound-rv-race",
            "uid-bound-rv",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome.disposition,
            super::super::assembly_support::support::BoundPodDeleteOutcome::Retry
        );
        assert!(
            outcome.raced,
            "test proposer must advance resourceVersion before actor delete CAS"
        );
        let created_resource_version = outcome.created_resource_version;
        let live = outcome.live;
        assert_eq!(live.uid, "uid-bound-rv");
        assert!(live.resource_version > created_resource_version);
    }
}
