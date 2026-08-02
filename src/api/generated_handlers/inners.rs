//! Test adapters for generic-command finalizer regressions.

#[cfg(test)]
mod test_impl {

    use std::sync::Arc;

    use klights_cluster_core::{Resource, ResourcePreconditions};

    use crate::api::{ApiState, AppError};
    use crate::datastore::DatastoreBackend;

    pub use k8s_native_service::generic_command::DeleteCompletion;

    pub struct GeneratedDeleteCompletionRequest<'a> {
        pub target: k8s_native_service::generic_command::ResourceDeleteTarget<'a>,
        pub initial_resource: Resource,
        pub delete_preconditions: ResourcePreconditions,
        pub orphan_children_before_completion: bool,
        pub uid_mismatch_is_conflict: bool,
    }

    pub async fn mark_foreground_deletion_with_retry(
        db: &dyn DatastoreBackend,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        initial_resource: Resource,
        delete_preconditions: ResourcePreconditions,
    ) -> Result<Resource, AppError> {
        let lifecycle =
            crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(db);
        k8s_native_service::generic_command::mark_foreground_deletion_with_retry(
            &lifecycle,
            api_version,
            kind,
            namespace,
            name,
            initial_resource,
            delete_preconditions,
            chrono::DateTime::from_timestamp(1_700_000_000, 0)
                .expect("fixed finalizer test timestamp"),
        )
        .await
    }

    pub async fn complete_non_foreground_delete_with_live_recheck(
        db: &dyn DatastoreBackend,
        request: GeneratedDeleteCompletionRequest<'_>,
    ) -> Result<DeleteCompletion, AppError> {
        let lifecycle =
            crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(db);
        k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
            &lifecycle,
            k8s_native_service::generic_command::NonForegroundDeleteRequest {
                target: request.target,
                initial_resource: request.initial_resource,
                delete_preconditions: request.delete_preconditions,
                orphan_children_before_completion: request.orphan_children_before_completion,
                uid_mismatch_is_conflict: request.uid_mismatch_is_conflict,
                grace_seconds: 0,
                operation_now: chrono::DateTime::from_timestamp(1_700_000_000, 0)
                    .expect("fixed finalizer test timestamp"),
            },
        )
        .await
    }

    pub(in crate::api) async fn delete_collection_listed_resource_inner(
        state: Arc<ApiState>,
        api_version: &'static str,
        kind: &'static str,
        namespace: Option<&str>,
        resource: Resource,
    ) -> Result<bool, AppError> {
        let resource_name = resource.name.clone();
        let resource_uid = resource.uid.clone();
        let strategy = k8s_native_service::generic_command::FinalizerAwareDeleteStrategy {
            resource_query: state.resource_mutation().resource_query.as_ref(),
            lifecycle: state.resource_mutation().finalizer_lifecycle.as_ref(),
            operation_now: klights_auth::clock::chrono_utc(state.operational().clock.now()),
        };
        let target = klights_types::ResourceKey::new(
            api_version,
            kind,
            namespace.map(str::to_string),
            resource_name,
        );
        let intent = k8s_native_service::generic_command::DeleteIntent::collection_item(
            k8s_native_service::generic_command::DryRunMode::Live,
            ResourcePreconditions::uid(resource_uid),
        );
        match k8s_native_service::generic_command::delete_loaded_with_strategy(
            &strategy, target, resource, &intent,
        )
        .await?
        {
            k8s_native_service::generic_command::DeleteResult::HardDeleted(resource) => {
                if api_version == "v1"
                    && kind == "Node"
                    && let Err(error) = state
                        .resource_mutation()
                        .generated_lifecycle
                        .delete_node_cleanup_intents(resource.name.clone())
                        .await
                {
                    tracing::warn!(node = %resource.name, error = ?error, "failed to delete pod cleanup intents for deleted node");
                }
                Ok(true)
            }
            k8s_native_service::generic_command::DeleteResult::MarkedTerminating(_)
            | k8s_native_service::generic_command::DeleteResult::GoneOrUidChanged => Ok(false),
        }
    }

    mod command_regressions {
        use super::*;
        use axum::body::Bytes;
        use axum::http::{HeaderMap, StatusCode};
        use base64::Engine;
        use k8s_native_service::generic_command::{
            CreateUpdateQuery, GeneratedDeleteInnerRequest, GeneratedNamedResource,
            GeneratedPatchInnerRequest, GeneratedUpdateInnerRequest, create_inner, delete_inner,
            patch_inner, update_inner,
        };
        use serde_json::{Value, json};

        fn default_query() -> CreateUpdateQuery {
            CreateUpdateQuery {
                dry_run: None,
                field_manager: None,
                field_validation: None,
                force: None,
                orphan_dependents: None,
                propagation_policy: None,
                grace_period_seconds: None,
            }
        }

        fn aggregate_widgets_rule() -> Value {
            json!({
                "verbs": ["get", "list"],
                "apiGroups": ["example.klights.io"],
                "resources": ["widgets"]
            })
        }

        async fn seeded_rbac_state() -> Arc<ApiState> {
            let state = Arc::new(crate::api::test_support::build_test_app_state().await);
            klights_controllers::rbac_reconcile::reconcile_default_rbac_objects(
                state.resource_mutation().db.as_ref(),
            )
            .await
            .expect("seed default RBAC");
            state
        }

        async fn create_labeled_aggregate_source(state: &Arc<ApiState>, name: &str, rule: Value) {
            state
                .resource_mutation()
                .db
                .create_resource(
                    "rbac.authorization.k8s.io/v1",
                    "ClusterRole",
                    None,
                    name,
                    json!({
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "ClusterRole",
                        "metadata": {
                            "name": name,
                            "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
                        },
                        "rules": [rule]
                    }),
                )
                .await
                .expect("create aggregate source");
            klights_controllers::rbac_reconcile::reconcile_cluster_role_aggregation(
                state.resource_mutation().db.as_ref(),
            )
            .await
            .expect("seed aggregate rules");
        }

        async fn view_has_rule(state: &Arc<ApiState>, expected: &Value) -> bool {
            let view = state
                .resource_mutation()
                .db
                .get_resource("rbac.authorization.k8s.io/v1", "ClusterRole", None, "view")
                .await
                .expect("read view")
                .expect("view ClusterRole exists");
            view.data
                .get("rules")
                .and_then(Value::as_array)
                .expect("view should have rules")
                .iter()
                .any(|rule| rule == expected)
        }

        fn kubelet_client_csr_b64(node_name: &str) -> String {
            use rcgen::{CertificateParams, DnType, KeyPair};

            let mut params = CertificateParams::default();
            params.distinguished_name = rcgen::DistinguishedName::new();
            params
                .distinguished_name
                .push(DnType::CommonName, format!("system:node:{node_name}"));
            params
                .distinguished_name
                .push(DnType::OrganizationName, "system:nodes".to_string());
            let key_pair = KeyPair::generate().expect("test keypair");
            let csr_pem = params
                .serialize_request(&key_pair)
                .expect("test CSR")
                .pem()
                .expect("CSR PEM");
            base64::engine::general_purpose::STANDARD.encode(csr_pem.as_bytes())
        }

        #[tokio::test]
        async fn create_certificate_signing_request_dispatches_csr_signer() {
            let mut state = crate::api::test_support::build_test_app_state().await;
            let signer = Arc::new(crate::api::test_support::RecordingCsrSigner::new());
            let issuer = Arc::new(crate::bootstrap::auth_adapters::AuthCsrIssuer::new(
                signer.clone(),
                Arc::new(klights_auth::clock::SystemClock),
                state.operational().task_supervisor.clone(),
            ));
            let dispatcher = Arc::new(crate::controllers::ControllerDispatcher::new_with_nodeport(
                state.controller_reconcile().service_ipam.clone(),
                state.controller_reconcile().nodeport_alloc.clone(),
                state.operational().task_supervisor.clone(),
                Some(issuer),
                crate::controllers::test_utils::deterministic_controller_identity(),
            ));
            dispatcher
                .set_sync_context(
                    state.resource_mutation().db.clone(),
                    state.operational().config.node_name.clone(),
                )
                .await;
            dispatcher
                .set_pod_repository(state.resource_mutation().pod_repository.clone())
                .await;
            state.controller_reconcile_mut().controller_dispatcher = dispatcher;
            let state = Arc::new(state);
            let body = json!({
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": {"name": "node-bootstrap-csr"},
                "spec": {
                    "request": kubelet_client_csr_b64("mn-worker"),
                    "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                    "usages": ["client auth"]
                }
            });
            let identity = klights_auth::AuthenticatedIdentity::bootstrap(
                "abcdef",
                &["system:bootstrappers:klights:worker".to_string()],
            );
            let (status, _) = create_inner(
                state.clone(),
                &identity,
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                default_query(),
                body,
            )
            .await
            .expect("create CSR");
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(signer.request_count(), 1);
            let stored = state
                .resource_mutation()
                .db
                .get_resource(
                    "certificates.k8s.io/v1",
                    "CertificateSigningRequest",
                    None,
                    "node-bootstrap-csr",
                )
                .await
                .expect("read CSR")
                .expect("CSR exists");
            assert!(stored.data.pointer("/status/certificate").is_some());
        }

        #[tokio::test]
        async fn apply_create_csr_cannot_forge_spec_identity() {
            let state = Arc::new(crate::api::test_support::build_test_app_state().await);
            let forged = json!({
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": {"name": "apply-forge-csr"},
                "spec": {
                    "request": kubelet_client_csr_b64("victim"),
                    "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                    "usages": ["client auth"],
                    "username": "system:node:victim",
                    "groups": ["system:nodes"],
                    "uid": "forged-uid"
                }
            });
            let mut headers = HeaderMap::new();
            headers.insert(
                "content-type",
                "application/apply-patch+yaml".parse().unwrap(),
            );
            let identity = klights_auth::AuthenticatedIdentity::bootstrap(
                "abcdef",
                &["system:bootstrappers:klights:worker".to_string()],
            );
            let _ = patch_inner(
                state.clone(),
                &identity,
                GeneratedPatchInnerRequest {
                    target: GeneratedNamedResource::new(
                        "certificates.k8s.io/v1",
                        "CertificateSigningRequest",
                        None,
                        "apply-forge-csr",
                    ),
                    query: default_query(),
                    headers,
                    body: Bytes::from(serde_json::to_vec(&forged).unwrap()),
                },
            )
            .await
            .expect("apply-create CSR");
            let stored = state
                .resource_mutation()
                .db
                .get_resource(
                    "certificates.k8s.io/v1",
                    "CertificateSigningRequest",
                    None,
                    "apply-forge-csr",
                )
                .await
                .expect("read CSR")
                .expect("CSR exists");
            assert_eq!(
                stored
                    .data
                    .pointer("/spec/username")
                    .and_then(Value::as_str),
                Some(identity.username.as_str())
            );
            assert!(
                !stored
                    .data
                    .pointer("/spec/groups")
                    .and_then(Value::as_array)
                    .is_some_and(|groups| groups
                        .iter()
                        .any(|group| group.as_str() == Some("system:nodes")))
            );
        }

        #[tokio::test]
        async fn cluster_role_commands_reconcile_aggregation_immediately() {
            let state = seeded_rbac_state().await;
            let rule = aggregate_widgets_rule();
            let identity = crate::api::test_support::test_admin("test-admin");
            let (status, _) = create_inner(
                state.clone(),
                &identity,
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                default_query(),
                json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": "aggregate-widgets-view",
                        "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
                    },
                    "rules": [rule.clone()]
                }),
            )
            .await
            .expect("create aggregating ClusterRole");
            assert_eq!(status, StatusCode::CREATED);
            assert!(view_has_rule(&state, &rule).await);

            let _ = update_inner(
                state.clone(),
                &identity,
                GeneratedUpdateInnerRequest {
                    target: GeneratedNamedResource::new(
                        "rbac.authorization.k8s.io/v1",
                        "ClusterRole",
                        None,
                        "aggregate-widgets-view",
                    ),
                    query: default_query(),
                    body: json!({
                        "apiVersion": "rbac.authorization.k8s.io/v1",
                        "kind": "ClusterRole",
                        "metadata": {"name": "aggregate-widgets-view"},
                        "rules": [rule.clone()]
                    }),
                },
            )
            .await
            .expect("remove aggregate label");
            assert!(!view_has_rule(&state, &rule).await);

            create_labeled_aggregate_source(&state, "aggregate-widgets-view-2", rule.clone()).await;
            assert!(view_has_rule(&state, &rule).await);
            let _ = delete_inner(
                state.clone(),
                &identity,
                GeneratedDeleteInnerRequest {
                    target: GeneratedNamedResource::new(
                        "rbac.authorization.k8s.io/v1",
                        "ClusterRole",
                        None,
                        "aggregate-widgets-view-2",
                    ),
                    query: default_query(),
                    body: Bytes::new(),
                },
            )
            .await
            .expect("delete aggregate source");
            assert!(!view_has_rule(&state, &rule).await);
        }

        #[tokio::test]
        async fn foreground_delete_returns_before_synchronous_pod_cascade() {
            let mut app_state = crate::api::test_support::build_test_app_state().await;
            let release_workqueue = Arc::new(tokio::sync::Notify::new());
            let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig {
                    pod_delete_workqueue: 1,
                    ..Default::default()
                },
            ));
            let held_workqueue = task_supervisor
                .spawn_async(
                    klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                    "hold_foreground_delete_workqueue_for_test",
                    {
                        let release_workqueue = release_workqueue.clone();
                        async move { release_workqueue.notified().await }
                    },
                )
                .await
                .expect("hold pod-delete workqueue permit");
            app_state.operational_mut().task_supervisor = task_supervisor;
            let state = Arc::new(app_state);
            let owner_uid = "fg-rc-owner-uid";
            state
                .resource_mutation()
                .db
                .create_resource(
                    "v1",
                    "ReplicationController",
                    Some("default"),
                    "fg-rc",
                    json!({
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "metadata": {"name": "fg-rc", "namespace": "default", "uid": owner_uid}
                    }),
                )
                .await
                .expect("create foreground RC");
            for index in 0..3 {
                let pod_name = format!("fg-rc-pod-{index}");
                state
                    .resource_mutation()
                    .db
                    .create_resource(
                        "v1",
                        "Pod",
                        Some("default"),
                        &pod_name,
                        json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": pod_name,
                                "namespace": "default",
                                "uid": format!("fg-rc-pod-{index}-uid"),
                                "ownerReferences": [{
                                    "apiVersion": "v1",
                                    "kind": "ReplicationController",
                                    "name": "fg-rc",
                                    "uid": owner_uid,
                                    "blockOwnerDeletion": true
                                }]
                            },
                            "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                        }),
                    )
                    .await
                    .expect("create RC child Pod");
            }
            let identity = crate::api::test_support::test_admin("test-admin");
            let (status, body) = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                delete_inner(
                    state.clone(),
                    &identity,
                    GeneratedDeleteInnerRequest {
                        target: GeneratedNamedResource::new(
                            "v1",
                            "ReplicationController",
                            Some("default"),
                            "fg-rc",
                        ),
                        query: CreateUpdateQuery {
                            propagation_policy: Some("Foreground".to_string()),
                            ..default_query()
                        },
                        body: Bytes::new(),
                    },
                ),
            )
            .await
            .expect("foreground delete response should not wait")
            .expect("foreground delete RC");
            assert_eq!(status, StatusCode::ACCEPTED);
            assert!(body.0.pointer("/metadata/deletionTimestamp").is_some());
            for index in 0..3 {
                let pod = state
                    .resource_mutation()
                    .db
                    .get_resource("v1", "Pod", Some("default"), &format!("fg-rc-pod-{index}"))
                    .await
                    .expect("read child Pod")
                    .expect("child Pod exists");
                assert!(pod.data.pointer("/metadata/deletionTimestamp").is_none());
            }
            release_workqueue.notify_waiters();
            held_workqueue.abort();
        }
    }
}

#[cfg(test)]
pub(in crate::api) use test_impl::*;
