use super::*;

#[tokio::test]
async fn test_replicaset_scale_subresource() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = crate::controllers::crd::CrdRegistry::new();
    let config = std::sync::Arc::new({
        let ns = "klights-test";
        crate::KlightsConfig {
            bridge_name: ns.to_string(),
            pod_subnet: "10.43.0.0/17".to_string(),
            cluster_cidr: "10.42.0.0/16".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            tls_port: 7443,
            api_fqdn: None,
            log_file: None,
            containerd_namespace: ns.to_string(),
            containerd_socket: None,
            node_name: "test-node".to_string(),
            node_ip: None,
            dataplane_encryption: crate::networking::wireguard::DataplaneEncryption::Enabled,
            external_endpoint: None,
            worker_dataplane_no_ingress: false,
            wireguard_device: crate::networking::wireguard::DEFAULT_WIREGUARD_DEVICE.to_string(),
            wireguard_port: crate::networking::wireguard::DEFAULT_WIREGUARD_PORT,
            cluster_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("cluster.db"),
            node_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("node.db"),
            in_memory: true,
            db_encryption: crate::DbEncryption::Disabled,
            db_key_file: None,
            datastore_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            node_local_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            oidc_issuer_url: None,
            oidc_client_id: None,
            oidc_username_claim: "sub".to_string(),
            oidc_groups_claim: "groups".to_string(),
            oidc_groups_prefix: String::new(),
            oidc_ca_bundle: None,
            webhook_auth_url: None,
            webhook_auth_client_cert: None,
            webhook_auth_client_key: None,
            webhook_auth_audiences: String::new(),
            webhook_auth_cache_authorized_ttl_secs: 300,
            webhook_auth_cache_unauthorized_ttl_secs: 30,
            webhook_auth_ca_bundle: None,
        }
    });
    let service_ipam = std::sync::Arc::new(crate::controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let controller_dispatcher = std::sync::Arc::new(
        crate::controller_dispatcher::ControllerDispatcher::new(service_ipam.clone()),
    );

    // Create namespace
    let ns =
        serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create ReplicaSet
    let rs = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "test-rs", "namespace": "test"},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });
    db.create_resource("apps/v1", "ReplicaSet", Some("test"), "test-rs", rs)
        .await
        .unwrap();

    // Build AppState and router
    let task_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
        std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db_handle.clone(),
            "test-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
    let side_effects = std::sync::Arc::new(crate::side_effects::default_registry(
        metrics.clone(),
        None,
        None,
        None,
    ));
    let pod_repository = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        task_supervisor.clone(),
        side_effects.clone(),
        metrics.clone(),
    ));
    let state = crate::api::AppState {
        db: db_handle.clone(),
        cluster_api,
        crd_registry: registry,
        mode: crate::bootstrap::NodeMode::Root,
        role: crate::bootstrap::NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
        },
        replication: None,
        network: crate::networking::test_support::mock_network(db_handle),
        config,
        service_ipam,
        nodeport_alloc: std::sync::Arc::new(crate::controllers::service::NodePortAllocator::new()),
        cri: None,
        controller_dispatcher,
        side_effects,
        metrics,
        apiservice_proxy_identity_cache: std::sync::Arc::new(tokio::sync::OnceCell::new()),
        apiservice_proxy_cache: std::sync::Arc::new(
            crate::api::apiservice_proxy::ApiServiceProxyCache::default(),
        ),
        task_supervisor,
        pod_repository,
        outbox: std::sync::Arc::new(crate::kubelet::outbox::Outbox::test_outbox().await),
        node_lease_tracker: std::sync::Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        pod_lifecycle_router: None,
        pod_probe_manager: None,
        pod_lifecycle_rx: None,
        pod_start_retry_state: None,
        is_raft_leader_rx: None,
        authorizer: std::sync::Arc::new(crate::auth::authorizer::AuthorizerChain::test_allow_all()),
        rbac_policy_store: std::sync::Arc::new(
            crate::auth::rbac_policy_store::InMemoryRbacPolicyStore::empty(),
        ),
        oidc_authenticator: None,
        webhook_authenticator: None,
        cluster_ca_pem: None,
    };
    let app = crate::api::build_router(state);

    // GET /apis/apps/v1/namespaces/test/replicasets/test-rs/scale
    let request = Request::builder()
        .method("GET")
        .uri("/apis/apps/v1/namespaces/test/replicasets/test-rs/scale")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    // Should return 200 OK with Scale object
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "GET scale subresource should return 200 OK"
    );
}

/// readyReplicas and availableReplicas must reflect actual pod Ready condition,
/// not be hardcoded to 0. Sonobuoy: RS scaled to 3 but ReadyReplicas stays 0.
#[tokio::test]
async fn test_replicaset_status_ready_replicas_reflects_pod_conditions() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-uid-ready-test";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "ready-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "ready-test"}},
            "template": {
                "metadata": {"labels": {"app": "ready-test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "ready-rs",
            replicaset,
        )
        .await
        .unwrap();

    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile: creates 3 pods (all Pending, none Ready)
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Simulate 2 of 3 pods becoming Ready
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        3,
        "Should have 3 pods after first reconcile"
    );

    let ready_condition = json!([{"type": "Ready", "status": "True"}]);
    for (i, pod) in pods.items.iter().enumerate().take(2) {
        let mut updated_pod: serde_json::Value = (*pod.data).clone();
        updated_pod["status"] = json!({"phase": "Running", "conditions": ready_condition});
        db.update_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            &pods.items[i].name.clone(),
            updated_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Second reconcile: no pods to create/delete, must update status from pod states
    let rs_after = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "ready-rs")
        .await
        .unwrap()
        .unwrap();
    let mut rs_with_rv2: serde_json::Value = (*rs_after.data).clone();
    if let Some(meta) = rs_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(rs_after.resource_version.to_string()),
        );
    }
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv2,
        "test-node",
    )
    .await
    .unwrap();

    let updated_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "ready-rs")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_rs.data["status"];
    assert_eq!(status["replicas"], 3, "status.replicas must be 3");
    assert_eq!(
        status["readyReplicas"], 2,
        "readyReplicas must reflect pods with Ready=True (2), not hardcoded 0, got: {}",
        status["readyReplicas"]
    );
    assert_eq!(
        status["availableReplicas"], 2,
        "availableReplicas must reflect ready pods (2), not hardcoded 0, got: {}",
        status["availableReplicas"]
    );
}

#[tokio::test]
async fn test_replicaset_deletes_itself_when_controller_deployment_missing() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "gc-race-ns",
        json!({"metadata": {"name": "gc-race-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "late-rs",
            "namespace": "gc-race-ns",
            "uid": "late-rs-uid",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "gone-deploy",
                "uid": "gone-deploy-uid",
                "controller": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "late-rs"}},
            "template": {
                "metadata": {"labels": {"app": "late-rs"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("gc-race-ns"), "late-rs", rs)
        .await
        .unwrap();

    let rs_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let rs_after = db
        .get_resource("apps/v1", "ReplicaSet", Some("gc-race-ns"), "late-rs")
        .await
        .unwrap();
    assert!(
        rs_after.is_none(),
        "ReplicaSet with missing controller Deployment should self-delete"
    );
}

#[tokio::test]
async fn test_replicaset_skips_reconcile_when_deletion_timestamp_set() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "deleting-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-del",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "defaults-rs", rs)
        .await
        .unwrap();
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "No pods should be created for a ReplicaSet being deleted"
    );
}

#[tokio::test]
async fn test_replicaset_stale_snapshot_after_delete_does_not_recreate_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "stale-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-stale"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "stale"}},
            "template": {
                "metadata": {"labels": {"app": "stale"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "stale-rs", rs)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("apps/v1", "ReplicaSet", Some("test-ns"), "stale-rs")
        .await
        .unwrap();

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &stale_snapshot,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "stale ReplicaSet reconcile after delete must not recreate pods"
    );
}

#[tokio::test]
async fn test_replicaset_created_pod_gets_api_pipeline_defaults() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "defaults-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-defaults"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "demo"}},
            "template": {
                "metadata": {"labels": {"app": "demo"}},
                "spec": {
                    "containers": [{
                        "name": "app",
                        "image": "busybox",
                        "terminationMessagePath": "",
                        "terminationMessagePolicy": "",
                        "livenessProbe": {"httpGet": {"port": 8080, "path": "", "scheme": ""}}
                    }]
                }
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "defaults-rs", rs)
        .await
        .unwrap();
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);
    let pod = &pods.items[0].data;
    assert_eq!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Pending")
    );
    assert_eq!(
        pod.pointer("/status/qosClass").and_then(|v| v.as_str()),
        Some("BestEffort")
    );
    assert!(
        pod.pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .is_some_and(|c| !c.is_empty())
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePath")
            .and_then(|v| v.as_str()),
        Some("/dev/termination-log")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/terminationMessagePolicy")
            .and_then(|v| v.as_str()),
        Some("File")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/path")
            .and_then(|v| v.as_str()),
        Some("/")
    );
    assert_eq!(
        pod.pointer("/spec/containers/0/livenessProbe/httpGet/scheme")
            .and_then(|v| v.as_str()),
        Some("HTTP")
    );
}

/// P0-API-01 race regression: a controller status write must never lose
/// a concurrent user `kubectl scale` (PATCH `.spec.replicas`).
///
/// Pre-fix: the controller did a read-modify-write through `update_resource`
/// using the spec it had snapshotted — if the user PATCHed `.spec.replicas`
/// after the snapshot but before the write, the user's value was clobbered
/// (~50% of races). Post-fix: status writes go through `write_status` →
/// `update_status_only` which uses `json_set(data, '$.status', ?)` so spec
/// is never read or written by the status path.
///
/// Asserts spec.replicas == 7 across race iterations. 25 iterations is enough
/// to exercise the user-scale-vs-controller-status race reliably under tokio's
/// scheduler — the original 100 was overkill (the bug, when present, fires in
/// most iterations, not 1 in 100).
#[tokio::test]
async fn test_replicaset_status_write_never_clobbers_user_scale_under_race() {
    use std::sync::Arc;

    let db: Arc<crate::datastore::sqlite::Datastore> = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );

    for iteration in 0..25 {
        let name = format!("rs-race-{iteration}");
        let initial = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": &name,
                "namespace": "default",
                "uid": format!("uid-race-{iteration}")
            },
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "race"}},
                "template": {
                    "metadata": {"labels": {"app": "race"}},
                    "spec": {"containers": [{"name": "c", "image": "x"}]}
                }
            },
            "status": {"replicas": 0}
        });

        // Create the RS and capture the controller's snapshot at this RV.
        let created = db
            .create_resource("apps/v1", "ReplicaSet", Some("default"), &name, initial)
            .await
            .unwrap();
        let mut controller_snapshot: serde_json::Value = (*created.data).clone();
        controller_snapshot["apiVersion"] = json!("apps/v1");
        controller_snapshot["kind"] = json!("ReplicaSet");
        // Note: deliberately omit metadata.resourceVersion so the controller's
        // status write skips CAS — modeling a controller that has no RV (or
        // a stale one) at the moment of write.

        let user_db = Arc::clone(&db);
        let user_name = name.clone();
        let user_handle = tokio::spawn(async move {
            // User scales to 7 via merge-patch (kubectl scale-style — no
            // explicit resourceVersion CAS so it cannot 409 against the
            // controller's status write).
            user_db
                .patch_resource_latest(
                    "apps/v1",
                    "ReplicaSet",
                    Some("default"),
                    &user_name,
                    crate::datastore::PatchKind::Merge,
                    json!({"spec": {"replicas": 7}}),
                )
                .await
                .unwrap();
        });

        let ctl_db = Arc::clone(&db);
        let ctl_snapshot = controller_snapshot.clone();
        let ctl_handle = tokio::spawn(async move {
            // Controller writes its computed status through the safe path.
            let new_status = json!({
                "replicas": 3,
                "readyReplicas": 3,
                "availableReplicas": 3,
                "fullyLabeledReplicas": 3,
                "observedGeneration": 1
            });
            crate::controllers::common::write_status(
                ctl_db.as_ref() as &dyn crate::datastore::DatastoreBackend,
                &ctl_snapshot,
                &new_status,
            )
            .await
            .unwrap();
        });

        user_handle.await.unwrap();
        ctl_handle.await.unwrap();

        // After both writes, the user's spec.replicas=7 must still be visible.
        let after = db
            .get_resource("apps/v1", "ReplicaSet", Some("default"), &name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.data["spec"]["replicas"], 7,
            "iteration {iteration}: user scale to 7 was clobbered by controller status write",
        );
    }
}
