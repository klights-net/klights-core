use super::*;
use crate::datastore::sqlite::Datastore;
use serde_json::json;

/// Test-only shim wrapping `reconcile_replicationcontroller` with the
/// repository-backed argument list, mirroring the pre-Task-18 signature.
async fn reconcile_rc_test(db: &Datastore, rc: &Value, node_name: &str) -> anyhow::Result<()> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    super::reconcile_replicationcontroller(
        db,
        repo.as_ref(),
        repo.as_ref(),
        repo.as_ref(),
        rc,
        node_name,
    )
    .await
}

async fn setup_db_with_rc(db: &Datastore, rc_name: &str) {
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata":{"name":"default"}}),
    )
    .await
    .unwrap();
    let rc = json!({
        "apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"name": rc_name, "namespace": "default", "uid": "rc-uid-1"},
        "spec": {"replicas": 1, "selector": {"app": "test"},
            "template": {"metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}}}
    });
    db.create_resource("v1", "ReplicationController", Some("default"), rc_name, rc)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_rc_publishes_replica_failure_condition_on_create_failure() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc").await;
    update_replicationcontroller_status(
        &db,
        "test-rc",
        "default",
        &[],
        Some("exceeded quota: pods count limit"),
    )
    .await
    .unwrap();
    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc")
        .await
        .unwrap()
        .unwrap();
    let conds = updated
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .expect("conditions must be present");
    let failure = conds
        .iter()
        .find(|c| c["type"] == "ReplicaFailure")
        .expect("ReplicaFailure condition must exist");
    assert_eq!(failure["status"], "True");
    assert_eq!(failure["reason"], "FailedCreate");
}

#[tokio::test]
async fn test_rc_clears_replica_failure_condition_when_healthy() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-ok").await;
    update_replicationcontroller_status(&db, "test-rc-ok", "default", &[], None)
        .await
        .unwrap();
    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc-ok")
        .await
        .unwrap()
        .unwrap();
    let conds = updated
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .expect("conditions present");
    assert!(
        conds.iter().all(|c| c["type"] != "ReplicaFailure"),
        "ReplicaFailure must be removed when controller is healthy"
    );
}

#[tokio::test]
async fn test_rc_returns_error_when_quota_blocks_pod_create() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-quota").await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "rq-pods-2",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "rq-pods-2", "namespace": "default"},
            "spec": {"hard": {"pods": "2"}}
        }),
    )
    .await
    .unwrap();

    let current_rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "test-rc-quota",
        )
        .await
        .unwrap()
        .unwrap();
    let updated_rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "test-rc-quota", "namespace": "default", "uid": "rc-uid-1"},
        "spec": {
            "replicas": 3,
            "selector": {"app": "test"},
            "template": {"metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}}
        }
    });
    db.update_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "test-rc-quota",
        updated_rc.clone(),
        current_rc.resource_version,
    )
    .await
    .unwrap();

    let result = reconcile_rc_test(&db, &updated_rc, "node1").await;
    assert!(result.is_err(), "quota denial should fail reconcile");

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        2,
        "reconcile should stop at quota boundary and return error"
    );

    let rc_after = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "test-rc-quota",
        )
        .await
        .unwrap()
        .expect("RC must still exist");
    let failure = rc_after
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conds| conds.iter().find(|c| c["type"] == "ReplicaFailure"))
        .cloned()
        .expect("quota-denied reconcile must publish ReplicaFailure condition");
    assert_eq!(failure["status"], "True");
    assert_eq!(failure["reason"], "FailedCreate");
}

/// P0-E2E-20260424b-06: GET/PUT/PATCH /replicationcontrollers/{name}/scale must work.
#[tokio::test]
async fn test_replicationcontroller_scale_subresource() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-scale").await;

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
            anonymous_auth: true,
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
        crd_registry: crate::controllers::crd::CrdRegistry::new(),
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

    // GET /api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["kind"], "Scale");
    assert_eq!(scale["spec"]["replicas"], 1);

    // PUT to update replicas to 3
    let put_body = serde_json::json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": {"name": "test-rc-scale", "namespace": "default"},
        "spec": {"replicas": 3}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&put_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PUT scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        scale["spec"]["replicas"], 3,
        "replicas must be updated to 3"
    );

    // PATCH replicas to 5 via merge-patch
    let patch_body = serde_json::json!({"spec": {"replicas": 5}});
    let resp = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PATCH scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        scale["spec"]["replicas"], 5,
        "replicas must be updated to 5"
    );
}
