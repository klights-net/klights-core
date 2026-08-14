use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine as _;
use tower::ServiceExt;

fn test_admin(username: &str) -> klights_auth::AuthenticatedIdentity {
    klights_auth::AuthenticatedIdentity::client_cert(
        username.to_string(),
        vec!["system:masters".to_string()],
    )
}

fn bootstrap_secret_token(secret: &serde_json::Value) -> String {
    let decode = |encoded: &str| {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("bootstrap Secret field must be base64"),
        )
        .expect("bootstrap Secret field must be UTF-8")
    };
    let data = secret["data"]
        .as_object()
        .expect("bootstrap Secret must contain data");
    if let Some(token) = data.get("token").and_then(serde_json::Value::as_str) {
        return decode(token);
    }
    format!(
        "{}.{}",
        decode(data["token-id"].as_str().expect("token-id must be encoded")),
        decode(
            data["token-secret"]
                .as_str()
                .expect("token-secret must be encoded")
        )
    )
}

fn pem_cert_der(pem: &str) -> Vec<u8> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .next()
        .expect("PEM must contain a cert")
        .expect("cert must parse")
        .as_ref()
        .to_vec()
}

fn generate_test_client_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
    common_name: &str,
) -> String {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
    params.signed_by(&key, ca_cert, ca_key).unwrap().pem()
}

#[tokio::test]
async fn test_task_supervisor_rejects_spoofed_remote_group_header() {
    let response = crate::bootstrap::native_api_composition::support::build_test_app_state_with_operational_endpoints()
        .await
        .router()
        .oneshot(
            Request::get("/klights/v1/task-supervisor/categories")
                .header("x-remote-group", "system:masters")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_task_supervisor_accepts_admin_client_certificate_identity() {
    let now = time::OffsetDateTime::now_utc();
    let (ca_cert, ca_key, _, _) = klights_auth::test_support::generate_ca_full_at(now).unwrap();
    let (admin_cert_pem, _) =
        klights_auth::test_support::generate_admin_cert_at(&ca_cert, &ca_key, now).unwrap();
    let response = crate::bootstrap::native_api_composition::support::build_test_app_state_with_operational_endpoints()
        .await
        .router()
        .oneshot(
            Request::get("/klights/v1/task-supervisor/categories")
                .extension(klights_types::TlsClientCertificate(pem_cert_der(
                    &admin_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_task_supervisor_rejects_non_admin_client_certificate_identity() {
    let now = time::OffsetDateTime::now_utc();
    let (ca_cert, ca_key, _, _) = klights_auth::test_support::generate_ca_full_at(now).unwrap();
    let (server_cert_pem, _) =
        klights_auth::test_support::generate_server_cert_at(&ca_cert, &ca_key, now).unwrap();
    let response = crate::bootstrap::native_api_composition::support::build_test_app_state_with_operational_endpoints()
        .await
        .router()
        .oneshot(
            Request::get("/klights/v1/task-supervisor/categories")
                .extension(klights_types::TlsClientCertificate(pem_cert_der(
                    &server_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_impersonated_request_authorizes_effective_subject_not_real_admin() {
    let authorizer =
        std::sync::Arc::new(klights_auth::authorizer::AuthorizerChain::default_chain());
    let app =
        crate::bootstrap::native_api_composition::support::build_test_app_state_with_authorizer(
            authorizer,
        )
        .await
        .router();
    let response = app
        .oneshot(
            Request::get("/api/v1/namespaces/default/configmaps")
                .header("impersonate-user", "system:serviceaccount:default:e2e")
                .header("impersonate-group", "system:authenticated")
                .header("impersonate-group", "system:serviceaccounts")
                .header("impersonate-group", "system:serviceaccounts:default")
                .extension(test_admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_api_rejects_invalid_bootstrap_bearer_token() {
    let response = crate::bootstrap::native_api_composition::support::build_test_router()
        .await
        .oneshot(
            Request::get("/api")
                .header("authorization", "Bearer abcdef.0123456789abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_trusted_api_proxy_identity_is_authorized_as_delegated_user() {
    let recording = std::sync::Arc::new(
        crate::bootstrap::native_api_composition::support::RecordingAuthorizer::allow(),
    );
    let app =
        crate::bootstrap::native_api_composition::support::build_test_app_state_with_authorizer(
            recording.clone(),
        )
        .await
        .router();
    let now = time::OffsetDateTime::now_utc();
    let (ca_cert, ca_key, _, _) = klights_auth::test_support::generate_ca_full_at(now).unwrap();
    let proxy_cert_pem = generate_test_client_cert(
        &ca_cert,
        &ca_key,
        "system:klights:api-proxy:mn-controlplane2",
    );
    let response = app
        .oneshot(
            Request::get("/api/v1/nodes")
                .header("x-remote-user", "delegated-user")
                .header("x-remote-group", "delegated-group")
                .header("x-remote-group", "system:authenticated")
                .extension(klights_types::TlsClientCertificate(pem_cert_der(
                    &proxy_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let recorded = recording.take_requests().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0.username, "delegated-user");
    assert!(
        recorded[0]
            .0
            .groups
            .contains(&"delegated-group".to_string())
    );
    assert!(!recorded[0].0.groups.contains(&"system:masters".to_string()));
}

#[tokio::test]
async fn test_server_cert_identity_cannot_delegate_requestheaders() {
    let recording = std::sync::Arc::new(
        crate::bootstrap::native_api_composition::support::RecordingAuthorizer::allow(),
    );
    let app =
        crate::bootstrap::native_api_composition::support::build_test_app_state_with_authorizer(
            recording.clone(),
        )
        .await
        .router();
    let now = time::OffsetDateTime::now_utc();
    let (ca_cert, ca_key, _, _) = klights_auth::test_support::generate_ca_full_at(now).unwrap();
    let (server_cert_pem, _) =
        klights_auth::test_support::generate_server_cert_at(&ca_cert, &ca_key, now).unwrap();
    let response = app
        .oneshot(
            Request::get("/api/v1/nodes")
                .header("x-remote-user", "delegated-user")
                .header("x-remote-group", "delegated-group")
                .extension(klights_types::TlsClientCertificate(pem_cert_der(
                    &server_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let recorded = recording.take_requests().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0.username, "klights-server");
    assert!(
        !recorded[0]
            .0
            .groups
            .contains(&"delegated-group".to_string())
    );
}

#[tokio::test]
async fn test_api_accepts_valid_bootstrap_bearer_token() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let token = crate::bootstrap::native_api_composition::support::create_worker_bootstrap_token(
        &state.resource_store(),
    )
    .await
    .unwrap();
    let response = state
        .router()
        .oneshot(
            Request::get("/api")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_accepts_valid_serviceaccount_bearer_token() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    state
        .resource_store()
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("default"),
            "token-user",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "token-user", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
    let app = state.router();
    let token_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/namespaces/default/serviceaccounts/token-user/token")
                .header("content-type", "application/json")
                .extension(test_admin("test-admin"))
                .body(Body::from(
                    r#"{"apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest","spec":{"audiences":["https://kubernetes.default.svc.cluster.local"]}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(token_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(token_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token_request: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let token = token_request["status"]["token"].as_str().unwrap();
    let response = app
        .oneshot(
            Request::get("/api")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_bootstrap_secret_returns_rotated_token_when_near_expiry() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let old_token = "abcdef.0123456789abcdef";
    crate::bootstrap::native_api_composition::support::create_worker_bootstrap_token_with_ttl(
        &state.resource_store(),
        old_token,
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::get("/api/v1/namespaces/kube-system/secrets/worker-bootstrap-token")
                .extension(test_admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let returned: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let returned_token = bootstrap_secret_token(&returned);

    let stored = state
        .resource_store()
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "worker-bootstrap-token",
        )
        .await
        .unwrap()
        .expect("worker bootstrap Secret must remain stored");
    assert_ne!(returned_token, old_token);
    assert_eq!(returned_token, bootstrap_secret_token(&stored.data));
    assert_eq!(
        returned["metadata"]["resourceVersion"],
        stored.resource_version.to_string()
    );
}

#[tokio::test]
async fn test_get_kube_system_nonfixed_bootstrap_secret_does_not_rotate() {
    let state = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    let db = state.resource_store();
    crate::bootstrap::native_api_composition::support::create_worker_bootstrap_token_with_ttl(
        &db,
        "aaaaaa.1111111111111111",
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();
    crate::bootstrap::native_api_composition::support::create_controlplane_bootstrap_token_with_ttl(
        &db,
        "bbbbbb.2222222222222222",
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();

    let encode = |value: &str| base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
    let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::minutes(14))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("kube-system"),
        "custom-bootstrap-token",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"namespace": "kube-system", "name": "custom-bootstrap-token"},
            "type": "bootstrap.kubernetes.io/token",
            "data": {
                "token-id": encode("abcdef"),
                "token-secret": encode("0123456789abcdef"),
                "description": encode("operator-managed bootstrap-like token"),
                "expiration": encode(&expires_at),
                "usage-bootstrap-authentication": encode("true"),
                "usage-bootstrap-signing": encode("true")
            }
        }),
    )
    .await
    .unwrap();

    let before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "custom-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();
    let worker_before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "worker-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();
    let controlplane_before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "controlplane-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::get("/api/v1/namespaces/kube-system/secrets/custom-bootstrap-token")
                .extension(test_admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let returned: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "custom-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();
    let worker_after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "worker-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();
    let controlplane_after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "controlplane-bootstrap-token",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bootstrap_secret_token(&returned), "abcdef.0123456789abcdef");
    assert_eq!(
        bootstrap_secret_token(&after.data),
        "abcdef.0123456789abcdef"
    );
    assert_eq!(before.resource_version, after.resource_version);
    assert_eq!(
        bootstrap_secret_token(&worker_before.data),
        bootstrap_secret_token(&worker_after.data)
    );
    assert_eq!(
        worker_before.resource_version,
        worker_after.resource_version
    );
    assert_eq!(
        bootstrap_secret_token(&controlplane_before.data),
        bootstrap_secret_token(&controlplane_after.data)
    );
    assert_eq!(
        controlplane_before.resource_version,
        controlplane_after.resource_version
    );
}

#[tokio::test]
async fn serviceaccount_token_request_uses_injected_api_wall_clock() {
    let fixed_now = time::OffsetDateTime::parse(
        "2026-07-28T12:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    let state =
        crate::bootstrap::native_api_composition::support::build_test_app_state_with_auth_clock(
            std::sync::Arc::new(klights_auth::clock::SnapshotClock::new(fixed_now)),
        )
        .await;
    state
        .resource_store()
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("default"),
            "clock-test",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": "clock-test",
                    "namespace": "default",
                    "uid": "clock-test-uid"
                }
            }),
        )
        .await
        .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::post("/api/v1/namespaces/default/serviceaccounts/clock-test/token")
                .header("content-type", "application/json")
                .extension(test_admin("test-admin"))
                .body(Body::from(
                    serde_json::json!({
                        "apiVersion": "authentication.k8s.io/v1",
                        "kind": "TokenRequest",
                        "spec": {"expirationSeconds": 600}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body.pointer("/metadata/creationTimestamp"),
        Some(&serde_json::json!("2026-07-28T12:00:00Z"))
    );
    assert_eq!(
        body.pointer("/status/expirationTimestamp"),
        Some(&serde_json::json!("2026-07-28T12:10:00Z"))
    );
}

async fn node_response(app: axum::Router, path: &str) -> axum::response::Response {
    app.oneshot(
        Request::get(path)
            .extension(test_admin("klights-admin"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn node_response_json(app: axum::Router, path: &str) -> serde_json::Value {
    let response = node_response(app, path).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn seed_node_with_observed_lease(
    state: &crate::bootstrap::native_api_composition::support::TestAppState,
) {
    state
        .resource_store()
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {"conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastTransitionTime": "2026-05-13T06:35:00Z"
                }]}
            }),
        )
        .await
        .unwrap();
    state
        .record_node_lease(
            "worker-a",
            &serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 30,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn node_get_and_list_inject_last_heartbeat_time_only_on_raft_leader() {
    let leader = crate::bootstrap::native_api_composition::support::build_test_app_state_with_leader_authority().await;
    seed_node_with_observed_lease(&leader).await;
    let leader_app = leader.router_with_authority(true);
    let get = node_response_json(leader_app.clone(), "/api/v1/nodes/worker-a").await;
    assert_eq!(
        get["status"]["conditions"][0]["lastHeartbeatTime"],
        "2026-05-13T06:35:10Z"
    );
    let list = node_response_json(leader_app, "/api/v1/nodes").await;
    assert_eq!(
        list["items"][0]["status"]["conditions"][0]["lastHeartbeatTime"],
        "2026-05-13T06:35:10Z"
    );

    let follower = crate::bootstrap::native_api_composition::support::build_test_app_state().await;
    seed_node_with_observed_lease(&follower).await;
    let response = node_response(
        follower.router_with_authority(false),
        "/api/v1/nodes/worker-a",
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["reason"], "ServiceUnavailable");
}
