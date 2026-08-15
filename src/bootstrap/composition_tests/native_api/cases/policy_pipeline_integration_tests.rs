use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderValue, Request, StatusCode, header::AUTHORIZATION};
use klights_auth::AuthenticatedIdentity;
use tower::ServiceExt;

use k8s_native_service::AppError;

const IMPERSONATE_USER: &str = "impersonate-user";
const IMPERSONATE_GROUP: &str = "impersonate-group";

async fn validate_sa_token_bindings(
    state: &crate::bootstrap::composition_tests::native_api::support::TestAppState,
    claims: &klights_auth::SaTokenClaims,
) -> Result<(), AppError> {
    crate::bootstrap::composition_tests::native_api::support::validate_sa_token_bindings(
        state, claims,
    )
    .await
}

fn sa_claims(value: serde_json::Value) -> klights_auth::SaTokenClaims {
    serde_json::from_value(value).expect("valid ServiceAccount token claims")
}

async fn seed_service_account(
    state: &crate::bootstrap::composition_tests::native_api::support::TestAppState,
    namespace: &str,
    name: &str,
    uid: &str,
) {
    state
        .resource_mutation()
        .store
        .create_resource(
            "v1",
            "ServiceAccount",
            Some(namespace),
            name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                    "uid": uid
                }
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn bound_pod_adapter_is_uid_qualified_and_fail_closed() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
    let claims = |pod_uid: &str| {
        sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "pod": {"name": "p1", "uid": pod_uid}
            }
        }))
    };

    assert!(
        validate_sa_token_bindings(&state, &claims("pod-uid-1"))
            .await
            .is_err(),
        "a token bound to a deleted Pod must be rejected"
    );

    state
        .resource_mutation()
        .store
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "p1",
                    "namespace": "default",
                    "uid": "pod-uid-1"
                }
            }),
        )
        .await
        .unwrap();
    assert!(
        validate_sa_token_bindings(&state, &claims("replacement-uid"))
            .await
            .is_err(),
        "a token bound to a same-name replacement Pod must be rejected"
    );
    assert!(
        validate_sa_token_bindings(&state, &claims("pod-uid-1"))
            .await
            .is_ok(),
        "a token bound to the current Pod UID must pass"
    );
}

#[tokio::test]
async fn bound_secret_adapter_is_uid_qualified_and_fail_closed() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
    let claims = sa_claims(serde_json::json!({
        "sub": "system:serviceaccount:default:myapp",
        "kubernetes.io": {
            "serviceaccount": {"uid": "sa-uid-1"},
            "secret": {"name": "s1", "uid": "secret-uid-1"}
        }
    }));

    assert!(
        validate_sa_token_bindings(&state, &claims).await.is_err(),
        "a token bound to a deleted Secret must be rejected"
    );
    state
        .resource_mutation()
        .store
        .create_resource(
            "v1",
            "Secret",
            Some("default"),
            "s1",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": "s1",
                    "namespace": "default",
                    "uid": "secret-uid-1"
                }
            }),
        )
        .await
        .unwrap();
    assert!(
        validate_sa_token_bindings(&state, &claims).await.is_ok(),
        "a token bound to the current Secret UID must pass"
    );
}

#[tokio::test]
async fn unbound_service_account_token_uses_only_service_account_identity() {
    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
    let claims = sa_claims(serde_json::json!({
        "sub": "system:serviceaccount:default:myapp",
        "kubernetes.io": {"serviceaccount": {"uid": "sa-uid-1"}}
    }));

    assert!(validate_sa_token_bindings(&state, &claims).await.is_ok());
}

#[tokio::test]
async fn authorization_denial_writes_structured_audit_event() {
    let audit_sink = Arc::new(k8s_native_service::audit::MemoryAuditSink::default());
    let authorizer: Arc<dyn klights_auth::authorizer::Authorizer> = Arc::new(
        klights_auth::test_support::RecordingAuthorizer::deny("policy denied secret read"),
    );
    let state = crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_authorizer_and_audit_sink(
        authorizer,
        audit_sink.clone(),
    )
    .await;
    let app = state.router();

    let mut request = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/secrets/db-password")
        .body(Body::empty())
        .unwrap();
    request
        .extensions_mut()
        .insert(AuthenticatedIdentity::client_cert(
            "alice".to_string(),
            Vec::new(),
        ));

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let events = audit_sink.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(
        event.stage,
        k8s_native_service::audit::AuditStage::Authorization
    );
    assert_eq!(event.user.username, "alice");
    assert_eq!(event.verb, "get");
    assert_eq!(event.api_group.as_deref(), None);
    assert_eq!(event.api_version.as_deref(), Some("v1"));
    assert_eq!(event.resource.as_deref(), Some("secrets"));
    assert_eq!(event.subresource.as_deref(), None);
    assert_eq!(event.namespace.as_deref(), Some("default"));
    assert_eq!(event.name.as_deref(), Some("db-password"));
    assert_eq!(event.non_resource_url.as_deref(), None);
    assert!(!event.allowed);
    assert_eq!(event.reason, "policy denied secret read");
}

#[tokio::test]
async fn pod_exec_authorization_writes_high_value_audit_event() {
    let audit_sink = Arc::new(k8s_native_service::audit::MemoryAuditSink::default());
    let authorizer: Arc<dyn klights_auth::authorizer::Authorizer> =
        Arc::new(klights_auth::test_support::RecordingAuthorizer::allow());
    let state = crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_authorizer_and_audit_sink(
        authorizer,
        audit_sink.clone(),
    )
    .await;
    let app = state.router();

    let mut request = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/pods/web/exec?container=app&command=id")
        .body(Body::empty())
        .unwrap();
    request
        .extensions_mut()
        .insert(AuthenticatedIdentity::client_cert(
            "operator".to_string(),
            Vec::new(),
        ));

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let events = audit_sink.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(
        event.stage,
        k8s_native_service::audit::AuditStage::Authorization
    );
    assert_eq!(event.user.username, "operator");
    assert_eq!(event.verb, "create");
    assert_eq!(event.resource.as_deref(), Some("pods"));
    assert_eq!(event.subresource.as_deref(), Some("exec"));
    assert_eq!(event.namespace.as_deref(), Some("default"));
    assert_eq!(event.name.as_deref(), Some("web"));
    assert!(event.allowed);
    assert!(event.high_value);
}

#[tokio::test]
async fn auth_policy_failures_have_json_and_protobuf_parity() {
    use k8s_native_service::test_protobuf::Message;

    #[derive(Clone, Copy)]
    enum Failure {
        InvalidAuthorization,
        InvalidImpersonation,
        ForbiddenImpersonation,
        AuthorizationDenied,
    }

    struct FailureCase {
        failure: Failure,
        expected_status: StatusCode,
        expected_reason: &'static str,
        expected_message: &'static str,
    }

    let failures = [
        FailureCase {
            failure: Failure::InvalidAuthorization,
            expected_status: StatusCode::UNAUTHORIZED,
            expected_reason: "Unauthorized",
            expected_message: "invalid Authorization header",
        },
        FailureCase {
            failure: Failure::InvalidImpersonation,
            expected_status: StatusCode::BAD_REQUEST,
            expected_reason: "BadRequest",
            expected_message: "Impersonate-User is required when using impersonation headers",
        },
        FailureCase {
            failure: Failure::ForbiddenImpersonation,
            expected_status: StatusCode::FORBIDDEN,
            expected_reason: "Forbidden",
            expected_message: "impersonation denied",
        },
        FailureCase {
            failure: Failure::AuthorizationDenied,
            expected_status: StatusCode::FORBIDDEN,
            expected_reason: "Forbidden",
            expected_message: "authorization denied",
        },
    ];
    let encodings = [
        (None, "application/json"),
        (
            Some("application/vnd.kubernetes.protobuf"),
            "application/vnd.kubernetes.protobuf",
        ),
    ];

    for failure in failures {
        for (accept, expected_content_type) in encodings {
            let state = match failure.failure {
                Failure::ForbiddenImpersonation => {
                    let authorizer: Arc<dyn klights_auth::authorizer::Authorizer> =
                        Arc::new(klights_auth::test_support::RecordingAuthorizer::deny(
                            "impersonation denied",
                        ));
                    crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_authorizer(authorizer).await
                }
                Failure::AuthorizationDenied => {
                    let authorizer: Arc<dyn klights_auth::authorizer::Authorizer> =
                        Arc::new(klights_auth::test_support::RecordingAuthorizer::deny(
                            "authorization denied",
                        ));
                    crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_authorizer(authorizer).await
                }
                Failure::InvalidAuthorization | Failure::InvalidImpersonation => {
                    crate::bootstrap::composition_tests::native_api::support::build_test_app_state()
                        .await
                }
            };
            let app = state.router();
            let mut request = Request::builder().uri("/api");
            if let Some(accept) = accept {
                request = request.header("accept", accept);
            }
            let mut request = request.body(Body::empty()).unwrap();
            match failure.failure {
                Failure::InvalidAuthorization => {
                    request.headers_mut().insert(
                        AUTHORIZATION,
                        HeaderValue::from_bytes(&[0xff]).expect("opaque invalid header"),
                    );
                }
                Failure::InvalidImpersonation => {
                    request
                        .extensions_mut()
                        .insert(AuthenticatedIdentity::client_cert(
                            "alice".to_string(),
                            Vec::new(),
                        ));
                    request
                        .headers_mut()
                        .insert(IMPERSONATE_GROUP, HeaderValue::from_static("developers"));
                }
                Failure::ForbiddenImpersonation => {
                    request
                        .extensions_mut()
                        .insert(AuthenticatedIdentity::client_cert(
                            "alice".to_string(),
                            Vec::new(),
                        ));
                    request
                        .headers_mut()
                        .insert(IMPERSONATE_USER, HeaderValue::from_static("bob"));
                }
                Failure::AuthorizationDenied => {
                    request
                        .extensions_mut()
                        .insert(AuthenticatedIdentity::client_cert(
                            "alice".to_string(),
                            Vec::new(),
                        ));
                }
            }

            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), failure.expected_status);
            assert_eq!(
                response
                    .headers()
                    .get("content-type")
                    .and_then(|value| value.to_str().ok()),
                Some(expected_content_type)
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let (kind, reason, code, message) = if expected_content_type == "application/json" {
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                (
                    value["kind"].as_str().unwrap().to_string(),
                    value["reason"].as_str().unwrap().to_string(),
                    value["code"].as_u64().unwrap() as u16,
                    value["message"].as_str().unwrap().to_string(),
                )
            } else {
                assert_eq!(&body[..4], b"k8s\0");
                let envelope =
                    k8s_native_service::test_protobuf::Unknown::decode(&body[4..]).unwrap();
                let type_meta = envelope.type_meta.expect("Status type metadata");
                let status =
                    k8s_native_service::test_protobuf::apimachinery::pkg::apis::meta::v1::Status::decode(
                        envelope.raw.as_slice(),
                    )
                    .unwrap();
                (
                    type_meta.kind,
                    status.reason.unwrap_or_default(),
                    u16::try_from(status.code.unwrap_or_default()).unwrap(),
                    status.message.unwrap_or_default(),
                )
            };
            assert_eq!(kind, "Status");
            assert_eq!(reason, failure.expected_reason);
            assert_eq!(
                code,
                failure.expected_status.as_u16(),
                "decoded Status code"
            );
            assert_eq!(message, failure.expected_message);
        }
    }
}
