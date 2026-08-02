use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{StatusCode, request::Builder},
    middleware::{self, Next},
    routing::post,
};
use klights_auth::{
    AuthenticatedIdentity,
    authorizer::{AuthorizationDecision, Authorizer},
    request_attributes::AuthorizationRequest,
};
use tower::ServiceExt;

use crate::policy_inputs::AuthorizationHttpInputs;

struct RecordingAuthorizer {
    requests: tokio::sync::Mutex<Vec<AuthorizationRequest>>,
    decision: AuthorizationDecision,
}

impl RecordingAuthorizer {
    fn allow() -> Self {
        Self::new(AuthorizationDecision::allow("focused policy test allow"))
    }

    fn deny() -> Self {
        Self::new(AuthorizationDecision::deny("focused policy test denial"))
    }

    fn new(decision: AuthorizationDecision) -> Self {
        Self {
            requests: tokio::sync::Mutex::new(Vec::new()),
            decision,
        }
    }

    async fn take_requests(&self) -> Vec<AuthorizationRequest> {
        std::mem::take(&mut *self.requests.lock().await)
    }
}

#[async_trait::async_trait]
impl Authorizer for RecordingAuthorizer {
    async fn authorize(
        &self,
        _identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        self.requests.lock().await.push(request.clone());
        self.decision.clone()
    }
}

fn policy_router(authorizer: Arc<RecordingAuthorizer>, reached: Arc<AtomicBool>) -> Router {
    let authorizer: Arc<dyn Authorizer> = authorizer;
    let audit: Arc<dyn crate::audit::AuditSink> = crate::audit::default_audit_sink();
    let inputs = Arc::new(AuthorizationHttpInputs::new(
        authorizer,
        audit,
        Arc::new(klights_auth::clock::SystemClock),
    ));

    Router::new()
        .fallback(move || {
            let reached = reached.clone();
            async move {
                reached.store(true, Ordering::SeqCst);
                StatusCode::NO_CONTENT
            }
        })
        .layer(middleware::from_fn(move |request: Request, next: Next| {
            let inputs = inputs.clone();
            async move { crate::auth_http::authorize_request(inputs, request, next).await }
        }))
}

fn request(method: &str, uri: &str, body: Body) -> axum::http::Request<Body> {
    Builder::new()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

#[tokio::test]
async fn pod_subresource_routes_denied_with_deny_all_authorizer() {
    let authorizer = Arc::new(RecordingAuthorizer::deny());
    let reached = Arc::new(AtomicBool::new(false));
    let app = policy_router(authorizer, reached.clone());
    let tests = [
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/log",
            "pod log",
        ),
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/exec",
            "pod exec",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/pods/test-pod/exec",
            "pod exec POST",
        ),
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/attach",
            "pod attach",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/pods/test-pod/attach",
            "pod attach POST",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/pods/test-pod/portforward",
            "pod portforward",
        ),
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "pod proxy",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/pods/test-pod/eviction",
            "pod eviction",
        ),
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/ephemeralcontainers",
            "pod ephemeralcontainers get",
        ),
        (
            "PUT",
            "/api/v1/namespaces/default/pods/test-pod/ephemeralcontainers",
            "pod ephemeralcontainers update",
        ),
        (
            "PATCH",
            "/api/v1/namespaces/default/pods/test-pod/ephemeralcontainers",
            "pod ephemeralcontainers patch",
        ),
        ("GET", "/api/v1/nodes/test-node/proxy", "node proxy"),
        (
            "DELETE",
            "/api/v1/namespaces/default/services/test-svc",
            "service delete",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/serviceaccounts/test-sa/token",
            "serviceaccount token",
        ),
        ("GET", "/debug/klights/pod-lifecycle", "debug endpoint"),
    ];

    for (method, uri, description) in tests {
        let body = if matches!(method, "POST" | "PUT" | "PATCH") {
            Body::from("{}")
        } else {
            Body::empty()
        };
        let response = app
            .clone()
            .oneshot(request(method, uri, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{description}");
    }
    assert!(
        !reached.load(Ordering::SeqCst),
        "denied routes must not reach handlers"
    );
}

#[tokio::test]
async fn pod_and_node_proxy_use_method_specific_rbac_verbs() {
    let authorizer = Arc::new(RecordingAuthorizer::allow());
    let app = policy_router(authorizer.clone(), Arc::new(AtomicBool::new(false)));
    let tests = [
        (
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "get",
            "pods",
        ),
        (
            "POST",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "create",
            "pods",
        ),
        (
            "PUT",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "update",
            "pods",
        ),
        (
            "PATCH",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "patch",
            "pods",
        ),
        (
            "DELETE",
            "/api/v1/namespaces/default/pods/test-pod/proxy",
            "delete",
            "pods",
        ),
        ("GET", "/api/v1/nodes/test-node/proxy/pods", "get", "nodes"),
        ("POST", "/api/v1/nodes/test-node/proxy", "create", "nodes"),
        ("PUT", "/api/v1/nodes/test-node/proxy", "update", "nodes"),
        ("PATCH", "/api/v1/nodes/test-node/proxy", "patch", "nodes"),
        ("DELETE", "/api/v1/nodes/test-node/proxy", "delete", "nodes"),
    ];

    for (method, uri, expected_verb, expected_resource) in tests {
        authorizer.take_requests().await;
        let _ = app
            .clone()
            .oneshot(request(method, uri, Body::empty()))
            .await
            .unwrap();
        let requests = authorizer.take_requests().await;
        let authorization = requests.last().expect("authorization request");
        assert_eq!(authorization.verb, expected_verb, "{method} {uri}");
        assert_eq!(
            authorization.resource.as_deref(),
            Some(expected_resource),
            "{method} {uri}"
        );
        assert_eq!(
            authorization.subresource.as_deref(),
            Some("proxy"),
            "{method} {uri}"
        );
    }
}

#[tokio::test]
async fn eviction_malformed_json_and_protobuf_are_both_bad_requests() {
    async fn accept_eviction(
        crate::current::LenientJson(_): crate::current::LenientJson<serde_json::Value>,
    ) -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new().route(
        "/api/v1/namespaces/default/pods/test-pod/eviction",
        post(accept_eviction),
    );
    for (content_type, body) in [
        ("application/json", Body::from("{")),
        (
            "application/vnd.kubernetes.protobuf",
            Body::from(&b"k8s\0\xff"[..]),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Builder::new()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/pods/test-pod/eviction")
                    .header("content-type", content_type)
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{content_type}");
    }
}

#[tokio::test]
async fn handwritten_routes_emit_exact_rbac_attributes() {
    struct RouteTest {
        method: &'static str,
        uri: &'static str,
        verb: &'static str,
        resource: &'static str,
        subresource: &'static str,
        namespace: Option<&'static str>,
        name: &'static str,
    }

    let tests = [
        RouteTest {
            method: "POST",
            uri: "/api/v1/namespaces/default/serviceaccounts/my-sa/token",
            verb: "create",
            resource: "serviceaccounts",
            subresource: "token",
            namespace: Some("default"),
            name: "my-sa",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/log",
            verb: "get",
            resource: "pods",
            subresource: "log",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/exec",
            verb: "create",
            resource: "pods",
            subresource: "exec",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/attach",
            verb: "create",
            resource: "pods",
            subresource: "attach",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/portforward",
            verb: "create",
            resource: "pods",
            subresource: "portforward",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/proxy",
            verb: "get",
            resource: "pods",
            subresource: "proxy",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "POST",
            uri: "/api/v1/namespaces/default/pods/test-pod/proxy",
            verb: "create",
            resource: "pods",
            subresource: "proxy",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "DELETE",
            uri: "/api/v1/namespaces/default/pods/test-pod/proxy",
            verb: "delete",
            resource: "pods",
            subresource: "proxy",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "POST",
            uri: "/api/v1/namespaces/default/pods/test-pod/eviction",
            verb: "create",
            resource: "pods",
            subresource: "eviction",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/nodes/test-node/proxy",
            verb: "get",
            resource: "nodes",
            subresource: "proxy",
            namespace: None,
            name: "test-node",
        },
        RouteTest {
            method: "PUT",
            uri: "/api/v1/namespaces/default/pods/test-pod/proxy",
            verb: "update",
            resource: "pods",
            subresource: "proxy",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "PATCH",
            uri: "/api/v1/namespaces/default/pods/test-pod/proxy",
            verb: "patch",
            resource: "pods",
            subresource: "proxy",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "GET",
            uri: "/api/v1/namespaces/default/pods/test-pod/ephemeralcontainers",
            verb: "get",
            resource: "pods",
            subresource: "ephemeralcontainers",
            namespace: Some("default"),
            name: "test-pod",
        },
        RouteTest {
            method: "POST",
            uri: "/api/v1/nodes/test-node/proxy",
            verb: "create",
            resource: "nodes",
            subresource: "proxy",
            namespace: None,
            name: "test-node",
        },
    ];
    let authorizer = Arc::new(RecordingAuthorizer::allow());
    let app = policy_router(authorizer.clone(), Arc::new(AtomicBool::new(false)));

    for test in tests {
        authorizer.take_requests().await;
        let _ = app
            .clone()
            .oneshot(request(test.method, test.uri, Body::from("{}")))
            .await
            .unwrap();
        let requests = authorizer.take_requests().await;
        let authorization = requests.first().expect("authorization request");
        assert_eq!(
            authorization.verb, test.verb,
            "{} {}",
            test.method, test.uri
        );
        assert_eq!(authorization.resource.as_deref(), Some(test.resource));
        assert_eq!(authorization.subresource.as_deref(), Some(test.subresource));
        assert_eq!(authorization.namespace.as_deref(), test.namespace);
        assert_eq!(authorization.name.as_deref(), Some(test.name));
    }
}

#[tokio::test]
async fn proxy_denied_does_not_connect_to_backend() {
    let reached = Arc::new(AtomicBool::new(false));
    let app = policy_router(Arc::new(RecordingAuthorizer::deny()), reached.clone());
    let response = app
        .oneshot(request(
            "GET",
            "/api/v1/namespaces/default/pods/test-pod/proxy/65535/test",
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        !reached.load(Ordering::SeqCst),
        "denial must stop before the proxy handler"
    );
}

async fn assert_denied(cases: &[(&str, &str)]) {
    let reached = Arc::new(AtomicBool::new(false));
    let app = policy_router(Arc::new(RecordingAuthorizer::deny()), reached.clone());
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(request(method, uri, Body::from("{}")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {uri}");
    }
    assert!(
        !reached.load(Ordering::SeqCst),
        "denied request reached handler"
    );
}

#[tokio::test]
async fn pod_crud_denied_returns_403_via_middleware() {
    assert_denied(&[
        ("GET", "/api/v1/namespaces/default/pods"),
        ("GET", "/api/v1/namespaces/default/pods/p1"),
        ("GET", "/api/v1/pods"),
        ("POST", "/api/v1/namespaces/default/pods"),
        ("DELETE", "/api/v1/namespaces/default/pods/p1"),
        ("DELETE", "/api/v1/namespaces/default/pods"),
    ])
    .await;
}

#[tokio::test]
async fn namespace_crud_and_finalize_denied_returns_403() {
    assert_denied(&[
        ("GET", "/api/v1/namespaces"),
        ("GET", "/api/v1/namespaces/ns1"),
        ("POST", "/api/v1/namespaces"),
        ("DELETE", "/api/v1/namespaces/ns1"),
        ("PUT", "/api/v1/namespaces/ns1/finalize"),
    ])
    .await;
}

#[tokio::test]
async fn service_proxy_denied_returns_403() {
    assert_denied(&[
        ("GET", "/api/v1/namespaces/default/services/s1/proxy"),
        (
            "GET",
            "/api/v1/namespaces/default/services/s1/proxy/some/path",
        ),
    ])
    .await;
}

#[tokio::test]
async fn pod_list_authorization_attributes_recorded_by_middleware() {
    let authorizer = Arc::new(RecordingAuthorizer::allow());
    let app = policy_router(authorizer.clone(), Arc::new(AtomicBool::new(false)));
    let _ = app
        .oneshot(request(
            "GET",
            "/api/v1/namespaces/default/pods",
            Body::empty(),
        ))
        .await
        .unwrap();
    let requests = authorizer.take_requests().await;
    let authorization = requests.first().expect("pod list authorization");
    assert_eq!(authorization.verb, "list");
    assert_eq!(authorization.resource.as_deref(), Some("pods"));
    assert_eq!(authorization.namespace.as_deref(), Some("default"));
    assert!(authorization.subresource.is_none());
}

#[tokio::test]
async fn k8s_non_resource_info_endpoints_still_require_authorization() {
    let reached = Arc::new(AtomicBool::new(false));
    let app = policy_router(Arc::new(RecordingAuthorizer::deny()), reached.clone());

    for uri in ["/openid/v1/jwks", "/.well-known/openid-configuration"] {
        let response = app
            .clone()
            .oneshot(request("GET", uri, Body::empty()))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{uri} must flow through native authorization"
        );
    }
    assert!(
        !reached.load(Ordering::SeqCst),
        "denied OIDC discovery request reached its handler"
    );
}
