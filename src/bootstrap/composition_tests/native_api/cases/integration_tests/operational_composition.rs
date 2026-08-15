use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

async fn denying_operational_router() -> axum::Router {
    let authorizer: Arc<dyn klights_auth::authorizer::Authorizer> =
        Arc::new(klights_auth::authorizer::DenyAuthorizer);
    crate::bootstrap::composition_tests::native_api::support::build_test_router_with_authorizer_and_operational_endpoints(authorizer).await
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn klights_status_route_exposes_role_and_metadata() {
    let state = crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_operational_endpoints().await;
    state.ensure_operational_cluster_metadata().await.unwrap();

    let response = state
        .router()
        .oneshot(
            Request::get("/klights/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status = response_json(response).await;
    assert_eq!(status["role"], "Leader");
    assert!(
        status["clusterId"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(status["leaderEpoch"], 0);
    assert!(status["currentResourceVersion"].as_i64().is_some());
}

#[tokio::test]
async fn klights_status_route_requires_authorization() {
    let response = denying_operational_router()
        .await
        .oneshot(
            Request::get("/klights/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn operational_routes_require_authorization_at_native_outer_layer() {
    let app = denying_operational_router().await;
    for path in [
        "/healthz",
        "/livez",
        "/readyz",
        "/metrics",
        "/version",
        "/klights/v1/status",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "{path} must pass through the native authorization layer"
        );
    }
}

#[tokio::test]
async fn leader_status_reports_follower_metrics() {
    let state = crate::bootstrap::composition_tests::native_api::support::build_test_app_state_with_operational_endpoints().await;
    state.ensure_operational_cluster_metadata().await.unwrap();
    state
        .register_operational_follower(
            klights_leader_api::NetworkDataplane::try_new(
                "replica-1",
                klights_leader_api::NetworkNodeMode::Root,
                klights_leader_api::DataplaneEncryption::Direct,
                None,
                "127.0.0.1".parse().unwrap(),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::get("/klights/v1/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status = response_json(response).await;
    assert_eq!(status["followerCount"], 1);
    assert_eq!(status["followers"][0]["nodeName"], "replica-1");
    assert_eq!(status["followers"][0]["encryption"], "disabled");
}
