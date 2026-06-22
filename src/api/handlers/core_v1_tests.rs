// Handler-family tests are relocated incrementally from src/api_tests.rs.

//! Service HTTP handler regression tests.
//!
//! These cover the Task 7 decoupling: `create_service`/`update_service`/
//! `patch_service` must NOT run endpoint/dataplane reconciliation inline on
//! the HTTP request path. Allocation/defaulting (ClusterIP, NodePort, type,
//! sessionAffinity, ports) stays synchronous so the API response stays
//! K8s-compatible; endpoint and route work is driven through the controller
//! dispatcher queue and the coalesced service-route sync.

use crate::networking::test_support::{MockNetworkProvider, MockServiceRouter};
use serde_json::json;
use std::sync::Arc;

/// Regression for the Service handler decoupling (Task 7 of fixnow):
/// `patch_service` must enqueue a Service reconcile through
/// `controller_dispatcher` and must NOT trigger an inline nft route rebuild
/// (`sync_services_now`) on the HTTP request path.
///
/// Previously `update_service`/`patch_service` called
/// `sync_service_routes_now_if_spec_changed`, which ran a full synchronous
/// nft rebuild inside the request handler whenever the Service spec changed.
/// The handler now only persists + enqueues reconcile; the controller worker
/// requests a coalesced sync via `request_services_sync`, never blocking the
/// request on nft.
#[tokio::test]
async fn service_patch_enqueues_reconcile_without_route_sync_on_request_path() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mut state = crate::api::test_support::build_test_app_state().await;

    // Swap the AppState's service router for an observable double so we can
    // assert the handler did not drive an inline nft rebuild. We also wire the
    // SAME double into the controller dispatcher's `services` slot so the
    // ServiceController's coalesced `request_services_sync` (run by the
    // sync-fallback reconcile) is observable on our counter.
    let provider = Arc::new(MockNetworkProvider::new());
    let services: Arc<MockServiceRouter> = Arc::new(MockServiceRouter::new());
    let services_dyn = services.clone() as Arc<dyn crate::networking::ServiceRouter>;
    state.controller_dispatcher.set_services(services_dyn).await;
    state.network = Arc::new(crate::networking::Network {
        datapath: provider.clone(),
        peering: provider,
        services: services.clone() as Arc<dyn crate::networking::ServiceRouter>,
        resolver: Arc::new(crate::networking::test_support::MockPodEndpointResolver),
    });
    let app = crate::api::build_router(state);

    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Create the Service through the HTTP handler. The handler allocates
    // ClusterIP/NodePort synchronously and enqueues a reconcile; it must not
    // inline-sync nft routes.
    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "patch-svc", "namespace": "default"},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/services")
        .header("content-type", "application/json")
        .body(Body::from(create_body.to_string()))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(
        create_resp.status(),
        StatusCode::CREATED,
        "Service create must succeed"
    );

    // Snapshot counters after create so the assertions below isolate the patch.
    let sync_now_after_create = services.sync_now_count();
    let sync_after_create = services.sync_count();

    // PATCH the Service SPEC (port bump). With the old handler this changed
    // `spec`, so `sync_service_routes_now_if_spec_changed` called
    // `sync_services_now` inline — blocking the request on a full nft rebuild.
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/services/patch-svc")
        .header("content-type", "application/strategic-merge-patch+json")
        .body(Body::from(
            json!({
                "spec": {"ports": [{"port": 8080, "targetPort": 8080, "protocol": "TCP"}]}
            })
            .to_string(),
        ))
        .unwrap();
    let patch_resp = app.oneshot(patch_req).await.unwrap();
    assert_eq!(
        patch_resp.status(),
        StatusCode::OK,
        "Service patch must succeed"
    );

    // THE regression assertion: the handler must not block the request on a
    // full nft route rebuild. `sync_services_now` must stay at its post-create
    // value.
    assert_eq!(
        services.sync_now_count(),
        sync_now_after_create,
        "patch_service must NOT call sync_services_now on the request path"
    );

    // A Service reconcile was dispatched: the ServiceController requests a
    // coalesced sync (request_services_sync) during reconcile, so sync_count
    // advances. This proves the reconcile was driven through the controller
    // dispatcher (queue or sync-fallback), not via an inline nft rebuild.
    assert!(
        services.sync_count() > sync_after_create,
        "patch_service must enqueue a Service reconcile (coalesced sync expected)"
    );
}
