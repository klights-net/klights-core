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
use crate::watch::{EventType, WatchTopic};
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::error::TryRecvError;

async fn post_service(
    app: axum::Router,
    name: &str,
    spec: Value,
) -> (axum::http::StatusCode, Value) {
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/services")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": name, "namespace": "default"},
                "spec": spec,
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
    (status, value)
}

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

#[tokio::test]
async fn service_create_persists_allocated_fields_in_single_service_write() {
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = crate::api::test_support::build_test_app_state().await;
    let mut watch_rx = state.db.subscribe_watch(WatchTopic::new("v1", "Service"));
    let app = crate::api::build_router(state);

    let create_body = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "single-write-svc", "namespace": "default"},
        "spec": {
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }
    });
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/services")
        .header("content-type", "application/json")
        .body(Body::from(create_body.to_string()))
        .unwrap();
    let create_resp = app.oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let create_body: Value =
        serde_json::from_slice(&to_bytes(create_resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(
        create_body
            .pointer("/spec/clusterIP")
            .and_then(|value| value.as_str()),
        Some("10.43.128.2"),
        "Service create response must include the allocated ClusterIP"
    );
    assert_eq!(
        create_body
            .pointer("/spec/ports/0/protocol")
            .and_then(|value| value.as_str()),
        Some("TCP"),
        "Service create response must include defaulted port protocol"
    );
    assert_eq!(
        create_body
            .pointer("/spec/ports/0/targetPort")
            .and_then(|value| value.as_u64()),
        Some(80),
        "Service create response must include defaulted targetPort"
    );

    let mut service_events = Vec::new();
    loop {
        match watch_rx.try_recv() {
            Ok(event)
                if event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|value| value.as_str())
                    == Some("single-write-svc")
                    && event
                        .object
                        .pointer("/metadata/namespace")
                        .and_then(|value| value.as_str())
                        == Some("default") =>
            {
                service_events.push(event);
            }
            Ok(_) => continue,
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Lagged(_)) => panic!("Service watch receiver lagged in unit test"),
            Err(TryRecvError::Closed) => panic!("Service watch receiver closed unexpectedly"),
        }
    }

    assert_eq!(
        service_events.len(),
        1,
        "Service create must persist allocated/defaulted fields in the initial create, not create then update"
    );
    assert_eq!(service_events[0].event_type, EventType::Added);
    assert_eq!(
        service_events[0]
            .object
            .pointer("/spec/clusterIP")
            .and_then(|value| value.as_str()),
        Some("10.43.128.2")
    );
    assert_eq!(
        service_events[0]
            .object
            .pointer("/spec/ports/0/protocol")
            .and_then(|value| value.as_str()),
        Some("TCP")
    );
    assert_eq!(
        service_events[0]
            .object
            .pointer("/spec/ports/0/targetPort")
            .and_then(|value| value.as_u64()),
        Some(80)
    );
}

#[tokio::test]
async fn service_create_enqueues_exactly_one_service_reconcile() {
    use axum::http::StatusCode;

    let state = crate::api::test_support::build_test_app_state().await;
    let services: Arc<MockServiceRouter> = Arc::new(MockServiceRouter::new());
    state
        .controller_dispatcher
        .set_services(services.clone() as Arc<dyn crate::networking::ServiceRouter>)
        .await;
    let app = crate::api::build_router(state);

    let (status, _) = post_service(
        app,
        "single-reconcile-svc",
        json!({
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    assert_eq!(
        services.sync_count(),
        1,
        "Service create must enqueue exactly one Service reconcile"
    );
    assert_eq!(
        services.sync_now_count(),
        0,
        "Service create must not run an immediate route sync on the request path"
    );
}

#[tokio::test]
async fn service_create_releases_allocated_cluster_ip_when_initial_create_conflicts() {
    use axum::http::StatusCode;

    let state = crate::api::test_support::build_test_app_state().await;
    let app = crate::api::build_router(state);

    let (first_status, first_body) = post_service(
        app.clone(),
        "conflict-svc",
        json!({
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }),
    )
    .await;
    assert_eq!(first_status, StatusCode::CREATED);
    assert_eq!(
        first_body
            .pointer("/spec/clusterIP")
            .and_then(|value| value.as_str()),
        Some("10.43.128.2")
    );

    let (conflict_status, _) = post_service(
        app.clone(),
        "conflict-svc",
        json!({
            "selector": {"app": "api"},
            "ports": [{"port": 8080}]
        }),
    )
    .await;
    assert_eq!(conflict_status, StatusCode::CONFLICT);

    let (next_status, next_body) = post_service(
        app,
        "after-conflict-svc",
        json!({
            "selector": {"app": "next"},
            "ports": [{"port": 8081}]
        }),
    )
    .await;
    assert_eq!(next_status, StatusCode::CREATED);
    assert_eq!(
        next_body
            .pointer("/spec/clusterIP")
            .and_then(|value| value.as_str()),
        Some("10.43.128.3"),
        "ClusterIP allocated for the failed duplicate create must be released"
    );
}

#[tokio::test]
async fn service_create_releases_cluster_ip_when_nodeport_allocation_fails() {
    use axum::http::StatusCode;

    let state = crate::api::test_support::build_test_app_state().await;
    for port in 30000..=32767 {
        state.nodeport_alloc.mark_used(port);
    }
    let app = crate::api::build_router(state);

    let (failed_status, _) = post_service(
        app.clone(),
        "nodeport-fail-svc",
        json!({
            "type": "NodePort",
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }),
    )
    .await;
    assert_eq!(failed_status, StatusCode::INTERNAL_SERVER_ERROR);

    let (next_status, next_body) = post_service(
        app,
        "after-nodeport-fail-svc",
        json!({
            "selector": {"app": "next"},
            "ports": [{"port": 8081}]
        }),
    )
    .await;
    assert_eq!(next_status, StatusCode::CREATED);
    assert_eq!(
        next_body
            .pointer("/spec/clusterIP")
            .and_then(|value| value.as_str()),
        Some("10.43.128.2"),
        "ClusterIP reserved before the failed NodePort allocation must be released"
    );
}

/// Task 10 regression: when Service allocation/defaulting fails inside the
/// create path (`create_inner` -> `prepare_service_for_create`), the handler
/// must return an error BEFORE reaching the
/// `enqueue_generated_controller_after_mutation` call at the bottom of
/// `create_inner`. A regression that swallows the allocation error and falls
/// through to the enqueue (or that enqueues before allocation) would re-arm a
/// Service reconcile for a Service that was never persisted.
///
/// This drives the real HTTP create path end-to-end and observes the enqueue
/// through `MockServiceRouter`: every successful Service reconcile (synchronous
/// fallback in the test dispatcher) bumps `sync_count`, so an enqueue after the
/// failed create would make `sync_count` non-zero. The test fails if a future
/// change enqueues after an allocation failure.
#[tokio::test]
async fn create_service_does_not_enqueue_reconcile_after_allocation_failure() {
    use axum::http::StatusCode;

    let state = crate::api::test_support::build_test_app_state().await;
    // Exhaust the NodePort range so NodePort allocation inside
    // `prepare_service_for_create` fails for a NodePort Service.
    for port in 30000..=32767 {
        state.nodeport_alloc.mark_used(port);
    }
    let services: Arc<MockServiceRouter> = Arc::new(MockServiceRouter::new());
    state
        .controller_dispatcher
        .set_services(services.clone() as Arc<dyn crate::networking::ServiceRouter>)
        .await;
    let app = crate::api::build_router(state);

    let (status, _) = post_service(
        app,
        "alloc-fail-no-enqueue-svc",
        json!({
            "type": "NodePort",
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "Service create must fail when NodePort allocation is exhausted"
    );

    assert_eq!(
        services.sync_count(),
        0,
        "create path must NOT enqueue a Service reconcile after allocation failure"
    );
    assert_eq!(
        services.sync_now_count(),
        0,
        "create path must NOT run an immediate route sync after allocation failure"
    );
}

/// Task 10 regression: a successful Service create must (a) populate the
/// allocated identity fields (`spec.clusterIP` / `spec.clusterIPs`) in the
/// persisted+returned object and (b) enqueue EXACTLY one Service reconcile
/// through `controller_dispatcher.enqueue(` at the end of `create_inner`.
///
/// Driving the real HTTP create path is required because
/// `prepare_service_for_create` performs allocation only — the enqueue happens
/// later in `create_inner`. Asserting `sync_count() == 1` here fences both an
/// accidental second enqueue and a regression that drops the enqueue entirely.
#[tokio::test]
async fn create_service_success_response_contains_allocated_fields_and_enqueues_once() {
    use axum::http::StatusCode;

    let state = crate::api::test_support::build_test_app_state().await;
    let services: Arc<MockServiceRouter> = Arc::new(MockServiceRouter::new());
    state
        .controller_dispatcher
        .set_services(services.clone() as Arc<dyn crate::networking::ServiceRouter>)
        .await;
    let app = crate::api::build_router(state);

    let (status, body) = post_service(
        app,
        "allocated-and-enqueued-svc",
        json!({
            "type": "ClusterIP",
            "selector": {"app": "web"},
            "ports": [{"port": 80}]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let cluster_ip = body
        .pointer("/spec/clusterIP")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert!(
        !cluster_ip.is_empty() && cluster_ip != "None",
        "create response must carry an allocated clusterIP, got: {cluster_ip:?}"
    );
    assert!(
        body.pointer("/spec/clusterIPs")
            .and_then(|value| value.as_array())
            .is_some_and(|arr| !arr.is_empty()),
        "create response must carry non-empty clusterIPs"
    );

    assert_eq!(
        services.sync_count(),
        1,
        "successful Service create must enqueue exactly one Service reconcile"
    );
    assert_eq!(
        services.sync_now_count(),
        0,
        "successful Service create must not run an immediate route sync on the request path"
    );
}
