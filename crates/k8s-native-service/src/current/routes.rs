use crate::current::*;
use axum::extract::Request;
use axum::{
    Json, Router,
    extract::State,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Middleware that gates K8s API requests on raft leadership.
/// On non-leader controlplanes:
async fn log_request(slow_threshold: Duration, request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();
    let query = uri.query().unwrap_or("");
    let request_text = if query.is_empty() {
        format!("{} {}", method, path)
    } else {
        format!("{} {}?{}", method, path, query)
    };
    let pod_log_follow = is_pod_log_follow_request(path, query);
    if query.is_empty() {
        tracing::info!(target: "klights::api", "{} {}", method, path);
    } else {
        tracing::info!(target: "klights::api", "{} {}?{}", method, path, query);
    }
    let started = Instant::now();
    let response = next.run(request).await;
    let elapsed = started.elapsed();
    let status = response.status();
    let elapsed_ms = elapsed.as_millis() as u64;
    if api_request_is_slow(elapsed, slow_threshold) {
        tracing::warn!(
            target: "klights::api",
            request = %request_text,
            status = %status,
            elapsed_ms,
            slow_threshold_ms = slow_threshold.as_millis() as u64,
            pod_log_follow,
            "slow API request completed"
        );
    } else if pod_log_follow {
        tracing::info!(
            target: "klights::api",
            request = %request_text,
            status = %status,
            elapsed_ms,
            "pod log follow HTTP response initialized"
        );
    } else {
        tracing::debug!(
            target: "klights::api",
            request = %request_text,
            status = %status,
            elapsed_ms,
            "API request completed"
        );
    }
    response
}

fn api_request_is_slow(elapsed: Duration, threshold: Duration) -> bool {
    elapsed >= threshold
}

fn is_pod_log_follow_request(path: &str, query: &str) -> bool {
    path.ends_with("/log")
        && query
            .split('&')
            .any(|pair| matches!(pair, "follow=true" | "follow=1"))
}

pub struct NativeApiOuterLayers {
    authentication_inputs: Arc<crate::policy_inputs::AuthenticationHttpInputs>,
    authorization_inputs: Arc<crate::current::policy_input_adapters::ApiAuthorizationHttpInputs>,
    priority_fairness_inputs:
        Arc<crate::current::policy_input_adapters::ApiPriorityFairnessHttpInputs>,
    slow_log_threshold: Duration,
}

impl NativeApiOuterLayers {
    pub fn finish(self, router: Router) -> Router {
        let router = self.apply_policy(router);
        self.apply_outer(router)
    }

    /// Apply native policy inside a permanent authority shell and native
    /// authentication outside it. Routes mounted by the permanent API-server
    /// shell are therefore covered by the same authorization chokepoint.
    pub fn finish_with_shell<F>(self, router: Router, shell: F) -> Router
    where
        F: FnOnce(Router) -> Router,
    {
        let router = self.apply_policy(router);
        let router = shell(router);
        self.apply_outer(router)
    }

    fn apply_policy(&self, router: Router) -> Router {
        router
            .layer({
                let authorization_inputs = self.authorization_inputs.clone();
                middleware::from_fn(move |request: Request, next: Next| {
                    let authorization_inputs = authorization_inputs.clone();
                    async move {
                        crate::auth_http::authorize_request(authorization_inputs, request, next)
                            .await
                    }
                })
            })
            .layer({
                let priority_fairness_inputs = self.priority_fairness_inputs.clone();
                middleware::from_fn(move |request: Request, next: Next| {
                    let priority_fairness_inputs = priority_fairness_inputs.clone();
                    async move {
                        crate::priority_fairness::admit_request(
                            priority_fairness_inputs,
                            request,
                            next,
                        )
                        .await
                    }
                })
            })
    }

    fn apply_outer(self, router: Router) -> Router {
        router
            .layer({
                let authentication_inputs = self.authentication_inputs;
                middleware::from_fn(move |request: Request, next: Next| {
                    let authentication_inputs = authentication_inputs.clone();
                    async move {
                        crate::auth_http::authenticate_request(authentication_inputs, request, next)
                            .await
                    }
                })
            })
            .layer(middleware::from_fn(move |request: Request, next: Next| {
                log_request(self.slow_log_threshold, request, next)
            }))
            // Outermost: content-negotiate error Status bodies to protobuf.
            // This also wraps a fail-closed 503 from the authority shell.
            .layer(middleware::from_fn(
                crate::response::negotiate_error_protobuf,
            ))
    }
}

pub(in crate::current) fn build_router_parts(
    state: ApiState,
) -> (crate::CurrentRouter, NativeApiOuterLayers) {
    let state = Arc::new(state);
    let slow_log_threshold = state.operational().config.runtime.slow_log_threshold;
    let authentication_inputs =
        Arc::new(crate::current::policy_input_adapters::authentication_http_inputs(&state));
    let authorization_inputs =
        Arc::new(crate::current::policy_input_adapters::authorization_http_inputs(&state));
    let priority_fairness_inputs =
        Arc::new(crate::current::policy_input_adapters::priority_fairness_http_inputs(&state));
    let router = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(openid_configuration),
        )
        .route("/openid/v1/jwks", get(openid_jwks))
        .route("/openapi/v2", get(get_openapi_v2))
        .route("/openapi/v3", get(get_openapi_v3_discovery))
        .route("/openapi/v3/api/v1", get(get_openapi_v3_api_v1))
        .route("/openapi/v3/apis", get(get_openapi_v3_apis))
        .route(
            "/openapi/v3/apis/{group}/{version}",
            get(get_openapi_v3_group_version),
        )
        .route("/api", get(api_versions))
        .route("/api/", get(api_versions))
        .route("/api/v1", get(api_v1_resources))
        .route("/api/v1/", get(api_v1_resources))
        .route("/apis", get(api_groups))
        .route("/apis/", get(api_groups))
        .route("/apis/autoscaling/v1", get(autoscaling_v1_resources))
        .route("/apis/autoscaling/v1/", get(autoscaling_v1_resources))
        .route("/apis/autoscaling/v2", get(autoscaling_v2_resources))
        .route("/apis/autoscaling/v2/", get(autoscaling_v2_resources))
        .route("/apis/apps/v1", get(apps_v1_resources))
        .route("/apis/apps/v1/", get(apps_v1_resources))
        .route("/apis/batch/v1", get(batch_v1_resources))
        .route("/apis/batch/v1/", get(batch_v1_resources))
        .route(
            "/apis/coordination.k8s.io/v1",
            get(coordination_v1_resources),
        )
        .route(
            "/apis/coordination.k8s.io/v1/",
            get(coordination_v1_resources),
        )
        .route("/apis/discovery.k8s.io/v1", get(discovery_v1_resources))
        .route("/apis/discovery.k8s.io/v1/", get(discovery_v1_resources))
        .route("/apis/events.k8s.io/v1", get(events_k8s_io_v1_resources))
        .route("/apis/events.k8s.io/v1/", get(events_k8s_io_v1_resources))
        .route("/apis/networking.k8s.io/v1", get(networking_v1_resources))
        .route("/apis/networking.k8s.io/v1/", get(networking_v1_resources))
        .route("/apis/storage.k8s.io/v1", get(storage_v1_resources))
        .route("/apis/storage.k8s.io/v1/", get(storage_v1_resources))
        .route("/apis/node.k8s.io/v1", get(node_k8s_io_v1_resources))
        .route("/apis/node.k8s.io/v1/", get(node_k8s_io_v1_resources))
        .route("/apis/scheduling.k8s.io/v1", get(scheduling_v1_resources))
        .route("/apis/scheduling.k8s.io/v1/", get(scheduling_v1_resources))
        .route("/apis/policy/v1", get(policy_v1_resources))
        .route("/apis/policy/v1/", get(policy_v1_resources))
        .route("/apis/rbac.authorization.k8s.io/v1", get(rbac_v1_resources))
        .route(
            "/apis/rbac.authorization.k8s.io/v1/",
            get(rbac_v1_resources),
        )
        .route(
            "/apis/authorization.k8s.io/v1",
            get(authorization_v1_resources),
        )
        .route(
            "/apis/authorization.k8s.io/v1/",
            get(authorization_v1_resources),
        )
        .route(
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            post(create_self_subject_access_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            post(create_subject_access_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            post(create_self_subject_rules_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/namespaces/{namespace}/localsubjectaccessreviews",
            post(create_local_subject_access_review),
        )
        .route(
            "/apis/certificates.k8s.io/v1",
            get(certificates_v1_resources),
        )
        .route(
            "/apis/certificates.k8s.io/v1/",
            get(certificates_v1_resources),
        )
        .route("/apis/apiextensions.k8s.io", get(apiextensions_group))
        .route("/apis/apiextensions.k8s.io/", get(apiextensions_group))
        .route(
            "/apis/apiextensions.k8s.io/v1",
            get(apiextensions_v1_resources),
        )
        .route(
            "/apis/apiextensions.k8s.io/v1/",
            get(apiextensions_v1_resources),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1",
            get(admissionregistration_v1_resources),
        )
        .route(
            "/apis/admissionregistration.k8s.io/v1/",
            get(admissionregistration_v1_resources),
        )
        .route("/apis/scheduling.k8s.io", get(scheduling_group))
        .route("/apis/scheduling.k8s.io/", get(scheduling_group))
        .route("/apis/node.k8s.io", get(node_k8s_io_group))
        .route("/apis/node.k8s.io/", get(node_k8s_io_group))
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1",
            get(flowcontrol_v1_resources),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/",
            get(flowcontrol_v1_resources),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1",
            get(apiregistration_v1_resources),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/",
            get(apiregistration_v1_resources),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices",
            get(list_apiservices)
                .post(create_apiservice)
                .delete(delete_collection_apiservices),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices/{name}",
            get(get_apiservice)
                .put(update_apiservice)
                .patch(patch_apiservice)
                .delete(delete_apiservice_with_cache_invalidation),
        )
        .route(
            "/apis/apiregistration.k8s.io/v1/apiservices/{name}/status",
            get(get_apiservice_status)
                .put(update_apiservice_status)
                .patch(patch_apiservice_status),
        )
        .route(
            "/apis/authentication.k8s.io/v1",
            get(authentication_v1_resources),
        )
        .route(
            "/apis/authentication.k8s.io/v1/",
            get(authentication_v1_resources),
        )
        .route(
            "/apis/authentication.k8s.io/v1/tokenreviews",
            post(create_token_review),
        )
        .route(
            "/apis/authentication.k8s.io/v1/tokenreviews/",
            post(create_token_review),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1",
            get(metrics_v1beta1_resources),
        )
        .route(
            "/apis/metrics.k8s.io/v1beta1/",
            get(metrics_v1beta1_resources),
        )
        .nest("/api/v1", handlers::core_v1::api_v1_routes())
        .nest(
            "/apis/metrics.k8s.io/v1beta1",
            handlers::metrics_v1beta1::metrics_v1beta1_routes(),
        )
        .nest(
            "/apis/autoscaling/v1",
            handlers::autoscaling_v1::autoscaling_v1_routes(),
        )
        .nest(
            "/apis/autoscaling/v2",
            handlers::autoscaling_v2::autoscaling_v2_routes(),
        )
        .nest("/apis/apps/v1", handlers::apps_v1::apps_v1_routes())
        .nest("/apis/batch/v1", handlers::batch_v1::batch_v1_routes())
        .nest(
            "/apis/coordination.k8s.io/v1",
            handlers::coordination_v1::coordination_v1_routes(),
        )
        .nest(
            "/apis/discovery.k8s.io/v1",
            handlers::discovery_v1::discovery_v1_routes(),
        )
        .nest(
            "/apis/events.k8s.io/v1",
            handlers::events_k8s_io_v1::events_k8s_io_v1_routes(),
        )
        .nest(
            "/apis/networking.k8s.io/v1",
            handlers::networking_v1::networking_v1_routes(),
        )
        .nest(
            "/apis/storage.k8s.io/v1",
            handlers::storage_v1::storage_v1_routes(),
        )
        .nest(
            "/apis/node.k8s.io/v1",
            handlers::node_k8s_io_v1::node_k8s_io_v1_routes(),
        )
        .nest(
            "/apis/scheduling.k8s.io/v1",
            handlers::scheduling_v1::scheduling_v1_routes(),
        )
        .nest("/apis/policy/v1", handlers::policy_v1::policy_v1_routes())
        .nest(
            "/apis/rbac.authorization.k8s.io/v1",
            handlers::rbac_v1::rbac_v1_routes(),
        )
        .nest(
            "/apis/certificates.k8s.io/v1",
            handlers::certificates_v1::certificates_v1_routes(),
        )
        .nest(
            "/apis/apiextensions.k8s.io/v1",
            handlers::apiextensions_v1::apiextensions_v1_routes(),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
            get(list_flowschemas)
                .post(create_flowschema)
                .delete(delete_collection_flowschemas),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/{name}",
            get(get_flowschema)
                .put(update_flowschema)
                .patch(patch_flowschema)
                .delete(delete_flowschema),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas/{name}/status",
            get(get_flowschema_status)
                .put(update_flowschema_status)
                .patch(patch_flowschema_status),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations",
            get(list_prioritylevelconfigurations)
                .post(create_prioritylevelconfiguration)
                .delete(delete_collection_prioritylevelconfigurations),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations/{name}",
            get(get_prioritylevelconfiguration)
                .put(update_prioritylevelconfiguration)
                .patch(patch_prioritylevelconfiguration)
                .delete(delete_prioritylevelconfiguration),
        )
        .route(
            "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations/{name}/status",
            get(get_prioritylevelconfiguration_status)
                .put(update_prioritylevelconfiguration_status)
                .patch(patch_prioritylevelconfiguration_status),
        )
        .nest(
            "/apis/admissionregistration.k8s.io/v1",
            handlers::admissionregistration_v1::admissionregistration_v1_routes(),
        )
        .route("/apis/{group}", get(api_group_by_name))
        .route("/apis/{group}/", get(api_group_by_name))
        .route("/apis/{group}/{version}", get(custom_resource_discovery))
        .route("/apis/{group}/{version}/", get(custom_resource_discovery))
        .route(
            "/apis/{group}/{version}/namespaces/{namespace}/{plural}",
            get(list_custom_resources)
                .post(create_custom_resource)
                .delete(delete_collection_custom_resources),
        )
        .route(
            "/apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}/{*subresource}",
            get(proxy_namespaced_custom_resource_subresource)
                .head(proxy_namespaced_custom_resource_subresource)
                .options(proxy_namespaced_custom_resource_subresource)
                .post(proxy_namespaced_custom_resource_subresource)
                .put(proxy_namespaced_custom_resource_subresource)
                .patch(proxy_namespaced_custom_resource_subresource)
                .delete(proxy_namespaced_custom_resource_subresource),
        )
        .route(
            "/apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}",
            get(get_custom_resource)
                .put(update_custom_resource)
                .patch(patch_custom_resource)
                .delete(delete_custom_resource),
        )
        .route(
            "/apis/{group}/{version}/{plural}",
            get(list_cluster_custom_resources)
                .post(create_cluster_custom_resource)
                .delete(delete_collection_cluster_custom_resources),
        )
        .route(
            "/apis/{group}/{version}/{plural}/{name}/{*subresource}",
            get(proxy_cluster_custom_resource_subresource)
                .head(proxy_cluster_custom_resource_subresource)
                .options(proxy_cluster_custom_resource_subresource)
                .post(proxy_cluster_custom_resource_subresource)
                .put(proxy_cluster_custom_resource_subresource)
                .patch(proxy_cluster_custom_resource_subresource)
                .delete(proxy_cluster_custom_resource_subresource),
        )
        .route(
            "/apis/{group}/{version}/{plural}/{name}",
            get(get_cluster_custom_resource)
                .put(update_cluster_custom_resource)
                .patch(patch_cluster_custom_resource)
                .delete(delete_cluster_custom_resource),
        )
        .route(
            "/debug/klights/pod-lifecycle",
            get(pod_lifecycle_debug_dump),
        );
    let router = router
        // Unmatched paths and unsupported methods must return a metav1.Status
        // body (not axum's empty-body default). Set BEFORE the auth/authz
        // layers so the fallbacks are still covered by authentication and
        // authorization (axum layers only wrap routes/fallbacks added earlier).
        .fallback(not_found_fallback)
        .method_not_allowed_fallback(method_not_allowed_fallback);
    let router = crate::CurrentRouter::new(router.with_state(state.clone()));
    (
        router,
        NativeApiOuterLayers {
            authentication_inputs,
            authorization_inputs,
            priority_fairness_inputs,
            slow_log_threshold,
        },
    )
}

#[cfg(test)]
pub(in crate::current) fn build_router_inner(state: ApiState) -> Router {
    let (router, outer_layers) = build_router_parts(state);
    outer_layers.finish(router.into_router())
}

#[cfg(test)]
pub(crate) fn build_router(state: ApiState) -> Router {
    build_router_inner(state)
}

/// 404 for any path the router does not recognise, shaped as a metav1.Status.
async fn not_found_fallback() -> crate::current::AppError {
    crate::current::AppError::NotFound(
        "the server could not find the requested resource".to_string(),
    )
}

/// 405 when a known path is hit with an unsupported method, shaped as a
/// metav1.Status (was axum's empty-body default).
async fn method_not_allowed_fallback() -> crate::current::AppError {
    crate::current::AppError::MethodNotAllowed(
        "the server does not allow this method on the requested resource".to_string(),
    )
}

#[cfg(test)]
mod phase17g_route_tests {
    use axum::body::Body;
    use axum::http::Request;
    use axum::middleware;
    use axum::routing::get;
    use prost::Message as _;
    use tower::ServiceExt;

    use super::*;

    fn fallback_router() -> Router {
        Router::new()
            .route("/api/v1", get(|| async { "ok" }))
            .fallback(not_found_fallback)
            .method_not_allowed_fallback(method_not_allowed_fallback)
            .layer(middleware::from_fn(
                crate::response::negotiate_error_protobuf,
            ))
    }

    async fn json_status(method: &str, uri: &str) -> (axum::http::StatusCode, Value) {
        let response = fallback_router()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn unmatched_path_returns_status_404() {
        let (status, body) = json_status("GET", "/this/path/does/not/exist").await;
        assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["reason"], "NotFound");
        assert_eq!(body["code"], 404);
    }

    #[tokio::test]
    async fn unsupported_method_returns_status_405() {
        let (status, body) = json_status("POST", "/api/v1").await;
        assert_eq!(status, axum::http::StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["reason"], "MethodNotAllowed");
        assert_eq!(body["code"], 405);
    }

    #[tokio::test]
    async fn error_response_negotiates_protobuf() {
        for (method, uri, reason, code) in [
            ("GET", "/no/such/path", "NotFound", 404),
            ("POST", "/api/v1", "MethodNotAllowed", 405),
        ] {
            let response = fallback_router()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("accept", "application/vnd.kubernetes.protobuf")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..4], b"k8s\0");
            let envelope = klights_kube_protobuf::Unknown::decode(&body[4..]).unwrap();
            let status = klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::Status::decode(
                &*envelope.raw,
            )
            .unwrap();
            assert_eq!(status.reason.as_deref(), Some(reason));
            assert_eq!(status.code, Some(code));
        }
    }

    #[tokio::test]
    async fn raft_materialization_conflict_maps_to_json_and_protobuf_status() {
        let diagnostic =
            "build log_apply commit for raft propose: resourceVersion precondition failed";
        let app = Router::new()
            .route(
                "/conflict",
                get(move || async move {
                    Err::<(), crate::AppError>(crate::AppError::Conflict(diagnostic.to_string()))
                }),
            )
            .layer(middleware::from_fn(
                crate::response::negotiate_error_protobuf,
            ));
        for (accept, protobuf) in [
            ("application/json", false),
            ("application/vnd.kubernetes.protobuf", true),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/conflict")
                        .header("accept", accept)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::CONFLICT);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            if protobuf {
                let envelope = klights_kube_protobuf::Unknown::decode(&body[4..]).unwrap();
                let status =
                    klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::Status::decode(
                        &*envelope.raw,
                    )
                    .unwrap();
                assert_eq!(status.code, Some(409));
                assert!(status.message.as_deref().unwrap().contains("log_apply"));
            } else {
                let status: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(status["code"], 409);
                assert!(status["message"].as_str().unwrap().contains("log_apply"));
            }
        }
    }

    #[tokio::test]
    async fn error_response_defaults_to_json() {
        let response = fallback_router()
            .oneshot(
                Request::builder()
                    .uri("/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
    }

    #[test]
    fn api_request_log_classifies_pod_log_follow_as_streaming() {
        assert!(is_pod_log_follow_request(
            "/api/v1/namespaces/default/pods/p/log",
            "container=main&follow=true"
        ));
        assert!(!is_pod_log_follow_request(
            "/api/v1/namespaces/default/pods/p/log",
            "container=main"
        ));
    }

    #[test]
    fn api_request_log_warns_when_elapsed_reaches_threshold() {
        assert!(api_request_is_slow(
            Duration::from_millis(250),
            Duration::from_millis(250)
        ));
        assert!(!api_request_is_slow(
            Duration::from_millis(249),
            Duration::from_millis(250)
        ));
    }
}

async fn openid_configuration(State(_state): State<Arc<ApiState>>) -> Json<Value> {
    let issuer = "https://kubernetes.default.svc.cluster.local";
    let jwks_uri = format!("{}/openid/v1/jwks", issuer);
    Json(serde_json::json!({
        "issuer": issuer,
        "jwks_uri": jwks_uri,
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"]
    }))
}

async fn openid_jwks(State(state): State<Arc<ApiState>>) -> Result<Json<Value>, AppError> {
    let signing_key_pem = state
        .operational()
        .signing_keys
        .service_account_signing_key_pem()
        .await
        .map_err(|error| {
            AppError::InternalError(format!("ServiceAccount signing key unavailable: {error}"))
        })?;
    let crypto =
        klights_supervisor::CryptoExecutor::new(state.operational().task_supervisor.clone());
    let jwks = crypto
        .run_blocking("build-openid-jwks", move || {
            build_openid_jwks(signing_key_pem.as_str())
        })
        .await
        .map_err(|error| AppError::InternalError(format!("OpenID JWK worker failed: {error}")))??;
    Ok(Json(jwks))
}

fn build_openid_jwks(signing_key_pem: &str) -> Result<Value, AppError> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey, traits::PublicKeyParts};
    use sha2::Digest;

    if let Ok(private_key) = RsaPrivateKey::from_pkcs8_pem(signing_key_pem) {
        let n_bytes = private_key.n().to_bytes_be();
        let e_bytes = private_key.e().to_bytes_be();
        let n_b64 = URL_SAFE_NO_PAD.encode(&n_bytes);
        let e_b64 = URL_SAFE_NO_PAD.encode(&e_bytes);
        let thumbprint_input = format!(r#"{{"e":"{}","kty":"RSA","n":"{}"}}"#, e_b64, n_b64);
        let kid = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(thumbprint_input.as_bytes()));
        return Ok(serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "n": n_b64,
                "e": e_b64,
                "kid": kid
            }]
        }));
    }

    let key_pair = rcgen::KeyPair::from_pem(signing_key_pem)
        .map_err(|e| AppError::InternalError(format!("Failed to parse signing key: {}", e)))?;
    let public_key_der = key_pair.public_key_der();
    let der_bytes: &[u8] = public_key_der.as_ref();
    if der_bytes.len() < 65 {
        return Err(AppError::InternalError(
            "Invalid EC public key DER".to_string(),
        ));
    }
    let point_start = der_bytes.len() - 65;
    if der_bytes[point_start] != 0x04 {
        return Err(AppError::InternalError(
            "Expected uncompressed EC point".to_string(),
        ));
    }
    let x_b64 = URL_SAFE_NO_PAD.encode(&der_bytes[point_start + 1..point_start + 33]);
    let y_b64 = URL_SAFE_NO_PAD.encode(&der_bytes[point_start + 33..point_start + 65]);
    let thumbprint_input = format!(
        r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
        x_b64, y_b64
    );
    let kid = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(thumbprint_input.as_bytes()));

    Ok(serde_json::json!({
        "keys": [{
            "kty": "EC",
            "crv": "P-256",
            "x": x_b64,
            "y": y_b64,
            "use": "sig",
            "alg": "ES256",
            "kid": kid
        }]
    }))
}
