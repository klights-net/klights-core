use crate::api::*;
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
async fn log_request(request: Request, next: Next) -> Response {
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
    let slow_threshold = api_slow_log_threshold();
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

fn api_slow_log_threshold() -> Duration {
    const DEFAULT_API_SLOW_LOG_MS: u64 = 250;
    let millis = std::env::var("KLIGHTS_API_SLOW_LOG_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .unwrap_or(DEFAULT_API_SLOW_LOG_MS);
    Duration::from_millis(millis)
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

pub fn build_router(state: AppState) -> Router {
    let state = Arc::new(state);
    Router::new()
        .route("/healthz", get(health_check))
        .route("/metrics", get(metrics_handler))
        .route("/version", get(version))
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
        )
        .nest(
            "/klights/v1/task-supervisor",
            crate::task_supervisor::api::routes(),
        )
        .route("/klights/v1/status", get(klights_status_handler))
        // Unmatched paths and unsupported methods must return a metav1.Status
        // body (not axum's empty-body default). Set BEFORE the auth/authz
        // layers so the fallbacks are still covered by authentication and
        // authorization (axum layers only wrap routes/fallbacks added earlier).
        .fallback(not_found_fallback)
        .method_not_allowed_fallback(method_not_allowed_fallback)
        // Global authorization chokepoint. Added before the authentication
        // and raft proxy layers so it executes *after* authentication and
        // after follower proxy fallback decisions (axum runs the first-added
        // layer innermost). Leader-proxied requests are authorized by the
        // leader using the delegated identity; local follower reads and watch
        // requests continue through this local authorization chokepoint.
        .layer({
            let authz_state = state.clone();
            middleware::from_fn(move |request: Request, next: Next| {
                let authz_state = authz_state.clone();
                async move { crate::auth::authorize_request(authz_state, request, next).await }
            })
        })
        .layer(middleware::from_fn_with_state(
            state.clone(),
            crate::api::raft_proxy::leader_proxy_middleware,
        ))
        .layer({
            let auth_state = state.clone();
            middleware::from_fn(move |request: Request, next: Next| {
                let auth_state = auth_state.clone();
                async move { crate::auth::authenticate_request(auth_state, request, next).await }
            })
        })
        .layer(middleware::from_fn(log_request))
        // Outermost: content-negotiate error Status bodies to protobuf. Added
        // last so it wraps every route, layer, and fallback.
        .layer(middleware::from_fn(negotiate_error_protobuf))
        .with_state(state)
}

/// Content-negotiate error responses: a JSON `metav1.Status` produced for a
/// 4xx/5xx is re-encoded to `application/vnd.kubernetes.protobuf` when the
/// client's `Accept` asked for protobuf. `AppError::into_response` cannot see
/// the request headers, so negotiation happens here, in the outermost layer
/// (covering the 404/405 fallbacks too). Success responses negotiate at their
/// handler via `K8sResponse`; streaming 2xx (watch) are never touched.
async fn negotiate_error_protobuf(request: Request, next: Next) -> Response {
    let wants_protobuf = crate::api::response::prefers_protobuf(request.headers());
    let response = next.run(request).await;
    if !wants_protobuf {
        return response;
    }
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"));
    if !is_json {
        return response;
    }

    // Buffer the (small) error body and try to re-encode it as a protobuf
    // Status. On any failure, fall back to the original JSON bytes.
    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return (parts.status, "error reading response body").into_response(),
    };
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            return Response::from_parts(parts, axum::body::Body::from(bytes));
        }
    };
    match crate::protobuf::encode_protobuf(&value) {
        Ok(pb) => {
            parts.headers.insert(
                axum::http::header::CONTENT_TYPE,
                "application/vnd.kubernetes.protobuf".parse().unwrap(),
            );
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            Response::from_parts(parts, axum::body::Body::from(pb))
        }
        Err(_) => {
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            Response::from_parts(parts, axum::body::Body::from(bytes))
        }
    }
}

/// 404 for any path the router does not recognise, shaped as a metav1.Status.
async fn not_found_fallback() -> crate::api::AppError {
    crate::api::AppError::NotFound("the server could not find the requested resource".to_string())
}

/// 405 when a known path is hit with an unsupported method, shaped as a
/// metav1.Status (was axum's empty-body default).
async fn method_not_allowed_fallback() -> crate::api::AppError {
    crate::api::AppError::MethodNotAllowed(
        "the server does not allow this method on the requested resource".to_string(),
    )
}

async fn health_check() -> &'static str {
    "ok"
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> String {
    state.metrics.render_prometheus()
}

async fn version() -> Json<crate::version::VersionInfo> {
    Json(crate::version::VersionInfo::new())
}

async fn openid_configuration(State(_state): State<Arc<AppState>>) -> Json<Value> {
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

async fn openid_jwks(State(state): State<Arc<AppState>>) -> Result<Json<Value>, AppError> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use rsa::{RsaPrivateKey, pkcs8::DecodePrivateKey, traits::PublicKeyParts};
    use sha2::Digest;

    let signing_key_pem =
        crate::auth::read_service_account_signing_key_async(&state.config.containerd_namespace)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to read signing key: {}", e)))?;

    if let Ok(private_key) = RsaPrivateKey::from_pkcs8_pem(&signing_key_pem) {
        let n_bytes = private_key.n().to_bytes_be();
        let e_bytes = private_key.e().to_bytes_be();
        let n_b64 = URL_SAFE_NO_PAD.encode(&n_bytes);
        let e_b64 = URL_SAFE_NO_PAD.encode(&e_bytes);
        let thumbprint_input = format!(r#"{{"e":"{}","kty":"RSA","n":"{}"}}"#, e_b64, n_b64);
        let kid = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(thumbprint_input.as_bytes()));
        return Ok(Json(serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "n": n_b64,
                "e": e_b64,
                "kid": kid
            }]
        })));
    }

    let key_pair = rcgen::KeyPair::from_pem(&signing_key_pem)
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

    Ok(Json(serde_json::json!({
        "keys": [{
            "kty": "EC",
            "crv": "P-256",
            "x": x_b64,
            "y": y_b64,
            "use": "sig",
            "alg": "ES256",
            "kid": kid
        }]
    })))
}

/// 2A-12: Role/status surface for manual promotion.
async fn klights_status_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, AppError> {
    let metadata = crate::bootstrap::cluster_meta::read_cluster_metadata(state.db.as_ref())
        .await
        .map_err(|e| AppError::InternalError(format!("failed to read cluster metadata: {}", e)))?;
    let role = match &state.role {
        crate::bootstrap::NodeRole::Leader { .. } => "Leader",
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints,
            as_learner: true,
            ..
        } if !leader_endpoints.is_empty() => "Replica",
        crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints, ..
        } if leader_endpoints.is_empty() => "ControlplaneSeed",
        crate::bootstrap::NodeRole::Controlplane { .. } => "ControlplaneJoin",
        crate::bootstrap::NodeRole::Worker { .. } => "Worker",
    };
    let leader_endpoint = match &state.role {
        crate::bootstrap::NodeRole::Worker {
            leader_endpoints, ..
        }
        | crate::bootstrap::NodeRole::Controlplane {
            leader_endpoints, ..
        } => leader_endpoints.first().cloned(),
        _ => None,
    };
    let follower_metrics = match &state.replication {
        Some(replication) => replication.follower_metrics().await,
        None => crate::replication::service::FollowerMetrics::default(),
    };
    let followers: Vec<Value> = follower_metrics
        .followers
        .into_iter()
        .map(|follower| {
            serde_json::json!({
                "nodeName": follower.node_name,
                "appliedResourceVersion": follower.applied_rv,
                "lag": follower.lag,
                "mode": follower.mode,
                "encryption": follower.encryption,
                "publicKey": follower.public_key,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "role": role,
        "leaderEndpoint": leader_endpoint,
        "clusterId": metadata.cluster_id,
        "leaderEpoch": metadata.leader_epoch,
        "currentResourceVersion": metadata.current_rv,
        "replicaLastAppliedResourceVersion": serde_json::Value::Null,
        "streamState": if matches!(
            state.role,
            crate::bootstrap::NodeRole::Worker { .. }
        ) { "streaming" } else { "local" },
        "streamLag": serde_json::Value::Null,
        "followers": followers,
        "followerCount": follower_metrics.follower_count,
        "maxFollowerLag": follower_metrics.max_lag,
    })))
}

#[cfg(test)]
mod status_tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn klights_status_route_exposes_role_and_metadata() {
        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["role"], "Leader");
        assert!(value["clusterId"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(value["leaderEpoch"], 0);
        assert!(value.get("currentResourceVersion").is_some());
        assert_eq!(value["followerCount"], 0);
        assert_eq!(value["maxFollowerLag"], 0);
    }

    #[tokio::test]
    async fn klights_status_route_requires_authorization() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(state.db.as_ref())
            .await
            .unwrap();
        state.authorizer = std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn metrics_route_requires_authorization() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        state.authorizer = std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn unmatched_path_returns_status_404() {
        let state = crate::api::test_support::build_test_app_state().await;
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/this/path/does/not/exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["reason"], "NotFound");
        assert_eq!(status["code"], 404);
    }

    #[tokio::test]
    async fn unsupported_method_returns_status_405() {
        let state = crate::api::test_support::build_test_app_state().await;
        let app = crate::api::build_router(state);

        // /api/v1 is GET-only; POST must return a 405 metav1.Status.
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::METHOD_NOT_ALLOWED
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["reason"], "MethodNotAllowed");
        assert_eq!(status["code"], 405);
    }

    #[tokio::test]
    async fn error_response_negotiates_protobuf() {
        let state = crate::api::test_support::build_test_app_state().await;
        let app = crate::api::build_router(state);

        // 404 with Accept: protobuf must return a protobuf-encoded Status.
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/no/such/path")
                    .header("accept", "application/vnd.kubernetes.protobuf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/vnd.kubernetes.protobuf"),
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        // K8s protobuf wire format: "k8s\0" magic prefix.
        assert_eq!(&body[..4], &[0x6b, 0x38, 0x73, 0x00]);
    }

    #[tokio::test]
    async fn error_response_defaults_to_json() {
        let state = crate::api::test_support::build_test_app_state().await;
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/no/such/path")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.starts_with("application/json"),
            "default error is JSON: {ct}"
        );
    }

    #[tokio::test]
    async fn join_token_route_is_removed() {
        let (app, db) = crate::api::test_support::build_test_router_with_db().await;
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/join-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn api_request_log_classifies_pod_log_follow_as_streaming() {
        assert!(super::is_pod_log_follow_request(
            "/api/v1/namespaces/default/pods/p/log",
            "container=main&follow=true"
        ));
        assert!(!super::is_pod_log_follow_request(
            "/api/v1/namespaces/default/pods/p/log",
            "container=main"
        ));
    }

    #[test]
    fn api_request_log_warns_when_elapsed_reaches_threshold() {
        assert!(super::api_request_is_slow(
            std::time::Duration::from_millis(250),
            std::time::Duration::from_millis(250)
        ));
        assert!(!super::api_request_is_slow(
            std::time::Duration::from_millis(249),
            std::time::Duration::from_millis(250)
        ));
    }

    #[tokio::test]
    async fn worker_status_reports_streaming_state() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        state.role = crate::bootstrap::NodeRole::Worker {
            leader_endpoints: vec!["127.0.0.1:17443".to_string()],
            token: Some("tok".to_string()),
            skip_ca: false,
        };
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(state.db.as_ref())
            .await
            .unwrap();
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["role"], "Worker");
        assert_eq!(value["streamState"], "streaming");
        assert_eq!(value["leaderEndpoint"], "127.0.0.1:17443");
    }

    #[tokio::test]
    async fn leader_status_reports_follower_metrics() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(state.db.as_ref())
            .await
            .unwrap();
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let replication = std::sync::Arc::new(crate::replication::ReplicationService::new(
            state.db.clone(),
            supervisor,
        ));
        let (_follower_rx, _follower_session) = replication
            .register_follower(
                crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    "replica-1".to_string(),
                    crate::networking::wireguard::DataplaneMode::Root,
                    crate::networking::wireguard::DataplaneEncryption::Disabled,
                    None,
                    Some("127.0.0.1".to_string()),
                    None,
                )
                .unwrap(),
            )
            .await;
        state.replication = Some(replication);
        let app = crate::api::build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/klights/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["followerCount"], 1);
        assert_eq!(value["followers"][0]["nodeName"], "replica-1");
        assert_eq!(value["followers"][0]["encryption"], "disabled");
    }
}
