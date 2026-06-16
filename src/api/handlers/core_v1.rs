use crate::api::*;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;

// Service and serviceaccount-token authorization is enforced by the global
// `authorize_request` middleware chokepoint (see src/auth/middleware.rs).

pub fn api_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Cluster-scoped resources
        .route("/namespaces", get(list_namespaces).post(create_namespace))
        .route(
            "/namespaces/{name}",
            get(get_namespace)
                .put(update_namespace)
                .patch(patch_namespace)
                .delete(delete_namespace),
        )
        .route("/namespaces/{name}/finalize", put(finalize_namespace))
        .route(
            "/namespaces/{name}/status",
            get(get_namespace_status)
                .put(update_namespace_status)
                .patch(patch_namespace_status),
        )
        .route("/nodes", get(list_nodes).post(create_node))
        .route(
            "/nodes/{name}",
            get(get_node)
                .put(update_node)
                .patch(patch_node)
                .delete(delete_node),
        )
        .route(
            "/nodes/{name}/status",
            get(get_node)
                .put(update_node_status)
                .patch(patch_node_status),
        )
        // Node proxy subresource — proxies to embedded kubelet API
        // Used by Sonobuoy: GET /api/v1/nodes/{name}/proxy/pods
        .route(
            "/nodes/{name}/proxy",
            get(node_proxy)
                .post(node_proxy)
                .put(node_proxy)
                .delete(node_proxy)
                .patch(node_proxy),
        )
        .route(
            "/nodes/{name}/proxy/",
            get(node_proxy)
                .post(node_proxy)
                .put(node_proxy)
                .delete(node_proxy)
                .patch(node_proxy),
        )
        .route(
            "/nodes/{name}/proxy/{*path}",
            get(node_proxy_with_path)
                .post(node_proxy_with_path)
                .put(node_proxy_with_path)
                .delete(node_proxy_with_path)
                .patch(node_proxy_with_path),
        )
        .route(
            "/persistentvolumes",
            get(list_persistent_volumes)
                .post(create_persistent_volume)
                .delete(delete_collection_persistent_volumes),
        )
        .route(
            "/persistentvolumes/{name}",
            get(get_persistent_volume)
                .put(update_persistent_volume)
                .patch(patch_persistent_volume)
                .delete(delete_persistent_volume),
        )
        .route(
            "/persistentvolumes/{name}/status",
            get(get_persistentvolume_status)
                .put(update_persistentvolume_status)
                .patch(patch_persistentvolume_status),
        )
        // Cluster-wide list (all namespaces) — used by kubectl --all-namespaces and Sonobuoy
        .route("/pods", get(list_all_pods))
        .route("/services", get(list_all_services))
        .route("/endpoints", get(list_all_endpoints))
        .route("/configmaps", get(list_all_configmaps))
        .route("/secrets", get(list_all_secrets))
        .route("/events", get(list_all_events))
        .route("/serviceaccounts", get(list_all_serviceaccounts))
        .route("/persistentvolumeclaims", get(list_all_pvcs))
        .route("/leases", get(list_all_leases_v1))
        .route(
            "/replicationcontrollers",
            get(list_all_replicationcontrollers),
        )
        .route("/podtemplates", get(list_all_podtemplates))
        .route("/limitranges", get(list_all_limitranges))
        .route("/resourcequotas", get(list_all_resourcequotas))
        // Namespaced resources
        .route(
            "/namespaces/{namespace}/pods",
            get(list_pods)
                .post(create_pod)
                .delete(delete_collection_pods),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}",
            get(get_pod)
                .put(update_pod)
                .patch(patch_pod)
                .delete(delete_pod),
        )
        // Pod subresources
        .route("/namespaces/{namespace}/pods/{name}/log", get(get_pod_log))
        .route(
            "/namespaces/{namespace}/pods/{name}/exec",
            get(pod_exec).post(pod_exec),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/attach",
            get(pod_attach).post(pod_attach),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/portforward",
            get(pod_portforward).post(pod_portforward),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/binding",
            post(pod_binding),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/status",
            get(get_pod_status)
                .put(update_pod_status_subresource)
                .patch(patch_pod_status_subresource),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/ephemeralcontainers",
            get(get_pod_ephemeral_containers)
                .put(update_pod_ephemeral_containers)
                .patch(patch_pod_ephemeral_containers),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/eviction",
            post(pod_eviction),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/proxy",
            get(pod_proxy)
                .post(pod_proxy)
                .put(pod_proxy)
                .delete(pod_proxy)
                .options(pod_proxy)
                .patch(pod_proxy),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/proxy/",
            get(pod_proxy)
                .post(pod_proxy)
                .put(pod_proxy)
                .delete(pod_proxy)
                .options(pod_proxy)
                .patch(pod_proxy),
        )
        .route(
            "/namespaces/{namespace}/pods/{name}/proxy/{*path}",
            get(pod_proxy_with_path)
                .post(pod_proxy_with_path)
                .put(pod_proxy_with_path)
                .delete(pod_proxy_with_path)
                .options(pod_proxy_with_path)
                .patch(pod_proxy_with_path),
        )
        .route(
            "/namespaces/{namespace}/services",
            get(list_services)
                .post(create_service)
                .delete(delete_collection_services),
        )
        .route(
            "/namespaces/{namespace}/services/{name}",
            get(get_service)
                .put(update_service)
                .patch(patch_service)
                .delete(delete_service),
        )
        .route(
            "/namespaces/{namespace}/services/{name}/status",
            get(get_service)
                .put(update_service_status)
                .patch(patch_service_status),
        )
        .route(
            "/namespaces/{namespace}/services/{name}/proxy",
            get(service_proxy)
                .post(service_proxy)
                .put(service_proxy)
                .delete(service_proxy)
                .options(service_proxy)
                .patch(service_proxy),
        )
        .route(
            "/namespaces/{namespace}/services/{name}/proxy/",
            get(service_proxy)
                .post(service_proxy)
                .put(service_proxy)
                .delete(service_proxy)
                .options(service_proxy)
                .patch(service_proxy),
        )
        .route(
            "/namespaces/{namespace}/services/{name}/proxy/{*path}",
            get(service_proxy_with_path)
                .post(service_proxy_with_path)
                .put(service_proxy_with_path)
                .delete(service_proxy_with_path)
                .options(service_proxy_with_path)
                .patch(service_proxy_with_path),
        )
        .route(
            "/namespaces/{namespace}/endpoints",
            get(list_endpoints)
                .post(create_endpoints)
                .delete(delete_collection_endpoints),
        )
        .route(
            "/namespaces/{namespace}/endpoints/{name}",
            get(get_endpoints)
                .put(update_endpoints)
                .patch(patch_endpoints)
                .delete(delete_endpoints),
        )
        .route(
            "/namespaces/{namespace}/configmaps",
            get(list_configmaps)
                .post(create_configmap)
                .delete(delete_collection_configmaps),
        )
        .route(
            "/namespaces/{namespace}/configmaps/{name}",
            get(get_configmap)
                .put(update_configmap)
                .patch(patch_configmap)
                .delete(delete_configmap),
        )
        .route(
            "/namespaces/{namespace}/secrets",
            get(list_secrets)
                .post(create_secret)
                .delete(delete_collection_secrets),
        )
        .route(
            "/namespaces/{namespace}/secrets/{name}",
            get(get_secret)
                .put(update_secret)
                .patch(patch_secret)
                .delete(delete_secret),
        )
        .route(
            "/namespaces/{namespace}/persistentvolumeclaims",
            get(list_persistent_volume_claims)
                .post(create_persistent_volume_claim)
                .delete(delete_collection_persistent_volume_claims),
        )
        .route(
            "/namespaces/{namespace}/persistentvolumeclaims/{name}",
            get(get_persistent_volume_claim)
                .put(update_persistent_volume_claim)
                .patch(patch_persistent_volume_claim)
                .delete(delete_persistent_volume_claim),
        )
        .route(
            "/namespaces/{namespace}/persistentvolumeclaims/{name}/status",
            get(get_persistentvolumeclaim_status)
                .put(update_persistentvolumeclaim_status)
                .patch(patch_persistentvolumeclaim_status),
        )
        .route(
            "/namespaces/{namespace}/serviceaccounts",
            get(list_service_accounts)
                .post(create_service_account)
                .delete(delete_collection_service_accounts),
        )
        .route(
            "/namespaces/{namespace}/serviceaccounts/{name}",
            get(get_service_account)
                .put(update_service_account)
                .patch(patch_service_account)
                .delete(delete_service_account),
        )
        .route(
            "/namespaces/{namespace}/serviceaccounts/{name}/token",
            post(create_serviceaccount_token),
        )
        .route(
            "/namespaces/{namespace}/events",
            get(list_events)
                .post(create_event)
                .delete(delete_collection_events),
        )
        .route(
            "/namespaces/{namespace}/events/{name}",
            get(get_event)
                .put(update_event)
                .patch(patch_event)
                .delete(delete_event),
        )
        .route(
            "/namespaces/{namespace}/leases",
            get(list_leases_v1)
                .post(create_lease_v1)
                .delete(delete_collection_leases_v1),
        )
        .route(
            "/namespaces/{namespace}/leases/{name}",
            get(get_lease_v1)
                .put(update_lease_v1)
                .patch(patch_lease_v1)
                .delete(delete_lease_v1),
        )
        .route(
            "/namespaces/{namespace}/replicationcontrollers",
            get(list_replicationcontrollers)
                .post(create_replicationcontroller)
                .delete(delete_collection_replicationcontrollers),
        )
        .route(
            "/namespaces/{namespace}/replicationcontrollers/{name}",
            get(get_replicationcontroller)
                .put(update_replicationcontroller)
                .patch(patch_replicationcontroller)
                .delete(delete_replicationcontroller),
        )
        .route(
            "/namespaces/{namespace}/limitranges",
            get(list_limitranges)
                .post(create_limitrange)
                .delete(delete_collection_limitranges),
        )
        .route(
            "/namespaces/{namespace}/limitranges/{name}",
            get(get_limitrange)
                .put(update_limitrange)
                .patch(patch_limitrange)
                .delete(delete_limitrange),
        )
        .route(
            "/namespaces/{namespace}/resourcequotas",
            get(list_resourcequotas)
                .post(create_resourcequota)
                .delete(delete_collection_resourcequotas),
        )
        .route(
            "/namespaces/{namespace}/resourcequotas/{name}",
            get(get_resourcequota)
                .put(update_resourcequota)
                .patch(patch_resourcequota)
                .delete(delete_resourcequota),
        )
        .route(
            "/namespaces/{namespace}/resourcequotas/{name}/status",
            get(get_resourcequota)
                .put(update_resourcequota_status)
                .patch(patch_resourcequota_status),
        )
        .route(
            "/namespaces/{namespace}/replicationcontrollers/{name}/status",
            get(get_replicationcontroller)
                .put(update_replicationcontroller_status)
                .patch(patch_replicationcontroller_status),
        )
        .route(
            "/namespaces/{namespace}/replicationcontrollers/{name}/scale",
            get(get_replicationcontroller_scale)
                .put(update_replicationcontroller_scale)
                .patch(patch_replicationcontroller_scale),
        )
        .route(
            "/namespaces/{namespace}/podtemplates",
            get(list_podtemplates)
                .post(create_podtemplate)
                .delete(delete_collection_podtemplates),
        )
        .route(
            "/namespaces/{namespace}/podtemplates/{name}",
            get(get_podtemplate)
                .put(update_podtemplate)
                .patch(patch_podtemplate)
                .delete(delete_podtemplate),
        )
}

async fn create_service(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    // F3-04: borrow `body` for the quota check, then move it into the inner
    // handler. Previously the body Value was deep-cloned for every Service
    // create even though the wrapper only needed a non-owning view of it.
    crate::api::quotas::check_service_type_quota(state.db.as_ref(), &namespace, &body).await?;

    // F6-02: Check if NodePort allocator is ready before allowing Service mutations.
    // This ensures leader promotion has completed the rebuild before accepting new allocations.
    if !state.nodeport_alloc.is_ready() {
        return Err(AppError::ServiceUnavailable(
            "NodePort allocator is not ready, please retry".to_string(),
        ));
    }

    let result = create_service_base(
        State(state),
        Path(namespace),
        Query(query),
        axum::Extension(identity),
        LenientJson(body),
    )
    .await?;

    Ok(result)
}

async fn update_service(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    // F6-02: Check if NodePort allocator is ready before allowing Service mutations.
    // This ensures leader promotion has completed the rebuild before accepting new allocations.
    if !state.nodeport_alloc.is_ready() {
        return Err(AppError::ServiceUnavailable(
            "NodePort allocator is not ready, please retry".to_string(),
        ));
    }

    // F3-04: nothing reads `body` after the inner call, so move it.
    let result = update_service_base(
        State(state.clone()),
        Path((namespace.clone(), name.clone())),
        Query(query),
        axum::Extension(identity),
        LenientJson(body),
    )
    .await?;

    // Allocation/defaulting runs synchronously so the API response reflects
    // allocated fields. Endpoint/EndpointSlice reconciliation and the dataplane
    // route sync run asynchronously via the controller dispatcher after the
    // enqueue below — never inline on the request path.
    let allocated = crate::controllers::service::allocate_service_fields_for_api_write(
        state.db.as_ref(),
        &result.0,
        state.service_ipam.as_ref(),
        state.nodeport_alloc.as_ref(),
    )
    .await
    .map_err(|e| AppError::Internal(format!("Failed to allocate service fields: {}", e)))?;

    let response = match allocated {
        Some(allocated) => {
            state.controller_dispatcher.enqueue(&allocated).await;
            allocated
        }
        None => result.0.clone(),
    };
    Ok(Json(response))
}

async fn patch_service(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    // F6-02: Check if NodePort allocator is ready before allowing Service mutations.
    // This ensures leader promotion has completed the rebuild before accepting new allocations.
    if !state.nodeport_alloc.is_ready() {
        return Err(AppError::ServiceUnavailable(
            "NodePort allocator is not ready, please retry".to_string(),
        ));
    }

    let result = patch_service_base(
        State(state.clone()),
        Path((namespace.clone(), name.clone())),
        Query(query),
        headers,
        axum::Extension(identity),
        body,
    )
    .await?;

    // Allocation/defaulting runs synchronously so the API response reflects
    // allocated fields. Endpoint/EndpointSlice reconciliation and the dataplane
    // route sync run asynchronously via the controller dispatcher after the
    // enqueue below — never inline on the request path.
    let allocated = crate::controllers::service::allocate_service_fields_for_api_write(
        state.db.as_ref(),
        &result.0,
        state.service_ipam.as_ref(),
        state.nodeport_alloc.as_ref(),
    )
    .await
    .map_err(|e| AppError::Internal(format!("Failed to allocate service fields: {}", e)))?;

    let response = match allocated {
        Some(allocated) => {
            state.controller_dispatcher.enqueue(&allocated).await;
            allocated
        }
        None => result.0.clone(),
    };
    Ok(Json(response))
}

async fn delete_service(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let resource = state
        .db
        .get_resource("v1", "Service", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound("Service not found".to_string()))?;

    state
        .db
        .delete_resource("v1", "Service", Some(&namespace), &name)
        .await?;

    crate::controllers::service::release_service_allocations_from_resource(
        state.service_ipam.as_ref(),
        state.nodeport_alloc.as_ref(),
        &resource.data,
    );

    let svc_name = name.clone();
    if let Err(e) = state
        .db
        .delete_resource("v1", "Endpoints", Some(&namespace), &name)
        .await
    {
        tracing::warn!(namespace = %namespace, name = %name, error = %e, "service delete: associated Endpoints delete failed");
    }

    let data_with_uid = inject_resource_version(resource.data.clone(), resource.resource_version);
    let owner_uid = data_with_uid
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    if let Err(e) = controllers::gc::cascade_delete_with_uid(
        state.db.as_ref(),
        &owner_uid,
        "v1",
        &svc_name,
        "Service",
        Some(namespace.clone()),
        state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
    )
    .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(namespace = %namespace, name = %svc_name, error = %e, "service delete: cascade delete failed");
    }

    state.network.services.request_services_sync();

    crate::side_effects::run_hooks_logged(
        &state.side_effects,
        &resource.data,
        state.db.as_ref(),
        &state.metrics,
        "service_delete",
    )
    .await;

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

async fn create_serviceaccount_token(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let sa = state
        .db
        .get_resource("v1", "ServiceAccount", Some(&namespace), &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("ServiceAccount {} not found", name)))?;

    let audiences: Vec<&str> = body
        .get("spec")
        .and_then(|s| s.get("audiences"))
        .and_then(|a| a.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_else(|| {
            vec![
                "https://kubernetes.default.svc.cluster.local",
                "https://kubernetes.default.svc",
                "api",
            ]
        });
    let expiration_seconds = crate::auth::normalize_service_account_token_expiration_seconds(
        body.get("spec")
            .and_then(|s| s.get("expirationSeconds"))
            .and_then(|v| v.as_i64()),
    );

    // Honor spec.boundObjectRef (kind Pod | Secret). A bound token must be tied
    // to the referenced object's lifetime: the object must exist (and its UID
    // match, if supplied) at mint time, and the binding is embedded so
    // validate_sa_token_bindings invalidates the token once the object is
    // deleted/recreated. Without this, a caller could mint a token nominally
    // "bound" to a Pod yet outliving it.
    let mut bound = crate::auth::BoundServiceAccountToken::default();
    let bound_pod_name;
    let bound_pod_uid;
    let bound_secret_name;
    let bound_secret_uid;
    let bound_object_ref = body
        .get("spec")
        .and_then(|s| s.get("boundObjectRef"))
        .filter(|v| !v.is_null())
        .cloned();
    if let Some(bref) = bound_object_ref.as_ref() {
        let kind = bref
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or_default();
        let ref_name = bref
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or_default();
        let requested_uid = bref
            .get("uid")
            .and_then(|u| u.as_str())
            .filter(|s| !s.is_empty());
        if ref_name.is_empty() {
            return Err(AppError::BadRequest(
                "spec.boundObjectRef.name: Required value".to_string(),
            ));
        }
        match kind {
            "Pod" => {
                // Pod reads go through the pod repository (actor-owned invariant).
                let pod = crate::kubelet::pod_repository::PodReader::get_pod(
                    state.pod_repository.as_ref(),
                    &namespace,
                    ref_name,
                )
                .await
                .ok()
                .flatten()
                .ok_or_else(|| AppError::NotFound(format!("pods \"{}\" not found", ref_name)))?;
                if let Some(req) = requested_uid
                    && req != pod.uid
                {
                    return Err(AppError::Conflict(format!(
                        "the UID in the bound object reference ({}) does not match the UID in record ({})",
                        req, pod.uid
                    )));
                }
                bound_pod_name = ref_name.to_string();
                bound_pod_uid = pod.uid;
                bound.pod_name = Some(&bound_pod_name);
                bound.pod_uid = Some(&bound_pod_uid);
            }
            "Secret" => {
                let secret = state
                    .db
                    .get_resource("v1", "Secret", Some(&namespace), ref_name)
                    .await?
                    .ok_or_else(|| {
                        AppError::NotFound(format!("secrets \"{}\" not found", ref_name))
                    })?;
                let secret_uid = secret
                    .data
                    .pointer("/metadata/uid")
                    .and_then(|u| u.as_str())
                    .unwrap_or_default()
                    .to_string();
                if let Some(req) = requested_uid
                    && req != secret_uid
                {
                    return Err(AppError::Conflict(format!(
                        "the UID in the bound object reference ({}) does not match the UID in record ({})",
                        req, secret_uid
                    )));
                }
                bound_secret_name = ref_name.to_string();
                bound_secret_uid = secret_uid;
                bound.secret_name = Some(&bound_secret_name);
                bound.secret_uid = Some(&bound_secret_uid);
            }
            other => {
                return Err(AppError::BadRequest(format!(
                    "cannot bind token request to object reference of kind {:?}: only Pod and Secret are supported",
                    other
                )));
            }
        }
    }

    let signing_key_pem =
        crate::auth::read_service_account_signing_key_async(&state.config.containerd_namespace)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to read signing key: {}", e)))?;

    let sa_uid = sa
        .data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str());
    bound.sa_uid = sa_uid;
    let token =
        crate::auth::generate_sa_token_with_bound_pod(crate::auth::ServiceAccountTokenRequest {
            ca_key_pem: &signing_key_pem,
            service_account: &name,
            namespace: &namespace,
            audiences: &audiences,
            expiration_seconds: Some(expiration_seconds),
            bound,
        })
        .map_err(|e| AppError::InternalError(format!("Failed to generate token: {}", e)))?;

    let now = crate::utils::k8s_timestamp();
    let expiration_timestamp = chrono::Utc::now() + chrono::Duration::seconds(expiration_seconds);

    let mut spec = serde_json::json!({
        "audiences": audiences,
        "expirationSeconds": expiration_seconds
    });
    if let Some(bref) = bound_object_ref {
        spec["boundObjectRef"] = bref;
    }

    Ok(Json(serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenRequest",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": now
        },
        "spec": spec,
        "status": {
            "token": token,
            "expirationTimestamp": expiration_timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        }
    })))
}
