use crate::current::*;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use std::sync::Arc;

pub(in crate::current) async fn get_apiservice_status(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<K8sResponse, AppError> {
    let resource = crate::current::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        "apiregistration.k8s.io/v1",
        "APIService",
        None,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound("APIService not found".to_string()))?;

    let data = inject_resource_version_with_identity(
        state.resource_mutation().identity.as_ref(),
        resource.data,
        resource.resource_version,
    );
    Ok(K8sResponse::new(data, &headers))
}

pub(in crate::current) async fn delete_collection_apiservices(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "apiregistration.k8s.io/v1",
        "APIService",
        klights_leader_api::ResourceListScope::Cluster,
        query.label_selector.as_deref(),
    )
    .await?;
    state.discovery().apiservice_proxy_cache.clear().await;
    Ok(Json(
        crate::generic_command::delete_collection_success_status(),
    ))
}

pub(in crate::current) async fn delete_apiservice_with_cache_invalidation(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<klights_auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let response = crate::generic_command::delete_inner(
        state.clone(),
        &identity,
        crate::generic_command::GeneratedDeleteInnerRequest {
            target: crate::generic_command::GeneratedNamedResource::new(
                "apiregistration.k8s.io/v1",
                "APIService",
                None,
                &name,
            ),
            query,
            body,
        },
    )
    .await?;

    crate::current::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
        &state,
        "apiregistration.k8s.io/v1",
        "APIService",
    )
    .await;

    Ok(response)
}
