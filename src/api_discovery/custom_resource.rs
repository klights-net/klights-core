use super::*;
pub async fn custom_resource_discovery(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((group, version)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let resources = state.crd_registry.list_resources(&group, &version).await;

    if resources.is_empty() {
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or_else(|| uri.path());
        // Use an authenticated identity for APIService discovery proxying
        // so backends that require authentication can serve the discovery
        // document (e.g., sample-apiserver).
        let discovery_identity = crate::auth::identity::AuthenticatedIdentity::client_cert(
            "system:apiserver".to_string(),
            vec!["system:authenticated".to_string()],
        );
        if let Some(resp) = crate::api::proxy_apiservice_request(
            &state,
            &group,
            &version,
            Method::GET,
            path_and_query,
            axum::body::Bytes::new(),
            Some(&headers),
            &discovery_identity,
        )
        .await?
        {
            return Ok(resp);
        }
        return Err(AppError::NotFound(format!(
            "APIGroup {}/{} not found",
            group, version
        )));
    }

    let resource_list: Vec<Value> = resources
        .iter()
        .map(|info| {
            serde_json::json!({
                "name": info.plural.clone(),
                "singularName": info.singular.clone(),
                "namespaced": info.namespaced,
                "kind": info.kind.clone(),
                "verbs": ["create", "delete", "get", "list", "patch", "update", "watch"],
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": format!("{}/{}", group, version),
        "resources": resource_list,
    }))
    .into_response())
}
