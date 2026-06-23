use super::*;

pub async fn get_pod_status(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?;

    match pod {
        Some(resource) => {
            let pod_data = resource.data;
            // K8s status subresource returns the full pod object
            // but clients typically only care about the status field
            let pod_with_rv =
                crate::api::inject_resource_version(pod_data, resource.resource_version);
            Ok(Json(pod_with_rv))
        }
        None => Err(AppError::NotFound(format!(
            "Pod {}/{} not found",
            namespace, name
        ))),
    }
}

// PATCH /api/v1/namespaces/{ns}/pods/{name}/status
pub async fn patch_pod_status_subresource(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, AppError> {
    // Content-type detection stays at the handler boundary; the repository
    // takes the strongly-typed enum.
    let patch_type = crate::kubelet::pod_repository::content_type_to_patch_type(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
    );

    let patch_value: Value = serde_json::from_slice(&body)
        .map_err(|e| AppError::BadRequest(format!("Invalid patch body: {}", e)))?;
    let requested_rv = metadata_resource_version(&patch_value);

    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let updated = crate::kubelet::pod_repository::PodSubresourceWriter::patch_status_from_api(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
        patch_value,
        patch_type,
        requested_rv.unwrap_or(pod.resource_version),
    )
    .await
    .map_err(|e| AppError::from(e).with_resource_context("v1", "Pod", &name))?;

    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

// PUT /api/v1/namespaces/{ns}/pods/{name}/status
pub async fn update_pod_status_subresource(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    crate::api::LenientJson(body): crate::api::LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    // PUT /status overwrites the existing status with the caller's. If the
    // request body omits `status`, preserve today's behaviour and reuse
    // the existing status (the write still bumps resourceVersion, matching
    // the no-op write previously performed by the inline path).
    let new_status = body
        .get("status")
        .cloned()
        .or_else(|| pod.data.get("status").cloned())
        .unwrap_or(Value::Null);
    let requested_rv = metadata_resource_version(&body);

    let updated = crate::kubelet::pod_repository::PodSubresourceWriter::replace_status_from_api(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
        new_status,
        requested_rv.unwrap_or(pod.resource_version),
    )
    .await
    .map_err(|e| AppError::from(e).with_resource_context("v1", "Pod", &name))?;

    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

fn metadata_resource_version(body: &Value) -> Option<i64> {
    body.pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i64>().ok())
}
