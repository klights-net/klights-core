use super::*;

pub async fn get_pod_status<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = get_pod(state.as_ref(), &namespace, &name).await?;

    match pod {
        Some(resource) => {
            let pod_data = resource.data;
            // K8s status subresource returns the full pod object
            // but clients typically only care about the status field
            let pod_with_rv = inject_resource_version(
                state.command_store().identity(),
                pod_data,
                resource.resource_version,
            );
            Ok(Json(pod_with_rv))
        }
        None => Err(AppError::NotFound(format!(
            "Pod {}/{} not found",
            namespace, name
        ))),
    }
}

// PATCH /api/v1/namespaces/{ns}/pods/{name}/status
pub async fn patch_pod_status_subresource<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, AppError> {
    // Content-type detection stays at the handler boundary; the repository
    // takes the strongly-typed enum.
    let patch_type = klights_pod_api::PodStatusPatchKind::from_content_type(
        headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
    );

    let patch_value: Value = serde_json::from_slice(&body)
        .map_err(|e| AppError::BadRequest(format!("Invalid patch body: {}", e)))?;
    let requested_rv = metadata_resource_version(&patch_value);

    let pod = get_pod(state.as_ref(), &namespace, &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let updated = state
        .command_store()
        .pod_subresource_mutation()
        .patch_status(klights_pod_api::PodStatusPatchRequest {
            namespace: namespace.clone(),
            name: name.clone(),
            patch: patch_value,
            patch_kind: patch_type,
            expected_resource_version: requested_rv.unwrap_or(pod.resource_version),
        })
        .await
        .map_err(|e| AppError::from(e).with_resource_context("v1", "Pod", &name))?;

    let result = inject_resource_version(
        state.command_store().identity(),
        updated.data,
        updated.resource_version,
    );
    Ok(Json(result))
}

// PUT /api/v1/namespaces/{ns}/pods/{name}/status
pub async fn update_pod_status_subresource<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let pod = get_pod(state.as_ref(), &namespace, &name)
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

    let updated = state
        .command_store()
        .pod_subresource_mutation()
        .replace_status(klights_pod_api::PodStatusReplaceRequest {
            namespace: namespace.clone(),
            name: name.clone(),
            expected_uid: None,
            status: new_status,
            expected_resource_version: requested_rv.unwrap_or(pod.resource_version),
        })
        .await
        .map_err(|e| AppError::from(e).with_resource_context("v1", "Pod", &name))?;

    let result = inject_resource_version(
        state.command_store().identity(),
        updated.data,
        updated.resource_version,
    );
    Ok(Json(result))
}

fn metadata_resource_version(body: &Value) -> Option<i64> {
    body.pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i64>().ok())
}
