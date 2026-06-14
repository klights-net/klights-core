use super::*;

pub async fn get_pod_ephemeral_containers(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let pod_with_rv = crate::api::inject_resource_version(pod.data, pod.resource_version);
    Ok(Json(pod_with_rv))
}

// PUT /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
pub async fn update_pod_ephemeral_containers(
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

    // K8s spec: ephemeral containers are append-only — existing containers cannot
    // be removed or modified. Validation (immutability of existing entries) is
    // performed in the handler; persistence runs through the repository.
    // P0-E2E-20260423-13: previously replaced the entire list; this caused the
    // conformance test's second PUT (adding a new container) to drop the first.
    let merged = if let Some(new_ephemeral_arr) = request_ephemeral_containers(&body) {
        let merged = merge_append_only_ephemeral_containers(
            &pod.data,
            new_ephemeral_arr,
            &namespace,
            &name,
            "PUT",
        );
        Some(merged)
    } else {
        tracing::warn!(
            "ephemeralcontainers PUT {}/{} missing ephemeral containers field; top_keys={:?} spec_keys={:?}",
            namespace,
            name,
            body.as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>()),
            body.get("spec")
                .and_then(|s| s.as_object())
                .map(|o| o.keys().cloned().collect::<Vec<_>>())
        );
        None
    };

    let updated = persist_ephemeral_containers(&state, &namespace, &name, &pod, merged).await?;
    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

// PATCH /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
pub async fn patch_pod_ephemeral_containers(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    let patch_value: Value = serde_json::from_slice(&body)
        .map_err(|e| AppError::BadRequest(format!("Invalid patch body: {}", e)))?;

    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    // Apply the patch to the full pod (immutability validation lives in the
    // handler; the repository only persists).
    let patched = crate::api::apply_patch(&pod.data, &patch_value, content_type)?;

    // Extract only spec.ephemeralContainers from the patch result.
    // K8s behavior: append-only by container name; existing ephemeral containers
    // are immutable and cannot be removed/replaced.
    let merged = request_ephemeral_containers(&patched).map(|new_ephemeral_arr| {
        merge_append_only_ephemeral_containers(
            &pod.data,
            new_ephemeral_arr,
            &namespace,
            &name,
            "PATCH",
        )
    });

    let updated = persist_ephemeral_containers(&state, &namespace, &name, &pod, merged).await?;
    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

/// Merge incoming ephemeral container specs with the pod's existing ones,
/// honouring K8s append-only semantics (existing entries are immutable).
/// Returns the final array to persist.
fn merge_append_only_ephemeral_containers(
    pod_data: &Value,
    new_ephemeral_arr: &[Value],
    namespace: &str,
    name: &str,
    verb: &str,
) -> Vec<Value> {
    let incoming_names: Vec<String> = new_ephemeral_arr
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    let existing: Vec<Value> = pod_data
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let existing_names: std::collections::HashSet<String> = existing
        .iter()
        .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(str::to_string))
        .collect();

    let mut merged = existing;
    for ec in new_ephemeral_arr {
        let cname = ec.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if !existing_names.contains(cname) {
            merged.push(ec.clone());
        }
    }
    let merged_names: Vec<String> = merged
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    tracing::info!(
        "ephemeralcontainers {} {}/{} incoming={:?} existing_count={} merged={:?}",
        verb,
        namespace,
        name,
        incoming_names,
        existing_names.len(),
        merged_names
    );
    merged
}

/// Persist the merged ephemeral containers list via the repository.
///
/// When `merged` is `None` (no ephemeral containers in the request payload),
/// the call still goes through the repository to bump `resourceVersion`,
/// matching today's behaviour for empty PUT/PATCH bodies. The repository
/// itself bumps `metadata.generation` when the new list grows, so the
/// handler does not need a second write.
async fn persist_ephemeral_containers(
    state: &Arc<AppState>,
    namespace: &str,
    name: &str,
    pod: &crate::datastore::Resource,
    merged: Option<Vec<Value>>,
) -> Result<crate::datastore::Resource, AppError> {
    let to_persist = match merged {
        Some(arr) => arr,
        // Empty request — keep the existing list verbatim so we still bump RV
        // (matches today's behaviour where update_resource was always called).
        None => pod
            .data
            .pointer("/spec/ephemeralContainers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default(),
    };

    crate::kubelet::pod_repository::PodSubresourceWriter::update_ephemeral_containers(
        state.pod_repository.as_ref(),
        namespace,
        name,
        to_persist,
        pod.resource_version,
    )
    .await
    .map_err(|e| AppError::InternalError(format!("ephemeralcontainers update failed: {e}")))
}

/// Kubernetes subresource clients can send ephemeral container updates in
/// either of these equivalent shapes:
/// 1) `{ "spec": { "ephemeralContainers": [...] } }`
/// 2) `{ "ephemeralContainers": [...] }`
fn request_ephemeral_containers(value: &Value) -> Option<&Vec<Value>> {
    // Preferred canonical forms.
    let direct = value
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .or_else(|| value.get("ephemeralContainers").and_then(|v| v.as_array()));
    if direct.is_some() {
        return direct;
    }

    // Be permissive for client serializer variants.
    if let Some(spec_obj) = value.get("spec").and_then(|s| s.as_object()) {
        for key in [
            "ephemeralcontainers",
            "ephemeral_containers",
            "ephemeralContainers",
        ] {
            if let Some(arr) = spec_obj.get(key).and_then(|v| v.as_array()) {
                return Some(arr);
            }
        }
        for (k, v) in spec_obj {
            if normalize_ephemeral_key(k)
                && let Some(arr) = v.as_array()
            {
                return Some(arr);
            }
        }
    }

    if let Some(top_obj) = value.as_object() {
        for key in [
            "ephemeralcontainers",
            "ephemeral_containers",
            "ephemeralContainers",
        ] {
            if let Some(arr) = top_obj.get(key).and_then(|v| v.as_array()) {
                return Some(arr);
            }
        }
        for (k, v) in top_obj {
            if normalize_ephemeral_key(k)
                && let Some(arr) = v.as_array()
            {
                return Some(arr);
            }
        }
    }

    None
}

fn normalize_ephemeral_key(key: &str) -> bool {
    key.to_ascii_lowercase().replace('_', "") == "ephemeralcontainers"
}
