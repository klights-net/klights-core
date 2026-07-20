use super::*;

pub async fn pod_eviction(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, AppError> {
    // Parse the Eviction object from the request body (JSON or protobuf)
    let eviction: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..]).map_err(|e| {
            AppError::BadRequest(format!("failed to decode eviction protobuf: {}", e))
        })?
    } else if body.is_empty() {
        serde_json::json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": name, "namespace": namespace}
        })
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("failed to parse eviction JSON: {}", e)))?
    };

    let request =
        klights_pod_api::PodGetRequest::try_by_name(&namespace, &name).map_err(AppError::from)?;
    let Some(pod) = klights_pod_api::PodQuery::get_pod(state.pod_repository.as_ref(), request)
        .await
        .map_err(AppError::from)?
    else {
        return Err(AppError::NotFound(format!(
            "pod {}/{} not found",
            namespace, name
        )));
    };

    // Enforce PodDisruptionBudget before deleting the pod.
    if !eviction_allowed_by_pdbs(&state, &pod).await? {
        let denied = serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "message": format!("Cannot evict pod {}/{} as it would violate the pod's disruption budget.", namespace, name),
            "reason": "TooManyRequests",
            "details": {
                "name": name,
                "kind": "pods",
                "causes": [{
                    "reason": "DisruptionBudget",
                    "message": "The disruption budget would be exceeded"
                }]
            },
            "code": 429
        });
        return Ok((StatusCode::TOO_MANY_REQUESTS, Json(denied)).into_response());
    }

    // Delete the pod — same effect as DELETE /pods/{name}
    let target = klights_pod_api::PodMutationTarget::try_by_identity(
        klights_types::PodIdentity::new(&namespace, &name, &pod.uid),
    )
    .map_err(AppError::from)?;
    klights_pod_api::PodMarkTerminating::mark_pod_terminating(
        state.pod_repository.as_ref(),
        klights_pod_api::PodMarkTerminatingRequest::new(target),
    )
    .await
    .map_err(AppError::from)?;

    tracing::info!("Evicted pod {}/{}", namespace, name);

    // Return 201 Created with the Eviction object (K8s spec)
    Ok((StatusCode::CREATED, Json(eviction)).into_response())
}

async fn eviction_allowed_by_pdbs(
    state: &Arc<AppState>,
    pod: &crate::datastore::Resource,
) -> Result<bool, AppError> {
    let namespace = pod.namespace.as_deref().ok_or_else(|| {
        AppError::InternalError("stored Pod is missing metadata.namespace".to_string())
    })?;

    // Ensure PDB status reflects latest pod state before evaluating disruptionsAllowed.
    crate::controllers::pdb::reconcile_pdbs_for_namespace(
        state.db.as_ref(),
        state.pod_repository.as_ref(),
        namespace,
    )
    .await;

    let pdbs = state
        .db
        .list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for pdb in pdbs.items {
        if !pod_matches_pdb_selector(&pod.data, &pdb.data) {
            continue;
        }

        let allowed = pdb
            .data
            .pointer("/status/disruptionsAllowed")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if allowed <= 0 {
            return Ok(false);
        }
    }

    Ok(true)
}

fn pod_matches_pdb_selector(pod: &Value, pdb: &Value) -> bool {
    // PDB without a selector matches every pod (K8s default — protects all).
    let selector = match pdb.pointer("/spec/selector") {
        Some(s) => s,
        None => return true,
    };

    // Route through the canonical LabelSelector helper so eviction shares
    // matchExpressions semantics with the PDB status reconciler. A
    // malformed selector treats the pod as not-matched, mirroring K8s
    // "no protection on broken selector" semantics.
    match klights_types::LabelSelector::from_k8s_selector(selector) {
        Ok(s) => s.matches_resource(pod),
        Err(_) => false,
    }
}
