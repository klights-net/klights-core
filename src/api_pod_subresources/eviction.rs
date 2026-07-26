use super::*;
use crate::api::query::CreateUpdateQuery;

#[derive(Default, serde::Deserialize)]
struct EvictionDeleteOptions {
    #[serde(rename = "propagationPolicy")]
    propagation_policy: Option<String>,
    #[serde(rename = "orphanDependents")]
    orphan_dependents: Option<bool>,
    #[serde(rename = "gracePeriodSeconds")]
    grace_period_seconds: Option<i64>,
    preconditions: Option<EvictionPreconditions>,
    #[serde(rename = "dryRun", default)]
    dry_run: Vec<String>,
}

#[derive(Default, serde::Deserialize)]
struct EvictionPreconditions {
    uid: Option<String>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<String>,
}

pub async fn pod_eviction(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    body: Bytes,
) -> Result<Response, AppError> {
    let eviction: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        klights_kube_protobuf::decode_protobuf(&body[4..]).map_err(|error| {
            AppError::BadRequest(format!("failed to decode eviction protobuf: {error}"))
        })?
    } else if body.is_empty() {
        serde_json::json!({
            "apiVersion": "policy/v1",
            "kind": "Eviction",
            "metadata": {"name": name, "namespace": namespace}
        })
    } else {
        serde_json::from_slice(&body).map_err(|error| {
            AppError::BadRequest(format!("failed to parse eviction JSON: {error}"))
        })?
    };
    validate_eviction_identity(&eviction, &namespace, &name)?;

    let delete_options = eviction
        .get("deleteOptions")
        .cloned()
        .filter(|value| !value.is_null())
        .map(serde_json::from_value::<EvictionDeleteOptions>)
        .transpose()
        .map_err(|error| AppError::BadRequest(format!("invalid eviction deleteOptions: {error}")))?
        .unwrap_or_default();
    let dry_run = eviction_dry_run(&query, &delete_options.dry_run)?;

    let request =
        klights_pod_api::PodGetRequest::try_by_name(&namespace, &name).map_err(AppError::from)?;
    let Some(pod) = klights_pod_api::PodQuery::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        request,
    )
    .await
    .map_err(AppError::from)?
    else {
        return Err(AppError::NotFound(format!(
            "pod {namespace}/{name} not found"
        )));
    };

    let preconditions = delete_options.preconditions.unwrap_or_default();
    let pod_delete_options = klights_pod_api::PodDeleteOptions::new(
        delete_options.propagation_policy,
        delete_options.orphan_dependents,
        delete_options.grace_period_seconds,
        klights_pod_api::PodDeletePreconditions::new(
            Some(preconditions.uid.unwrap_or_else(|| pod.uid.clone())),
            preconditions.resource_version,
        ),
    );

    let admission = state
        .resource_mutation()
        .pod_repository
        .eviction_admission_port()
        .admit_pod_eviction(klights_reconcile_api::PodEvictionAdmissionRequest { pod, dry_run })
        .await
        .map_err(|error| AppError::ServiceUnavailable(error.to_string()))?;
    if let Some(response) = admission_response(admission, &name) {
        return Ok(response);
    }

    let outcome = klights_pod_api::PodEvictionDelete::delete_for_eviction(
        state.resource_mutation().pod_repository.as_ref(),
        klights_pod_api::PodEvictionDeleteRequest::try_new(
            &namespace,
            &name,
            pod_delete_options,
            dry_run,
        )
        .map_err(AppError::from)?,
    )
    .await
    .map_err(AppError::from)?;

    if matches!(
        outcome,
        klights_pod_api::PodEvictionDeleteOutcome::Persisted(_)
    ) {
        tracing::info!("Evicted pod {namespace}/{name}");
    }
    Ok((StatusCode::CREATED, Json(eviction)).into_response())
}

fn validate_eviction_identity(
    eviction: &Value,
    namespace: &str,
    name: &str,
) -> Result<(), AppError> {
    if eviction.get("kind").and_then(Value::as_str) != Some("Eviction") {
        return Err(AppError::BadRequest(
            "request body must be a policy/v1 Eviction".to_string(),
        ));
    }
    if let Some(body_name) = eviction.pointer("/metadata/name").and_then(Value::as_str)
        && body_name != name
    {
        return Err(AppError::BadRequest(
            "name in URL does not match name in Eviction object".to_string(),
        ));
    }
    if let Some(body_namespace) = eviction
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        && body_namespace != namespace
    {
        return Err(AppError::BadRequest(
            "namespace in URL does not match namespace in Eviction object".to_string(),
        ));
    }
    Ok(())
}

fn eviction_dry_run(query: &CreateUpdateQuery, body: &[String]) -> Result<bool, AppError> {
    Ok(crate::api::mutation::DryRunMode::from_eviction(query, body)?.is_all())
}

fn admission_response(
    outcome: klights_reconcile_api::PodEvictionAdmissionOutcome,
    pod_name: &str,
) -> Option<Response> {
    use klights_reconcile_api::PodEvictionAdmissionOutcome;

    let (status, reason, message, details, retry_after) = match outcome {
        PodEvictionAdmissionOutcome::Allowed => return None,
        PodEvictionAdmissionOutcome::DisruptionBudgetDenied {
            pdb_name,
            desired_healthy,
            current_healthy,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            "TooManyRequests",
            "Cannot evict pod as it would violate the pod's disruption budget.".to_string(),
            serde_json::json!({
                "name": pod_name,
                "kind": "pods",
                "causes": [{
                    "reason": "DisruptionBudget",
                    "message": format!(
                        "The disruption budget {pdb_name} needs {desired_healthy} healthy pods and has {current_healthy} currently"
                    )
                }]
            }),
            Some("10"),
        ),
        PodEvictionAdmissionOutcome::MultipleDisruptionBudgets { pdb_names } => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!(
                "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support: {}",
                pdb_names.join(", ")
            ),
            Value::Null,
            None,
        ),
        PodEvictionAdmissionOutcome::InvalidDisruptionBudget { pdb_name, message } => (
            StatusCode::FORBIDDEN,
            "Forbidden",
            format!("PodDisruptionBudget {pdb_name} is invalid: {message}"),
            serde_json::json!({"name": pdb_name, "kind": "poddisruptionbudgets"}),
            None,
        ),
    };
    let body = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": reason,
        "details": details,
        "code": status.as_u16()
    });
    let mut response = (status, Json(body)).into_response();
    if let Some(retry_after) = retry_after {
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static(retry_after),
        );
    }
    Some(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_accepts_query_or_body_and_rejects_unsupported_body_values() {
        let mut query = CreateUpdateQuery {
            dry_run: Some("All".to_string()),
            field_manager: None,
            field_validation: None,
            force: None,
            orphan_dependents: None,
            propagation_policy: None,
            grace_period_seconds: None,
        };
        assert!(eviction_dry_run(&query, &[]).unwrap());
        query.dry_run = None;
        assert!(eviction_dry_run(&query, &["All".to_string()]).unwrap());
        assert!(eviction_dry_run(&query, &["Other".to_string()]).is_err());
    }

    #[test]
    fn disruption_budget_denial_is_typed_and_retryable() {
        let response = admission_response(
            klights_reconcile_api::PodEvictionAdmissionOutcome::DisruptionBudgetDenied {
                pdb_name: "budget".to_string(),
                desired_healthy: 2,
                current_healthy: 1,
            },
            "victim",
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(axum::http::header::RETRY_AFTER),
            Some(&axum::http::HeaderValue::from_static("10"))
        );
    }
}
