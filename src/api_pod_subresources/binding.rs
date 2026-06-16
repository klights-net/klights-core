use super::*;
use crate::api::AdmissionContextRequest;

#[derive(Deserialize)]
pub struct BindingQuery {
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
}

pub async fn pod_binding(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<BindingQuery>,
    body: Bytes,
) -> Result<Response, AppError> {
    let dry_run = query.dry_run == Some("All".to_string());
    let binding: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("failed to decode binding protobuf: {e}")))?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("failed to parse binding JSON: {e}")))?
    };
    let mut admission_context = build_admission_context(AdmissionContextRequest {
        api_version: "v1",
        kind: "Binding",
        operation: "CREATE",
        namespace: Some(namespace.clone()),
        name: Some(name.clone()),
        object: binding,
        old_object: None,
        dry_run,
        subresource: Some("binding"),
        options: None,
    });
    admission_context.resource = "pods".to_string();
    let binding = run_admission_for_request(state.db.as_ref(), admission_context).await?;
    state
        .pod_repository
        .bind_pod_from_api(&namespace, &name, binding, dry_run)
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Status",
            "metadata": {},
            "status": "Success",
            "code": 201
        })),
    )
        .into_response())
}
