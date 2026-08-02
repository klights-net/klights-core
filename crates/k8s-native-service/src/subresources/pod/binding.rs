use super::*;

#[derive(Deserialize)]
pub struct BindingQuery {
    #[serde(rename = "dryRun")]
    pub dry_run: Option<String>,
}

impl BindingQuery {
    fn dry_run_mode(&self) -> Result<crate::generic_command::DryRunMode, AppError> {
        crate::generic_command::DryRunMode::from_query(self.dry_run.as_deref())
    }
}

pub async fn pod_binding<S: GenericCommandState + 'static>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<BindingQuery>,
    body: Bytes,
) -> Result<Response, AppError> {
    let dry_run = query.dry_run_mode()?;
    let dry_run = dry_run.is_all();
    let binding: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        klights_kube_protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("failed to decode binding protobuf: {e}")))?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("failed to parse binding JSON: {e}")))?
    };
    let binding = state
        .command_admission()
        .admission()
        .admit(ResourceAdmissionRequest {
            api_version: "v1".to_string(),
            kind: "Binding".to_string(),
            resource: Some("pods".to_string()),
            operation: "CREATE".to_string(),
            namespace: Some(namespace.clone()),
            name: Some(name.clone()),
            object: binding,
            old_object: None,
            dry_run,
            subresource: Some("binding".to_string()),
            options: None,
        })
        .await?;
    state
        .command_store()
        .pod_mutation()
        .bind_pod(klights_pod_api::PodBindingRequest {
            namespace,
            name,
            binding,
            dry_run,
        })
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
