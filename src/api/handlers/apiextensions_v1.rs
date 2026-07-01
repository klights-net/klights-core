use crate::api::*;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;

pub fn apiextensions_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/customresourcedefinitions",
            get(list_customresourcedefinitions)
                .post(create_crd_with_registration)
                .delete(delete_collection_customresourcedefinitions),
        )
        .route(
            "/customresourcedefinitions/{name}",
            get(get_customresourcedefinition)
                .put(update_crd_with_registration)
                .patch(patch_crd_with_registration)
                .delete(delete_crd_with_deregistration),
        )
        .route(
            "/customresourcedefinitions/{name}/status",
            get(get_crd_status)
                .put(update_crd_status)
                .patch(patch_crd_status),
        )
}

pub fn crd_is_namespaced(crd: &Value) -> bool {
    crd.pointer("/spec/scope")
        .and_then(|s| s.as_str())
        .is_some_and(|scope| scope.eq_ignore_ascii_case("Namespaced"))
}

pub fn crd_versions(crd: &Value) -> Vec<String> {
    crd.pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .map(|versions| {
            versions
                .iter()
                .filter_map(|ver| {
                    ver.get("name")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub async fn delete_custom_resources_for_crd(
    state: &Arc<AppState>,
    crd: &Value,
) -> Result<(), AppError> {
    let Some(group) = crd.pointer("/spec/group").and_then(|g| g.as_str()) else {
        return Ok(());
    };
    let Some(kind) = crd.pointer("/spec/names/kind").and_then(|k| k.as_str()) else {
        return Ok(());
    };
    let namespaced = crd_is_namespaced(crd);
    let versions = crd_versions(crd);
    if versions.is_empty() {
        return Ok(());
    }

    let mut targets = Vec::<(String, Option<String>, String)>::new();
    for version in &versions {
        let api_version = format!("{group}/{version}");
        let keys = state
            .db
            .list_resource_keys_for_scope(api_version.clone(), kind.to_string(), namespaced)
            .await?;
        for (namespace, name) in keys {
            targets.push((api_version.clone(), namespace, name));
        }
    }
    targets.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    targets.dedup();

    for (api_version, namespace, name) in targets {
        state
            .db
            .delete_resource(&api_version, kind, namespace.as_deref(), &name)
            .await?;
    }
    Ok(())
}

pub async fn delete_crd_with_deregistration(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
) -> Result<Json<Value>, AppError> {
    let resource = state
        .db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound("CustomResourceDefinition not found".to_string()))?;

    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    if dry_run.is_all() {
        return Ok(Json(std::sync::Arc::unwrap_or_clone(resource.data)));
    }

    // Remove from CRD registry so custom resource routes return 404
    let group = resource
        .data
        .pointer("/spec/group")
        .and_then(|g| g.as_str())
        .unwrap_or("");
    let plural = resource
        .data
        .pointer("/spec/names/plural")
        .and_then(|p| p.as_str())
        .unwrap_or("");
    if let Some(versions) = resource
        .data
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
    {
        for ver in versions {
            if let Some(ver_name) = ver.get("name").and_then(|n| n.as_str()) {
                state.crd_registry.remove(group, ver_name, plural).await;
            }
        }
    }

    delete_custom_resources_for_crd(&state, &resource.data).await?;

    // Set deletionTimestamp
    let mut del_data: Value = (*resource.data).clone();
    if let Some(meta) = del_data.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert(
            "deletionTimestamp".to_string(),
            serde_json::Value::String(crate::utils::k8s_timestamp()),
        );
    }
    let _ = state
        .db
        .update_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
            del_data,
            resource.resource_version,
        )
        .await;

    // hard-delete
    state
        .db
        .delete_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
        )
        .await?;

    Ok(Json(std::sync::Arc::unwrap_or_clone(resource.data)))
}

pub async fn delete_collection_customresourcedefinitions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in &list.items {
        // Deregister each CRD from the registry before deleting
        let group = resource
            .data
            .pointer("/spec/group")
            .and_then(|g| g.as_str())
            .unwrap_or("");
        let plural = resource
            .data
            .pointer("/spec/names/plural")
            .and_then(|p| p.as_str())
            .unwrap_or("");
        if let Some(versions) = resource
            .data
            .pointer("/spec/versions")
            .and_then(|v| v.as_array())
        {
            for ver in versions {
                if let Some(ver_name) = ver.get("name").and_then(|n| n.as_str()) {
                    state.crd_registry.remove(group, ver_name, plural).await;
                }
            }
        }
        delete_custom_resources_for_crd(&state, &resource.data).await?;
        let _ = state
            .db
            .delete_resource(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

/// Merge existing storedVersions with the current storage version.
/// Preserves all previously-seen versions and adds the new storage version.
/// This matches K8s behavior: storedVersions accumulates versions that
/// have ever been used for storage, and entries are only removed by
/// explicit status update after migration.
pub fn merge_stored_versions(existing: &[String], new_spec_versions: &Value) -> Value {
    let mut merged: std::collections::BTreeSet<String> = existing.iter().cloned().collect();

    // Add the current storage version(s)
    if let Some(versions) = new_spec_versions.as_array() {
        for ver in versions {
            let is_storage = ver
                .get("storage")
                .and_then(|s| s.as_bool())
                .unwrap_or(false);
            if is_storage && let Some(name) = ver.get("name").and_then(|n| n.as_str()) {
                merged.insert(name.to_string());
            }
        }
    }

    Value::Array(merged.into_iter().map(Value::String).collect())
}

/// Validate the `api-approved.kubernetes.io` annotation for protected groups.
/// Protected groups are those ending in `.k8s.io` or `.kubernetes.io`.
/// On create, old_annotations is None. On update, old_annotations is Some
/// and if the annotation is unchanged, validation is skipped.
pub fn validate_api_approval(
    group: &str,
    annotations: Option<&Value>,
    old_annotations: Option<&Value>,
) -> Result<(), crate::api::AppError> {
    use crate::api::AppError;

    // Only protected groups need approval
    if !group.ends_with(".k8s.io") && !group.ends_with(".kubernetes.io") {
        return Ok(());
    }

    // On update, if annotation hasn't changed, skip validation
    if let (Some(old), Some(new)) = (old_annotations, annotations) {
        let old_val = old
            .get("api-approved.kubernetes.io")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let new_val = new
            .get("api-approved.kubernetes.io")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if old_val == new_val {
            return Ok(());
        }
    }

    let annotation_value = annotations
        .and_then(|a| a.get("api-approved.kubernetes.io"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if annotation_value.is_empty() {
        return Err(AppError::BadRequest(
            "protected groups must have approval annotation \"api-approved.kubernetes.io\", see https://github.com/kubernetes/enhancements/pull/1111".to_string()
        ));
    }

    if annotation_value.starts_with("https://") || annotation_value == "unapproved" {
        return Ok(());
    }

    Err(AppError::BadRequest(format!(
        "invalid value for metadata.annotations[api-approved.kubernetes.io]: {:?} \
         must be a URL starting with https:// or \"unapproved\"",
        annotation_value
    )))
}

// Helper to add Established condition to CRD status
pub fn add_crd_established_condition(mut body: Value) -> Value {
    let now = crate::utils::k8s_timestamp();

    let established_condition = serde_json::json!({
        "type": "Established",
        "status": "True",
        "reason": "InitialNamesAccepted",
        "message": "the initial names have been accepted",
        "lastTransitionTime": now
    });

    let names_accepted_condition = serde_json::json!({
        "type": "NamesAccepted",
        "status": "True",
        "reason": "NoConflicts",
        "message": "no conflicts found",
        "lastTransitionTime": now
    });

    // Extract spec values before mutable borrow on status
    let accepted_names = body.pointer("/spec/names").cloned();
    let stored_versions: Vec<Value> = body
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .map(|versions| {
            versions
                .iter()
                .filter(|v| v.get("storage").and_then(|s| s.as_bool()).unwrap_or(false))
                .filter_map(|v| v.get("name").cloned())
                .collect()
        })
        .unwrap_or_default();

    // Ensure status.conditions exists
    ensure_object(&mut body, "status");
    let conditions = ensure_array(&mut body["status"], "conditions");
    conditions.push(established_condition);
    conditions.push(names_accepted_condition);
    // Get the status object for inserting other fields
    let status = body["status"]
        .as_object_mut()
        .expect("just ensured as object");

    // Set acceptedNames from spec.names
    if let Some(names) = accepted_names {
        status.insert("acceptedNames".to_string(), names);
    }

    // Set storedVersions
    if !stored_versions.is_empty() {
        status.insert("storedVersions".to_string(), Value::Array(stored_versions));
    }

    body
}

// Custom CRD create handler that registers the CRD in the registry
async fn create_crd_with_registration(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    // Validate api-approved.kubernetes.io annotation for protected groups
    let group = body
        .pointer("/spec/group")
        .and_then(|g| g.as_str())
        .unwrap_or("");
    validate_api_approval(
        group,
        body.get("metadata").and_then(|m| m.get("annotations")),
        None,
    )?;

    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();
    let admitted = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "apiextensions.k8s.io/v1",
            kind: "CustomResourceDefinition",
            operation: "CREATE",
            namespace: None,
            name: body
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(ToString::to_string),
            object: body.clone(),
            old_object: None,
            dry_run: is_dry_run,
            subresource: None,
            options: None,
        }),
    )
    .await?;
    if is_dry_run {
        return Ok((StatusCode::CREATED, Json(admitted)));
    }

    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| AppError::BadRequest("Missing metadata.name".to_string()))?
        .to_string();

    // Create the CRD first WITHOUT the Established condition (matches real K8s behavior).
    // Real K8s creates the CRD first, then the CRD controller updates status,
    // causing a MODIFIED event. Tests watch for this MODIFIED event.
    let resource = state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
            admitted.clone(),
        )
        .await?;

    // Register the CRD in the registry immediately (so API routes are ready)
    let body_with_status = add_crd_established_condition(body.clone());
    if let Err(e) =
        crate::controllers::crd::register_crd_from_value(&state.crd_registry, &body_with_status)
            .await
    {
        tracing::error!("Failed to register CRD: {}", e);
    }

    // Emit a MODIFIED event by updating the status to Established.
    // This mimics the K8s CRD controller updating the status after creation.
    // Return the latest RV in the create response so watch catch-up from this RV
    // does not replay this intermediate MODIFIED event before DELETE.
    let updated = state
        .db
        .update_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
            body_with_status,
            resource.resource_version,
        )
        .await;

    let response_resource = match updated {
        Ok(updated) => updated,
        Err(e) => {
            tracing::warn!("Failed to set CRD Established status after create: {}", e);
            resource
        }
    };
    let data = inject_resource_version(response_resource.data, response_resource.resource_version);
    Ok((StatusCode::CREATED, Json(data)))
}

async fn update_crd_with_registration(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(mut body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let current = state
        .db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound("CustomResourceDefinition not found".to_string()))?;

    // Validate api-approved.kubernetes.io annotation for protected groups
    let group = body
        .pointer("/spec/group")
        .and_then(|g| g.as_str())
        .unwrap_or("");
    validate_api_approval(
        group,
        body.get("metadata").and_then(|m| m.get("annotations")),
        current
            .data
            .get("metadata")
            .and_then(|m| m.get("annotations")),
    )?;

    check_field_validation_strict(&query, &body)?;
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();
    body = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "apiextensions.k8s.io/v1",
            kind: "CustomResourceDefinition",
            operation: "UPDATE",
            namespace: None,
            name: Some(name.clone()),
            object: body,
            old_object: Some((*current.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: None,
        }),
    )
    .await?;

    if is_dry_run {
        return Ok(Json(body));
    }

    // Remove old versions from registry
    let old_group = current
        .data
        .pointer("/spec/group")
        .and_then(|g| g.as_str())
        .unwrap_or("");
    let old_plural = current
        .data
        .pointer("/spec/names/plural")
        .and_then(|p| p.as_str())
        .unwrap_or("");
    if let Some(versions) = current
        .data
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
    {
        for ver in versions {
            if let Some(ver_name) = ver.get("name").and_then(|n| n.as_str()) {
                state
                    .crd_registry
                    .remove(old_group, ver_name, old_plural)
                    .await;
            }
        }
    }

    // Merge storedVersions: preserve old entries and add new storage version
    let existing_stored: Vec<String> = current
        .data
        .pointer("/status/storedVersions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    let new_spec_versions = body
        .pointer("/spec/versions")
        .cloned()
        .unwrap_or(Value::Array(vec![]));
    let merged = merge_stored_versions(&existing_stored, &new_spec_versions);
    // Set status.storedVersions on the update body
    ensure_object(&mut body, "status");
    if let Some(status) = body["status"].as_object_mut() {
        status.insert("storedVersions".to_string(), merged);
    }

    let resource = state
        .db
        .update_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &name,
            body.clone(),
            current.resource_version,
        )
        .await?;

    // Re-register with new spec
    if let Err(e) =
        crate::controllers::crd::register_crd_from_value(&state.crd_registry, &resource.data).await
    {
        tracing::error!("Failed to re-register CRD: {}", e);
    }

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

async fn patch_crd_with_registration(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();

    let patch: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))?
    } else if content_type == Some("application/apply-patch+yaml") {
        parse_apply_yaml(&body)?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))?
    };

    // Server-side apply PATCH is create-or-update for CRDs.
    if content_type == Some("application/apply-patch+yaml") {
        let exists = state
            .db
            .get_resource(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                &name,
            )
            .await?
            .is_some();
        if !exists {
            let mut create_body = patch.clone();
            if create_body.get("metadata").is_none_or(|v| !v.is_object()) {
                create_body["metadata"] = serde_json::json!({});
            }
            if create_body
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .is_none()
            {
                create_body["metadata"]["name"] = serde_json::json!(name.clone());
            }

            let admitted = run_admission_for_request(
                state.db.as_ref(),
                build_admission_context(AdmissionContextRequest {
                    api_version: "apiextensions.k8s.io/v1",
                    kind: "CustomResourceDefinition",
                    operation: "CREATE",
                    namespace: None,
                    name: Some(name.clone()),
                    object: create_body,
                    old_object: None,
                    dry_run: is_dry_run,
                    subresource: None,
                    options: None,
                }),
            )
            .await?;

            if is_dry_run {
                return Ok(Json(admitted));
            }

            let resource = state
                .db
                .create_resource(
                    "apiextensions.k8s.io/v1",
                    "CustomResourceDefinition",
                    None,
                    &name,
                    admitted.clone(),
                )
                .await?;

            let body_with_status = add_crd_established_condition(admitted);
            if let Err(e) = crate::controllers::crd::register_crd_from_value(
                &state.crd_registry,
                &body_with_status,
            )
            .await
            {
                tracing::error!("Failed to register CRD from apply PATCH create: {}", e);
            }

            let response_resource = match state
                .db
                .update_resource(
                    "apiextensions.k8s.io/v1",
                    "CustomResourceDefinition",
                    None,
                    &name,
                    body_with_status,
                    resource.resource_version,
                )
                .await
            {
                Ok(updated) => updated,
                Err(e) => {
                    tracing::warn!(
                        "Failed to set CRD Established status after apply PATCH create: {}",
                        e
                    );
                    resource
                }
            };
            let data =
                inject_resource_version(response_resource.data, response_resource.resource_version);
            return Ok(Json(data));
        }
    }

    let max_retries = 5;
    for attempt in 0..max_retries {
        let current = state
            .db
            .get_resource(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                &name,
            )
            .await?
            .ok_or_else(|| AppError::NotFound("CustomResourceDefinition not found".to_string()))?;

        let patched = apply_patch(&current.data, &patch, content_type)?;
        let patched = run_admission_for_request(
            state.db.as_ref(),
            build_admission_context(AdmissionContextRequest {
                api_version: "apiextensions.k8s.io/v1",
                kind: "CustomResourceDefinition",
                operation: "UPDATE",
                namespace: None,
                name: Some(name.clone()),
                object: patched,
                old_object: Some((*current.data).clone()),
                dry_run: is_dry_run,
                subresource: None,
                options: None,
            }),
        )
        .await?;

        if is_dry_run {
            return Ok(Json(patched));
        }

        match state
            .db
            .update_resource(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                &name,
                patched.clone(),
                current.resource_version,
            )
            .await
        {
            Ok(resource) => {
                // Remove old versions from registry
                let old_group = current
                    .data
                    .pointer("/spec/group")
                    .and_then(|g| g.as_str())
                    .unwrap_or("");
                let old_plural = current
                    .data
                    .pointer("/spec/names/plural")
                    .and_then(|p| p.as_str())
                    .unwrap_or("");
                if let Some(versions) = current
                    .data
                    .pointer("/spec/versions")
                    .and_then(|v| v.as_array())
                {
                    for ver in versions {
                        if let Some(ver_name) = ver.get("name").and_then(|n| n.as_str()) {
                            state
                                .crd_registry
                                .remove(old_group, ver_name, old_plural)
                                .await;
                        }
                    }
                }
                // Re-register with new spec
                if let Err(e) = crate::controllers::crd::register_crd_from_value(
                    &state.crd_registry,
                    &resource.data,
                )
                .await
                {
                    tracing::error!("Failed to re-register CRD: {}", e);
                }
                return Ok(Json(std::sync::Arc::unwrap_or_clone(resource.data)));
            }
            Err(e)
                if attempt < max_retries - 1 && crate::datastore::errors::is_conflict_error(&e) =>
            {
                tracing::debug!(
                    "PATCH CRD {}: conflict on attempt {}, retrying",
                    name,
                    attempt
                );
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    unreachable!("PATCH CRD retry loop exhausted without returning");
}
