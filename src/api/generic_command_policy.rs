//! Root-owned Kubernetes policy plugged into native generic command orchestration.

use axum::body::Bytes;
use axum::http::HeaderMap;
use klights_cluster_core::Resource;
use serde_json::Value;

use k8s_native_service::generic_command::{
    CreateUpdateQuery, DryRunMode, GenericCommandAuthorization, GenericCommandFuture,
    GenericCommandPolicy, PreparedCreate, ResourceAdmissionRequest,
};

use super::generated_handlers::helpers::{
    initialize_statefulset_revision_status_on_create, stamp_csr_identity,
};
use super::state_composition::{ApiAuthPolicy, ApiResourceMutationServices};
use super::{
    AppError, apply_patch, apply_pod_create_defaults_at, apply_pv_create_defaults,
    apply_pvc_create_defaults, apply_replicationcontroller_selector_default,
    apply_resourcequota_create_status, apply_workload_replicas_default, check_content_type,
    check_field_validation_strict_typed, check_immutable_fields, check_resource_quota_for_creation,
    check_resource_quota_for_pod_update, check_resource_quota_for_pvc_update,
    ensure_namespace_status_phase_active, metadata_name_uses_path_segment_validation,
    normalize_resource_for_storage, prepare_admissionregistration_resource,
    process_secret_stringdata, resolve_resource_name, validate_builtin_resource_spec,
    validate_metadata_name_for_kind, validate_pod_resource_requirements_immutable,
    validate_pod_sysctls, validate_priorityclass_update_immutable, validate_secret_data,
};

impl GenericCommandAuthorization for ApiAuthPolicy {
    fn enforce_rbac_write_authorization<'a>(
        &'a self,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        object: &'a Value,
    ) -> GenericCommandFuture<'a, ()> {
        Box::pin(async move {
            super::rbac_admission::enforce_rbac_write_authorization_with_inputs(
                &super::rbac_admission::RbacWriteAuthorizationInputs {
                    authorizer: self.authorizer.as_ref(),
                    policy_store: self.rbac_policy_store.as_ref(),
                    resource_query,
                },
                identity,
                api_version,
                kind,
                namespace,
                object,
            )
            .await
        })
    }
}

impl GenericCommandPolicy for ApiResourceMutationServices {
    fn decode_patch<'a>(
        &'a self,
        headers: &'a HeaderMap,
        body: &'a Bytes,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            check_content_type(headers)?;
            let content_type = headers
                .get("content-type")
                .and_then(|header| header.to_str().ok());
            if body.len() >= 4 && &body[..4] == b"k8s\x00" {
                klights_kube_protobuf::decode_protobuf(&body[4..]).map_err(|error| {
                    AppError::BadRequest(format!("Failed to decode protobuf: {error}"))
                })
            } else if content_type == Some("application/apply-patch+yaml") {
                super::parse_apply_yaml(body)
            } else {
                serde_json::from_slice(body)
                    .map_err(|error| AppError::BadRequest(format!("Invalid JSON: {error}")))
            }
        })
    }

    fn validate_patch_request(
        &self,
        api_version: &str,
        kind: &str,
        query: &CreateUpdateQuery,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<(), AppError> {
        if matches!(
            content_type,
            Some("application/apply-patch+yaml") | Some("application/apply-patch+json")
        ) {
            check_field_validation_strict_typed(api_version, kind, query, patch)?;
        }
        Ok(())
    }

    fn before_create<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        query: &'a CreateUpdateQuery,
        mut body: Value,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            check_field_validation_strict_typed(api_version, kind, query, &body)?;
            if let Some(name) = body
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
            {
                super::validation::validate_metadata_name_for_kind(
                    api_version,
                    kind,
                    name,
                    &format!("metadata.name for {kind}"),
                )?;
            }
            if api_version == "v1" && kind == "Pod" && namespace.is_some() {
                return Ok(body);
            }
            if kind == "Pod" {
                self.builtin_admission_defaults
                    .validate_pod_volume_paths(&body)?;
                validate_pod_sysctls(&body)?;
            }
            if kind == "CertificateSigningRequest" {
                stamp_csr_identity(&mut body, identity);
            }
            prepare_admissionregistration_resource(kind, &mut body)?;
            authorization
                .enforce_rbac_write_authorization(
                    resource_query,
                    identity,
                    api_version,
                    kind,
                    namespace,
                    &body,
                )
                .await?;
            Ok(body)
        })
    }

    fn prepare_create<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        mut body: Value,
        operation_now: Option<chrono::DateTime<chrono::Utc>>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, PreparedCreate> {
        Box::pin(async move {
            validate_builtin_resource_spec(kind, &body)?;
            if dry_run.is_all() {
                return Ok(PreparedCreate {
                    resource_name: String::new(),
                    body,
                });
            }
            if kind != "ResourceQuota"
                && let Some(namespace) = namespace
            {
                check_resource_quota_for_creation(
                    self.quota_runtime.as_ref(),
                    namespace,
                    kind,
                    &body,
                )
                .await?;
            }
            let resource_name = resolve_resource_name(&mut body)?;
            if !validate_metadata_name_for_kind(api_version, kind, &resource_name) {
                let detail = if metadata_name_uses_path_segment_validation(api_version, kind)
                    || kind == "IPAddress"
                {
                    "must be a valid path segment (not '.', '..', and no '/' or '%')"
                } else {
                    "must be a valid DNS subdomain (lowercase alphanumeric, hyphens, dots; max 253 chars; cannot start/end with hyphen or dot)"
                };
                return Err(AppError::UnprocessableEntity(format!(
                    "Invalid metadata.name '{resource_name}': {detail}"
                )));
            }
            let operation_now = operation_now.expect("live create operation timestamp");
            k8s_native_service::generic_command::prepare_create_metadata(
                namespace,
                &mut body,
                &resource_name,
                operation_now,
            );
            match kind {
                "Pod" => apply_pod_create_defaults_at(&mut body, operation_now),
                "PersistentVolumeClaim" => apply_pvc_create_defaults(&mut body),
                "PersistentVolume" => apply_pv_create_defaults(&mut body),
                "Namespace" => ensure_namespace_status_phase_active(&mut body),
                "ResourceQuota" => apply_resourcequota_create_status(&mut body),
                _ => {}
            }
            apply_workload_replicas_default(kind, &mut body);
            if kind == "ReplicationController" {
                apply_replicationcontroller_selector_default(&mut body);
            }
            if kind == "StatefulSet" {
                initialize_statefulset_revision_status_on_create(&resource_name, &mut body);
            }
            prepare_secret_for_storage(kind, &mut body)?;
            normalize_resource_for_storage(api_version, kind, &mut body);
            Ok(PreparedCreate {
                resource_name,
                body,
            })
        })
    }

    fn before_update<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        current: &'a Resource,
        mut body: Value,
        _dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            if matches!(kind, "ConfigMap" | "Secret")
                && current
                    .data
                    .get("immutable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            {
                check_immutable_fields(&current.data, &body, kind, namespace.unwrap_or(""), name)?;
            }
            check_field_validation_strict_typed(api_version, kind, query, &body)?;
            if kind == "Pod" {
                self.builtin_admission_defaults
                    .validate_pod_volume_paths(&body)?;
                validate_pod_sysctls(&body)?;
            }
            prepare_admissionregistration_resource(kind, &mut body)?;
            authorization
                .enforce_rbac_write_authorization(
                    resource_query,
                    identity,
                    api_version,
                    kind,
                    namespace,
                    &body,
                )
                .await?;
            Ok(body)
        })
    }

    fn after_update_admission<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        current: &'a Resource,
        mut body: Value,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            if kind == "Pod"
                && let Some(namespace) = namespace
            {
                validate_pod_resource_requirements_immutable(&current.data, &body)?;
                check_resource_quota_for_pod_update(
                    self.quota_runtime.as_ref(),
                    namespace,
                    &current.data,
                    &body,
                )
                .await?;
            }
            if kind == "PersistentVolumeClaim"
                && let Some(namespace) = namespace
            {
                check_resource_quota_for_pvc_update(
                    self.quota_runtime.as_ref(),
                    namespace,
                    &current.data,
                    &body,
                )
                .await?;
            }
            if kind == "PriorityClass" {
                validate_priorityclass_update_immutable(&current.data, &body)?;
            }
            validate_builtin_resource_spec(kind, &body)?;
            normalize_resource_for_storage(api_version, kind, &mut body);
            Ok(body)
        })
    }

    fn prepare_update_for_persistence<'a>(
        &'a self,
        kind: &'a str,
        mut body: Value,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            prepare_secret_for_storage(kind, &mut body)?;
            Ok(body)
        })
    }

    fn prepare_apply_create<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        mut patch: Value,
        operation_now: chrono::DateTime<chrono::Utc>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            if kind == "CertificateSigningRequest" {
                stamp_csr_identity(&mut patch, identity);
            }
            authorization
                .enforce_rbac_write_authorization(
                    resource_query,
                    identity,
                    api_version,
                    kind,
                    namespace,
                    &patch,
                )
                .await?;
            let manager =
                super::server_side_apply::resolve_field_manager(query.field_manager.as_deref());
            let applied = super::server_side_apply::server_side_apply(
                None,
                &patch,
                &manager,
                api_version,
                &klights_cluster_core::k8s_time::format_time(operation_now),
                query.force.unwrap_or(false),
            )
            .map_err(|conflicts| AppError::Conflict(conflicts.message()))?;
            let mut admitted = self
                .admission
                .admit(ResourceAdmissionRequest {
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    resource: None,
                    operation: "CREATE".to_string(),
                    namespace: namespace.map(str::to_string),
                    name: Some(name.to_string()),
                    object: applied,
                    old_object: None,
                    dry_run: dry_run.is_all(),
                    subresource: None,
                    options: None,
                })
                .await?;
            if let Some(namespace) = namespace {
                check_resource_quota_for_creation(
                    self.quota_runtime.as_ref(),
                    namespace,
                    kind,
                    &admitted,
                )
                .await?;
            }
            normalize_resource_for_storage(api_version, kind, &mut admitted);
            Ok(admitted)
        })
    }

    fn prepare_patch_update<'a>(
        &'a self,
        authorization: &'a dyn GenericCommandAuthorization,
        resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
        identity: &'a klights_auth::AuthenticatedIdentity,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        query: &'a CreateUpdateQuery,
        current: &'a Resource,
        patch: &'a Value,
        content_type: Option<&'a str>,
        operation_now: chrono::DateTime<chrono::Utc>,
        dry_run: DryRunMode,
    ) -> GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            let is_apply = matches!(
                content_type,
                Some("application/apply-patch+yaml") | Some("application/apply-patch+json")
            );
            let mut patched = if is_apply {
                let manager =
                    super::server_side_apply::resolve_field_manager(query.field_manager.as_deref());
                super::server_side_apply::server_side_apply(
                    Some(&current.data),
                    patch,
                    &manager,
                    api_version,
                    &klights_cluster_core::k8s_time::format_time(operation_now),
                    query.force.unwrap_or(false),
                )
                .map_err(|conflicts| AppError::Conflict(conflicts.message()))?
            } else {
                let merged = apply_patch(&current.data, patch, content_type)?;
                check_field_validation_strict_typed(api_version, kind, query, &merged)?;
                merged
            };
            prepare_admissionregistration_resource(kind, &mut patched)?;
            authorization
                .enforce_rbac_write_authorization(
                    resource_query,
                    identity,
                    api_version,
                    kind,
                    namespace,
                    &patched,
                )
                .await?;
            patched = self
                .admission
                .admit(ResourceAdmissionRequest {
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    resource: None,
                    operation: "UPDATE".to_string(),
                    namespace: namespace.map(str::to_string),
                    name: Some(name.to_string()),
                    object: patched,
                    old_object: Some((*current.data).clone()),
                    dry_run: dry_run.is_all(),
                    subresource: None,
                    options: None,
                })
                .await?;
            if matches!(kind, "ConfigMap" | "Secret")
                && current
                    .data
                    .get("immutable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            {
                check_immutable_fields(
                    &current.data,
                    &patched,
                    kind,
                    namespace.unwrap_or(""),
                    name,
                )?;
            }
            if kind == "Pod"
                && let Some(namespace) = namespace
            {
                validate_pod_resource_requirements_immutable(&current.data, &patched)?;
                check_resource_quota_for_pod_update(
                    self.quota_runtime.as_ref(),
                    namespace,
                    &current.data,
                    &patched,
                )
                .await?;
            }
            if kind == "PersistentVolumeClaim"
                && let Some(namespace) = namespace
            {
                check_resource_quota_for_pvc_update(
                    self.quota_runtime.as_ref(),
                    namespace,
                    &current.data,
                    &patched,
                )
                .await?;
            }
            if kind == "PriorityClass" {
                validate_priorityclass_update_immutable(&current.data, &patched)?;
            }
            validate_builtin_resource_spec(kind, &patched)?;
            prepare_secret_for_storage(kind, &mut patched)?;
            Ok(patched)
        })
    }

    fn normalize_for_storage(&self, api_version: &str, kind: &str, body: &mut Value) {
        normalize_resource_for_storage(api_version, kind, body);
    }
}

fn prepare_secret_for_storage(kind: &str, body: &mut Value) -> Result<(), AppError> {
    if kind != "Secret" {
        return Ok(());
    }
    validate_secret_data(body).map_err(AppError::UnprocessableEntity)?;
    process_secret_stringdata(body);
    Ok(())
}
