//! Generic built-in create/update/patch/delete handler orchestration.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use klights_cluster_core::{Resource, ResourcePreconditions};
use serde_json::Value;

use crate::AppError;

use super::{
    CreateStrategy, CreateUpdateQuery, DeleteCollectionQuery, DeleteIntent, DeleteResult,
    DryRunMode, FinalizerAwareDeleteStrategy, GenericCommandState, MutationEvent, PatchStrategy,
    ResourceAdmissionRequest, UpdateStrategy, WriteResult, create_non_pod_resource,
    create_with_strategy, delete_loaded_with_strategy, dispatch_mutation_event,
    ensure_delete_preconditions_match, patch_with_strategy, persisted_object,
    preserve_deletion_timestamp_on_update, set_deletion_timestamp_at, update_non_pod_resource,
    update_with_strategy,
};

pub struct GeneratedNamedResource<'a> {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
}

impl<'a> GeneratedNamedResource<'a> {
    pub fn new(
        api_version: &'static str,
        kind: &'static str,
        namespace: Option<&'a str>,
        name: &'a str,
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace,
            name,
        }
    }
}

pub struct GeneratedUpdateInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub body: Value,
}

pub struct GeneratedDeleteInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub body: Bytes,
}

pub struct GeneratedPatchInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub headers: HeaderMap,
    pub body: Bytes,
}

async fn dispatch_generated_mutation_event<S: GenericCommandState + ?Sized>(
    state: &S,
    operation: klights_reconcile_api::MutationOperation,
    resource: &Value,
    context: &'static str,
) {
    dispatch_mutation_event(
        state.command_lifecycle().mutation_effects(),
        MutationEvent {
            operation,
            resource,
            old_resource: None,
            persisted: true,
            dry_run: DryRunMode::Live,
            context,
        },
    )
    .await;
}

async fn reconcile_owner_refs_after_mutation<S: GenericCommandState + ?Sized>(
    state: &S,
    resource: &Resource,
    context: &'static str,
) {
    if resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty)
    {
        return;
    }
    if let Err(error) = state
        .command_lifecycle()
        .gc_owner_lifecycle()
        .reconcile_owner_references(resource.clone())
        .await
    {
        state
            .command_reconcile()
            .failure_metrics()
            .record_cascade_delete_failure();
        tracing::error!(
            context,
            api_version = %resource.api_version,
            kind = %resource.kind,
            namespace = ?resource.namespace,
            name = %resource.name,
            error = %error,
            "ownerReference GC reconciliation failed"
        );
    }
}

async fn invalidate_apiservice_cache<S: GenericCommandState + ?Sized>(
    state: &S,
    api_version: &str,
    kind: &str,
) {
    if api_version == "apiregistration.k8s.io/v1" && kind == "APIService" {
        state.apiservice_proxy_cache().clear().await;
    }
}

async fn enqueue_generated_controller_after_mutation<S: GenericCommandState + ?Sized>(
    state: &S,
    resource: &Value,
) {
    state
        .command_reconcile()
        .controller_dispatcher()
        .enqueue(resource)
        .await;
}

async fn maybe_reconcile_cluster_role_aggregation<S: GenericCommandState + ?Sized>(
    state: &S,
    api_version: &str,
    kind: &str,
) {
    if (api_version, kind) != ("rbac.authorization.k8s.io/v1", "ClusterRole") {
        return;
    }
    if let Err(error) = state
        .command_lifecycle()
        .generated_lifecycle()
        .reconcile_cluster_role_aggregation()
        .await
    {
        tracing::warn!(error = ?error, "failed to reconcile ClusterRole aggregation after mutation");
    }
}

struct BuiltinCreateStrategy<'a, S: GenericCommandState + ?Sized> {
    state: &'a S,
    identity: &'a klights_auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&'a str>,
    query: &'a CreateUpdateQuery,
}

impl<S: GenericCommandState + ?Sized> BuiltinCreateStrategy<'_, S> {
    fn is_generated_pod_create(&self) -> bool {
        self.api_version == "v1" && self.kind == "Pod" && self.namespace.is_some()
    }
}

#[async_trait::async_trait]
impl<S: GenericCommandState + ?Sized> CreateStrategy for BuiltinCreateStrategy<'_, S> {
    async fn before_admission(&self, body: Value) -> Result<Value, AppError> {
        if let Some(namespace) = self.namespace {
            self.state
                .command_admission()
                .builtin_admission_defaults()
                .ensure_namespace_active(namespace.to_string())
                .await?;
        }
        self.state
            .command_policy()
            .before_create(
                self.state.command_authorization(),
                self.state.command_store().resource_query(),
                self.identity,
                self.api_version,
                self.kind,
                self.namespace,
                self.query,
                body,
            )
            .await
    }

    async fn admit(&self, body: Value, dry_run: DryRunMode) -> Result<Value, AppError> {
        if self.is_generated_pod_create() {
            return Ok(body);
        }
        self.state
            .command_admission()
            .admission()
            .admit(ResourceAdmissionRequest {
                api_version: self.api_version.to_string(),
                kind: self.kind.to_string(),
                resource: None,
                operation: "CREATE".to_string(),
                namespace: self.namespace.map(str::to_string),
                name: body
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                object: body,
                old_object: None,
                dry_run: dry_run.is_all(),
                subresource: None,
                options: None,
            })
            .await
    }

    async fn persist_create(
        &self,
        mut body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let is_dry_run = dry_run.is_all();
        if self.is_generated_pod_create() {
            let namespace = self.namespace.expect("generated Pod namespace");
            let resource_name = body
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let result = self
                .state
                .command_store()
                .pod_mutation()
                .create_pod(klights_pod_api::PodApiCreateRequest {
                    namespace: namespace.to_string(),
                    body,
                    dry_run: is_dry_run,
                })
                .await
                .map_err(|error| {
                    AppError::from(error).with_resource_context("v1", "Pod", &resource_name)
                })?;
            return match result.resource {
                Some(resource) => Ok(WriteResult::Persisted(resource)),
                None => Ok(WriteResult::DryRun(result.body)),
            };
        }

        if self.kind == "Pod"
            && let Some(namespace) = self.namespace
        {
            body = self
                .state
                .command_admission()
                .builtin_admission_defaults()
                .prepare_pod_create(namespace.to_string(), body)
                .await?;
        }
        if self.kind == "PersistentVolumeClaim"
            && let Some(namespace) = self.namespace
        {
            body = self
                .state
                .command_admission()
                .builtin_admission_defaults()
                .prepare_pvc_create(namespace.to_string(), body)
                .await?;
        }

        let operation_now = (!is_dry_run)
            .then(|| klights_auth::clock::chrono_utc(self.state.command_runtime().clock().now()));
        let prepared = self
            .state
            .command_policy()
            .prepare_create(
                self.api_version,
                self.kind,
                self.namespace,
                body,
                operation_now,
                dry_run,
            )
            .await?;
        if is_dry_run {
            return Ok(WriteResult::DryRun(prepared.body));
        }

        let mut body = prepared.body;
        let pending_service_allocations = if self.api_version == "v1" && self.kind == "Service" {
            Some(
                self.state
                    .command_reconcile()
                    .service_allocations()
                    .prepare_create(&mut body)
                    .await
                    .map_err(|error| {
                        AppError::Internal(format!("Failed to allocate service fields: {error}"))
                    })?,
            )
        } else {
            None
        };

        match create_non_pod_resource(
            self.state.command_store().resource_command(),
            self.api_version,
            self.kind,
            self.namespace,
            &prepared.resource_name,
            body,
        )
        .await
        {
            Ok(resource) => Ok(WriteResult::Persisted(resource)),
            Err(error) => {
                if let Some(pending) = pending_service_allocations {
                    pending.release();
                }
                Err(error.with_resource_context(
                    self.api_version,
                    self.kind,
                    &prepared.resource_name,
                ))
            }
        }
    }
}

pub async fn create_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    identity: &klights_auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    query: CreateUpdateQuery,
    body: Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let dry_run = DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinCreateStrategy {
        state: state.as_ref(),
        identity,
        api_version,
        kind,
        namespace,
        query: &query,
    };
    match create_with_strategy(&strategy, body, dry_run).await? {
        WriteResult::Persisted(resource) => {
            if api_version == "v1" && kind == "Pod" && namespace.is_some() {
                dispatch_generated_mutation_event(
                    state.as_ref(),
                    klights_reconcile_api::MutationOperation::Create,
                    &resource.data,
                    "pod_create",
                )
                .await;
                return Ok((
                    StatusCode::CREATED,
                    Json(persisted_object(
                        state.command_store().identity(),
                        resource.data,
                        resource.resource_version,
                    )),
                ));
            }

            let resource_name = resource.name.clone();
            let context = if namespace.is_some() {
                "namespaced_create"
            } else {
                "cluster_create"
            };
            reconcile_owner_refs_after_mutation(state.as_ref(), &resource, context).await;
            invalidate_apiservice_cache(state.as_ref(), api_version, kind).await;
            if kind == "Namespace" {
                if let Err(error) = state
                    .command_lifecycle()
                    .generated_lifecycle()
                    .create_default_service_account(resource_name.clone())
                    .await
                {
                    tracing::warn!(
                        "Failed to create default ServiceAccount in namespace {}: {:#?}",
                        resource_name,
                        error
                    );
                }
                if let Err(error) = state
                    .command_lifecycle()
                    .generated_lifecycle()
                    .create_root_ca_config_map(resource_name.clone())
                    .await
                {
                    tracing::warn!(
                        "Failed to create kube-root-ca.crt ConfigMap in namespace {}: {:#?}",
                        resource_name,
                        error
                    );
                }
            }
            dispatch_generated_mutation_event(
                state.as_ref(),
                klights_reconcile_api::MutationOperation::Create,
                &resource.data,
                context,
            )
            .await;
            let data = persisted_object(
                state.command_store().identity(),
                resource.data,
                resource.resource_version,
            );
            enqueue_generated_controller_after_mutation(state.as_ref(), &data).await;
            maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
            Ok((StatusCode::CREATED, Json(data)))
        }
        WriteResult::DryRun(value) => Ok((StatusCode::CREATED, Json(value))),
        _ => unreachable!("create strategy returned unexpected WriteResult variant"),
    }
}

struct BuiltinUpdateStrategy<'a, S: GenericCommandState + ?Sized> {
    state: &'a S,
    identity: &'a klights_auth::AuthenticatedIdentity,
    target: GeneratedNamedResource<'a>,
    query: &'a CreateUpdateQuery,
}

#[async_trait::async_trait]
impl<S: GenericCommandState + ?Sized> UpdateStrategy for BuiltinUpdateStrategy<'_, S> {
    async fn load_current(&self) -> Result<Resource, AppError> {
        crate::generic_read::get_resource(
            self.state.command_store().resource_query(),
            self.target.api_version,
            self.target.kind,
            self.target.namespace,
            self.target.name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} not found", self.target.kind)))
    }

    async fn prepare_update(
        &self,
        current: &Resource,
        body: Value,
        dry_run: DryRunMode,
    ) -> Result<Value, AppError> {
        let body = self
            .state
            .command_policy()
            .before_update(
                self.state.command_authorization(),
                self.state.command_store().resource_query(),
                self.identity,
                self.target.api_version,
                self.target.kind,
                self.target.namespace,
                self.target.name,
                self.query,
                current,
                body,
                dry_run,
            )
            .await?;
        let body = self
            .state
            .command_admission()
            .admission()
            .admit(ResourceAdmissionRequest {
                api_version: self.target.api_version.to_string(),
                kind: self.target.kind.to_string(),
                resource: None,
                operation: "UPDATE".to_string(),
                namespace: self.target.namespace.map(str::to_string),
                name: Some(self.target.name.to_string()),
                object: body,
                old_object: Some((*current.data).clone()),
                dry_run: dry_run.is_all(),
                subresource: None,
                options: None,
            })
            .await?;
        self.state
            .command_policy()
            .after_update_admission(
                self.target.api_version,
                self.target.kind,
                self.target.namespace,
                current,
                body,
            )
            .await
    }

    async fn persist_update(
        &self,
        current: Resource,
        body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let mut body = self
            .state
            .command_policy()
            .prepare_update_for_persistence(self.target.kind, body)
            .await?;
        let requested_rv = metadata_resource_version(&body);
        super::prepare_builtin_generation_for_update(self.target.kind, &current.data, &mut body);
        klights_types::preserve_status_subresource_on_main_update(
            self.target.api_version,
            self.target.kind,
            &current.data,
            &mut body,
        );
        preserve_deletion_timestamp_on_update(&current.data, &mut body);
        if dry_run.is_all() {
            return Ok(WriteResult::DryRun(body));
        }
        let resource = self
            .state
            .command_store()
            .generated_mutations()
            .update_main_resource(
                self.target.api_version.to_string(),
                self.target.kind.to_string(),
                self.target.namespace.map(str::to_string),
                self.target.name.to_string(),
                body,
                ResourcePreconditions {
                    uid: Some(current.uid.clone()),
                    resource_version: requested_rv,
                },
            )
            .await?;
        Ok(WriteResult::Persisted(resource))
    }
}

pub async fn update_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    identity: &klights_auth::AuthenticatedIdentity,
    request: GeneratedUpdateInnerRequest<'_>,
) -> Result<Json<Value>, AppError> {
    let GeneratedUpdateInnerRequest {
        target,
        query,
        body,
    } = request;
    let api_version = target.api_version;
    let kind = target.kind;
    let namespace = target.namespace;
    let name = target.name;
    let dry_run = DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinUpdateStrategy {
        state: state.as_ref(),
        identity,
        target: GeneratedNamedResource::new(api_version, kind, namespace, name),
        query: &query,
    };
    match update_with_strategy(&strategy, body, dry_run).await? {
        WriteResult::DryRun(value) => Ok(Json(value)),
        WriteResult::Persisted(resource) => {
            finalize_after_update_if_ready(state.as_ref(), kind, namespace, name, &resource).await;
            let context = if namespace.is_some() {
                "namespaced_update"
            } else {
                "cluster_update"
            };
            reconcile_owner_refs_after_mutation(state.as_ref(), &resource, context).await;
            invalidate_apiservice_cache(state.as_ref(), api_version, kind).await;
            dispatch_generated_mutation_event(
                state.as_ref(),
                klights_reconcile_api::MutationOperation::Update,
                &resource.data,
                context,
            )
            .await;
            if kind == "ConfigMap"
                && name == "kube-root-ca.crt"
                && let Some(namespace) = namespace
                && resource
                    .data
                    .pointer("/data/ca.crt")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                && let Err(error) = state
                    .command_lifecycle()
                    .generated_lifecycle()
                    .reconcile_root_ca_data(namespace.to_string())
                    .await
            {
                tracing::warn!(namespace, error = ?error, "failed to reconcile kube-root-ca.crt after data modification");
            }
            let data = persisted_object(
                state.command_store().identity(),
                resource.data,
                resource.resource_version,
            );
            if !(api_version == "v1" && kind == "Service") {
                enqueue_generated_controller_after_mutation(state.as_ref(), &data).await;
            }
            maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
            Ok(Json(data))
        }
        _ => unreachable!("update strategy returned unexpected WriteResult variant"),
    }
}

pub async fn finalize_after_update_if_ready<S: GenericCommandState + ?Sized>(
    state: &S,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource: &Resource,
) {
    if kind == "Pod" {
        if let Some(namespace) = namespace {
            let _ = state
                .command_lifecycle()
                .generated_lifecycle()
                .maybe_finalize_pod_after_finalizers_drained(
                    namespace.to_string(),
                    name.to_string(),
                    (*resource.data).clone(),
                )
                .await;
        }
        return;
    }
    if !super::ready_to_finalize_after_update(&resource.data) {
        return;
    }
    let preconditions = ResourcePreconditions::uid_and_resource_version(
        resource.uid.clone(),
        resource.resource_version,
    );
    match super::delete_non_pod_resource(
        state.command_store().resource_command(),
        resource.api_version.as_str(),
        resource.kind.as_str(),
        resource.namespace.as_deref(),
        resource.name.as_str(),
        preconditions,
    )
    .await
    {
        Ok(_) => {}
        Err(AppError::NotFound(_) | AppError::Conflict(_)) => return,
        Err(error) => {
            tracing::warn!(
                api_version = %resource.api_version,
                kind = %resource.kind,
                namespace = ?resource.namespace,
                name = %resource.name,
                error = ?error,
                "finalizer-drained hard delete failed"
            );
            return;
        }
    }
    invalidate_apiservice_cache(state, &resource.api_version, &resource.kind).await;
    if resource.api_version == "v1" && resource.kind == "Service" {
        state
            .command_reconcile()
            .service_allocations()
            .release_resource(&resource.data);
    }
    if let Err(error) = state
        .command_store()
        .finalizer_lifecycle()
        .run_finalized_effects(klights_reconcile_api::FinalizerEffectsRequest {
            resource: resource.clone(),
        })
        .await
    {
        tracing::error!(
            namespace = ?resource.namespace,
            name = %resource.name,
            error = %error,
            "finalizer-drained post-delete effects failed"
        );
    }
}

async fn maybe_reconcile_service_after_controller_endpointslice_delete<
    S: GenericCommandState + ?Sized,
>(
    state: &S,
    namespace: &str,
    deleted: &Value,
) -> Result<(), AppError> {
    let managed_by = deleted
        .pointer("/metadata/labels/endpointslice.kubernetes.io~1managed-by")
        .and_then(Value::as_str);
    if managed_by != Some("endpointslice-controller.k8s.io") {
        return Ok(());
    }
    let Some(service_name) = deleted
        .pointer("/metadata/labels/kubernetes.io~1service-name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
    else {
        return Ok(());
    };
    let Some(service) = crate::generic_read::get_resource(
        state.command_store().resource_query(),
        "v1",
        "Service",
        Some(namespace),
        service_name,
    )
    .await?
    else {
        return Ok(());
    };
    let service_type = service
        .data
        .pointer("/spec/type")
        .and_then(Value::as_str)
        .unwrap_or("ClusterIP");
    if service_type == "ExternalName" {
        return Ok(());
    }
    klights_reconcile_api::ServiceReconcileSink::enqueue_service_reconcile_batch(
        state.command_reconcile().controller_dispatcher(),
        vec![klights_reconcile_api::ServiceReconcileKey::new(
            namespace,
            service_name,
        )],
    )
    .await
    .map_err(|error| AppError::Internal(error.to_string()))
}

fn metadata_resource_version(body: &Value) -> Option<i64> {
    body.pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<i64>().ok())
}

struct BuiltinPatchStrategy<'a, S: GenericCommandState + ?Sized> {
    state: &'a S,
    identity: &'a klights_auth::AuthenticatedIdentity,
    target: GeneratedNamedResource<'a>,
    query: &'a CreateUpdateQuery,
    headers: &'a HeaderMap,
}

#[async_trait::async_trait]
impl<S: GenericCommandState + ?Sized> PatchStrategy for BuiltinPatchStrategy<'_, S> {
    async fn apply_patch(
        &self,
        patch: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let content_type = self
            .headers
            .get("content-type")
            .and_then(|header| header.to_str().ok());
        let supports_apply_create = content_type == Some("application/apply-patch+yaml");
        let operation_now =
            klights_auth::clock::chrono_utc(self.state.command_runtime().clock().now());
        let api_version = self.target.api_version;
        let kind = self.target.kind;
        let namespace = self.target.namespace;
        let name = self.target.name;

        self.state.command_policy().validate_patch_request(
            api_version,
            kind,
            self.query,
            &patch,
            content_type,
        )?;

        if supports_apply_create
            && crate::generic_read::get_resource(
                self.state.command_store().resource_query(),
                api_version,
                kind,
                namespace,
                name,
            )
            .await?
            .is_none()
        {
            let body = self
                .state
                .command_policy()
                .prepare_apply_create(
                    self.state.command_authorization(),
                    self.state.command_store().resource_query(),
                    self.identity,
                    api_version,
                    kind,
                    namespace,
                    name,
                    self.query,
                    patch,
                    operation_now,
                    dry_run,
                )
                .await?;
            if dry_run.is_all() {
                return Ok(WriteResult::Response {
                    status: StatusCode::CREATED,
                    body,
                });
            }
            let resource = create_non_pod_resource(
                self.state.command_store().resource_command(),
                api_version,
                kind,
                namespace,
                name,
                body,
            )
            .await?;
            let context = if namespace.is_some() {
                "namespaced_apply_create"
            } else {
                "cluster_apply_create"
            };
            reconcile_owner_refs_after_mutation(self.state, &resource, context).await;
            invalidate_apiservice_cache(self.state, api_version, kind).await;
            dispatch_generated_mutation_event(
                self.state,
                klights_reconcile_api::MutationOperation::Create,
                &resource.data,
                context,
            )
            .await;
            maybe_reconcile_cluster_role_aggregation(self.state, api_version, kind).await;
            return Ok(WriteResult::Response {
                status: StatusCode::CREATED,
                body: persisted_object(
                    self.state.command_store().identity(),
                    resource.data,
                    resource.resource_version,
                ),
            });
        }

        const MAX_RETRIES: u32 = 20;
        for attempt in 0..MAX_RETRIES {
            let current = crate::generic_read::get_resource(
                self.state.command_store().resource_query(),
                api_version,
                kind,
                namespace,
                name,
            )
            .await?
            .ok_or_else(|| AppError::NotFound(format!("{kind} not found")))?;
            let mut patched = self
                .state
                .command_policy()
                .prepare_patch_update(
                    self.state.command_authorization(),
                    self.state.command_store().resource_query(),
                    self.identity,
                    api_version,
                    kind,
                    namespace,
                    name,
                    self.query,
                    &current,
                    &patch,
                    content_type,
                    operation_now,
                    dry_run,
                )
                .await?;
            super::prepare_builtin_generation_for_update(kind, &current.data, &mut patched);
            klights_types::preserve_status_subresource_on_main_update(
                api_version,
                kind,
                &current.data,
                &mut patched,
            );
            preserve_deletion_timestamp_on_update(&current.data, &mut patched);
            self.state
                .command_policy()
                .normalize_for_storage(api_version, kind, &mut patched);
            if dry_run.is_all() {
                return Ok(WriteResult::DryRun(patched));
            }
            match update_non_pod_resource(
                self.state.command_store().resource_command(),
                api_version,
                kind,
                namespace,
                name,
                patched,
                current.resource_version,
            )
            .await
            {
                Ok(resource) => return Ok(WriteResult::Persisted(resource)),
                Err(error)
                    if attempt < MAX_RETRIES - 1 && matches!(error, AppError::Conflict(_)) =>
                {
                    tracing::debug!(kind, ?namespace, name, attempt, "PATCH conflict; retrying");
                    let backoff_ms = std::cmp::min(20u64.saturating_mul(1u64 << attempt), 250);
                    let _ = self
                        .state
                        .command_runtime()
                        .task_supervisor()
                        .sleep(
                            "patch_conflict_retry_backoff",
                            Duration::from_millis(backoff_ms),
                        )
                        .await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("PATCH retry loop exhausted without returning")
    }
}

pub async fn patch_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    identity: &klights_auth::AuthenticatedIdentity,
    request: GeneratedPatchInnerRequest<'_>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let GeneratedPatchInnerRequest {
        target,
        query,
        headers,
        body,
    } = request;
    let api_version = target.api_version;
    let kind = target.kind;
    let namespace = target.namespace;
    let name = target.name;
    let patch = state.command_policy().decode_patch(&headers, &body).await?;
    let dry_run = DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinPatchStrategy {
        state: state.as_ref(),
        identity,
        target: GeneratedNamedResource::new(api_version, kind, namespace, name),
        query: &query,
        headers: &headers,
    };
    let result = match patch_with_strategy(&strategy, patch, dry_run).await? {
        WriteResult::Persisted(resource) => {
            finalize_after_update_if_ready(state.as_ref(), kind, namespace, name, &resource).await;
            let context = if namespace.is_some() {
                "namespaced_patch"
            } else {
                "cluster_patch"
            };
            reconcile_owner_refs_after_mutation(state.as_ref(), &resource, context).await;
            invalidate_apiservice_cache(state.as_ref(), api_version, kind).await;
            dispatch_generated_mutation_event(
                state.as_ref(),
                klights_reconcile_api::MutationOperation::Patch,
                &resource.data,
                context,
            )
            .await;
            let data = persisted_object(
                state.command_store().identity(),
                resource.data.clone(),
                resource.resource_version,
            );
            if !(api_version == "v1" && kind == "Service") {
                enqueue_generated_controller_after_mutation(state.as_ref(), &data).await;
            }
            maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
            WriteResult::Persisted(resource)
        }
        other => other,
    };
    let (status, data) =
        result.into_response_parts(state.command_store().identity(), StatusCode::OK);
    Ok((status, Json(data)))
}

// Delete orchestration is kept below create/update/patch so concurrent generic-read
// extraction never needs to edit this command-specific destination module.

#[allow(clippy::too_many_arguments)]
async fn run_owner_cascade_sweeps(
    gc_owner_lifecycle: Arc<dyn klights_reconcile_api::GcOwnerLifecyclePort>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    api_version: String,
    owner_uid: String,
    owner_name: String,
    owner_kind: String,
    namespace: Option<String>,
) {
    const MAX_SWEEPS: u32 = 30;
    for attempt in 0..MAX_SWEEPS {
        let backoff_ms = std::cmp::min(200u64.saturating_mul(1u64 << attempt.min(5)), 5_000);
        if supervisor
            .sleep(
                "owner_cascade_sweep_backoff",
                Duration::from_millis(backoff_ms),
            )
            .await
            .is_err()
        {
            return;
        }
        match gc_owner_lifecycle
            .sweep_dependents(klights_reconcile_api::GcOwnerIdentity::new(
                &api_version,
                &owner_kind,
                namespace.clone(),
                &owner_name,
                &owner_uid,
            ))
            .await
        {
            Ok(false) => return,
            Ok(true) => continue,
            Err(error) => {
                metrics.record_cascade_delete_failure();
                tracing::error!(?namespace, owner_name, error = %error, "owner cascade sweep failed");
            }
        }
    }
}

async fn run_post_hard_delete_effects<S: GenericCommandState + ?Sized>(
    state: &S,
    api_version: &str,
    kind: &str,
    resource: &Resource,
) {
    invalidate_apiservice_cache(state, api_version, kind).await;
    if api_version == "v1" && kind == "Service" {
        state
            .command_reconcile()
            .service_allocations()
            .release_resource(&resource.data);
    }
    dispatch_generated_mutation_event(
        state,
        klights_reconcile_api::MutationOperation::HardDelete,
        &resource.data,
        "generated_hard_delete",
    )
    .await;
}

async fn schedule_foreground_owner_finalization<S: GenericCommandState + 'static>(
    state: Arc<S>,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    name: &str,
    owner: Resource,
) {
    let namespace = namespace.map(str::to_string);
    let name = name.to_string();
    let dispatch_namespace = namespace.clone();
    let dispatch_name = name.clone();
    let dispatch_state = state.clone();
    if let Err(error) = state
        .command_runtime()
        .task_supervisor()
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "foreground_owner_finalization_dispatch",
            async move {
                let worker_supervisor = dispatch_state
                    .command_runtime()
                    .task_supervisor_owned();
                let worker_gc = dispatch_state
                    .command_lifecycle()
                    .gc_owner_lifecycle_owned();
                let worker_metrics = dispatch_state
                    .command_reconcile()
                    .failure_metrics_owned();
                if let Err(error) = worker_supervisor
                    .spawn_async(
                        klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                        "foreground_owner_finalization",
                        async move {
                            if let Err(error) = worker_gc.finalize_foreground_owner(owner).await {
                                worker_metrics.record_cascade_delete_failure();
                                tracing::error!(
                                    namespace = ?dispatch_namespace,
                                    name = %dispatch_name,
                                    api_version,
                                    kind,
                                    error = %error,
                                    "foreground owner finalization failed"
                                );
                            }
                        },
                    )
                    .await
                {
                    dispatch_state
                        .command_reconcile()
                        .failure_metrics()
                        .record_cascade_delete_failure();
                    tracing::warn!(error = %error, "failed to schedule foreground owner finalization work task");
                }
            },
        )
        .await
    {
        state
            .command_reconcile()
            .failure_metrics()
            .record_cascade_delete_failure();
        tracing::warn!(?namespace, name, error = %error, "failed to schedule foreground owner finalization dispatch task");
    }
}

pub async fn delete_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    _identity: &klights_auth::AuthenticatedIdentity,
    request: GeneratedDeleteInnerRequest<'_>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let GeneratedDeleteInnerRequest {
        target,
        query,
        body,
    } = request;
    let api_version = target.api_version;
    let kind = target.kind;
    let namespace = target.namespace;
    let name = target.name;
    let delete_intent = DeleteIntent::from_query_and_body(&query, &body)?;
    let is_dry_run = delete_intent.dry_run.is_all();
    let resource = crate::generic_read::get_resource(
        state.command_store().resource_query(),
        api_version,
        kind,
        namespace,
        name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("{kind} not found")))?;
    ensure_delete_preconditions_match(&resource, &delete_intent.preconditions)?;

    let options =
        serde_json::to_value(&delete_intent.options).unwrap_or_else(|_| serde_json::json!({}));
    let _ = state
        .command_admission()
        .admission()
        .admit(ResourceAdmissionRequest {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            resource: None,
            operation: "DELETE".to_string(),
            namespace: namespace.map(str::to_string),
            name: Some(name.to_string()),
            object: Value::Null,
            old_object: Some((*resource.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: Some(options),
        })
        .await?;

    if kind == "Pod"
        && let Some(namespace) = namespace
    {
        let outcome = state
            .command_store()
            .pod_mutation()
            .delete_pod(klights_pod_api::PodApiDeleteRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                options: delete_intent.options.into(),
                dry_run: is_dry_run,
            })
            .await?;
        return match outcome {
            klights_pod_api::PodApiDeleteOutcome::DryRun(value) => {
                Ok((StatusCode::OK, Json(value)))
            }
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
                tracing::info!(
                    kind = %resource.data.get("kind").and_then(|value| value.as_str()).unwrap_or("?"),
                    name = %resource.name,
                    namespace = %resource.data.pointer("/metadata/namespace").and_then(|value| value.as_str()).unwrap_or("?"),
                    "pod delete GracefulSet: firing side effects"
                );
                dispatch_generated_mutation_event(
                    state.as_ref(),
                    klights_reconcile_api::MutationOperation::DeleteMark,
                    &resource.data,
                    "pod_delete_mark",
                )
                .await;
                Ok((
                    StatusCode::ACCEPTED,
                    Json(super::accepted_object(
                        state.command_store().identity(),
                        resource.data,
                        resource.resource_version,
                    )),
                ))
            }
        };
    }

    if is_dry_run {
        let mut data = (*resource.data).clone();
        set_deletion_timestamp_at(
            &mut data,
            klights_auth::clock::chrono_utc(state.command_runtime().clock().now()),
        );
        return Ok((
            StatusCode::OK,
            Json(persisted_object(
                state.command_store().identity(),
                data,
                resource.resource_version,
            )),
        ));
    }

    let target_identity =
        klights_types::ResourceKey::new(api_version, kind, namespace.map(str::to_string), name);
    let strategy = FinalizerAwareDeleteStrategy {
        resource_query: state.command_store().resource_query(),
        lifecycle: state.command_store().finalizer_lifecycle(),
        operation_now: klights_auth::clock::chrono_utc(state.command_runtime().clock().now()),
    };
    let resource =
        match delete_loaded_with_strategy(&strategy, target_identity, resource, &delete_intent)
            .await?
        {
            DeleteResult::MarkedTerminating(updated) => {
                schedule_foreground_owner_finalization(
                    state.clone(),
                    api_version,
                    kind,
                    namespace,
                    name,
                    updated.clone(),
                )
                .await;
                invalidate_apiservice_cache(state.as_ref(), api_version, kind).await;
                maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
                return Ok((
                    StatusCode::ACCEPTED,
                    Json(super::accepted_object(
                        state.command_store().identity(),
                        updated.data,
                        updated.resource_version,
                    )),
                ));
            }
            DeleteResult::GoneOrUidChanged => {
                return Err(AppError::NotFound(format!("{kind} not found")));
            }
            DeleteResult::HardDeleted(resource) => resource,
        };

    let owner_uid = resource.uid.clone();
    let owner_name = resource.name.clone();
    run_post_hard_delete_effects(state.as_ref(), api_version, kind, &resource).await;
    if api_version == "v1"
        && kind == "Node"
        && let Err(error) = state
            .command_lifecycle()
            .generated_lifecycle()
            .delete_node_cleanup_intents(resource.name.clone())
            .await
    {
        tracing::warn!(node = %resource.name, error = ?error, "failed to delete pod cleanup intents for deleted node");
    }

    if !delete_intent.orphan_children {
        let owner = klights_reconcile_api::GcOwnerIdentity::new(
            api_version,
            kind,
            namespace.map(str::to_string),
            &owner_name,
            &owner_uid,
        );
        if let Err(error) = state
            .command_lifecycle()
            .gc_owner_lifecycle()
            .cascade_delete(owner)
            .await
        {
            state
                .command_reconcile()
                .failure_metrics()
                .record_cascade_delete_failure();
            tracing::error!(?namespace, owner_name, error = %error, "cascade delete failed");
        }
        let sweeps = run_owner_cascade_sweeps(
            state.command_lifecycle().gc_owner_lifecycle_owned(),
            state.command_runtime().task_supervisor_owned(),
            state.command_reconcile().failure_metrics_owned(),
            api_version.to_string(),
            owner_uid,
            owner_name,
            kind.to_string(),
            namespace.map(str::to_string),
        );
        if let Err(error) = state
            .command_runtime()
            .task_supervisor()
            .spawn_async(
                klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                "owner_cascade_sweeps",
                sweeps,
            )
            .await
        {
            tracing::warn!(error = %error, "Failed to schedule owner cascade sweep");
        }
    }

    if kind == "ConfigMap"
        && name == "kube-root-ca.crt"
        && let Some(namespace) = namespace
        && let Err(error) = state
            .command_lifecycle()
            .generated_lifecycle()
            .reconcile_root_ca(namespace.to_string())
            .await
    {
        tracing::warn!(namespace, error = ?error, "failed to recreate kube-root-ca.crt after deletion");
    }
    if kind == "EndpointSlice"
        && let Some(namespace) = namespace
    {
        maybe_reconcile_service_after_controller_endpointslice_delete(
            state.as_ref(),
            namespace,
            &resource.data,
        )
        .await?;
    }
    if kind != "ResourceQuota" && kind != "Endpoints" {
        dispatch_generated_mutation_event(
            state.as_ref(),
            klights_reconcile_api::MutationOperation::DeleteMark,
            &resource.data,
            "generated_delete_reconcile",
        )
        .await;
    }
    let data = persisted_object(
        state.command_store().identity(),
        resource.data,
        resource.resource_version,
    );
    maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
    Ok((StatusCode::OK, Json(data)))
}

pub async fn delete_collection_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    identity: &klights_auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: &str,
    query: DeleteCollectionQuery,
) -> Result<Json<Value>, AppError> {
    delete_collection_shared_inner(
        state,
        identity,
        api_version,
        kind,
        klights_leader_api::ResourceListScope::Namespace(namespace.to_string()),
        query,
    )
    .await
}

pub async fn delete_collection_shared_inner<S: GenericCommandState + 'static>(
    state: Arc<S>,
    _identity: &klights_auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    scope: klights_leader_api::ResourceListScope,
    query: DeleteCollectionQuery,
) -> Result<Json<Value>, AppError> {
    let dry_run = DryRunMode::from_delete_collection_query(&query)?;
    let namespace = scope.namespace();
    let list = crate::generic_read::list_resources(
        state.command_store().resource_query(),
        api_version,
        kind,
        scope.clone(),
        query.label_selector.as_deref(),
        None,
        None,
        None,
    )
    .await?;
    if dry_run.is_all() {
        return Ok(Json(super::delete_collection_success_status()));
    }
    let strategy = FinalizerAwareDeleteStrategy {
        resource_query: state.command_store().resource_query(),
        lifecycle: state.command_store().finalizer_lifecycle(),
        operation_now: klights_auth::clock::chrono_utc(state.command_runtime().clock().now()),
    };
    for resource in list.into_items() {
        let owner_uid = resource.uid.clone();
        let resource_name = resource.name.clone();
        let target = klights_types::ResourceKey::new(
            api_version,
            kind,
            namespace.map(str::to_string),
            resource_name.clone(),
        );
        let intent =
            DeleteIntent::collection_item(dry_run, ResourcePreconditions::uid(owner_uid.clone()));
        match delete_loaded_with_strategy(&strategy, target, resource, &intent).await {
            Ok(DeleteResult::HardDeleted(deleted)) => {
                run_post_hard_delete_effects(state.as_ref(), api_version, kind, &deleted).await;
                if let Err(error) = state
                    .command_lifecycle()
                    .gc_owner_lifecycle()
                    .cascade_delete(klights_reconcile_api::GcOwnerIdentity::new(
                        api_version,
                        kind,
                        namespace.map(str::to_string),
                        &resource_name,
                        &owner_uid,
                    ))
                    .await
                {
                    state
                        .command_reconcile()
                        .failure_metrics()
                        .record_cascade_delete_failure();
                    tracing::error!(?namespace, resource_name, error = %error, "delete collection: cascade delete failed");
                }
            }
            Ok(DeleteResult::MarkedTerminating(_) | DeleteResult::GoneOrUidChanged) => {}
            Err(error) => {
                state
                    .command_reconcile()
                    .failure_metrics()
                    .record_cascade_delete_failure();
                tracing::error!(?namespace, resource_name, error = ?error, "delete collection: resource delete failed");
            }
        }
    }

    if kind != "ResourceQuota" && kind != "Endpoints" {
        let metadata = namespace
            .map(|namespace| serde_json::json!({"namespace": namespace}))
            .unwrap_or_else(|| serde_json::json!({}));
        let stub = serde_json::json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": metadata,
        });
        dispatch_generated_mutation_event(
            state.as_ref(),
            klights_reconcile_api::MutationOperation::DeleteMark,
            &stub,
            "generated_delete_collection",
        )
        .await;
    }
    maybe_reconcile_cluster_role_aggregation(state.as_ref(), api_version, kind).await;
    Ok(Json(super::delete_collection_success_status()))
}
