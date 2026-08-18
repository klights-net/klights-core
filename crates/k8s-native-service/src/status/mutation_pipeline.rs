use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::{Json, body::Bytes};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::Value;

use crate::generic_command::GenericCommandState;
use crate::{AppError, inject_resource_version};
use klights_cluster_core::{
    PatchKind, Resource, ResourcePatchRequest, ResourcePreconditions, StatusApplyFreshness,
    StatusApplyOrigin, merge_status_for_apply,
};

#[derive(Clone, Debug)]
pub struct StatusMutationTarget {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

impl StatusMutationTarget {
    pub fn namespaced(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }

    pub fn cluster(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: None,
            name: name.into(),
        }
    }

    fn not_found_message(&self) -> String {
        match self.namespace.as_deref() {
            Some(namespace) => format!("{} {}/{} not found", self.kind, namespace, self.name),
            None => format!("{} {} not found", self.kind, self.name),
        }
    }
}

pub trait StatusMutationDecoder: Send + Sync {
    fn decode_patch_body(&self, body: &Bytes) -> Result<Value, AppError>;
}

pub struct K8sStatusMutationDecoder;

impl StatusMutationDecoder for K8sStatusMutationDecoder {
    fn decode_patch_body(&self, body: &Bytes) -> Result<Value, AppError> {
        if body.len() >= 4 && &body[..4] == b"k8s\x00" {
            klights_kube_protobuf::decode_protobuf(&body[4..])
                .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))
        } else {
            serde_json::from_slice(body)
                .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))
        }
    }
}

pub trait StatusMutationOperation: Send + Sync {
    fn operation_name(&self) -> &'static str;
    fn working_document(
        &self,
        current: &Resource,
        patcher: &dyn PatchApplication,
    ) -> Result<Value, AppError>;
    fn precondition_document(&self) -> &Value;
    fn status_value(&self, working_document: &Value) -> Option<Value>;
    fn metadata_patch(&self, current: &Resource, working_document: &Value) -> Option<Value>;
}

pub struct StatusPutOperation {
    body: Value,
}

impl StatusPutOperation {
    pub fn new(body: Value) -> Self {
        Self { body }
    }
}

impl StatusMutationOperation for StatusPutOperation {
    fn operation_name(&self) -> &'static str {
        "update"
    }

    fn working_document(
        &self,
        _current: &Resource,
        _patcher: &dyn PatchApplication,
    ) -> Result<Value, AppError> {
        Ok(self.body.clone())
    }

    fn precondition_document(&self) -> &Value {
        &self.body
    }

    fn status_value(&self, working_document: &Value) -> Option<Value> {
        Some(
            working_document
                .get("status")
                .cloned()
                .unwrap_or(Value::Null),
        )
    }

    fn metadata_patch(&self, _current: &Resource, working_document: &Value) -> Option<Value> {
        build_status_metadata_patch(working_document.get("metadata"))
    }
}

pub struct StatusPatchOperation {
    patch: Value,
    content_type: Option<String>,
    patch_metadata: bool,
}

impl StatusPatchOperation {
    pub fn new(patch: Value, content_type: Option<String>) -> Self {
        Self {
            patch,
            content_type,
            patch_metadata: true,
        }
    }

    pub fn status_only(patch: Value, content_type: Option<String>) -> Self {
        Self {
            patch,
            content_type,
            patch_metadata: false,
        }
    }
}

impl StatusMutationOperation for StatusPatchOperation {
    fn operation_name(&self) -> &'static str {
        "patch"
    }

    fn working_document(
        &self,
        current: &Resource,
        patcher: &dyn PatchApplication,
    ) -> Result<Value, AppError> {
        patcher.apply_patch(&current.data, &self.patch, self.content_type.as_deref())
    }

    fn precondition_document(&self) -> &Value {
        &self.patch
    }

    fn status_value(&self, working_document: &Value) -> Option<Value> {
        working_document.get("status").cloned()
    }

    fn metadata_patch(&self, current: &Resource, working_document: &Value) -> Option<Value> {
        if !self.patch_metadata {
            return None;
        }
        build_status_metadata_patch_from_diff(
            current.data.get("metadata"),
            working_document.get("metadata"),
        )
    }
}

pub trait StatusMutationPrecondition: Send + Sync {
    fn expected_resource_version(&self, document: &Value) -> Option<i64>;
}

pub struct LenientStatusResourceVersionPrecondition;

impl StatusMutationPrecondition for LenientStatusResourceVersionPrecondition {
    fn expected_resource_version(&self, document: &Value) -> Option<i64> {
        document
            .pointer("/metadata/resourceVersion")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<i64>().ok())
    }
}

pub trait StatusMutationMergePolicy: Send + Sync {
    fn merge_status(&self, target: &StatusMutationTarget, current: &Resource, status: &mut Value);
}

pub struct ApiSubresourceStatusMergePolicy {
    pre_merge: Option<fn(Option<&Value>, &mut Value)>,
}

impl ApiSubresourceStatusMergePolicy {
    pub fn new(pre_merge: Option<fn(Option<&Value>, &mut Value)>) -> Self {
        Self { pre_merge }
    }
}

impl StatusMutationMergePolicy for ApiSubresourceStatusMergePolicy {
    fn merge_status(&self, target: &StatusMutationTarget, current: &Resource, status: &mut Value) {
        if let Some(pre_merge) = self.pre_merge {
            pre_merge(current.data.get("status"), status);
        }
        merge_status_for_apply(
            &target.api_version,
            &target.kind,
            current.data.as_ref(),
            status,
            StatusApplyFreshness::Fresh,
            StatusApplyOrigin::ApiSubresource,
        );
    }
}

pub trait PatchApplication: Send + Sync {
    fn apply_patch(
        &self,
        current: &Value,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<Value, AppError>;
}

#[async_trait]
pub trait StatusMutationWriter: PatchApplication + Send + Sync {
    async fn get(&self, target: &StatusMutationTarget) -> Result<Option<Resource>, AppError>;
    async fn write_status(
        &self,
        target: &StatusMutationTarget,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource, AppError>;
    async fn patch_metadata(
        &self,
        target: &StatusMutationTarget,
        metadata_patch: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource, AppError>;
}

pub struct DatastoreStatusMutationWriter<S> {
    state: Arc<S>,
}

impl<S> DatastoreStatusMutationWriter<S> {
    pub fn new(state: Arc<S>) -> Self {
        Self { state }
    }
}

impl<S: GenericCommandState> PatchApplication for DatastoreStatusMutationWriter<S> {
    fn apply_patch(
        &self,
        current: &Value,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<Value, AppError> {
        self.state
            .command_policy()
            .apply_patch(current, patch, content_type)
    }
}

#[async_trait]
impl<S: GenericCommandState> StatusMutationWriter for DatastoreStatusMutationWriter<S> {
    async fn get(&self, target: &StatusMutationTarget) -> Result<Option<Resource>, AppError> {
        crate::generic_read::get_resource(
            self.state.command_store().resource_query(),
            &target.api_version,
            &target.kind,
            target.namespace.as_deref(),
            &target.name,
        )
        .await
    }

    async fn write_status(
        &self,
        target: &StatusMutationTarget,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource, AppError> {
        crate::generic_command::update_resource_status(
            self.state.command_store().resource_command(),
            &target.api_version,
            &target.kind,
            target.namespace.as_deref(),
            &target.name,
            status,
            preconditions,
        )
        .await
    }

    async fn patch_metadata(
        &self,
        target: &StatusMutationTarget,
        metadata_patch: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource, AppError> {
        // A server-side merge patch: the datastore applies it against the live
        // row at apply time, so a concurrent controller change to metadata is
        // preserved by construction and no field this request does not own is
        // ever rewritten. The pipeline never composes a full body from a
        // snapshot it read earlier, so there is no read-modify-write window a
        // racing write can be lost in. The status commit is ordered after this
        // one, which is what keeps a controller status write from leaking into
        // the API response.
        crate::generic_command::patch_non_pod_resource(
            self.state.command_store().resource_command(),
            &target.api_version,
            &target.kind,
            target.namespace.as_deref(),
            &target.name,
            ResourcePatchRequest::new(PatchKind::Merge, metadata_patch, preconditions),
        )
        .await
    }
}

pub trait StatusMutationResponder: Send + Sync {
    fn build_response(
        &self,
        target: &StatusMutationTarget,
        final_resource: Resource,
    ) -> Result<Value, AppError>;
}

pub struct ResourceStatusResponder {
    identity: Arc<dyn crate::ApiIdentityGenerator>,
    ensure_type_meta: bool,
}

impl ResourceStatusResponder {
    pub fn new(identity: Arc<dyn crate::ApiIdentityGenerator>, ensure_type_meta: bool) -> Self {
        Self {
            identity,
            ensure_type_meta,
        }
    }
}

impl StatusMutationResponder for ResourceStatusResponder {
    fn build_response(
        &self,
        target: &StatusMutationTarget,
        final_resource: Resource,
    ) -> Result<Value, AppError> {
        let data = if self.ensure_type_meta {
            super::helpers::ensure_type_meta(
                final_resource.data.clone(),
                &target.api_version,
                &target.kind,
            )
        } else {
            std::sync::Arc::unwrap_or_clone(final_resource.data)
        };
        Ok(inject_resource_version(
            self.identity.as_ref(),
            data,
            final_resource.resource_version,
        ))
    }
}

pub struct StatusMutationPipeline<W, M, P, R>
where
    W: StatusMutationWriter,
    M: StatusMutationMergePolicy,
    P: StatusMutationPrecondition,
    R: StatusMutationResponder,
{
    writer: W,
    merge_policy: M,
    precondition: P,
    responder: R,
}

pub struct StatusMutationResult {
    pub final_resource: Resource,
    pub response: Value,
}

impl<W, M, P, R> StatusMutationPipeline<W, M, P, R>
where
    W: StatusMutationWriter,
    M: StatusMutationMergePolicy,
    P: StatusMutationPrecondition,
    R: StatusMutationResponder,
{
    pub fn new(writer: W, merge_policy: M, precondition: P, responder: R) -> Self {
        Self {
            writer,
            merge_policy,
            precondition,
            responder,
        }
    }

    pub async fn execute<O>(
        &self,
        target: &StatusMutationTarget,
        operation: &O,
    ) -> Result<StatusMutationResult, AppError>
    where
        O: StatusMutationOperation,
    {
        let current = self
            .writer
            .get(target)
            .await?
            .ok_or_else(|| AppError::NotFound(target.not_found_message()))?;
        let working = operation.working_document(&current, &self.writer)?;
        let expected_rv = self
            .precondition
            .expected_resource_version(operation.precondition_document());

        let status_value = operation.status_value(&working);
        let metadata_patch = operation.metadata_patch(&current, &working);

        // A request carrying both status and metadata commits twice, because
        // status and metadata are two distinct native commands. Both commands
        // are partial and applied server-side against the live row, so neither
        // can clobber a concurrent controller change to a field this request
        // does not own. Metadata commits first and status commits last, which
        // makes the response the request's own status commit: a controller
        // status write landing between the two commits cannot leak into it
        // (the CronJob conformance lastScheduleTime race).
        let mut committed_resource = None;
        let mut status_expected_rv = expected_rv;
        if let Some(metadata_patch) = metadata_patch {
            let metadata_resource = self
                .writer
                .patch_metadata(
                    target,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        // The metadata commit is the request's first commit
                        // whenever a status commit follows, so it carries the
                        // resourceVersion precondition the status commit used
                        // to enforce. Metadata-only requests keep their
                        // previous unconditional behavior.
                        resource_version: if status_value.is_some() {
                            expected_rv
                        } else {
                            None
                        },
                    },
                )
                .await?;
            // This request just advanced the row's resourceVersion, so the
            // status commit must compare against what this request produced
            // rather than the client's value, which is now legitimately stale.
            status_expected_rv = expected_rv.map(|_| metadata_resource.resource_version);
            committed_resource = Some(metadata_resource);
        }

        if let Some(mut status) = status_value {
            self.merge_policy
                .merge_status(target, &current, &mut status);
            // Return the exact object committed by the last mutation. A later
            // controller write may race after this command, but rereading
            // would return a different object than this request committed,
            // and splicing bodies from separate commits would fabricate a
            // resourceVersion/body pair that never existed.
            committed_resource = Some(
                self.writer
                    .write_status(
                        target,
                        status,
                        ResourcePreconditions {
                            uid: Some(current.uid.clone()),
                            resource_version: status_expected_rv,
                        },
                    )
                    .await?,
            );
        }

        let final_resource = committed_resource.unwrap_or(current);
        let response = self
            .responder
            .build_response(target, final_resource.clone())?;
        Ok(StatusMutationResult {
            final_resource,
            response,
        })
    }
}

#[derive(Clone, Copy)]
pub enum ScaleSelectorStyle {
    MatchLabels,
    FlatSelector,
}

#[derive(Clone, Debug)]
pub struct ScaleMutationTarget {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

impl ScaleMutationTarget {
    pub fn namespaced(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    fn not_found_message(&self) -> String {
        format!("{} {} not found", self.kind.to_lowercase(), self.name)
    }
}

pub trait ScaleMutationOperation: Send + Sync {
    fn desired_replicas(
        &self,
        current_scale: &Value,
        patcher: &dyn PatchApplication,
    ) -> Result<i32, AppError>;
    fn expected_resource_version(&self) -> Result<Option<i64>, AppError>;
    fn strict_resource_version(&self) -> bool;
}

pub struct ScalePutOperation {
    body: Value,
}

impl ScalePutOperation {
    pub fn new(body: Value) -> Self {
        Self { body }
    }
}

impl ScaleMutationOperation for ScalePutOperation {
    fn desired_replicas(
        &self,
        _current_scale: &Value,
        _patcher: &dyn PatchApplication,
    ) -> Result<i32, AppError> {
        extract_scale_replicas(&self.body)
    }

    fn expected_resource_version(&self) -> Result<Option<i64>, AppError> {
        extract_scale_resource_version(&self.body)
    }

    fn strict_resource_version(&self) -> bool {
        true
    }
}

pub struct ScalePatchOperation {
    patch: Value,
    content_type: Option<String>,
}

impl ScalePatchOperation {
    pub fn new(patch: Value, content_type: Option<String>) -> Self {
        Self {
            patch,
            content_type,
        }
    }
}

impl ScaleMutationOperation for ScalePatchOperation {
    fn desired_replicas(
        &self,
        current_scale: &Value,
        patcher: &dyn PatchApplication,
    ) -> Result<i32, AppError> {
        let patched =
            patcher.apply_patch(current_scale, &self.patch, self.content_type.as_deref())?;
        extract_scale_replicas(&patched)
    }

    fn expected_resource_version(&self) -> Result<Option<i64>, AppError> {
        Ok(None)
    }

    fn strict_resource_version(&self) -> bool {
        false
    }
}

#[async_trait]
pub trait ScaleMutationWriter: PatchApplication + Send + Sync {
    async fn get(&self, target: &ScaleMutationTarget) -> Result<Option<Resource>>;
    async fn write_replicas(
        &self,
        target: &ScaleMutationTarget,
        replicas: i32,
        preconditions: ResourcePreconditions,
        strict_resource_version: bool,
    ) -> Result<Option<Resource>>;
    async fn enqueue_reconcile(&self, resource: &Resource);
}

pub struct DatastoreScaleMutationWriter<S> {
    state: Arc<S>,
}

impl<S> DatastoreScaleMutationWriter<S> {
    pub fn new(state: Arc<S>) -> Self {
        Self { state }
    }
}

impl<S: GenericCommandState> PatchApplication for DatastoreScaleMutationWriter<S> {
    fn apply_patch(
        &self,
        current: &Value,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<Value, AppError> {
        self.state
            .command_policy()
            .apply_patch(current, patch, content_type)
    }
}

#[async_trait]
impl<S: GenericCommandState> ScaleMutationWriter for DatastoreScaleMutationWriter<S> {
    async fn get(&self, target: &ScaleMutationTarget) -> Result<Option<Resource>> {
        crate::generic_read::get_resource(
            self.state.command_store().resource_query(),
            &target.api_version,
            &target.kind,
            Some(&target.namespace),
            &target.name,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    async fn write_replicas(
        &self,
        target: &ScaleMutationTarget,
        replicas: i32,
        preconditions: ResourcePreconditions,
        strict_resource_version: bool,
    ) -> Result<Option<Resource>> {
        let request = ResourcePatchRequest::new(
            PatchKind::Merge,
            serde_json::json!({"spec": {"replicas": replicas}}),
            preconditions,
        );
        let request = if strict_resource_version {
            request.with_strict_resource_version()
        } else {
            request
        };
        crate::generic_command::patch_non_pod_resource(
            self.state.command_store().resource_command(),
            &target.api_version,
            &target.kind,
            Some(&target.namespace),
            &target.name,
            request,
        )
        .await
        .map(Some)
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    async fn enqueue_reconcile(&self, resource: &Resource) {
        self.state
            .command_reconcile()
            .controller_dispatcher()
            .enqueue(&resource.data)
            .await;
    }
}

pub trait ScaleMutationResponder: Send + Sync {
    fn current_scale(
        &self,
        target: &ScaleMutationTarget,
        current: &Resource,
    ) -> Result<Value, AppError>;
    fn response(
        &self,
        target: &ScaleMutationTarget,
        updated: &Resource,
        replicas: i32,
    ) -> Result<Value, AppError>;
}

pub struct JsonScaleMutationResponder {
    selector_style: ScaleSelectorStyle,
}

impl JsonScaleMutationResponder {
    pub fn new(selector_style: ScaleSelectorStyle) -> Self {
        Self { selector_style }
    }

    fn selector_string(&self, resource: &Resource) -> String {
        match self.selector_style {
            ScaleSelectorStyle::MatchLabels => selector_string_from_match_labels(resource),
            ScaleSelectorStyle::FlatSelector => selector_string_from_flat_selector(resource),
        }
    }
}

impl ScaleMutationResponder for JsonScaleMutationResponder {
    fn current_scale(
        &self,
        target: &ScaleMutationTarget,
        current: &Resource,
    ) -> Result<Value, AppError> {
        let replicas = current
            .data
            .pointer("/spec/replicas")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        let status_replicas = current
            .data
            .pointer("/status/replicas")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        Ok(build_scale_response(
            &target.name,
            &target.namespace,
            current.resource_version,
            replicas,
            status_replicas,
            self.selector_string(current),
        ))
    }

    fn response(
        &self,
        target: &ScaleMutationTarget,
        updated: &Resource,
        replicas: i32,
    ) -> Result<Value, AppError> {
        let status_replicas = updated
            .data
            .pointer("/status/replicas")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        Ok(build_scale_response(
            &target.name,
            &target.namespace,
            updated.resource_version,
            replicas as i64,
            status_replicas,
            self.selector_string(updated),
        ))
    }
}

pub struct ScaleMutationPipeline<W, R>
where
    W: ScaleMutationWriter,
    R: ScaleMutationResponder,
{
    writer: W,
    responder: R,
}

impl<W, R> ScaleMutationPipeline<W, R>
where
    W: ScaleMutationWriter,
    R: ScaleMutationResponder,
{
    pub fn new(writer: W, responder: R) -> Self {
        Self { writer, responder }
    }

    pub async fn execute<O>(
        &self,
        target: &ScaleMutationTarget,
        operation: &O,
    ) -> Result<Json<Value>, AppError>
    where
        O: ScaleMutationOperation,
    {
        let current = self
            .writer
            .get(target)
            .await?
            .ok_or_else(|| AppError::NotFound(target.not_found_message()))?;
        let current_scale = self.responder.current_scale(target, &current)?;
        let replicas = operation.desired_replicas(&current_scale, &self.writer)?;
        let expected_resource_version = operation.expected_resource_version()?;
        let updated = self
            .writer
            .write_replicas(
                target,
                replicas,
                ResourcePreconditions {
                    uid: Some(current.uid),
                    resource_version: expected_resource_version,
                },
                operation.strict_resource_version(),
            )
            .await?
            .ok_or_else(|| AppError::NotFound(target.not_found_message()))?;
        self.writer.enqueue_reconcile(&updated).await;
        Ok(Json(self.responder.response(target, &updated, replicas)?))
    }
}

#[derive(Clone, Debug)]
pub struct NamespaceStatusMutationTarget {
    pub name: String,
}

impl NamespaceStatusMutationTarget {
    fn not_found_message(&self) -> String {
        format!("Namespace {} not found", self.name)
    }
}

pub trait NamespaceStatusMutationOperation: Send + Sync {
    fn operation_name(&self) -> &'static str;
    fn working_document(
        &self,
        current: &Resource,
        patcher: &dyn PatchApplication,
    ) -> Result<Value, AppError>;
    fn precondition_document(&self) -> &Value;
}

impl<T> NamespaceStatusMutationOperation for T
where
    T: StatusMutationOperation,
{
    fn operation_name(&self) -> &'static str {
        StatusMutationOperation::operation_name(self)
    }

    fn working_document(
        &self,
        current: &Resource,
        patcher: &dyn PatchApplication,
    ) -> Result<Value, AppError> {
        StatusMutationOperation::working_document(self, current, patcher)
    }

    fn precondition_document(&self) -> &Value {
        StatusMutationOperation::precondition_document(self)
    }
}

pub trait NamespaceStatusMutationPrecondition: Send + Sync {
    fn expected_resource_version(&self, current: &Resource, request_document: &Value) -> i64;
}

pub struct CurrentNamespaceResourceVersionPrecondition;

impl NamespaceStatusMutationPrecondition for CurrentNamespaceResourceVersionPrecondition {
    fn expected_resource_version(&self, current: &Resource, _request_document: &Value) -> i64 {
        current.resource_version
    }
}

pub trait NamespaceStatusMutationMergePolicy: Send + Sync {
    fn merge<O>(
        &self,
        target: &NamespaceStatusMutationTarget,
        current: &Resource,
        operation: &O,
        working_document: Value,
    ) -> Result<Value, AppError>
    where
        O: NamespaceStatusMutationOperation;
}

pub struct NamespaceStatusMergePolicy;

pub(super) fn ensure_namespace_status_phase_active(data: &mut Value) {
    let Some(object) = data.as_object_mut() else {
        return;
    };
    let status = object
        .entry("status".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !status.is_object() {
        *status = serde_json::json!({});
    }
    if let Some(status) = status.as_object_mut()
        && status.get("phase").is_none_or(|phase| {
            phase.is_null() || phase.as_str().is_some_and(|phase| phase.trim().is_empty())
        })
    {
        status.insert("phase".to_string(), Value::String("Active".to_string()));
    }
}

impl NamespaceStatusMutationMergePolicy for NamespaceStatusMergePolicy {
    fn merge<O>(
        &self,
        _target: &NamespaceStatusMutationTarget,
        current: &Resource,
        operation: &O,
        working_document: Value,
    ) -> Result<Value, AppError>
    where
        O: NamespaceStatusMutationOperation,
    {
        let mut merged = if matches!(operation.operation_name(), "update") {
            let mut resource_data: Value = std::sync::Arc::unwrap_or_clone(current.data.clone());
            if let Some(new_status) = working_document.get("status")
                && let Some(obj) = resource_data.as_object_mut()
            {
                obj.insert("status".to_string(), new_status.clone());
            }
            resource_data
        } else {
            working_document
        };
        ensure_namespace_status_phase_active(&mut merged);
        Ok(merged)
    }
}

#[async_trait]
pub trait NamespaceStatusMutationWriter: PatchApplication + Send + Sync {
    async fn get(&self, target: &NamespaceStatusMutationTarget) -> Result<Option<Resource>>;
    async fn update(
        &self,
        target: &NamespaceStatusMutationTarget,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource>;
}

pub struct DatastoreNamespaceStatusMutationWriter<S> {
    state: Arc<S>,
}

impl<S> DatastoreNamespaceStatusMutationWriter<S> {
    pub fn new(state: Arc<S>) -> Self {
        Self { state }
    }
}

impl<S: GenericCommandState> PatchApplication for DatastoreNamespaceStatusMutationWriter<S> {
    fn apply_patch(
        &self,
        current: &Value,
        patch: &Value,
        content_type: Option<&str>,
    ) -> Result<Value, AppError> {
        self.state
            .command_policy()
            .apply_patch(current, patch, content_type)
    }
}

#[async_trait]
impl<S: GenericCommandState> NamespaceStatusMutationWriter
    for DatastoreNamespaceStatusMutationWriter<S>
{
    async fn get(&self, target: &NamespaceStatusMutationTarget) -> Result<Option<Resource>> {
        crate::generic_read::get_resource(
            self.state.command_store().resource_query(),
            "v1",
            "Namespace",
            None,
            &target.name,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    async fn update(
        &self,
        target: &NamespaceStatusMutationTarget,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        crate::generic_command::update_namespace(
            self.state.command_store().resource_command(),
            &target.name,
            body,
            expected_rv,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }
}

pub trait NamespaceStatusMutationResponder: Send + Sync {
    fn response(
        &self,
        target: &NamespaceStatusMutationTarget,
        updated: Resource,
    ) -> Result<Value, AppError>;
}

pub struct NamespaceStatusResponder {
    identity: Arc<dyn crate::ApiIdentityGenerator>,
}

impl NamespaceStatusResponder {
    pub fn new(identity: Arc<dyn crate::ApiIdentityGenerator>) -> Self {
        Self { identity }
    }
}

impl NamespaceStatusMutationResponder for NamespaceStatusResponder {
    fn response(
        &self,
        _target: &NamespaceStatusMutationTarget,
        updated: Resource,
    ) -> Result<Value, AppError> {
        Ok(inject_resource_version(
            self.identity.as_ref(),
            updated.data,
            updated.resource_version,
        ))
    }
}

pub struct NamespaceStatusMutationPipeline<W, M, P, R>
where
    W: NamespaceStatusMutationWriter,
    M: NamespaceStatusMutationMergePolicy,
    P: NamespaceStatusMutationPrecondition,
    R: NamespaceStatusMutationResponder,
{
    writer: W,
    merge_policy: M,
    precondition: P,
    responder: R,
}

impl<W, M, P, R> NamespaceStatusMutationPipeline<W, M, P, R>
where
    W: NamespaceStatusMutationWriter,
    M: NamespaceStatusMutationMergePolicy,
    P: NamespaceStatusMutationPrecondition,
    R: NamespaceStatusMutationResponder,
{
    pub fn new(writer: W, merge_policy: M, precondition: P, responder: R) -> Self {
        Self {
            writer,
            merge_policy,
            precondition,
            responder,
        }
    }

    pub async fn execute<O>(
        &self,
        target: &NamespaceStatusMutationTarget,
        operation: &O,
    ) -> Result<Json<Value>, AppError>
    where
        O: NamespaceStatusMutationOperation,
    {
        let current = self
            .writer
            .get(target)
            .await?
            .ok_or_else(|| AppError::NotFound(target.not_found_message()))?;
        let working = operation.working_document(&current, &self.writer)?;
        let expected_rv = self
            .precondition
            .expected_resource_version(&current, operation.precondition_document());
        let merged = self
            .merge_policy
            .merge(target, &current, operation, working)?;
        let updated = self.writer.update(target, merged, expected_rv).await?;
        Ok(Json(self.responder.response(target, updated)?))
    }
}

fn build_status_metadata_patch(body_meta: Option<&Value>) -> Option<Value> {
    let body_meta = body_meta?.as_object()?;
    let mut patch_meta = serde_json::Map::new();
    if let Some(annotations) = body_meta.get("annotations") {
        patch_meta.insert("annotations".to_string(), annotations.clone());
    }
    if let Some(labels) = body_meta.get("labels") {
        patch_meta.insert("labels".to_string(), labels.clone());
    }
    if patch_meta.is_empty() {
        None
    } else {
        Some(serde_json::json!({"metadata": Value::Object(patch_meta)}))
    }
}

fn build_status_metadata_patch_from_diff(
    before: Option<&Value>,
    after: Option<&Value>,
) -> Option<Value> {
    let after_obj = after?.as_object()?;
    let mut patch_meta = serde_json::Map::new();
    let before_annotations = before.and_then(|m| m.get("annotations"));
    let after_annotations = after_obj.get("annotations");
    if after_annotations != before_annotations
        && let Some(v) = after_annotations
    {
        patch_meta.insert("annotations".to_string(), v.clone());
    }
    let before_labels = before.and_then(|m| m.get("labels"));
    let after_labels = after_obj.get("labels");
    if after_labels != before_labels
        && let Some(v) = after_labels
    {
        patch_meta.insert("labels".to_string(), v.clone());
    }
    if patch_meta.is_empty() {
        None
    } else {
        Some(serde_json::json!({"metadata": Value::Object(patch_meta)}))
    }
}

pub fn extract_scale_replicas(body: &Value) -> Result<i32, AppError> {
    let replicas_value = body
        .pointer("/spec/replicas")
        .ok_or_else(|| AppError::BadRequest("spec.replicas is required".to_string()))?;
    let as_i64 = replicas_value
        .as_i64()
        .ok_or_else(|| AppError::BadRequest("spec.replicas must be an integer".to_string()))?;
    i32::try_from(as_i64)
        .map_err(|_| AppError::BadRequest("spec.replicas must fit in a 32-bit integer".to_string()))
}

pub fn extract_scale_resource_version(body: &Value) -> Result<Option<i64>, AppError> {
    let Some(resource_version) = body
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
    else {
        return Ok(None);
    };
    if resource_version.is_empty() {
        return Ok(None);
    }
    resource_version.parse::<i64>().map(Some).map_err(|_| {
        AppError::BadRequest("metadata.resourceVersion must be an integer string".to_string())
    })
}

pub fn build_scale_response(
    name: &str,
    namespace: &str,
    resource_version: i64,
    replicas: i64,
    status_replicas: i64,
    selector_str: String,
) -> Value {
    let scale = k8s_openapi::api::autoscaling::v1::Scale {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            resource_version: Some(resource_version.to_string()),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::autoscaling::v1::ScaleSpec {
            replicas: Some(replicas as i32),
        }),
        status: Some(k8s_openapi::api::autoscaling::v1::ScaleStatus {
            replicas: status_replicas as i32,
            selector: if selector_str.is_empty() {
                None
            } else {
                Some(selector_str)
            },
        }),
    };
    serde_json::to_value(scale).unwrap_or_default()
}

fn selector_string_from_match_labels(resource: &Resource) -> String {
    resource
        .data
        .pointer("/spec/selector")
        .and_then(|selector| selector.pointer("/matchLabels"))
        .and_then(|match_labels| match_labels.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(key, value)| format!("{}={}", key, value.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn selector_string_from_flat_selector(resource: &Resource) -> String {
    resource
        .data
        .pointer("/spec/selector")
        .and_then(|selector| selector.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(key, value)| format!("{}={}", key, value.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct EchoStatusResponder;

    impl StatusMutationResponder for EchoStatusResponder {
        fn build_response(
            &self,
            _target: &StatusMutationTarget,
            final_resource: Resource,
        ) -> Result<Value, AppError> {
            Ok((*final_resource.data).clone())
        }
    }

    struct ControllerRaceStatusWriter {
        current: Arc<Mutex<Resource>>,
        get_count: Arc<Mutex<usize>>,
    }

    impl ControllerRaceStatusWriter {
        fn new() -> Self {
            let resource = Resource::try_from_data(Arc::new(serde_json::json!({
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": {
                    "name": "cron",
                    "namespace": "default",
                    "uid": "cron-uid",
                    "resourceVersion": "1"
                },
                "spec": {"schedule": "* * * * *"},
                "status": {}
            })))
            .expect("valid CronJob fixture");
            Self {
                current: Arc::new(Mutex::new(resource)),
                get_count: Arc::new(Mutex::new(0)),
            }
        }

        fn resource_from(data: Value, resource_version: i64) -> Resource {
            let mut resource = Resource::try_from_data(Arc::new(data)).expect("valid resource");
            resource.resource_version = resource_version;
            resource
        }
    }

    impl PatchApplication for ControllerRaceStatusWriter {
        fn apply_patch(
            &self,
            current: &Value,
            _patch: &Value,
            _content_type: Option<&str>,
        ) -> Result<Value, AppError> {
            Ok(current.clone())
        }
    }

    #[async_trait]
    impl StatusMutationWriter for ControllerRaceStatusWriter {
        async fn get(&self, _target: &StatusMutationTarget) -> Result<Option<Resource>, AppError> {
            let mut get_count = self.get_count.lock().unwrap();
            *get_count += 1;
            if *get_count == 2 {
                // The obsolete final reread observes a controller write that
                // follows both API mutations. The fixed pipeline never makes
                // this second read, so it returns the exact prior commit.
                let mut controller = (*self.current.lock().unwrap().data).clone();
                controller["status"]["lastScheduleTime"] = serde_json::json!("controller");
                controller["metadata"]["resourceVersion"] = serde_json::json!("4");
                *self.current.lock().unwrap() = Self::resource_from(controller, 4);
            }
            Ok(Some(self.current.lock().unwrap().clone()))
        }

        async fn write_status(
            &self,
            _target: &StatusMutationTarget,
            status: Value,
            _preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            // The status commit runs last, so it lands on RV 3.
            let mut committed = (*self.current.lock().unwrap().data).clone();
            committed["status"] = status;
            committed["metadata"]["resourceVersion"] = serde_json::json!("3");
            let committed_resource = Self::resource_from(committed.clone(), 3);
            *self.current.lock().unwrap() = committed_resource.clone();
            Ok(committed_resource)
        }

        async fn patch_metadata(
            &self,
            _target: &StatusMutationTarget,
            metadata_patch: Value,
            _preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            // The metadata commit runs first, so it lands on RV 2.
            let mut data = (*self.current.lock().unwrap().data).clone();
            if let Some(annotations) = metadata_patch.pointer("/metadata/annotations") {
                data["metadata"]["annotations"] = annotations.clone();
            }
            data["metadata"]["resourceVersion"] = serde_json::json!("2");
            let resource = Self::resource_from(data, 2);
            *self.current.lock().unwrap() = resource.clone();
            Ok(resource)
        }
    }

    #[tokio::test]
    async fn status_put_response_preserves_requested_status_when_controller_races() {
        let writer = ControllerRaceStatusWriter::new();
        let get_count = writer.get_count.clone();
        let pipeline = StatusMutationPipeline::new(
            writer,
            ApiSubresourceStatusMergePolicy::new(None),
            LenientStatusResourceVersionPrecondition,
            EchoStatusResponder,
        );
        let target = StatusMutationTarget::namespaced("batch/v1", "CronJob", "default", "cron");
        let requested = "2026-08-17T13:27:59Z";
        let result = pipeline
            .execute(
                &target,
                &StatusPutOperation::new(serde_json::json!({
                    "metadata": {"annotations": {"patchedstatus": "true"}},
                    "status": {"lastScheduleTime": requested}
                })),
            )
            .await
            .expect("status update succeeds");

        assert_eq!(
            result.response["status"]["lastScheduleTime"], requested,
            "the API response must represent its committed status, not a later controller write"
        );
        assert_eq!(
            result.response["metadata"]["annotations"]["patchedstatus"],
            "true"
        );
        assert_eq!(
            result.response["metadata"]["resourceVersion"], "3",
            "the response must retain the resourceVersion of the exact status commit"
        );
        assert_eq!(
            *result.final_resource.data, result.response,
            "the response body must be the exact committed resource body"
        );
        assert_eq!(
            *get_count.lock().unwrap(),
            1,
            "the response must not reread after the committed mutation"
        );
    }

    /// Simulates the datastore's separate-commit behavior for a PUT /status
    /// that carries both status and metadata: the metadata commit applies
    /// (RV 2), then a controller write lands (RV 4, controller status), and
    /// only then does the status commit apply against the live row (RV 5).
    struct TwoCommitRaceWriter {
        current: Arc<Mutex<Resource>>,
    }

    impl TwoCommitRaceWriter {
        fn new() -> Self {
            let resource = Resource::try_from_data(Arc::new(serde_json::json!({
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": {
                    "name": "cron",
                    "namespace": "default",
                    "uid": "cron-uid",
                    "resourceVersion": "1"
                },
                "spec": {"schedule": "* * * * *"},
                "status": {}
            })))
            .expect("valid CronJob fixture");
            Self {
                current: Arc::new(Mutex::new(resource)),
            }
        }

        fn resource_from(data: Value, resource_version: i64) -> Resource {
            let mut resource = Resource::try_from_data(Arc::new(data)).expect("valid resource");
            resource.resource_version = resource_version;
            resource
        }

        fn controller_write(&self) {
            // The CronJob controller fires between the request's two commits.
            let mut data = (*self.current.lock().unwrap().data).clone();
            data["status"]["lastScheduleTime"] = serde_json::json!("2026-08-18T10:13:00Z");
            data["metadata"]["resourceVersion"] = serde_json::json!("4");
            *self.current.lock().unwrap() = Self::resource_from(data, 4);
        }
    }

    impl PatchApplication for TwoCommitRaceWriter {
        fn apply_patch(
            &self,
            current: &Value,
            _patch: &Value,
            _content_type: Option<&str>,
        ) -> Result<Value, AppError> {
            Ok(current.clone())
        }
    }

    #[async_trait]
    impl StatusMutationWriter for TwoCommitRaceWriter {
        async fn get(&self, _target: &StatusMutationTarget) -> Result<Option<Resource>, AppError> {
            Ok(Some(self.current.lock().unwrap().clone()))
        }

        async fn write_status(
            &self,
            _target: &StatusMutationTarget,
            status: Value,
            _preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            // Models the native status command: applied server-side against
            // the live row, touching only `status`. It runs last, so it lands
            // on RV 5 after the controller's RV 4.
            let mut committed = (*self.current.lock().unwrap().data).clone();
            committed["status"] = status;
            committed["metadata"]["resourceVersion"] = serde_json::json!("5");
            let committed_resource = Self::resource_from(committed.clone(), 5);
            *self.current.lock().unwrap() = committed_resource.clone();
            Ok(committed_resource)
        }

        async fn patch_metadata(
            &self,
            _target: &StatusMutationTarget,
            metadata_patch: Value,
            _preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            // Models the native merge-patch command: applied server-side
            // against the live row, touching only the patched metadata.
            let mut data = (*self.current.lock().unwrap().data).clone();
            if let Some(annotations) = metadata_patch.pointer("/metadata/annotations") {
                data["metadata"]["annotations"] = annotations.clone();
            }
            data["metadata"]["resourceVersion"] = serde_json::json!("2");
            let resource = Self::resource_from(data, 2);
            *self.current.lock().unwrap() = resource.clone();
            // Simulate the controller firing between the request's metadata
            // commit and its status commit: the live row now carries the
            // controller's next-slot status at a newer RV.
            self.controller_write();
            Ok(resource)
        }
    }

    #[tokio::test]
    async fn status_put_with_status_and_metadata_survives_controller_write_between_commits() {
        let writer = TwoCommitRaceWriter::new();
        let pipeline = StatusMutationPipeline::new(
            writer,
            ApiSubresourceStatusMergePolicy::new(None),
            LenientStatusResourceVersionPrecondition,
            EchoStatusResponder,
        );
        let target = StatusMutationTarget::namespaced("batch/v1", "CronJob", "default", "cron");

        // The test's PUT /status carries both a status change and a metadata
        // change. The metadata commit applies (RV 2), then the controller
        // fires and writes 10:13:00 (RV 4), and only then does the status
        // commit apply.
        let result = pipeline
            .execute(
                &target,
                &StatusPutOperation::new(serde_json::json!({
                    "metadata": {"annotations": {"patchedstatus": "true"}},
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
            )
            .await
            .expect("status update succeeds");

        // The API response must be the request's own commit: the status it
        // wrote (10:12:59), never the controller's 10:13:00 that landed on the
        // live row between the two commits.
        assert_eq!(
            result.response["status"]["lastScheduleTime"], "2026-08-18T10:12:59+00:00",
            "the API response must represent its committed status, not a later controller write"
        );
    }

    /// Same race as above, but the controller also changes metadata between
    /// the two commits — after the request's metadata commit has already
    /// landed. Because both commits are partial server-side applies, the
    /// controller's metadata change must survive the request's status commit
    /// while the response still carries the request's own status.
    #[tokio::test]
    async fn status_put_preserves_controller_metadata_changes_across_two_commits() {
        // A variant writer where the controller adds a metadata annotation
        // (e.g., setting a finalizer or updating a label) between the two
        // commits.
        struct ControllerMetadataWriter {
            current: Arc<Mutex<Resource>>,
        }

        impl ControllerMetadataWriter {
            fn new() -> Self {
                let resource = Resource::try_from_data(Arc::new(serde_json::json!({
                    "apiVersion": "batch/v1",
                    "kind": "CronJob",
                    "metadata": {
                        "name": "cron",
                        "namespace": "default",
                        "uid": "cron-uid",
                        "resourceVersion": "1"
                    },
                    "spec": {"schedule": "* * * * *"},
                    "status": {}
                })))
                .expect("valid CronJob fixture");
                Self {
                    current: Arc::new(Mutex::new(resource)),
                }
            }

            fn resource_from(data: Value, resource_version: i64) -> Resource {
                let mut resource = Resource::try_from_data(Arc::new(data)).expect("valid resource");
                resource.resource_version = resource_version;
                resource
            }

            fn controller_write(&self) {
                let mut data = (*self.current.lock().unwrap().data).clone();
                // Controller changes both status and metadata. Its metadata
                // change is a merge (a controller adds its own annotation
                // rather than replacing the map), so the request's annotation
                // committed a moment earlier must still be there afterwards.
                data["status"]["lastScheduleTime"] = serde_json::json!("2026-08-18T10:13:00Z");
                if !data["metadata"]["annotations"].is_object() {
                    data["metadata"]["annotations"] = serde_json::json!({});
                }
                data["metadata"]["annotations"]["controller-added"] = serde_json::json!("yes");
                data["metadata"]["resourceVersion"] = serde_json::json!("4");
                *self.current.lock().unwrap() = Self::resource_from(data, 4);
            }
        }

        impl PatchApplication for ControllerMetadataWriter {
            fn apply_patch(
                &self,
                current: &Value,
                patch: &Value,
                _content_type: Option<&str>,
            ) -> Result<Value, AppError> {
                // Realistic merge-patch: shallow-merge top-level keys, then
                // deep-merge /metadata/annotations.
                let mut out = current.clone();
                if let Some(obj) = patch.as_object() {
                    for (k, v) in obj {
                        if k == "metadata" {
                            if let (Some(out_meta), Some(patch_meta)) =
                                (out.get_mut("metadata"), v.as_object())
                            {
                                for (mk, mv) in patch_meta {
                                    if mk == "annotations" {
                                        let ann = out_meta
                                            .as_object_mut()
                                            .unwrap()
                                            .entry("annotations")
                                            .or_insert_with(|| serde_json::json!({}));
                                        if let (Some(ann_obj), Some(mv_obj)) =
                                            (ann.as_object_mut(), mv.as_object())
                                        {
                                            for (ak, av) in mv_obj {
                                                ann_obj.insert(ak.clone(), av.clone());
                                            }
                                        }
                                    } else {
                                        out_meta[mk] = mv.clone();
                                    }
                                }
                            }
                        } else {
                            out[k] = v.clone();
                        }
                    }
                }
                Ok(out)
            }
        }

        #[async_trait]
        impl StatusMutationWriter for ControllerMetadataWriter {
            async fn get(&self, _target: &StatusMutationTarget) -> Result<Option<Resource>, AppError> {
                Ok(Some(self.current.lock().unwrap().clone()))
            }

            async fn write_status(
                &self,
                _target: &StatusMutationTarget,
                status: Value,
                _preconditions: ResourcePreconditions,
            ) -> Result<Resource, AppError> {
                // Models the native status command: applied server-side
                // against the live row, touching only `status`. It runs last,
                // so it lands on RV 5 after the controller's RV 4 and carries
                // the controller's metadata change forward untouched.
                let mut committed = (*self.current.lock().unwrap().data).clone();
                committed["status"] = status;
                committed["metadata"]["resourceVersion"] = serde_json::json!("5");
                let committed_resource = Self::resource_from(committed.clone(), 5);
                *self.current.lock().unwrap() = committed_resource.clone();
                Ok(committed_resource)
            }

            async fn patch_metadata(
                &self,
                _target: &StatusMutationTarget,
                metadata_patch: Value,
                _preconditions: ResourcePreconditions,
            ) -> Result<Resource, AppError> {
                // Models the native merge-patch command: applied server-side
                // against the live row, touching only the patched metadata.
                let live = (*self.current.lock().unwrap().data).clone();
                let mut full =
                    self.apply_patch(&live, &metadata_patch, Some("application/merge-patch+json"))?;
                full["metadata"]["resourceVersion"] = serde_json::json!("2");
                let resource = Self::resource_from(full, 2);
                *self.current.lock().unwrap() = resource.clone();
                // The controller fires between the request's metadata commit
                // and its status commit.
                self.controller_write();
                Ok(resource)
            }
        }

        let writer = ControllerMetadataWriter::new();
        let pipeline = StatusMutationPipeline::new(
            writer,
            ApiSubresourceStatusMergePolicy::new(None),
            LenientStatusResourceVersionPrecondition,
            EchoStatusResponder,
        );
        let target = StatusMutationTarget::namespaced("batch/v1", "CronJob", "default", "cron");

        let result = pipeline
            .execute(
                &target,
                &StatusPutOperation::new(serde_json::json!({
                    "metadata": {"annotations": {"patchedstatus": "true"}},
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
            )
            .await
            .expect("status update succeeds");

        // Request's own status preserved — controller status not leaked.
        assert_eq!(
            result.response["status"]["lastScheduleTime"], "2026-08-18T10:12:59+00:00",
            "the API response must represent its committed status"
        );
        // Request's annotation preserved.
        assert_eq!(
            result.response["metadata"]["annotations"]["patchedstatus"], "true",
            "the request's metadata patch must be applied"
        );
        // Controller's annotation also preserved (zero data loss).
        assert_eq!(
            result.response["metadata"]["annotations"]["controller-added"], "yes",
            "the controller's metadata change must be preserved, not overwritten"
        );
    }

    fn merge_patch_into(target: &mut Value, patch: &Value) {
        let (Some(target_obj), Some(patch_obj)) = (target.as_object_mut(), patch.as_object())
        else {
            *target = patch.clone();
            return;
        };
        for (key, value) in patch_obj {
            match target_obj.get_mut(key) {
                Some(existing) if existing.is_object() && value.is_object() => {
                    merge_patch_into(existing, value)
                }
                _ => {
                    target_obj.insert(key.clone(), value.clone());
                }
            }
        }
    }

    fn cronjob_fixture(with_status: bool) -> Value {
        let mut fixture = serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "cron",
                "namespace": "default",
                "uid": "cron-uid",
                "resourceVersion": "1"
            },
            "spec": {"schedule": "* * * * *"}
        });
        if with_status {
            fixture["status"] = serde_json::json!({});
        }
        fixture
    }

    /// One native command a request issued, as its name paired with the
    /// resourceVersion precondition it carried.
    type CommitRecord = (&'static str, Option<i64>);
    type CommitLog = Arc<Mutex<Vec<CommitRecord>>>;

    /// Records the order and the preconditions of the native commands the
    /// pipeline issues, applying each one server-side against the live row
    /// the way the datastore's partial commands do.
    struct OrderRecordingWriter {
        current: Arc<Mutex<Resource>>,
        commits: CommitLog,
    }

    impl OrderRecordingWriter {
        fn with_fixture(fixture: Value) -> Self {
            Self {
                current: Arc::new(Mutex::new(
                    Resource::try_from_data(Arc::new(fixture)).expect("valid CronJob fixture"),
                )),
                commits: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Applies a mutation server-side against the live row and advances
        /// the resourceVersion, the way a native command does.
        fn commit(&self, mutate: impl FnOnce(&mut Value)) -> Resource {
            let mut guard = self.current.lock().unwrap();
            let next_rv = guard.resource_version + 1;
            let mut data = (*guard.data).clone();
            mutate(&mut data);
            data["metadata"]["resourceVersion"] = serde_json::json!(next_rv.to_string());
            let mut resource = Resource::try_from_data(Arc::new(data)).expect("valid resource");
            resource.resource_version = next_rv;
            *guard = resource.clone();
            resource
        }
    }

    impl PatchApplication for OrderRecordingWriter {
        fn apply_patch(
            &self,
            current: &Value,
            patch: &Value,
            _content_type: Option<&str>,
        ) -> Result<Value, AppError> {
            let mut merged = current.clone();
            merge_patch_into(&mut merged, patch);
            Ok(merged)
        }
    }

    #[async_trait]
    impl StatusMutationWriter for OrderRecordingWriter {
        async fn get(&self, _target: &StatusMutationTarget) -> Result<Option<Resource>, AppError> {
            Ok(Some(self.current.lock().unwrap().clone()))
        }

        async fn write_status(
            &self,
            _target: &StatusMutationTarget,
            status: Value,
            preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            self.commits
                .lock()
                .unwrap()
                .push(("status", preconditions.resource_version));
            Ok(self.commit(|data| data["status"] = status))
        }

        async fn patch_metadata(
            &self,
            _target: &StatusMutationTarget,
            metadata_patch: Value,
            preconditions: ResourcePreconditions,
        ) -> Result<Resource, AppError> {
            self.commits
                .lock()
                .unwrap()
                .push(("metadata", preconditions.resource_version));
            Ok(self.commit(|data| {
                if let Some(metadata) = metadata_patch.get("metadata") {
                    merge_patch_into(&mut data["metadata"], metadata);
                }
            }))
        }
    }

    enum StatusRequest {
        Put(Value),
        Patch(Value),
        PatchStatusOnly(Value),
    }

    async fn run_status_request(
        fixture: Value,
        request: StatusRequest,
    ) -> (Vec<CommitRecord>, StatusMutationResult) {
        let writer = OrderRecordingWriter::with_fixture(fixture);
        let commits = writer.commits.clone();
        let pipeline = StatusMutationPipeline::new(
            writer,
            ApiSubresourceStatusMergePolicy::new(None),
            LenientStatusResourceVersionPrecondition,
            EchoStatusResponder,
        );
        let target = StatusMutationTarget::namespaced("batch/v1", "CronJob", "default", "cron");
        let merge = || Some("application/merge-patch+json".to_string());
        let result = match request {
            StatusRequest::Put(body) => {
                pipeline
                    .execute(&target, &StatusPutOperation::new(body))
                    .await
            }
            StatusRequest::Patch(patch) => {
                pipeline
                    .execute(&target, &StatusPatchOperation::new(patch, merge()))
                    .await
            }
            StatusRequest::PatchStatusOnly(patch) => {
                pipeline
                    .execute(&target, &StatusPatchOperation::status_only(patch, merge()))
                    .await
            }
        }
        .expect("status mutation succeeds");
        let commits = commits.lock().unwrap().clone();
        (commits, result)
    }

    /// Every shape of status request, and the exact sequence of native
    /// commands each one must issue. This covers both sides of the two
    /// branches the commit reordering introduced: whether the metadata commit
    /// inherits the client's resourceVersion precondition (only when a status
    /// commit follows it), and whether the status commit chains onto the
    /// resourceVersion the metadata commit produced (only when there was one,
    /// and only when the client supplied a resourceVersion at all).
    #[tokio::test]
    async fn status_mutation_commit_order_and_precondition_matrix() {
        struct Case {
            name: &'static str,
            fixture: Value,
            request: StatusRequest,
            expected_commits: Vec<CommitRecord>,
        }

        let cases = vec![
            Case {
                name: "put with status and metadata, client resourceVersion: \
                       metadata commits first carrying it, status chains onto the RV it produced",
                fixture: cronjob_fixture(true),
                request: StatusRequest::Put(serde_json::json!({
                    "metadata": {
                        "resourceVersion": "1",
                        "annotations": {"patchedstatus": "true"}
                    },
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
                expected_commits: vec![("metadata", Some(1)), ("status", Some(2))],
            },
            Case {
                name: "put with status and metadata, no client resourceVersion: \
                       both commits stay lenient rather than inventing a precondition",
                fixture: cronjob_fixture(true),
                request: StatusRequest::Put(serde_json::json!({
                    "metadata": {"annotations": {"patchedstatus": "true"}},
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
                expected_commits: vec![("metadata", None), ("status", None)],
            },
            Case {
                name: "put with status only: no metadata commit, so the status commit \
                       keeps the client's resourceVersion with nothing to chain onto",
                fixture: cronjob_fixture(true),
                request: StatusRequest::Put(serde_json::json!({
                    "metadata": {"resourceVersion": "1"},
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
                expected_commits: vec![("status", Some(1))],
            },
            Case {
                name: "patch with metadata only: single commit that keeps its previous \
                       unconditional behavior instead of gaining a new precondition",
                fixture: cronjob_fixture(false),
                request: StatusRequest::Patch(serde_json::json!({
                    "metadata": {
                        "resourceVersion": "1",
                        "annotations": {"patchedstatus": "true"}
                    }
                })),
                expected_commits: vec![("metadata", None)],
            },
            Case {
                name: "patch routed status-only: metadata in the body is not committed, \
                       so the status commit keeps the client's resourceVersion",
                fixture: cronjob_fixture(true),
                request: StatusRequest::PatchStatusOnly(serde_json::json!({
                    "metadata": {
                        "resourceVersion": "1",
                        "annotations": {"ignored": "true"}
                    },
                    "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
                })),
                expected_commits: vec![("status", Some(1))],
            },
        ];

        for case in cases {
            let (commits, _) = run_status_request(case.fixture, case.request).await;
            assert_eq!(commits, case.expected_commits, "case: {}", case.name);
        }
    }

    /// Locks in the commit order and the resourceVersion precondition chain
    /// for a status request that carries both status and metadata.
    ///
    /// Ordering metadata first and status last is what makes the response the
    /// request's own status commit without the pipeline ever composing a full
    /// body from a snapshot it read earlier. That composition was the only
    /// place a concurrent write could be silently lost, because it was a
    /// client-side read-modify-write with no resourceVersion compare-and-swap
    /// closing the window between the read and the write.
    ///
    /// The chain matters too: the metadata commit inherits the client's
    /// resourceVersion precondition (it is now the request's first commit),
    /// and the status commit compares against the resourceVersion this
    /// request just produced rather than the client's now-stale value, so
    /// reordering cannot turn valid requests into spurious conflicts.
    #[tokio::test]
    async fn status_put_commits_metadata_before_status_and_chains_preconditions() {
        let (commits, result) = run_status_request(
            cronjob_fixture(true),
            StatusRequest::Put(serde_json::json!({
                "metadata": {
                    "resourceVersion": "1",
                    "annotations": {"patchedstatus": "true"}
                },
                "status": {"lastScheduleTime": "2026-08-18T10:12:59+00:00"}
            })),
        )
        .await;

        assert_eq!(
            commits,
            vec![("metadata", Some(1)), ("status", Some(2))],
            "metadata must commit first carrying the client's resourceVersion, \
             then status must commit against the resourceVersion this request produced"
        );
        assert_eq!(
            result.response["metadata"]["resourceVersion"], "3",
            "the response must be the status commit, which is the request's last commit"
        );
        assert_eq!(
            result.response["status"]["lastScheduleTime"],
            "2026-08-18T10:12:59+00:00"
        );
        assert_eq!(
            result.response["metadata"]["annotations"]["patchedstatus"], "true",
            "the metadata commit must still be visible in the status commit"
        );
        assert_eq!(
            *result.final_resource.data, result.response,
            "the response body must be the exact committed resource body"
        );
    }

    #[test]
    fn phase17c_scale_response_preserves_resource_version_and_status_shape() {
        let response = build_scale_response(
            "web",
            "default",
            42,
            3,
            2,
            "app=web,tier=frontend".to_string(),
        );

        assert_eq!(response["apiVersion"], "autoscaling/v1");
        assert_eq!(response["kind"], "Scale");
        assert_eq!(response["metadata"]["name"], "web");
        assert_eq!(response["metadata"]["namespace"], "default");
        assert_eq!(response["metadata"]["resourceVersion"], "42");
        assert_eq!(response["spec"]["replicas"], 3);
        assert_eq!(response["status"]["replicas"], 2);
        assert_eq!(response["status"]["selector"], "app=web,tier=frontend");
    }

    #[test]
    fn phase17c_scale_resource_version_validation_remains_exact() {
        let cases = [
            (serde_json::json!({}), None),
            (
                serde_json::json!({"metadata": {"resourceVersion": ""}}),
                None,
            ),
            (
                serde_json::json!({"metadata": {"resourceVersion": "17"}}),
                Some(17),
            ),
        ];

        for (body, expected) in cases {
            assert_eq!(extract_scale_resource_version(&body).unwrap(), expected);
        }
        let error = extract_scale_resource_version(
            &serde_json::json!({"metadata": {"resourceVersion": "not-an-integer"}}),
        )
        .unwrap_err();
        assert!(matches!(error, AppError::BadRequest(_)));
        assert!(matches!(
            error,
            AppError::BadRequest(message)
                if message == "metadata.resourceVersion must be an integer string"
        ));
    }
}
