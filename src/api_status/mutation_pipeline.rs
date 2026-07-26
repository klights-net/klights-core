use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::{Json, body::Bytes};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde_json::Value;

use crate::api::{AppError, AppState, apply_patch, inject_resource_version};
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

    fn disappeared_message(&self, operation: &str) -> String {
        match self.namespace.as_deref() {
            Some(namespace) => format!(
                "{} {}/{} disappeared after status {}",
                self.kind, namespace, self.name, operation
            ),
            None => format!(
                "{} {} disappeared after status {}",
                self.kind, self.name, operation
            ),
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
    fn working_document(&self, current: &Resource) -> Result<Value, AppError>;
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

    fn working_document(&self, _current: &Resource) -> Result<Value, AppError> {
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

    fn working_document(&self, current: &Resource) -> Result<Value, AppError> {
        apply_patch(&current.data, &self.patch, self.content_type.as_deref())
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

#[async_trait]
pub trait StatusMutationWriter: Send + Sync {
    async fn get(&self, target: &StatusMutationTarget) -> Result<Option<Resource>>;
    async fn write_status(
        &self,
        target: &StatusMutationTarget,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;
    async fn patch_metadata(
        &self,
        target: &StatusMutationTarget,
        metadata_patch: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;
}

pub struct DatastoreStatusMutationWriter {
    state: Arc<AppState>,
}

impl DatastoreStatusMutationWriter {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl StatusMutationWriter for DatastoreStatusMutationWriter {
    async fn get(&self, target: &StatusMutationTarget) -> Result<Option<Resource>> {
        self.state
            .resource_mutation()
            .db
            .get_resource(
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
    ) -> Result<()> {
        self.state
            .resource_mutation()
            .db
            .update_status_only_with_preconditions(
                &target.api_version,
                &target.kind,
                target.namespace.as_deref(),
                &target.name,
                status,
                preconditions,
            )
            .await?;
        Ok(())
    }

    async fn patch_metadata(
        &self,
        target: &StatusMutationTarget,
        metadata_patch: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.state
            .resource_mutation()
            .db
            .patch_resource_latest_with_preconditions(
                &target.api_version,
                &target.kind,
                target.namespace.as_deref(),
                &target.name,
                ResourcePatchRequest::new(PatchKind::Merge, metadata_patch, preconditions),
            )
            .await?;
        Ok(())
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
    ensure_type_meta: bool,
}

impl ResourceStatusResponder {
    pub fn new(ensure_type_meta: bool) -> Self {
        Self { ensure_type_meta }
    }
}

impl StatusMutationResponder for ResourceStatusResponder {
    fn build_response(
        &self,
        target: &StatusMutationTarget,
        final_resource: Resource,
    ) -> Result<Value, AppError> {
        let data = if self.ensure_type_meta {
            crate::api_status::ensure_type_meta(
                final_resource.data.clone(),
                &target.api_version,
                &target.kind,
            )
        } else {
            std::sync::Arc::unwrap_or_clone(final_resource.data)
        };
        Ok(inject_resource_version(
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
        let working = operation.working_document(&current)?;
        let expected_rv = self
            .precondition
            .expected_resource_version(operation.precondition_document());

        if let Some(mut status) = operation.status_value(&working) {
            self.merge_policy
                .merge_status(target, &current, &mut status);
            self.writer
                .write_status(
                    target,
                    status,
                    ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        resource_version: expected_rv,
                    },
                )
                .await?;
        }

        if let Some(metadata_patch) = operation.metadata_patch(&current, &working) {
            self.writer
                .patch_metadata(
                    target,
                    metadata_patch,
                    ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        resource_version: None,
                    },
                )
                .await?;
        }

        let final_resource = self.writer.get(target).await?.ok_or_else(|| {
            AppError::NotFound(target.disappeared_message(operation.operation_name()))
        })?;
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
    fn desired_replicas(&self, current_scale: &Value) -> Result<i32, AppError>;
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
    fn desired_replicas(&self, _current_scale: &Value) -> Result<i32, AppError> {
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
    fn desired_replicas(&self, current_scale: &Value) -> Result<i32, AppError> {
        let patched = apply_patch(current_scale, &self.patch, self.content_type.as_deref())?;
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
pub trait ScaleMutationWriter: Send + Sync {
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

pub struct DatastoreScaleMutationWriter {
    state: Arc<AppState>,
}

impl DatastoreScaleMutationWriter {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ScaleMutationWriter for DatastoreScaleMutationWriter {
    async fn get(&self, target: &ScaleMutationTarget) -> Result<Option<Resource>> {
        self.state
            .resource_mutation()
            .db
            .get_resource(
                &target.api_version,
                &target.kind,
                Some(&target.namespace),
                &target.name,
            )
            .await
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
        self.state
            .resource_mutation()
            .db
            .patch_resource_latest_with_preconditions(
                &target.api_version,
                &target.kind,
                Some(&target.namespace),
                &target.name,
                request,
            )
            .await
    }

    async fn enqueue_reconcile(&self, resource: &Resource) {
        self.state
            .controller_reconcile()
            .controller_dispatcher
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
        let replicas = operation.desired_replicas(&current_scale)?;
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
    fn working_document(&self, current: &Resource) -> Result<Value, AppError>;
    fn precondition_document(&self) -> &Value;
}

impl<T> NamespaceStatusMutationOperation for T
where
    T: StatusMutationOperation,
{
    fn operation_name(&self) -> &'static str {
        StatusMutationOperation::operation_name(self)
    }

    fn working_document(&self, current: &Resource) -> Result<Value, AppError> {
        StatusMutationOperation::working_document(self, current)
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
        crate::api::ensure_namespace_status_phase_active(&mut merged);
        Ok(merged)
    }
}

#[async_trait]
pub trait NamespaceStatusMutationWriter: Send + Sync {
    async fn get(&self, target: &NamespaceStatusMutationTarget) -> Result<Option<Resource>>;
    async fn update(
        &self,
        target: &NamespaceStatusMutationTarget,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource>;
}

pub struct DatastoreNamespaceStatusMutationWriter {
    state: Arc<AppState>,
}

impl DatastoreNamespaceStatusMutationWriter {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl NamespaceStatusMutationWriter for DatastoreNamespaceStatusMutationWriter {
    async fn get(&self, target: &NamespaceStatusMutationTarget) -> Result<Option<Resource>> {
        self.state
            .resource_mutation()
            .db
            .get_namespace(&target.name)
            .await
    }

    async fn update(
        &self,
        target: &NamespaceStatusMutationTarget,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.state
            .resource_mutation()
            .db
            .update_namespace(&target.name, body, expected_rv)
            .await
    }
}

pub trait NamespaceStatusMutationResponder: Send + Sync {
    fn response(
        &self,
        target: &NamespaceStatusMutationTarget,
        updated: Resource,
    ) -> Result<Value, AppError>;
}

pub struct NamespaceStatusResponder;

impl NamespaceStatusMutationResponder for NamespaceStatusResponder {
    fn response(
        &self,
        _target: &NamespaceStatusMutationTarget,
        updated: Resource,
    ) -> Result<Value, AppError> {
        Ok(inject_resource_version(
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
        let working = operation.working_document(&current)?;
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
