use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use serde_json::Value;

use crate::api::{
    AppError, AppState, LenientJson, ensure_namespace_status_phase_active, inject_resource_version,
};
use crate::api_status::{
    ApiSubresourceStatusMergePolicy, CurrentNamespaceResourceVersionPrecondition,
    DatastoreNamespaceStatusMutationWriter, DatastoreStatusMutationWriter,
    LenientStatusResourceVersionPrecondition, NamespaceStatusMergePolicy,
    NamespaceStatusMutationPipeline, NamespaceStatusMutationTarget, NamespaceStatusResponder,
    ResourceStatusResponder, StatusMutationPipeline, StatusMutationTarget, StatusPatchOperation,
    StatusPutOperation, decode_patch_body, get_cluster_status_subresource,
    patch_cluster_status_subresource, preserve_node_extended_resources,
    update_cluster_status_subresource,
};

// Cluster subresource (status) authorization is enforced by the global
// `authorize_request` middleware chokepoint (see src/auth/middleware.rs).

pub async fn patch_node_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let patch: Value = decode_patch_body(&body)?;
    let target = StatusMutationTarget::cluster("v1", "Node", &name);
    let pipeline = StatusMutationPipeline::new(
        DatastoreStatusMutationWriter::new(state),
        ApiSubresourceStatusMergePolicy::new(Some(preserve_node_extended_resources)),
        LenientStatusResourceVersionPrecondition,
        ResourceStatusResponder::new(false),
    );
    let outcome = pipeline
        .execute(
            &target,
            &StatusPatchOperation::status_only(patch, content_type.map(str::to_string)),
        )
        .await?;
    Ok(Json(outcome.response))
}

crate::cluster_status_get_handler!(
    get_validatingadmissionpolicy_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicy"
);
crate::cluster_status_update_handler!(
    update_validatingadmissionpolicy_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicy"
);
crate::cluster_status_patch_handler!(
    patch_validatingadmissionpolicy_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicy"
);

crate::cluster_status_get_handler!(
    get_validatingadmissionpolicybinding_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicyBinding"
);
crate::cluster_status_update_handler!(
    update_validatingadmissionpolicybinding_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicyBinding"
);
crate::cluster_status_patch_handler!(
    patch_validatingadmissionpolicybinding_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicyBinding"
);

crate::namespaced_status_update_handler!(update_resourcequota_status, "v1", "ResourceQuota");
crate::namespaced_status_patch_handler!(patch_resourcequota_status, "v1", "ResourceQuota");
crate::namespaced_status_update_handler!(
    update_poddisruptionbudget_status,
    "policy/v1",
    "PodDisruptionBudget"
);
crate::namespaced_status_patch_handler!(
    patch_poddisruptionbudget_status,
    "policy/v1",
    "PodDisruptionBudget"
);

crate::namespaced_status_update_handler!(
    update_replicationcontroller_status,
    "v1",
    "ReplicationController"
);
crate::namespaced_status_patch_handler!(
    patch_replicationcontroller_status,
    "v1",
    "ReplicationController"
);

crate::cluster_status_get_handler!(get_csinode_status, "storage.k8s.io/v1", "CSINode");
crate::cluster_status_update_handler!(update_csinode_status, "storage.k8s.io/v1", "CSINode");
crate::cluster_status_patch_handler!(patch_csinode_status, "storage.k8s.io/v1", "CSINode");
crate::cluster_status_get_handler!(get_persistentvolume_status, "v1", "PersistentVolume");
crate::cluster_status_update_handler!(update_persistentvolume_status, "v1", "PersistentVolume");
crate::cluster_status_patch_handler!(patch_persistentvolume_status, "v1", "PersistentVolume");

crate::cluster_status_get_handler!(
    get_volumeattachment_status,
    "storage.k8s.io/v1",
    "VolumeAttachment"
);
crate::cluster_status_update_handler!(
    update_volumeattachment_status,
    "storage.k8s.io/v1",
    "VolumeAttachment"
);
crate::cluster_status_patch_handler!(
    patch_volumeattachment_status,
    "storage.k8s.io/v1",
    "VolumeAttachment"
);

crate::cluster_status_get_handler!(
    get_crd_status,
    "apiextensions.k8s.io/v1",
    "CustomResourceDefinition"
);
crate::cluster_status_update_handler!(
    update_crd_status,
    "apiextensions.k8s.io/v1",
    "CustomResourceDefinition"
);
crate::cluster_status_patch_handler!(
    patch_crd_status,
    "apiextensions.k8s.io/v1",
    "CustomResourceDefinition"
);

crate::cluster_status_update_handler!(
    update_apiservice_status,
    "apiregistration.k8s.io/v1",
    "APIService"
);
crate::cluster_status_patch_handler!(
    patch_apiservice_status,
    "apiregistration.k8s.io/v1",
    "APIService"
);

// Namespace status subresource handlers (cluster-scoped)

pub async fn get_namespace_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    // Namespaces are stored in the dedicated `namespaces` table (not `cluster_resources`),
    // so we must use get_namespace rather than the generic get_cluster_status_subresource.
    let resource = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;
    let mut data: Value = std::sync::Arc::unwrap_or_clone(resource.data);
    ensure_namespace_status_phase_active(&mut data);
    let result = inject_resource_version(data, resource.resource_version);
    Ok(Json(result))
}

pub async fn update_namespace_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let pipeline = NamespaceStatusMutationPipeline::new(
        DatastoreNamespaceStatusMutationWriter::new(state),
        NamespaceStatusMergePolicy,
        CurrentNamespaceResourceVersionPrecondition,
        NamespaceStatusResponder,
    );
    pipeline
        .execute(
            &NamespaceStatusMutationTarget { name },
            &StatusPutOperation::new(body),
        )
        .await
}

pub async fn patch_namespace_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let patch: Value = decode_patch_body(&body)?;
    let pipeline = NamespaceStatusMutationPipeline::new(
        DatastoreNamespaceStatusMutationWriter::new(state),
        NamespaceStatusMergePolicy,
        CurrentNamespaceResourceVersionPrecondition,
        NamespaceStatusResponder,
    );
    pipeline
        .execute(
            &NamespaceStatusMutationTarget { name },
            &StatusPatchOperation::new(patch, content_type.map(str::to_string)),
        )
        .await
}

crate::cluster_status_get_handler!(
    get_csr_status,
    "certificates.k8s.io/v1",
    "CertificateSigningRequest"
);
crate::cluster_status_update_handler!(
    update_csr_status,
    "certificates.k8s.io/v1",
    "CertificateSigningRequest"
);
crate::cluster_status_patch_handler!(
    patch_csr_status,
    "certificates.k8s.io/v1",
    "CertificateSigningRequest"
);

// CertificateSigningRequest approval subresource GET handler
// GET /apis/certificates.k8s.io/v1/certificatesigningrequests/{name}/approval
pub async fn get_csr_approval(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    get_cluster_status_subresource(
        state,
        "certificates.k8s.io/v1".to_string(),
        "CertificateSigningRequest".to_string(),
        name,
    )
    .await
}

// CertificateSigningRequest approval subresource handler
// PUT /apis/certificates.k8s.io/v1/certificatesigningrequests/{name}/approval
// The approval endpoint updates the CSR's status.conditions with Approved/Denied
pub async fn update_csr_approval(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    update_cluster_status_subresource(
        state,
        "certificates.k8s.io/v1".to_string(),
        "CertificateSigningRequest".to_string(),
        name,
        body,
    )
    .await
}

pub async fn patch_csr_approval(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let patch = decode_patch_body(&body)?;

    patch_cluster_status_subresource(
        state,
        "certificates.k8s.io/v1".to_string(),
        "CertificateSigningRequest".to_string(),
        name,
        patch,
        content_type,
    )
    .await
}

crate::namespaced_status_update_handler!(update_cronjob_status, "batch/v1", "CronJob");
crate::namespaced_status_patch_handler!(patch_cronjob_status, "batch/v1", "CronJob");
crate::namespaced_status_update_handler!(update_job_status, "batch/v1", "Job");
crate::namespaced_status_patch_handler!(patch_job_status, "batch/v1", "Job");
crate::namespaced_status_update_handler!(
    update_hpa_v1_status,
    "autoscaling/v1",
    "HorizontalPodAutoscaler"
);
crate::namespaced_status_patch_handler!(
    patch_hpa_v1_status,
    "autoscaling/v1",
    "HorizontalPodAutoscaler"
);
crate::namespaced_status_update_handler!(
    update_hpa_v2_status,
    "autoscaling/v2",
    "HorizontalPodAutoscaler"
);
crate::namespaced_status_patch_handler!(
    patch_hpa_v2_status,
    "autoscaling/v2",
    "HorizontalPodAutoscaler"
);

crate::cluster_status_get_handler!(
    get_flowschema_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "FlowSchema"
);
crate::cluster_status_update_handler!(
    update_flowschema_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "FlowSchema"
);
crate::cluster_status_patch_handler!(
    patch_flowschema_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "FlowSchema"
);

crate::cluster_status_get_handler!(
    get_mutatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "MutatingWebhookConfiguration"
);
crate::cluster_status_update_handler!(
    update_mutatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "MutatingWebhookConfiguration"
);
crate::cluster_status_patch_handler!(
    patch_mutatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "MutatingWebhookConfiguration"
);

crate::cluster_status_get_handler!(
    get_validatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingWebhookConfiguration"
);
crate::cluster_status_update_handler!(
    update_validatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingWebhookConfiguration"
);
crate::cluster_status_patch_handler!(
    patch_validatingwebhookconfiguration_status,
    "admissionregistration.k8s.io/v1",
    "ValidatingWebhookConfiguration"
);

crate::cluster_status_get_handler!(
    get_prioritylevelconfiguration_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "PriorityLevelConfiguration"
);
crate::cluster_status_update_handler!(
    update_prioritylevelconfiguration_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "PriorityLevelConfiguration"
);
crate::cluster_status_patch_handler!(
    patch_prioritylevelconfiguration_status,
    "flowcontrol.apiserver.k8s.io/v1",
    "PriorityLevelConfiguration"
);

// Macro to generate cluster-wide list handlers (GET /api/v1/pods, etc.)
// These list resources across ALL namespaces (namespace=None in DB query).
