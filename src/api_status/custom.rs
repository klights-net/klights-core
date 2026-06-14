use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};
use serde_json::Value;

use crate::api::{
    AppError, AppState, LenientJson, apply_patch, ensure_namespace_status_phase_active,
    inject_resource_version,
};
use crate::api_status::{
    decode_patch_body, get_cluster_status_subresource, patch_cluster_status_subresource,
    preserve_node_extended_resources, update_cluster_status_subresource,
};
use crate::datastore::ResourcePreconditions;

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

    let resource = state
        .db
        .get_resource("v1", "Node", None, &name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Node {} not found", name)))?;

    let patched = apply_patch(&resource.data, &patch, content_type)?;
    if let Some(new_status) = patched.get("status") {
        let mut new_status = new_status.clone();
        preserve_node_extended_resources(resource.data.get("status"), &mut new_status);
        // Atomic status write — leaves `.spec.taints` and other Node fields
        // untouched against any concurrent kubelet update.
        state
            .db
            .update_status_only_with_preconditions(
                "v1",
                "Node",
                None,
                &name,
                new_status,
                ResourcePreconditions {
                    uid: Some(resource.uid.clone()),
                    resource_version: None,
                },
            )
            .await?;
    }

    let final_resource = state
        .db
        .get_resource("v1", "Node", None, &name)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("Node {} disappeared after status patch", name))
        })?;
    let result = inject_resource_version(final_resource.data, final_resource.resource_version);
    Ok(Json(result))
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
    // Namespaces live in the dedicated `namespaces` table — read via get_namespace
    // and write via update_namespace, mirroring the regular PUT handler.
    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    let mut resource_data: Value = std::sync::Arc::unwrap_or_clone(current.data);
    if let Some(new_status) = body.get("status")
        && let Some(obj) = resource_data.as_object_mut()
    {
        obj.insert("status".to_string(), new_status.clone());
    }
    ensure_namespace_status_phase_active(&mut resource_data);

    let updated = state
        .db
        .update_namespace(&name, resource_data, current.resource_version)
        .await?;
    let result = inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

pub async fn patch_namespace_status(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    // Namespaces live in the dedicated `namespaces` table — read via get_namespace
    // and write via update_namespace, mirroring the regular PATCH handler.
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());
    let patch: Value = decode_patch_body(&body)?;

    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    // Use the full merged document so non-status fields the patch touches
    // (e.g. metadata.resourceVersion for optimistic concurrency, additional
    // sub-fields) are preserved — matches every other PATCH handler.
    let mut resource_data = apply_patch(&current.data, &patch, content_type)?;
    ensure_namespace_status_phase_active(&mut resource_data);

    let updated = state
        .db
        .update_namespace(&name, resource_data, current.resource_version)
        .await?;
    let result = inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
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
