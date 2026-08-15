use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt as _;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use klights_reconcile_api::{GcForegroundDeleteCoordination, GcPodDeleteRequest, GcPodDeleteSink};
use klights_types::PodIdentity;
use std::collections::HashSet;

#[async_trait]
pub trait GcResourceStore: Send + Sync {
    async fn list_custom_resource_definitions(&self) -> ControllerStoreResult<Vec<Resource>>;
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource>;
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource>;
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>>;
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> ControllerStoreResult<Vec<Resource>>;
    fn gc_store_error_is_conflict(&self, error: &ControllerStoreError) -> bool {
        error.is_conflict()
    }
}

const OWNER_REF_UPDATE_MAX_CONFLICT_RETRIES: usize = 8;

fn is_core_pod(resource: &Resource) -> bool {
    resource.api_version == "v1" && resource.kind == "Pod"
}

fn has_deletion_timestamp(resource: &Resource) -> bool {
    resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|value| value.as_str())
        .is_some_and(|value| !value.trim().is_empty())
}

fn same_resource_uid_or_unknown(expected: &Resource, live: &Resource) -> bool {
    expected.uid.is_empty() || live.uid.is_empty() || expected.uid == live.uid
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnerReferenceReconcile {
    NoOwnerReferences,
    HasLiveOwner,
    OwnerReferencesUpdated,
    Deleted,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum GcDeleteOutcome {
    HardDeleted,
    MarkedTerminating,
    Gone,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OwnerRefState {
    Solid,
    Dangling,
    WaitingForDependentsDeletion,
    Unresolvable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OwnerScope {
    Namespaced,
    Cluster,
    Unknown,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct CascadeOwnerKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    uid: String,
    name: String,
}

impl CascadeOwnerKey {
    fn new(
        owner_uid: &str,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<String>,
    ) -> Option<Self> {
        if owner_uid.is_empty() && owner_name.is_empty() {
            return None;
        }

        Some(Self {
            api_version: owner_api_version.to_string(),
            kind: owner_kind.to_string(),
            namespace,
            uid: owner_uid.to_string(),
            name: owner_name.to_string(),
        })
    }
}

fn has_finalizer(resource: &serde_json::Value, finalizer: &str) -> bool {
    resource
        .pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .any(|v| v.as_str().is_some_and(|s| s == finalizer))
        })
}

fn is_waiting_for_dependents_deletion(resource: &serde_json::Value) -> bool {
    resource.pointer("/metadata/deletionTimestamp").is_some()
        && has_finalizer(resource, "foregroundDeletion")
}

fn known_cluster_scoped_kind(kind: &str) -> bool {
    matches!(
        kind,
        "APIService"
            | "CertificateSigningRequest"
            | "ClusterRole"
            | "ClusterRoleBinding"
            | "CSIDriver"
            | "CSINode"
            | "CustomResourceDefinition"
            | "FlowSchema"
            | "IngressClass"
            | "MutatingWebhookConfiguration"
            | "Namespace"
            | "Node"
            | "PersistentVolume"
            | "PriorityClass"
            | "PriorityLevelConfiguration"
            | "RuntimeClass"
            | "ServiceCIDR"
            | "StorageClass"
            | "ValidatingAdmissionPolicy"
            | "ValidatingAdmissionPolicyBinding"
            | "ValidatingWebhookConfiguration"
            | "VolumeAttachment"
    )
}

fn is_builtin_api_version(api_version: &str) -> bool {
    matches!(
        api_version,
        "v1" | "apps/v1"
            | "autoscaling/v1"
            | "autoscaling/v2"
            | "batch/v1"
            | "certificates.k8s.io/v1"
            | "coordination.k8s.io/v1"
            | "discovery.k8s.io/v1"
            | "events.k8s.io/v1"
            | "networking.k8s.io/v1"
            | "node.k8s.io/v1"
            | "policy/v1"
            | "rbac.authorization.k8s.io/v1"
            | "scheduling.k8s.io/v1"
            | "storage.k8s.io/v1"
            | "authentication.k8s.io/v1"
            | "authorization.k8s.io/v1"
            | "admissionregistration.k8s.io/v1"
            | "apiregistration.k8s.io/v1"
            | "apiextensions.k8s.io/v1"
            | "flowcontrol.apiserver.k8s.io/v1"
    )
}

fn builtin_owner_scope(api_version: &str, kind: &str) -> Option<OwnerScope> {
    if !is_builtin_api_version(api_version) {
        return None;
    }

    if known_cluster_scoped_kind(kind) {
        Some(OwnerScope::Cluster)
    } else {
        Some(OwnerScope::Namespaced)
    }
}

async fn custom_resource_scope(
    db: &(impl GcResourceStore + ?Sized),
    api_version: &str,
    kind: &str,
) -> Result<Option<OwnerScope>> {
    let Some((group, version)) = api_version.rsplit_once('/') else {
        return Ok(None);
    };

    let crds = db.list_custom_resource_definitions().await?;

    for crd in crds {
        let spec = crd.data.get("spec").unwrap_or(&serde_json::Value::Null);
        if spec.get("group").and_then(|v| v.as_str()) != Some(group) {
            continue;
        }
        if spec.pointer("/names/kind").and_then(|v| v.as_str()) != Some(kind) {
            continue;
        }

        let version_served =
            spec.get("versions")
                .and_then(|v| v.as_array())
                .is_none_or(|versions| {
                    versions.iter().any(|v| {
                        v.get("name").and_then(|name| name.as_str()) == Some(version)
                            && v.get("served")
                                .and_then(|served| served.as_bool())
                                .unwrap_or(true)
                    })
                });
        if !version_served {
            continue;
        }

        return Ok(match spec.get("scope").and_then(|v| v.as_str()) {
            Some("Namespaced") => Some(OwnerScope::Namespaced),
            Some("Cluster") => Some(OwnerScope::Cluster),
            _ => None,
        });
    }

    Ok(None)
}

async fn owner_scope(
    db: &(impl GcResourceStore + ?Sized),
    api_version: &str,
    kind: &str,
) -> Result<OwnerScope> {
    if let Some(scope) = builtin_owner_scope(api_version, kind) {
        return Ok(scope);
    }

    Ok(custom_resource_scope(db, api_version, kind)
        .await?
        .unwrap_or(OwnerScope::Unknown))
}

fn owner_ref_matches(
    owner_ref: &serde_json::Value,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
) -> bool {
    let ref_uid = owner_ref.get("uid").and_then(|u| u.as_str()).unwrap_or("");
    if !owner_uid.is_empty() && ref_uid == owner_uid {
        return true;
    }

    if !ref_uid.is_empty() {
        return false;
    }

    let name_matches = owner_ref.get("name").and_then(|n| n.as_str()) == Some(owner_name);
    let kind_matches = owner_ref.get("kind").and_then(|k| k.as_str()) == Some(owner_kind);
    let api_version_matches = owner_ref
        .get("apiVersion")
        .and_then(|a| a.as_str())
        .map(|a| a == owner_api_version)
        .unwrap_or(true);

    name_matches && kind_matches && api_version_matches
}

fn resource_has_matching_owner_ref(
    resource: &serde_json::Value,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
) -> bool {
    resource
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
        .map(|refs| {
            refs.iter().any(|owner_ref| {
                owner_ref_matches(
                    owner_ref,
                    owner_uid,
                    owner_api_version,
                    owner_name,
                    owner_kind,
                )
            })
        })
        .unwrap_or(false)
}

fn resource_has_blocking_owner_ref(
    resource: &serde_json::Value,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
) -> bool {
    resource
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
        .map(|refs| {
            refs.iter().any(|owner_ref| {
                owner_ref_matches(
                    owner_ref,
                    owner_uid,
                    owner_api_version,
                    owner_name,
                    owner_kind,
                ) && owner_ref
                    .get("blockOwnerDeletion")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn owner_ref_identity_matches(candidate: &serde_json::Value, target: &serde_json::Value) -> bool {
    let target_uid = target.get("uid").and_then(|u| u.as_str()).unwrap_or("");
    if !target_uid.is_empty() {
        return candidate.get("uid").and_then(|u| u.as_str()) == Some(target_uid);
    }

    let target_api_version = target
        .get("apiVersion")
        .and_then(|a| a.as_str())
        .unwrap_or("");
    let target_kind = target.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    let target_name = target.get("name").and_then(|n| n.as_str()).unwrap_or("");

    candidate.get("apiVersion").and_then(|a| a.as_str()) == Some(target_api_version)
        && candidate.get("kind").and_then(|k| k.as_str()) == Some(target_kind)
        && candidate.get("name").and_then(|n| n.as_str()) == Some(target_name)
}

async fn owner_ref_state(
    db: &(impl GcResourceStore + ?Sized),
    owner_ref: &serde_json::Value,
    dependent_namespace: Option<&str>,
) -> Result<OwnerRefState> {
    let api_version = match owner_ref.get("apiVersion").and_then(|a| a.as_str()) {
        Some(api_version) => api_version,
        None => return Ok(OwnerRefState::Unresolvable),
    };
    let kind = match owner_ref.get("kind").and_then(|k| k.as_str()) {
        Some(kind) => kind,
        None => return Ok(OwnerRefState::Unresolvable),
    };
    let name = match owner_ref.get("name").and_then(|n| n.as_str()) {
        Some(name) => name,
        None => return Ok(OwnerRefState::Unresolvable),
    };
    let uid = owner_ref.get("uid").and_then(|u| u.as_str()).unwrap_or("");

    let scope = owner_scope(db, api_version, kind).await?;
    let namespaces = match (dependent_namespace, scope) {
        (Some(ns), OwnerScope::Namespaced) => vec![Some(ns.to_string())],
        (Some(_), OwnerScope::Cluster) => vec![None],
        (None, OwnerScope::Cluster) => vec![None],
        (None, OwnerScope::Namespaced) | (_, OwnerScope::Unknown) => {
            return Ok(OwnerRefState::Unresolvable);
        }
    };

    for namespace in namespaces {
        let Some(owner) = db
            .get_resource(api_version, kind, namespace.as_deref(), name)
            .await?
        else {
            continue;
        };

        if !uid.is_empty() {
            let owner_uid = owner
                .data
                .pointer("/metadata/uid")
                .and_then(|u| u.as_str())
                .unwrap_or("");
            if owner_uid != uid {
                continue;
            }
        }

        if is_waiting_for_dependents_deletion(&owner.data) {
            return Ok(OwnerRefState::WaitingForDependentsDeletion);
        }

        return Ok(OwnerRefState::Solid);
    }

    Ok(OwnerRefState::Dangling)
}

async fn owner_ref_points_to_live_owner(
    db: &(impl GcResourceStore + ?Sized),
    owner_ref: &serde_json::Value,
    dependent_namespace: Option<&str>,
) -> Result<bool> {
    Ok(matches!(
        owner_ref_state(db, owner_ref, dependent_namespace).await?,
        OwnerRefState::Solid | OwnerRefState::Unresolvable
    ))
}

async fn has_another_live_owner(
    db: &(impl GcResourceStore + ?Sized),
    resource: &Resource,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
) -> Result<bool> {
    let Some(owner_refs) = resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
    else {
        return Ok(false);
    };

    for owner_ref in owner_refs {
        if owner_ref_matches(
            owner_ref,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        ) {
            continue;
        }

        if owner_ref_points_to_live_owner(db, owner_ref, resource.namespace.as_deref()).await? {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn remove_owner_ref_from_resource(
    db: &(impl GcResourceStore + ?Sized),
    resource: Resource,
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
) -> Result<()> {
    let mut current = resource;

    for attempt in 0..=OWNER_REF_UPDATE_MAX_CONFLICT_RETRIES {
        let mut data: serde_json::Value = (*current.data).clone();
        let mut changed = false;
        if let Some(metadata) = data.get_mut("metadata").and_then(|m| m.as_object_mut())
            && let Some(owner_refs) = metadata
                .get_mut("ownerReferences")
                .and_then(|refs| refs.as_array_mut())
        {
            let before = owner_refs.len();
            owner_refs.retain(|owner_ref| {
                !owner_ref_matches(
                    owner_ref,
                    owner_uid,
                    owner_api_version,
                    owner_name,
                    owner_kind,
                )
            });
            changed = owner_refs.len() != before;
            if owner_refs.is_empty() {
                metadata.remove("ownerReferences");
            }
        }

        if !changed {
            return Ok(());
        }

        match db
            .update_main_resource_with_preconditions(
                &current.api_version,
                &current.kind,
                current.namespace.as_deref(),
                &current.name,
                data,
                ResourcePreconditions::from_resource(&current),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(err)
                if db.gc_store_error_is_conflict(&err)
                    && attempt < OWNER_REF_UPDATE_MAX_CONFLICT_RETRIES =>
            {
                let Some(live) = db
                    .get_resource(
                        &current.api_version,
                        &current.kind,
                        current.namespace.as_deref(),
                        &current.name,
                    )
                    .await?
                else {
                    return Ok(());
                };
                if !same_resource_uid_or_unknown(&current, &live) {
                    return Ok(());
                }
                current = live;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(())
}

async fn remove_owner_refs_from_resource(
    db: &(impl GcResourceStore + ?Sized),
    resource: Resource,
    refs_to_remove: &[serde_json::Value],
) -> Result<bool> {
    let mut current = resource;

    for attempt in 0..=OWNER_REF_UPDATE_MAX_CONFLICT_RETRIES {
        let mut data: serde_json::Value = (*current.data).clone();
        let mut changed = false;
        if let Some(metadata) = data.get_mut("metadata").and_then(|m| m.as_object_mut())
            && let Some(owner_refs) = metadata
                .get_mut("ownerReferences")
                .and_then(|refs| refs.as_array_mut())
        {
            let before = owner_refs.len();
            owner_refs.retain(|owner_ref| {
                !refs_to_remove
                    .iter()
                    .any(|target| owner_ref_identity_matches(owner_ref, target))
            });
            changed = owner_refs.len() != before;
            if owner_refs.is_empty() {
                metadata.remove("ownerReferences");
            }
        }

        if !changed {
            return Ok(false);
        }

        match db
            .update_main_resource_with_preconditions(
                &current.api_version,
                &current.kind,
                current.namespace.as_deref(),
                &current.name,
                data,
                ResourcePreconditions::from_resource(&current),
            )
            .await
        {
            Ok(_) => return Ok(true),
            Err(err)
                if db.gc_store_error_is_conflict(&err)
                    && attempt < OWNER_REF_UPDATE_MAX_CONFLICT_RETRIES =>
            {
                let Some(live) = db
                    .get_resource(
                        &current.api_version,
                        &current.kind,
                        current.namespace.as_deref(),
                        &current.name,
                    )
                    .await?
                else {
                    return Ok(false);
                };
                if !same_resource_uid_or_unknown(&current, &live) {
                    return Ok(false);
                }
                current = live;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(false)
}

async fn has_current_matching_dependents(
    db: &(impl GcResourceStore + ?Sized),
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
) -> Result<bool> {
    let mut owned = if !owner_uid.is_empty() {
        db.find_owned_resources(owner_uid, namespace.clone().as_deref())
            .await?
    } else {
        vec![]
    };

    if !owner_name.is_empty() && !owner_kind.is_empty() {
        let by_name = db
            .find_owned_by_name_kind_empty_uid(
                owner_api_version,
                owner_name,
                owner_kind,
                namespace.clone().as_deref(),
            )
            .await?;
        let uid_found_names: std::collections::HashSet<String> =
            owned.iter().map(|r| r.name.clone()).collect();
        for r in by_name {
            if !uid_found_names.contains(&r.name) {
                owned.push(r);
            }
        }
    }

    for resource in owned {
        let Some(current) = db
            .get_resource(
                &resource.api_version,
                &resource.kind,
                resource.namespace.as_deref(),
                &resource.name,
            )
            .await?
        else {
            continue;
        };

        if resource_has_blocking_owner_ref(
            &current.data,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        ) {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn delete_resource_and_dependents(
    db: &(impl GcResourceStore + ?Sized),
    resource: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<()> {
    let owner_uid = resource
        .data
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string();
    let owner_name = resource.name.clone();
    let owner_kind = resource.kind.clone();
    let owner_api_version = resource.api_version.clone();
    let owner_namespace = resource.namespace.clone();
    let orphan_dependents = has_finalizer(&resource.data, "orphan");

    if orphan_dependents {
        orphan_children(
            db,
            &owner_uid,
            &owner_api_version,
            &owner_name,
            &owner_kind,
            owner_namespace.clone(),
        )
        .await?;
    }

    let delete_outcome =
        delete_resource_for_gc(db, resource, pod_delete_sink, non_pod_finalization).await?;

    if !orphan_dependents && delete_outcome == GcDeleteOutcome::HardDeleted {
        Box::pin(cascade_delete_with_uid(
            db,
            &owner_uid,
            &owner_api_version,
            &owner_name,
            &owner_kind,
            owner_namespace,
            pod_delete_sink,
            non_pod_finalization,
            coordination,
        ))
        .await?;
    }

    Ok(())
}

/// Reconcile one observed object's ownerReferences using Kubernetes GC rules.
///
/// Objects with at least one solid owner survive. Dangling/foreground-waiting
/// owner references are removed when a solid owner remains. Objects with no
/// solid owners are deleted and the delete cascades to their dependents.
pub async fn reconcile_owner_references(
    db: &(impl GcResourceStore + ?Sized),
    resource: Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<OwnerReferenceReconcile> {
    let Some(owner_refs) = resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
        .cloned()
    else {
        return Ok(OwnerReferenceReconcile::NoOwnerReferences);
    };

    if owner_refs.is_empty() {
        return Ok(OwnerReferenceReconcile::NoOwnerReferences);
    }

    let mut has_solid_owner = false;
    let mut refs_to_remove = Vec::new();

    for owner_ref in &owner_refs {
        match owner_ref_state(db, owner_ref, resource.namespace.as_deref()).await? {
            OwnerRefState::Solid | OwnerRefState::Unresolvable => {
                has_solid_owner = true;
            }
            OwnerRefState::Dangling | OwnerRefState::WaitingForDependentsDeletion => {
                refs_to_remove.push(owner_ref.clone());
            }
        }
    }

    if has_solid_owner {
        if refs_to_remove.is_empty() {
            return Ok(OwnerReferenceReconcile::HasLiveOwner);
        }

        if remove_owner_refs_from_resource(db, resource, &refs_to_remove).await? {
            return Ok(OwnerReferenceReconcile::OwnerReferencesUpdated);
        }

        return Ok(OwnerReferenceReconcile::HasLiveOwner);
    }

    delete_resource_and_dependents(
        db,
        &resource,
        pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await?;
    Ok(OwnerReferenceReconcile::Deleted)
}

/// GC-aware resource deletion helper. Pods must be routed through the
/// Pod lifecycle actor (`GcPodDeleteSink`). Non-Pod resources use the
/// shared finalizer-aware delete path.
async fn delete_resource_for_gc(
    _db: &(impl GcResourceStore + ?Sized),
    resource: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<GcDeleteOutcome> {
    if is_core_pod(resource) {
        if has_deletion_timestamp(resource) {
            return Ok(GcDeleteOutcome::MarkedTerminating);
        }
        let namespace = resource.namespace.as_deref().unwrap_or("default");
        let uid = &resource.uid;
        return request_gc_pod_delete_for_gc(pod_delete_sink, namespace, &resource.name, uid).await;
    }

    match non_pod_finalization
        .finalize_non_pod(klights_reconcile_api::GcNonPodFinalizationRequest {
            resource: resource.clone(),
            orphan_children: has_finalizer(&resource.data, "orphan"),
            remove_foreground_finalizer: false,
        })
        .await
    {
        Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::HardDeleted) => {
            Ok(GcDeleteOutcome::HardDeleted)
        }
        Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::MarkedTerminating) => {
            Ok(GcDeleteOutcome::MarkedTerminating)
        }
        Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone) => Ok(GcDeleteOutcome::Gone),
        Err(error) => Err(anyhow::anyhow!(error.to_string())),
    }
}

async fn request_gc_pod_delete_for_gc(
    pod_delete_sink: &dyn GcPodDeleteSink,
    namespace: &str,
    name: &str,
    uid: &str,
) -> Result<GcDeleteOutcome> {
    if uid.is_empty() {
        tracing::warn!(
            namespace = %namespace,
            pod = %name,
            "GC Pod delete skipped: empty UID"
        );
        return Ok(GcDeleteOutcome::Gone);
    }
    match pod_delete_sink
        .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
            namespace, name, uid,
        )))
        .await
    {
        Ok(()) => {}
        Err(error) if error.is_gone_or_identity_changed() => {
            tracing::debug!(
                namespace = %namespace,
                pod = %name,
                uid = %uid,
                error = %error,
                "GC Pod delete skipped: Pod is gone or its UID changed"
            );
            return Ok(GcDeleteOutcome::Gone);
        }
        Err(e) => return Err(e.into()),
    }
    Ok(GcDeleteOutcome::MarkedTerminating)
}

/// Cascade delete owned resources when owner is deleted.
/// owner_uid: the deleted resource's UID (for normal ownerRef lookup)
/// owner_api_version: the deleted resource's apiVersion (for empty-UID
///   ownerRef lookup; pairs with kind+name to disambiguate two owners
///   from different API groups with the same kind/name)
/// owner_name: the deleted resource's name (for empty-UID ownerRef lookup)
/// owner_kind: the deleted resource's kind (for empty-UID ownerRef lookup)
#[expect(
    clippy::too_many_arguments,
    reason = "the UID-qualified Kubernetes owner identity stays explicit at this public lifecycle boundary"
)]
pub async fn cascade_delete_with_uid(
    db: &(impl GcResourceStore + ?Sized),
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<()> {
    let mut visited = HashSet::new();
    cascade_delete_with_uid_inner(
        db,
        CascadeDeleteRequest {
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        },
        pod_delete_sink,
        non_pod_finalization,
        coordination,
        &mut visited,
    )
    .await
}

/// Run the controller-owned post-finalization cascade and mutation effects.
///
/// The resource has already been hard-deleted by the lifecycle owner. A
/// cascade failure is therefore observable and retry-independent: record it,
/// continue the remaining side effects, and preserve the historical
/// best-effort completion contract.
pub async fn run_finalized_resource_effects(
    db: &(impl GcResourceStore + ?Sized),
    resource: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
    side_effects: &crate::side_effects::SideEffectRegistry,
    metrics: &crate::side_effects::SideEffectMetrics,
) {
    if let Err(error) = cascade_delete_with_uid(
        db,
        &resource.uid,
        &resource.api_version,
        &resource.name,
        &resource.kind,
        resource.namespace.clone(),
        pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await
    {
        metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            namespace = ?resource.namespace,
            name = %resource.name,
            error = %error,
            "cascade delete after finalizer-drained hard delete failed"
        );
    }
    let _ = side_effects.run_hooks(&resource.data).await;
}

struct CascadeDeleteRequest<'a> {
    owner_uid: &'a str,
    owner_api_version: &'a str,
    owner_name: &'a str,
    owner_kind: &'a str,
    namespace: Option<String>,
}

/// bug-grpc Pillar C: one sweep of the **durable owner cascade**.
///
/// Runs [`cascade_delete_with_uid`] (idempotent — already-terminating
/// children are left to their lifecycle actor) and then re-enumerates the
/// owner's dependents. Returns `true` when at least one owned child still
/// lacks a `deletionTimestamp` — i.e. a dependent that was created *after* the
/// previous sweep's snapshot and so was missed. The caller reschedules another
/// sweep on `true`, closing the cascade-vs-create race that leaves an EmptyDir
/// wrapper Pod orphaned. Returns `false` (self-extinguish) once every owned
/// child is terminating or gone — the per-child durable `pod_workqueue` entries
/// then drive each to actor-owned finalization.
///
/// HR#11-safe: Pod deletes route exclusively through `pod_delete_sink`
/// (mark terminating); the cascade never hard-deletes a Pod row.
#[expect(
    clippy::too_many_arguments,
    reason = "the UID-qualified Kubernetes owner identity stays explicit at this public lifecycle boundary"
)]
pub async fn owner_cascade_sweep_once(
    db: &(impl GcResourceStore + ?Sized),
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<bool> {
    cascade_delete_with_uid(
        db,
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace.clone(),
        pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await?;

    // Only a uid-keyed owner can have its dependents re-enumerated; an
    // empty-uid owner (circular-ownerRef conformance pattern) cannot, so there
    // is nothing further to sweep.
    if owner_uid.is_empty() {
        return Ok(false);
    }

    let owned = db
        .find_owned_resources(owner_uid, namespace.as_deref())
        .await?;
    let needs_another_sweep = owned.iter().any(|child| !has_deletion_timestamp(child));
    Ok(needs_another_sweep)
}

async fn cascade_delete_with_uid_inner(
    db: &(impl GcResourceStore + ?Sized),
    request: CascadeDeleteRequest<'_>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
    visited: &mut HashSet<CascadeOwnerKey>,
) -> Result<()> {
    let CascadeDeleteRequest {
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace,
    } = request;

    if owner_uid.is_empty() && owner_name.is_empty() {
        tracing::warn!("cascade_delete: empty UID and name provided");
        return Ok(());
    }

    let Some(owner_key) = CascadeOwnerKey::new(
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace.clone(),
    ) else {
        return Ok(());
    };

    if !visited.insert(owner_key) {
        tracing::debug!(
            owner_uid = %owner_uid,
            owner_api_version = %owner_api_version,
            owner_kind = %owner_kind,
            owner_name = %owner_name,
            namespace = ?namespace,
            "cascade_delete: skipping already visited owner"
        );
        return Ok(());
    }

    // Find all resources owned by this uid (normal ownerRef with non-empty uid)
    let mut owned = if !owner_uid.is_empty() {
        db.find_owned_resources(owner_uid, namespace.clone().as_deref())
            .await?
    } else {
        vec![]
    };

    // Also find resources with ownerRef.uid=="" AND apiVersion+name+kind
    // matching the deleted owner. Handles K8s conformance test pattern
    // where circular ownerRefs use empty UIDs. apiVersion included so
    // two owners from different groups with the same kind/name don't
    // collide.
    if !owner_name.is_empty() && !owner_kind.is_empty() {
        let by_name = db
            .find_owned_by_name_kind_empty_uid(
                owner_api_version,
                owner_name,
                owner_kind,
                namespace.clone().as_deref(),
            )
            .await?;
        // Deduplicate by name (avoid deleting the same resource twice)
        let uid_found_names: std::collections::HashSet<String> =
            owned.iter().map(|r| r.name.clone()).collect();
        for r in by_name {
            if !uid_found_names.contains(&r.name) {
                owned.push(r);
            }
        }
    }

    if !owned.is_empty() {
        tracing::info!(
            "Cascade delete: {} resources owned by uid={} name={}/{}",
            owned.len(),
            owner_uid,
            owner_kind,
            owner_name,
        );
    }

    // Delete each owned resource (which will trigger their own cascade).
    // Continue the sweep after a child failure so independent dependents still
    // make progress, but return the first error so durable callers retain
    // their delivery item and retry the incomplete cascade.
    let mut first_error = None;
    for resource in owned {
        let current = match db
            .get_resource(
                &resource.api_version.clone(),
                &resource.kind.clone(),
                resource.namespace.clone().as_deref(),
                &resource.name.clone(),
            )
            .await?
        {
            Some(current) => current,
            None => continue,
        };

        if !resource_has_matching_owner_ref(
            &current.data,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        ) {
            continue;
        }

        if has_another_live_owner(
            db,
            &current,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        )
        .await?
        {
            remove_owner_ref_from_resource(
                db,
                current,
                owner_uid,
                owner_api_version,
                owner_name,
                owner_kind,
            )
            .await?;
            continue;
        }

        tracing::debug!(
            "Cascade deleting {}/{} {}/{}",
            current.api_version,
            current.kind,
            current.namespace.as_deref().unwrap_or(""),
            current.name
        );

        let child_uid = current
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        let child_name = current.name.clone();
        let child_kind = current.kind.clone();
        let child_api_version = current.api_version.clone();
        let child_ns = current.namespace.clone();
        let child_key = CascadeOwnerKey::new(
            &child_uid,
            &child_api_version,
            &child_name,
            &child_kind,
            child_ns.clone(),
        );
        if child_key.as_ref().is_some_and(|key| visited.contains(key)) {
            tracing::debug!(
                child_uid = %child_uid,
                child_api_version = %child_api_version,
                child_kind = %child_kind,
                child_name = %child_name,
                namespace = ?child_ns,
                "cascade_delete: skipping child already on cascade path"
            );
            continue;
        }

        let delete_outcome =
            match delete_resource_for_gc(db, &current, pod_delete_sink, non_pod_finalization).await
            {
                Ok(outcome) => outcome,
                Err(e) => {
                    tracing::warn!("Failed to cascade delete resource: {}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    continue;
                }
            };
        let should_recurse = delete_outcome == GcDeleteOutcome::HardDeleted
            || (delete_outcome == GcDeleteOutcome::MarkedTerminating && is_core_pod(&current));
        if !should_recurse {
            continue;
        }

        // Recursively cascade delete resources owned by this one. Pods are
        // only marked terminating here, but Kubernetes background GC still
        // needs to process their dependents.
        if let Err(error) = Box::pin(cascade_delete_with_uid_inner(
            db,
            CascadeDeleteRequest {
                owner_uid: &child_uid,
                owner_api_version: &child_api_version,
                owner_name: &child_name,
                owner_kind: &child_kind,
                namespace: child_ns,
            },
            pod_delete_sink,
            non_pod_finalization,
            coordination,
            visited,
        ))
        .await
        {
            tracing::warn!("Failed to recursively cascade delete resource: {error}");
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

/// Orphan deletion: remove ownerReferences from children but don't delete them.
/// Same parameter contract as `cascade_delete_with_uid` — apiVersion paired
/// with name/kind disambiguates two owners from different API groups.
pub async fn orphan_children(
    db: &(impl GcResourceStore + ?Sized),
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
) -> Result<()> {
    if owner_uid.is_empty() && owner_name.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Orphan deletion: removing ownerReferences for uid={} owner={}/{}/{} in namespace {:?}",
        owner_uid,
        owner_api_version,
        owner_kind,
        owner_name,
        namespace
    );

    let mut owned = if !owner_uid.is_empty() {
        db.find_owned_resources(owner_uid, namespace.clone().as_deref())
            .await?
    } else {
        vec![]
    };

    if !owner_name.is_empty() && !owner_kind.is_empty() {
        let by_name = db
            .find_owned_by_name_kind_empty_uid(
                owner_api_version,
                owner_name,
                owner_kind,
                namespace.clone().as_deref(),
            )
            .await?;
        let mut seen: std::collections::HashSet<(String, String, String)> = owned
            .iter()
            .map(|r| {
                (
                    r.kind.clone(),
                    r.namespace.clone().unwrap_or_default(),
                    r.name.clone(),
                )
            })
            .collect();
        for child in by_name {
            let key = (
                child.kind.clone(),
                child.namespace.clone().unwrap_or_default(),
                child.name.clone(),
            );
            if seen.insert(key) {
                owned.push(child);
            }
        }
    }

    let mut first_error = None;
    for resource in owned {
        if let Err(e) = remove_owner_ref_from_resource(
            db,
            resource,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        )
        .await
        {
            tracing::warn!("Failed to orphan resource: {}", e);
            if first_error.is_none() {
                first_error = Some(e);
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

/// Foreground deletion: delete children first, then parent can be deleted
/// Returns true if all children are deleted and parent can now be removed
#[expect(
    clippy::too_many_arguments,
    reason = "the UID-qualified Kubernetes owner identity stays explicit at this public lifecycle boundary"
)]
pub async fn check_foreground_deletion_ready(
    db: &(impl GcResourceStore + ?Sized),
    owner_uid: &str,
    owner_api_version: &str,
    owner_name: &str,
    owner_kind: &str,
    namespace: Option<String>,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<bool> {
    if owner_uid.is_empty() {
        return Ok(true);
    }

    let mut owned = db
        .find_owned_resources(owner_uid, namespace.clone().as_deref())
        .await?;

    // Also find resources with ownerRef.uid=="" AND apiVersion+name+kind
    // matching deleted owner (handles circular ownerRef pattern)
    if !owner_name.is_empty() && !owner_kind.is_empty() {
        let by_name = db
            .find_owned_by_name_kind_empty_uid(
                owner_api_version,
                owner_name,
                owner_kind,
                namespace.clone().as_deref(),
            )
            .await?;
        // Deduplicate by name
        let uid_found_names: std::collections::HashSet<String> =
            owned.iter().map(|r| r.name.clone()).collect();
        for r in by_name {
            if !uid_found_names.contains(&r.name) {
                owned.push(r);
            }
        }
    }

    let mut seen_child_uids = HashSet::new();

    if owned.is_empty() {
        // No children left, parent can be deleted
        coordination.clear_owner(owner_uid);
        return Ok(true);
    }

    let mut pending_child_delete = false;
    let mut pending_pod_deletes: Vec<(String, String, String)> = Vec::new();

    // Process children - delete if no other live owners, else remove owner ref
    for resource in owned {
        let current = match db
            .get_resource(
                &resource.api_version.clone(),
                &resource.kind.clone(),
                resource.namespace.clone().as_deref(),
                &resource.name.clone(),
            )
            .await?
        {
            Some(curr) => curr,
            None => {
                let child_uid = resource.uid.clone();
                if !child_uid.is_empty() {
                    coordination.release(owner_uid, &child_uid);
                    seen_child_uids.insert(child_uid);
                }
                continue;
            }
        };

        tracing::debug!(
            "Foreground deletion: checking child {} owned by {} owner={} api={} kind={}",
            current.kind,
            current.name,
            owner_uid,
            owner_api_version,
            owner_kind
        );

        let _owner_refs = current
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array());

        if !resource_has_blocking_owner_ref(
            &current.data,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        ) {
            continue;
        }

        // Check if this child has another live owner
        if has_another_live_owner(
            db,
            &current,
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
        )
        .await?
        {
            if is_core_pod(&current) {
                let child_uid = if !resource.uid.is_empty() {
                    Some(resource.uid.clone())
                } else {
                    current
                        .data
                        .get("metadata")
                        .and_then(|m| m.get("uid"))
                        .and_then(|u| u.as_str())
                        .map(std::string::ToString::to_string)
                };
                if let Some(child_uid) = child_uid {
                    coordination.release(owner_uid, &child_uid);
                    seen_child_uids.insert(child_uid);
                }
            }
            // Child has another live owner - just remove our owner ref
            tracing::info!(
                "Foreground deletion: child {}/{} has another live owner, removing owner ref for {} owner={}",
                current.kind,
                current.name,
                owner_kind,
                owner_uid
            );
            remove_owner_ref_from_resource(
                db,
                current,
                owner_uid,
                owner_api_version,
                owner_name,
                owner_kind,
            )
            .await?;
            continue;
        }

        if is_core_pod(&current) && has_deletion_timestamp(&current) {
            tracing::debug!(
                "Foreground deletion: child {}/{} already terminating, skipping delete request",
                current.api_version,
                current.name
            );
            continue;
        }

        // No other live owners - delete this child
        tracing::info!(
            "Foreground deletion: child {}/{} has no other live owners, DELETING it",
            current.kind,
            current.name
        );
        let child_uid = current
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        let child_name = current.name.clone();
        let child_kind = current.kind.clone();
        let child_api_version = current.api_version.clone();
        let child_ns = current.namespace.clone();
        seen_child_uids.insert(child_uid.clone());

        if is_core_pod(&current) {
            if child_uid.is_empty() {
                continue;
            }
            if has_deletion_timestamp(&current) {
                coordination.release(owner_uid, &child_uid);
                continue;
            }

            if !coordination.reserve(owner_uid, &child_uid) {
                tracing::debug!(
                    "Foreground deletion: child {}/{} is already queued for Pod delete; skipping duplicate request",
                    current.api_version,
                    current.name
                );
                pending_child_delete = true;
                continue;
            }

            let namespace = current
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string());
            pending_pod_deletes.push((namespace, child_name, child_uid));
            pending_child_delete = true;
            continue;
        }

        let delete_outcome =
            match delete_resource_for_gc(db, &current, pod_delete_sink, non_pod_finalization).await
            {
                Ok(outcome) => outcome,
                Err(e) => {
                    tracing::warn!("Failed to delete child during foreground deletion: {}", e);
                    continue;
                }
            };
        match delete_outcome {
            GcDeleteOutcome::MarkedTerminating => {
                pending_child_delete = true;
                continue;
            }
            GcDeleteOutcome::Gone => continue,
            GcDeleteOutcome::HardDeleted => {}
        }

        // Recursively cascade delete
        {
            let _ = Box::pin(cascade_delete_with_uid(
                db,
                &child_uid,
                &child_api_version,
                &child_name,
                &child_kind,
                child_ns,
                pod_delete_sink,
                non_pod_finalization,
                coordination,
            ))
            .await;
        }
    }

    let mut pod_delete_results = futures::stream::iter(pending_pod_deletes)
        .map(|(namespace, name, uid)| async move {
            let outcome =
                request_gc_pod_delete_for_gc(pod_delete_sink, &namespace, &name, &uid).await;
            if !matches!(outcome, Ok(GcDeleteOutcome::MarkedTerminating)) {
                coordination.release(owner_uid, &uid);
            }
            outcome
        })
        .buffer_unordered(16);

    while let Some(delete_result) = pod_delete_results.next().await {
        match delete_result {
            Ok(GcDeleteOutcome::MarkedTerminating) => pending_child_delete = true,
            Ok(GcDeleteOutcome::Gone) | Ok(GcDeleteOutcome::HardDeleted) => {}
            Err(e) => tracing::warn!("Failed to delete Pod child during foreground deletion: {e}"),
        }
    }

    if pending_child_delete {
        coordination.retain_owner_children(owner_uid, &seen_child_uids);
        return Ok(false);
    }

    let ready = !has_current_matching_dependents(
        db,
        owner_uid,
        owner_api_version,
        owner_name,
        owner_kind,
        namespace,
    )
    .await?;
    if ready {
        coordination.clear_owner(owner_uid);
    }
    coordination.retain_owner_children(owner_uid, &seen_child_uids);

    Ok(ready)
}

/// Re-check foreground-deleting owners after a dependent Pod row is removed by
/// the actor-owned finalization path.
pub async fn finalize_foreground_owners_after_dependent_delete(
    db: &(impl GcResourceStore + ?Sized),
    deleted_resource: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<()> {
    let Some(owner_refs) = deleted_resource
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|refs| refs.as_array())
        .cloned()
    else {
        return Ok(());
    };

    for owner_ref in owner_refs {
        let Some(owner) =
            foreground_owner_from_ref(db, &owner_ref, deleted_resource.namespace.as_deref())
                .await?
        else {
            continue;
        };
        let _ = finalize_foreground_owner_if_ready(
            db,
            &owner,
            pod_delete_sink,
            non_pod_finalization,
            coordination,
        )
        .await?;
    }

    Ok(())
}

async fn foreground_owner_from_ref(
    db: &(impl GcResourceStore + ?Sized),
    owner_ref: &serde_json::Value,
    dependent_namespace: Option<&str>,
) -> Result<Option<Resource>> {
    let Some(api_version) = owner_ref.get("apiVersion").and_then(|a| a.as_str()) else {
        return Ok(None);
    };
    let Some(kind) = owner_ref.get("kind").and_then(|k| k.as_str()) else {
        return Ok(None);
    };
    let Some(name) = owner_ref.get("name").and_then(|n| n.as_str()) else {
        return Ok(None);
    };

    let namespace = match (
        owner_scope(db, api_version, kind).await?,
        dependent_namespace,
    ) {
        (OwnerScope::Namespaced, Some(ns)) => Some(ns),
        (OwnerScope::Cluster, _) => None,
        (OwnerScope::Namespaced, None) | (OwnerScope::Unknown, _) => return Ok(None),
    };

    let Some(owner) = db.get_resource(api_version, kind, namespace, name).await? else {
        return Ok(None);
    };

    if let Some(uid) = owner_ref
        .get("uid")
        .and_then(|u| u.as_str())
        .filter(|uid| !uid.is_empty())
        && owner.uid != uid
    {
        return Ok(None);
    }

    if is_waiting_for_dependents_deletion(&owner.data) {
        Ok(Some(owner))
    } else {
        Ok(None)
    }
}

/// Delete or unblock a foreground-deleting owner when no dependents remain.
///
/// Returns true when the owner was deleted or updated. Pod owners are never
/// hard-deleted here; their foreground finalizer is removed and the Pod actor
/// remains the only owner allowed to remove the Pod row.
pub async fn finalize_foreground_owner_if_ready(
    db: &(impl GcResourceStore + ?Sized),
    owner: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    coordination: &dyn GcForegroundDeleteCoordination,
) -> Result<bool> {
    if !is_waiting_for_dependents_deletion(&owner.data) {
        return Ok(false);
    }

    let ready = check_foreground_deletion_ready(
        db,
        &owner.uid,
        &owner.api_version,
        &owner.name,
        &owner.kind,
        owner.namespace.clone(),
        pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await?;
    if !ready {
        return Ok(false);
    }

    finalize_foreground_owner_resource(db, owner, pod_delete_sink, non_pod_finalization).await?;
    Ok(true)
}

async fn finalize_foreground_owner_resource(
    db: &(impl GcResourceStore + ?Sized),
    owner: &Resource,
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
) -> Result<()> {
    let finalizers = owner
        .data
        .pointer("/metadata/finalizers")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let non_foreground_finalizers: Vec<serde_json::Value> = finalizers
        .into_iter()
        .filter(|value| value.as_str() != Some("foregroundDeletion"))
        .collect();

    let mut data: serde_json::Value = (*owner.data).clone();
    if let Some(metadata) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        if non_foreground_finalizers.is_empty() {
            metadata.remove("finalizers");
        } else {
            metadata.insert(
                "finalizers".to_string(),
                serde_json::Value::Array(non_foreground_finalizers.clone()),
            );
        }
    }
    if !is_core_pod(owner) {
        non_pod_finalization
            .finalize_non_pod(klights_reconcile_api::GcNonPodFinalizationRequest {
                resource: owner.clone(),
                orphan_children: false,
                remove_foreground_finalizer: true,
            })
            .await
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        return Ok(());
    }
    db.update_resource_with_preconditions(
        &owner.api_version,
        &owner.kind,
        owner.namespace.as_deref(),
        &owner.name,
        data,
        ResourcePreconditions::from_resource(owner),
    )
    .await?;
    let namespace = owner.namespace.as_deref().unwrap_or("default");
    pod_delete_sink
        .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
            namespace,
            &owner.name,
            &owner.uid,
        )))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn builtin_owner_scope_is_deterministic() {
        let cases = [
            ("v1", "Pod", Some(OwnerScope::Namespaced)),
            ("v1", "Namespace", Some(OwnerScope::Cluster)),
            ("apps/v1", "Deployment", Some(OwnerScope::Namespaced)),
            ("example.test/v1", "Widget", None),
        ];

        for (api_version, kind, expected) in cases {
            assert_eq!(builtin_owner_scope(api_version, kind), expected);
        }
    }

    #[test]
    fn owner_reference_matching_preserves_uid_identity() {
        let exact_uid = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "name": "replacement-name",
            "uid": "owner-uid"
        });
        assert!(owner_ref_matches(
            &exact_uid,
            "owner-uid",
            "apps/v1",
            "owner",
            "Deployment"
        ));

        let replacement = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "name": "owner",
            "uid": "replacement-uid"
        });
        assert!(!owner_ref_matches(
            &replacement,
            "owner-uid",
            "apps/v1",
            "owner",
            "Deployment"
        ));
    }

    #[test]
    fn empty_uid_owner_reference_matches_full_type_and_name() {
        let reference = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "name": "owner",
            "uid": ""
        });
        assert!(owner_ref_matches(
            &reference,
            "",
            "apps/v1",
            "owner",
            "Deployment"
        ));
        assert!(!owner_ref_matches(
            &reference,
            "",
            "apps/v1",
            "owner",
            "StatefulSet"
        ));
    }

    #[test]
    fn pod_and_termination_classification_uses_kubernetes_identity() {
        let pod = Resource::from_data_lossy(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "occupied",
                "namespace": "default",
                "uid": "pod-uid",
                "deletionTimestamp": "2026-08-01T00:00:00Z"
            }
        })));
        assert!(is_core_pod(&pod));
        assert!(has_deletion_timestamp(&pod));

        let same_name_replacement = Resource::from_data_lossy(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "occupied",
                "namespace": "default",
                "uid": "replacement-uid"
            }
        })));
        assert!(is_core_pod(&same_name_replacement));
        assert!(!has_deletion_timestamp(&same_name_replacement));
        assert_ne!(pod.uid, same_name_replacement.uid);
    }

    #[test]
    fn cascade_owner_key_requires_uid_or_name_and_keeps_namespace() {
        assert!(CascadeOwnerKey::new("", "apps/v1", "", "Deployment", None).is_none());
        let key = CascadeOwnerKey::new(
            "owner-uid",
            "apps/v1",
            "owner",
            "Deployment",
            Some("default".to_string()),
        )
        .expect("UID-qualified owner identity must be accepted");
        assert_eq!(key.uid, "owner-uid");
        assert_eq!(key.namespace.as_deref(), Some("default"));
    }

    #[tokio::test]
    async fn finalized_resource_effects_are_controller_owned() {
        let store = crate::internal_test_support::in_memory().await;
        let owner = store
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "owner",
                json!({"metadata": {"uid": "owner-uid"}}),
            )
            .await
            .unwrap();
        store
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "child",
                json!({
                    "metadata": {
                        "uid": "child-uid",
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "Deployment",
                            "name": "owner",
                            "uid": "owner-uid"
                        }]
                    },
                    "spec": {"nodeName": "worker-1"}
                }),
            )
            .await
            .unwrap();
        let coordination = crate::ControllerCoordination::new();
        let side_effects = crate::side_effects::SideEffectRegistry::new();
        let metrics = crate::side_effects::SideEffectMetrics::new();

        run_finalized_resource_effects(
            &store,
            &owner,
            &store,
            &store,
            &coordination,
            &side_effects,
            &metrics,
        )
        .await;

        let child = store
            .get_resource("v1", "Pod", Some("default"), "child")
            .await
            .unwrap()
            .expect("actor-owned Pod row remains present");
        assert_eq!(
            child
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(serde_json::Value::as_str),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(
            metrics
                .cascade_delete_failures_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }
}
