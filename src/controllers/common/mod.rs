//! Common helper functions shared across controllers.
//!
//! Extracted from repeated inline patterns in deployment, replicaset, statefulset,
//! daemonset, and job controllers.

use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use anyhow::{Context, Result, anyhow};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference as K8sOwnerReference;
use serde::Serialize;
use serde_json::{Value, json};

pub trait OwnerRefManager: Send + Sync {
    fn build_owner_ref(&self, api_version: &str, kind: &str, name: &str, uid: &str) -> Value;
    fn is_owned_by(&self, resource_data: &Value, owner_uid: &str) -> bool;
    fn is_controlled_by(&self, resource_data: &Value, owner_uid: &str) -> bool;
}

pub trait ConditionBuilder: Send + Sync {
    fn build_condition(&self, type_: &str, status: &str, reason: &str, message: &str) -> Value;
}

pub trait PodCounter: Send + Sync {
    fn count_ready_pods(&self, pods: &[Resource]) -> usize;
    fn is_pod_ready(&self, pod: &Value) -> bool;
}

pub struct DefaultControllerCommon;

pub trait ControllerCommon: OwnerRefManager + ConditionBuilder + PodCounter + Send + Sync {}

impl<T> ControllerCommon for T where T: OwnerRefManager + ConditionBuilder + PodCounter {}

static DEFAULT_CONTROLLER_COMMON: DefaultControllerCommon = DefaultControllerCommon;

pub fn controller_common() -> &'static dyn ControllerCommon {
    &DEFAULT_CONTROLLER_COMMON
}

impl OwnerRefManager for DefaultControllerCommon {
    fn build_owner_ref(&self, api_version: &str, kind: &str, name: &str, uid: &str) -> Value {
        build_owner_ref(api_version, kind, name, uid)
    }

    fn is_owned_by(&self, resource_data: &Value, owner_uid: &str) -> bool {
        is_owned_by(resource_data, owner_uid)
    }

    fn is_controlled_by(&self, resource_data: &Value, owner_uid: &str) -> bool {
        is_controlled_by(resource_data, owner_uid)
    }
}

impl ConditionBuilder for DefaultControllerCommon {
    fn build_condition(&self, type_: &str, status: &str, reason: &str, message: &str) -> Value {
        build_condition(type_, status, reason, message)
    }
}

impl PodCounter for DefaultControllerCommon {
    fn count_ready_pods(&self, pods: &[Resource]) -> usize {
        count_ready_pods(pods)
    }

    fn is_pod_ready(&self, pod: &Value) -> bool {
        is_pod_ready_value(pod)
    }
}

/// Append an ownerReference to a resource's metadata.ownerReferences array.
/// Creates array if it does not exist.
///
/// Delegates to typed `OwnerReferenceList` for consistent ownership handling.
pub fn append_owner_reference(resource: &mut Value, owner_ref: Value) {
    let owner = match OwnerReference::try_from(&owner_ref) {
        Ok(o) => o,
        Err(_) => {
            // Fallback to direct JSON for malformed refs (shouldn't happen in production)
            if let Some(meta) = resource.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                let refs = meta
                    .entry("ownerReferences".to_string())
                    .or_insert_with(|| json!([]));
                if let Some(refs_arr) = refs.as_array_mut() {
                    refs_arr.push(owner_ref);
                }
            }
            return;
        }
    };

    let mut list = OwnerReferenceList::from_json(resource);
    list.append(owner);
    list.write_to_resource(resource);
}

/// Remove owner references matching `kind` + `uid` from metadata.ownerReferences.
/// Returns `true` if at least one reference was removed, `false` otherwise.
/// Preserves the order of remaining references. If the removal empties the list,
/// the field is left as an empty array (not removed) to match K8s semantics.
/// Remove owner references matching `kind` + `uid` from metadata.ownerReferences.
/// Returns `true` if at least one reference was removed, `false` otherwise.
/// Preserves order of remaining references. If the removal empties the list,
/// the field is left as an empty array (not removed) to match K8s semantics.
///
/// Delegates to typed `OwnerReferenceList` for consistent ownership handling.
pub fn remove_owner_reference_by_uid(resource: &mut Value, kind: &str, uid: &str) -> bool {
    let mut list = OwnerReferenceList::from_json(resource);
    let removed = list.remove_by_uid(kind, uid);
    list.write_to_resource(resource);
    removed
}

/// Build an ownerReference JSON object for embedding in pod/resource metadata.
///
/// Every controller that creates pods builds this exact structure. Using this
/// helper ensures all owner references are consistent and correct.
pub fn build_owner_ref(api_version: &str, kind: &str, name: &str, uid: &str) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "name": name,
        "uid": uid,
        "controller": true,
        "blockOwnerDeletion": true
    })
}

/// Return true if the resource's ownerReferences contains an entry with the given UID.
///
/// This is the canonical ownership check used by all controllers to find resources
/// owned by a given parent (e.g., pods owned by a ReplicaSet or StatefulSet).
pub fn is_owned_by(resource_data: &Value, owner_uid: &str) -> bool {
    resource_data
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|o| o.as_array())
        .map(|refs| {
            refs.iter()
                .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(owner_uid))
        })
        .unwrap_or(false)
}

/// Like `is_owned_by` but only matches ownerReferences where `controller` is
/// `true` — the "controlling" owner.  Deployment uses this to distinguish
/// active ReplicaSets from disowned ones (see rolling‑update disown).
pub fn is_controlled_by(resource_data: &Value, owner_uid: &str) -> bool {
    resource_data
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|o| o.as_array())
        .map(|refs| {
            refs.iter().any(|r| {
                r.get("controller").and_then(|c| c.as_bool()) == Some(true)
                    && r.get("uid").and_then(|u| u.as_str()) == Some(owner_uid)
            })
        })
        .unwrap_or(false)
}

/// Build a standard status condition object.
pub fn build_condition(type_: &str, status: &str, reason: &str, message: &str) -> Value {
    json!({
        "type": type_,
        "status": status,
        "reason": reason,
        "message": message
    })
}

/// Count pods with a Ready=True condition.
///
/// The `pods` slice may be any collection of `Resource` values. Pods without
/// a Ready condition, or with Ready=False/Unknown, are not counted.
pub fn count_ready_pods(pods: &[Resource]) -> usize {
    pods.iter()
        .filter(|pod| is_pod_ready_value(&pod.data))
        .count()
}

/// Return true if the pod Value has a Ready condition with status="True".
///
/// This is the low-level predicate used by `count_ready_pods` and also
/// useful when callers already hold a `&Value` rather than a `&Resource`.
pub fn is_pod_ready_value(pod: &Value) -> bool {
    let status = match pod.get("status") {
        Some(s) => s,
        None => return false,
    };

    if status
        .get("conditions")
        .and_then(|c| c.as_array())
        .map(|conditions| {
            conditions.iter().any(|c| {
                c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Fallback for pods whose condition writers lag behind container status:
    // if the pod is Running and every reported container status is ready=true,
    // treat the pod as ready for controller replica accounting.
    let is_running = status
        .get("phase")
        .and_then(|p| p.as_str())
        .is_some_and(|p| p == "Running");
    let all_containers_ready = status
        .get("containerStatuses")
        .and_then(|cs| cs.as_array())
        .filter(|arr| !arr.is_empty())
        .is_some_and(|arr| {
            arr.iter()
                .all(|cs| cs.get("ready").and_then(|r| r.as_bool()).unwrap_or(false))
        });
    is_running && all_containers_ready
}

/// Identifying tuple for a workload controller that owns a child Pod.
/// Used by `build_child_pod` so each call site declares its identity
/// once instead of plumbing four `&str`s through every helper.
pub struct OwnerInfo<'a> {
    pub api_version: &'a str,
    pub kind: &'a str,
    pub name: &'a str,
    pub uid: &'a str,
}

/// Construct a child Pod object from a controller's pod template,
/// stamped with the canonical metadata (name, namespace, labels +
/// ownerReferences, optional annotations), top-level apiVersion/kind/
/// status=Pending, and `spec.nodeName`. Returns the Value ready to
/// hand to `pod_create::create_controller_pod` — controllers that need
/// extra fields (StatefulSet `spec.hostname`, etc.) mutate it in place
/// after this call returns.
///
/// One source of truth for child-pod construction across ReplicaSet,
/// ReplicationController, StatefulSet, DaemonSet, Job, and any new
/// workload controller. Avoids per-controller drift on owner-ref shape,
/// label merging, status initialization, and pod-template inheritance.
///
/// Returns `Err` when the template is not a JSON object — the caller
/// should propagate so the reconciler retries instead of submitting an
/// empty Pod that K8s admission later rejects.
pub fn build_child_pod(
    template: &Value,
    pod_name: &str,
    namespace: &str,
    node_name: &str,
    owner: OwnerInfo<'_>,
    extra_labels: &[(&str, &str)],
    extra_annotations: &[(&str, &str)],
) -> Result<Value> {
    anyhow::ensure!(
        template.is_object(),
        "child pod template is not a JSON object: {template:?}"
    );

    let mut pod = template.clone();
    let pod_obj = pod
        .as_object_mut()
        .expect("template.is_object() asserted above");

    let mut labels = template
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object())
        .cloned()
        .unwrap_or_default();
    for (k, v) in extra_labels {
        labels.insert((*k).to_string(), Value::String((*v).to_string()));
    }

    let mut metadata = json!({
        "name": pod_name,
        "namespace": namespace,
        "labels": labels,
        "ownerReferences": [build_owner_ref(owner.api_version, owner.kind, owner.name, owner.uid)],
    });
    if !extra_annotations.is_empty()
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        let mut anns = serde_json::Map::with_capacity(extra_annotations.len());
        for (k, v) in extra_annotations {
            anns.insert((*k).to_string(), Value::String((*v).to_string()));
        }
        meta_obj.insert("annotations".to_string(), Value::Object(anns));
    }
    pod_obj.insert("metadata".to_string(), metadata);
    pod_obj.insert("apiVersion".to_string(), json!("v1"));
    pod_obj.insert("kind".to_string(), json!("Pod"));
    pod_obj.insert("status".to_string(), json!({"phase": "Pending"}));

    let spec = pod_obj.entry("spec").or_insert_with(|| json!({}));
    if let Some(spec_obj) = spec.as_object_mut() {
        let template_node_name = spec_obj.get("nodeName").and_then(|value| value.as_str());
        let should_stamp_node_name = match template_node_name {
            Some(existing) => existing.is_empty() && !node_name.is_empty(),
            None => !node_name.is_empty(),
        };
        if should_stamp_node_name {
            spec_obj.insert("nodeName".to_string(), json!(node_name));
        }
    }

    Ok(pod)
}

/// Write `status` as the new `.status` subtree of `resource`, leaving `.spec`
/// and other top-level fields untouched.
///
/// This is the safe write path for owner controllers (ReplicaSet, Deployment,
/// StatefulSet, Job, ReplicationController, DaemonSet). Compared to the old
/// pattern of read-modify-write via `update_resource`, this path:
///
/// - Cannot lose a concurrent user edit to `.spec` between the controller's
///   read and write — the merge happens atomically inside SQLite via
///   `json_set(data, '$.status', ?)`.
/// - Honors `metadata.resourceVersion` as a CAS guard when present (returns
///   409 Conflict on mismatch); skips the check when missing.
pub async fn write_status<S: Serialize>(
    db: &dyn DatastoreBackend,
    resource: &Value,
    status: &S,
) -> Result<Resource> {
    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .context("write_status: missing apiVersion")?;
    let kind = resource
        .get("kind")
        .and_then(|v| v.as_str())
        .context("write_status: missing kind")?;
    let metadata = resource
        .get("metadata")
        .context("write_status: missing metadata")?;
    let name = metadata
        .get("name")
        .and_then(|v| v.as_str())
        .context("write_status: missing metadata.name")?;
    let namespace = metadata.get("namespace").and_then(|v| v.as_str());
    let status_value =
        serde_json::to_value(status).context("write_status: failed to serialize status payload")?;
    let mut expected_rv = metadata
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok());
    let observed_uid = metadata
        .get("uid")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if resource.get("status") == Some(&status_value)
        && let Some(current) = db
            .get_resource(api_version, kind, namespace, name)
            .await
            .context("write_status: read current unchanged resource")?
    {
        if current.data.get("status") == Some(&status_value) {
            crate::datastore::diagnostics::log_noop_resource_write(
                crate::datastore::diagnostics::NoopResourceWrite {
                    operation: "controller_write_status",
                    api_version,
                    kind,
                    namespace,
                    name,
                    uid: &current.uid,
                    resource_version: current.resource_version,
                    reason: "status unchanged",
                },
            );
            return Ok(current);
        }
        if !same_status_retry_identity(resource, &current.data) {
            return Err(anyhow!(
                "write_status: resource spec or generation changed before status fast path"
            ));
        }
        expected_rv = Some(current.resource_version);
    }
    write_status_with_retry(
        db,
        StatusWriteRequest {
            api_version,
            kind,
            namespace,
            name,
            original: resource,
            status_value,
            expected_rv,
            observed_uid,
        },
    )
    .await
}

/// `Resource`-flavored `write_status` for callers that already hold a hydrated
/// `Resource` row (its `resource_version` column is the canonical CAS guard).
pub async fn write_status_for_resource<S: Serialize>(
    db: &dyn DatastoreBackend,
    resource: &Resource,
    status: &S,
) -> Result<Resource> {
    let status_value = serde_json::to_value(status)
        .context("write_status_for_resource: failed to serialize status payload")?;
    let mut expected_rv = Some(resource.resource_version);
    if resource.data.get("status") == Some(&status_value)
        && let Some(current) = db
            .get_resource(
                &resource.api_version,
                &resource.kind,
                resource.namespace.as_deref(),
                &resource.name,
            )
            .await
            .context("write_status_for_resource: read current unchanged resource")?
    {
        if current.data.get("status") == Some(&status_value) {
            crate::datastore::diagnostics::log_noop_resource_write(
                crate::datastore::diagnostics::NoopResourceWrite {
                    operation: "controller_write_status_for_resource",
                    api_version: &resource.api_version,
                    kind: &resource.kind,
                    namespace: resource.namespace.as_deref(),
                    name: &resource.name,
                    uid: &current.uid,
                    resource_version: current.resource_version,
                    reason: "status unchanged",
                },
            );
            return Ok(current);
        }
        if !same_status_retry_identity(&resource.data, &current.data) {
            return Err(anyhow!(
                "write_status_for_resource: resource spec or generation changed before status fast path"
            ));
        }
        expected_rv = Some(current.resource_version);
    }
    write_status_with_retry(
        db,
        StatusWriteRequest {
            api_version: &resource.api_version,
            kind: &resource.kind,
            namespace: resource.namespace.as_deref(),
            name: &resource.name,
            original: &resource.data,
            status_value,
            expected_rv,
            observed_uid: Some(resource.uid.clone()),
        },
    )
    .await
}

const STATUS_WRITE_MAX_ATTEMPTS: usize = 3;

struct StatusWriteRequest<'a> {
    api_version: &'a str,
    kind: &'a str,
    namespace: Option<&'a str>,
    name: &'a str,
    original: &'a Value,
    status_value: Value,
    expected_rv: Option<i64>,
    observed_uid: Option<String>,
}

async fn write_status_with_retry(
    db: &dyn DatastoreBackend,
    request: StatusWriteRequest<'_>,
) -> Result<Resource> {
    let StatusWriteRequest {
        api_version,
        kind,
        namespace,
        name,
        original,
        status_value,
        expected_rv,
        observed_uid,
    } = request;
    let mut expected_rv = expected_rv;

    let mut last_err = None;
    for attempt in 0..STATUS_WRITE_MAX_ATTEMPTS {
        match db
            .update_status_only_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                status_value.clone(),
                ResourcePreconditions {
                    uid: observed_uid.clone(),
                    resource_version: expected_rv,
                },
            )
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(err) if is_status_cas_error(&err) && attempt + 1 < STATUS_WRITE_MAX_ATTEMPTS => {
                let Some(current) = db
                    .get_resource(api_version, kind, namespace, name)
                    .await
                    .with_context(|| {
                        format!("write_status: reread {api_version}/{kind} {namespace:?}/{name}")
                    })?
                else {
                    return Err(err).context("write_status: resource deleted during CAS retry");
                };
                if !same_status_retry_identity(original, &current.data) {
                    return Err(err).context(
                        "write_status: resource spec or generation changed during CAS retry",
                    );
                }
                expected_rv = Some(current.resource_version);
                last_err = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_err
        .unwrap_or_else(|| anyhow!("status CAS retry exhausted without captured conflict"))
        .context("write_status: CAS retries exhausted"))
}

fn is_status_cas_error(err: &anyhow::Error) -> bool {
    crate::datastore::errors::is_conflict_error(err)
}

fn same_status_retry_identity(original: &Value, current: &Value) -> bool {
    original.get("spec") == current.get("spec")
        && metadata_generation(original) == metadata_generation(current)
}

fn metadata_generation(resource: &Value) -> Option<i64> {
    resource
        .pointer("/metadata/generation")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

/// Typed wrapper for owner references that provides safe append, remove, and lookup operations.
///
/// This is the durable replacement for the `&mut Value` stop-gap helpers in F4-01.
/// Controllers should use this type instead of manipulating `metadata.ownerReferences`
/// directly with JSON mutations.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnerReferenceList(Vec<OwnerReference>);

/// Typed owner reference based on k8s-openapi's `OwnerReference`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReference {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
    pub controller: bool,
    pub block_owner_deletion: bool,
}

impl OwnerReference {
    /// Create a new owner reference, typically used by controllers to claim child resources.
    pub fn new(
        api_version: String,
        kind: String,
        name: String,
        uid: String,
        controller: bool,
        block_owner_deletion: bool,
    ) -> Self {
        Self {
            api_version,
            kind,
            name,
            uid,
            controller,
            block_owner_deletion,
        }
    }

    /// Create a controller owner reference with `controller=true` and `blockOwnerDeletion=true`.
    pub fn controller(api_version: &str, kind: &str, name: &str, uid: &str) -> Self {
        Self::new(
            api_version.to_string(),
            kind.to_string(),
            name.to_string(),
            uid.to_string(),
            true,
            true,
        )
    }
}

impl From<K8sOwnerReference> for OwnerReference {
    fn from(k8s: K8sOwnerReference) -> Self {
        Self {
            api_version: k8s.api_version,
            kind: k8s.kind,
            name: k8s.name,
            uid: k8s.uid,
            controller: k8s.controller.unwrap_or(false),
            block_owner_deletion: k8s.block_owner_deletion.unwrap_or(false),
        }
    }
}

impl From<OwnerReference> for K8sOwnerReference {
    fn from(owner: OwnerReference) -> Self {
        Self {
            api_version: owner.api_version,
            block_owner_deletion: Some(owner.block_owner_deletion),
            controller: Some(owner.controller),
            kind: owner.kind,
            name: owner.name,
            uid: owner.uid,
        }
    }
}

impl From<OwnerReference> for Value {
    fn from(owner: OwnerReference) -> Self {
        // Only output fields that have non-default values to preserve original shape
        let mut map = serde_json::Map::new();
        if !owner.api_version.is_empty() {
            map.insert("apiVersion".to_string(), json!(owner.api_version));
        }
        map.insert("kind".to_string(), json!(owner.kind));
        if !owner.name.is_empty() {
            map.insert("name".to_string(), json!(owner.name));
        }
        map.insert("uid".to_string(), json!(owner.uid));
        if owner.controller {
            map.insert("controller".to_string(), json!(true));
        }
        if owner.block_owner_deletion {
            map.insert("blockOwnerDeletion".to_string(), json!(true));
        }
        Value::Object(map)
    }
}

impl TryFrom<&Value> for OwnerReference {
    type Error = String;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let api_version = value
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let kind = value
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or("missing kind")?
            .to_string();
        let name = value
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let uid = value
            .get("uid")
            .and_then(|v| v.as_str())
            .ok_or("missing uid")?
            .to_string();
        let controller = value
            .get("controller")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let block_owner_deletion = value
            .get("blockOwnerDeletion")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self::new(
            api_version,
            kind,
            name,
            uid,
            controller,
            block_owner_deletion,
        ))
    }
}

impl OwnerReferenceList {
    /// Create an empty owner reference list.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create an owner reference list from the JSON `metadata.ownerReferences` field.
    ///
    /// Returns `Ok(Self)` even if the field is missing or malformed (treats as empty list).
    pub fn from_json(resource: &Value) -> Self {
        let refs = resource
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| OwnerReference::try_from(v).ok())
                    .collect()
            })
            .unwrap_or_default();
        Self(refs)
    }

    /// Append an owner reference to the list.
    pub fn append(&mut self, owner: OwnerReference) {
        self.0.push(owner);
    }

    /// Remove owner references matching `kind` + `uid`.
    ///
    /// Returns `true` if at least one reference was removed, `false` otherwise.
    /// Preserves the order of remaining references.
    pub fn remove_by_uid(&mut self, kind: &str, uid: &str) -> bool {
        let before = self.0.len();
        self.0
            .retain(|owner| !(owner.kind == kind && owner.uid == uid));
        before != self.0.len()
    }

    /// Find the controller owner reference (if any).
    ///
    /// Returns `Some(&OwnerReference)` if a reference with `controller=true` exists.
    pub fn find_controller(&self) -> Option<&OwnerReference> {
        self.0.iter().find(|owner| owner.controller)
    }

    /// Check if the list contains an owner reference with the given UID.
    pub fn contains_uid(&self, uid: &str) -> bool {
        self.0.iter().any(|owner| owner.uid == uid)
    }

    /// Write the owner references to a resource's `metadata.ownerReferences` field.
    ///
    /// Creates the metadata and ownerReferences arrays if they don't exist.
    pub fn write_to_resource(&self, resource: &mut Value) {
        // Ensure metadata exists
        if resource.get("metadata").is_none() {
            resource["metadata"] = json!({});
        }

        if let Some(meta) = resource.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            let refs: Vec<Value> = self.0.iter().cloned().map(Value::from).collect();
            meta.insert("ownerReferences".to_string(), json!(refs));
        }
    }

    /// Convert to JSON array value.
    pub fn to_json_array(&self) -> Value {
        json!(self.0.iter().cloned().map(Value::from).collect::<Vec<_>>())
    }

    /// Get the underlying vector as a slice.
    pub fn as_slice(&self) -> &[OwnerReference] {
        &self.0
    }

    /// Get the number of owner references.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for OwnerReferenceList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
