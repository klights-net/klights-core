use crate::datastore::{DatastoreBackend, ResourcePreconditions};
use anyhow::{Context, Result};
use serde_json::{Value, json};

/// Inject resourceVersion into resource metadata
fn inject_resource_version(data: &mut Value, rv: i64) {
    if let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert("resourceVersion".to_string(), json!(rv.to_string()));
    }
}

/// Provision a PV for a PVC that has a storageClassName matching a known provisioner.
/// Currently supports "local-path" (hostPath under KLIGHTS_DATA_ROOT/local-path-provisioner/).
/// Returns the created PV name, or None if storageClassName is not provisioned.
async fn provision_pv_for_pvc(db: &dyn DatastoreBackend, pvc: &Value) -> Result<Option<String>> {
    let metadata = pvc
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("PVC missing metadata"))?;
    let spec = pvc
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("PVC missing spec"))?;

    let namespace = metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("PVC missing namespace"))?;
    let pvc_name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("PVC missing name"))?;

    // Check storageClassName
    let storage_class = spec.get("storageClassName").and_then(|s| s.as_str());

    // Only provision for explicitly "local-path"
    if storage_class != Some("local-path") {
        return Ok(None);
    }

    // Get PVC UID for PV name, fallback to namespace-name
    let pvc_uid = metadata.get("uid").and_then(|u| u.as_str());
    let pv_name = if let Some(uid) = pvc_uid {
        format!("pvc-{}", uid)
    } else {
        format!("pvc-{}-{}", namespace, pvc_name)
    };

    // Get requested storage and accessModes
    let requested_storage = spec
        .get("resources")
        .and_then(|r| r.get("requests"))
        .and_then(|r| r.get("storage"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| anyhow::anyhow!("PVC missing resources.requests.storage"))?;

    let access_modes = spec
        .get("accessModes")
        .and_then(|a| a.as_array())
        .ok_or_else(|| anyhow::anyhow!("PVC missing accessModes"))?;

    // Create host directory under the klights data root.
    let runtime_ns = crate::paths::runtime_namespace();
    let host_path = crate::paths::local_path_provisioner_root_path(&runtime_ns)
        .join(namespace)
        .join(pvc_name)
        .to_string_lossy()
        .into_owned();
    crate::utils::create_dir_all_async(&host_path)
        .await
        .with_context(|| format!("Failed to create directory {}", host_path))?;

    // Create PV
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": pv_name
        },
        "spec": {
            "capacity": {
                "storage": requested_storage
            },
            "accessModes": access_modes,
            "storageClassName": "local-path",
            "hostPath": {
                "path": host_path
            },
            "persistentVolumeReclaimPolicy": "Delete"
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, &pv_name, pv)
        .await?;

    tracing::info!(
        "Provisioned PV {} for PVC {}/{} (local-path)",
        pv_name,
        namespace,
        pvc_name
    );

    Ok(Some(pv_name))
}

/// Reconcile a PersistentVolumeClaim - bind to matching PersistentVolume
/// Returns the updated PVC resource
pub async fn reconcile_pvc(db: &dyn DatastoreBackend, pvc: &Value) -> Result<Value> {
    let input_metadata = pvc
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    let name = input_metadata
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing name"))?;
    let namespace = input_metadata
        .get("namespace")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?;

    let Some(live_pvc) = db
        .get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
        .await?
    else {
        return Ok(pvc.clone());
    };
    let pvc = crate::api::inject_resource_version(live_pvc.data, live_pvc.resource_version);

    let metadata = pvc
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(pvc);
    }
    let spec = pvc
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    // Check if already bound
    if let Some(status) = pvc.get("status")
        && status.get("phase").and_then(|p| p.as_str()) == Some("Bound")
    {
        // Already bound, nothing to do
        return Ok(pvc.clone());
    }

    // Extract PVC identity fields for claimRef
    let pvc_uid = metadata
        .get("uid")
        .and_then(|u| u.as_str())
        .map(String::from);
    let pvc_resource_version = metadata
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Get requested resources
    let requests = spec
        .get("resources")
        .and_then(|r| r.get("requests"))
        .ok_or_else(|| anyhow::anyhow!("Missing resources.requests"))?;

    let requested_storage = requests
        .get("storage")
        .and_then(|s| s.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing storage request"))?;

    let access_modes = spec
        .get("accessModes")
        .and_then(|a| a.as_array())
        .ok_or_else(|| anyhow::anyhow!("Missing accessModes"))?;

    // Build claimRef for the PV spec
    let mut claim_ref = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "name": name,
        "namespace": namespace
    });
    if let Some(ref uid) = pvc_uid {
        claim_ref["uid"] = json!(uid);
    }
    if let Some(ref rv) = pvc_resource_version {
        claim_ref["resourceVersion"] = json!(rv);
    }

    // Find an available PV that matches capacity and accessModes
    let pvs = db
        .list_resources(
            "v1",
            "PersistentVolume",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for pv in &pvs.items {
        // Check if PV is Available
        if let Some(phase) = pv
            .data
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
        {
            if phase != "Available" {
                continue;
            }
        } else {
            // No status yet - consider it Available
        }

        // Check capacity
        // TODO(Phase 2): PV capacity >= PVC request, not exact match
        if let Some(pv_capacity) = pv
            .data
            .get("spec")
            .and_then(|s| s.get("capacity"))
            .and_then(|c| c.get("storage"))
            .and_then(|s| s.as_str())
        {
            if pv_capacity != requested_storage {
                continue;
            }
        } else {
            continue;
        }

        // Check access modes
        if let Some(pv_access_modes) = pv
            .data
            .get("spec")
            .and_then(|s| s.get("accessModes"))
            .and_then(|a| a.as_array())
        {
            let pv_modes: Vec<String> = pv_access_modes
                .iter()
                .filter_map(|m| m.as_str().map(String::from))
                .collect();
            let pvc_modes: Vec<String> = access_modes
                .iter()
                .filter_map(|m| m.as_str().map(String::from))
                .collect();

            // PVC access modes must be a subset of PV access modes
            if !pvc_modes.iter().all(|mode| pv_modes.contains(mode)) {
                continue;
            }
        } else {
            continue;
        }

        // Found a matching PV - bind it
        let pv_name = pv
            .data
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("PV missing name"))?;

        // Inject resourceVersion into PV metadata
        let mut updated_pv: Value = (*pv.data).clone();
        inject_resource_version(&mut updated_pv, pv.resource_version);

        // Update PV status to Bound and set claimRef
        if let Some(pv_obj) = updated_pv.as_object_mut() {
            let pv_status = json!({
                "phase": "Bound",
                "accessModes": pv.data.get("spec").and_then(|s| s.get("accessModes")).cloned().unwrap_or(json!([])),
                "capacity": pv.data.get("spec").and_then(|s| s.get("capacity")).cloned().unwrap_or(json!({}))
            });
            pv_obj.insert("status".to_string(), pv_status);

            // Set claimRef in PV spec to reference the bound PVC
            let pv_spec = pv_obj
                .entry("spec".to_string())
                .or_insert_with(|| json!({}));
            if let Some(spec_obj) = pv_spec.as_object_mut() {
                spec_obj.insert("claimRef".to_string(), claim_ref.clone());
            }
        }

        db.update_resource_with_preconditions(
            "v1",
            "PersistentVolume",
            None,
            pv_name,
            updated_pv,
            ResourcePreconditions::from_resource(pv),
        )
        .await?;

        // Update PVC status to Bound
        let mut updated_pvc = pvc.clone();
        if let Some(pvc_obj) = updated_pvc.as_object_mut() {
            let pvc_status = json!({
                "phase": "Bound",
                "accessModes": access_modes,
                "capacity": {
                    "storage": requested_storage
                },
                "volumeName": pv_name
            });
            pvc_obj.insert("status".to_string(), pvc_status);
        }

        let pvc_metadata = updated_pvc
            .get("metadata")
            .ok_or_else(|| anyhow::anyhow!("PVC missing metadata"))?;
        let pvc_rv = crate::utils::extract_resource_version(pvc_metadata);
        let pvc_preconditions = ResourcePreconditions::from_metadata(pvc_metadata, pvc_rv)?;

        let result = db
            .update_resource_with_preconditions(
                "v1",
                "PersistentVolumeClaim",
                Some(namespace),
                name,
                updated_pvc,
                pvc_preconditions,
            )
            .await?;

        return Ok(std::sync::Arc::unwrap_or_clone(result.data));
    }

    // No matching PV found - try to provision a PV
    if let Some(provisioned_pv_name) = provision_pv_for_pvc(db, &pvc).await? {
        // PV was provisioned, now bind PVC to it
        let provisioned_pv = db
            .get_resource("v1", "PersistentVolume", None, &provisioned_pv_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Provisioned PV not found"))?;

        // Inject resourceVersion into PV
        let mut updated_pv: Value = (*provisioned_pv.data).clone();
        inject_resource_version(&mut updated_pv, provisioned_pv.resource_version);

        // Update PV status to Bound
        let pv_access_modes = updated_pv
            .get("spec")
            .and_then(|s| s.get("accessModes"))
            .cloned()
            .unwrap_or(json!([]));
        let pv_capacity = updated_pv
            .get("spec")
            .and_then(|s| s.get("capacity"))
            .cloned()
            .unwrap_or(json!({}));

        if let Some(pv_obj) = updated_pv.as_object_mut() {
            let pv_status = json!({
                "phase": "Bound",
                "accessModes": pv_access_modes,
                "capacity": pv_capacity
            });
            pv_obj.insert("status".to_string(), pv_status);

            // Set claimRef in PV spec to reference the bound PVC
            let pv_spec = pv_obj
                .entry("spec".to_string())
                .or_insert_with(|| json!({}));
            if let Some(spec_obj) = pv_spec.as_object_mut() {
                spec_obj.insert("claimRef".to_string(), claim_ref.clone());
            }
        }

        let pv_metadata = updated_pv
            .get("metadata")
            .ok_or_else(|| anyhow::anyhow!("PV missing metadata"))?;
        let pv_rv = crate::utils::extract_resource_version(pv_metadata);
        let pv_preconditions = ResourcePreconditions::from_metadata(pv_metadata, pv_rv)?;

        db.update_resource_with_preconditions(
            "v1",
            "PersistentVolume",
            None,
            &provisioned_pv_name,
            updated_pv,
            pv_preconditions,
        )
        .await?;

        // Update PVC status to Bound
        let mut updated_pvc = pvc.clone();
        if let Some(pvc_obj) = updated_pvc.as_object_mut() {
            let pvc_status = json!({
                "phase": "Bound",
                "accessModes": access_modes,
                "capacity": {
                    "storage": requested_storage
                },
                "volumeName": provisioned_pv_name
            });
            pvc_obj.insert("status".to_string(), pvc_status);
        }

        let pvc_metadata = updated_pvc
            .get("metadata")
            .ok_or_else(|| anyhow::anyhow!("PVC missing metadata"))?;
        let pvc_rv = crate::utils::extract_resource_version(pvc_metadata);
        let pvc_preconditions = ResourcePreconditions::from_metadata(pvc_metadata, pvc_rv)?;
        let pvc_name = metadata
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("PVC missing name"))?;

        let result = db
            .update_resource_with_preconditions(
                "v1",
                "PersistentVolumeClaim",
                Some(namespace),
                pvc_name,
                updated_pvc,
                pvc_preconditions,
            )
            .await?;

        return Ok(std::sync::Arc::unwrap_or_clone(result.data));
    }

    if pvc
        .pointer("/status/phase")
        .and_then(|phase| phase.as_str())
        == Some("Pending")
    {
        return Ok(pvc.clone());
    }

    // No provisioning happened - set status to Pending
    let mut updated_pvc = pvc.clone();
    if let Some(pvc_obj) = updated_pvc.as_object_mut() {
        let pvc_status = json!({
            "phase": "Pending"
        });
        pvc_obj.insert("status".to_string(), pvc_status);
    }

    let pvc_metadata = updated_pvc
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("PVC missing metadata"))?;
    let pvc_rv = crate::utils::extract_resource_version(pvc_metadata);
    let pvc_preconditions = ResourcePreconditions::from_metadata(pvc_metadata, pvc_rv)?;

    let result = db
        .update_resource_with_preconditions(
            "v1",
            "PersistentVolumeClaim",
            Some(namespace),
            name,
            updated_pvc,
            pvc_preconditions,
        )
        .await?;

    Ok(std::sync::Arc::unwrap_or_clone(result.data))
}

#[cfg(test)]
mod tests;
