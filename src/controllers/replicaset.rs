use crate::controllers::gc::GcPodDeleteSink;
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

type ReplicaSetReconcileLocks = HashMap<String, Arc<tokio::sync::Mutex<()>>>;

static REPLICASET_RECONCILE_LOCKS: LazyLock<tokio::sync::Mutex<ReplicaSetReconcileLocks>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

async fn replicaset_reconcile_lock(namespace: &str, name: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = REPLICASET_RECONCILE_LOCKS.lock().await;
    locks
        .entry(format!("{namespace}/{name}"))
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

pub async fn reconcile_replicaset(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn GcPodDeleteSink,
    replicaset: &Value,
    node_name: &str,
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    let input_metadata = replicaset
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
    let reconcile_lock = replicaset_reconcile_lock(namespace, name).await;
    let _reconcile_guard = reconcile_lock.lock().await;

    // Preserve validation semantics for malformed reconcile payloads.
    // Controller tests expect missing spec to be rejected.
    replicaset
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    // Re-read the live object from storage. Watch/retry queues can deliver stale
    // snapshots after a ReplicaSet has already been deleted; reconciling that
    // stale payload must not recreate pods.
    let live_resource = match db
        .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
        .await?
    {
        Some(r) => r,
        None => return Ok(()),
    };

    let live_resource = match crate::controllers::gc::reconcile_owner_references(
        db,
        live_resource.clone(),
        pod_delete_sink,
    )
    .await?
    {
        crate::controllers::gc::OwnerReferenceReconcile::Deleted => return Ok(()),
        crate::controllers::gc::OwnerReferenceReconcile::OwnerReferencesUpdated => {
            match db
                .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
                .await?
            {
                Some(r) => r,
                None => return Ok(()),
            }
        }
        _ => live_resource,
    };

    let live_replicaset =
        crate::api::inject_resource_version(live_resource.data, live_resource.resource_version);

    let metadata = live_replicaset
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Skip reconciliation if the resource is being deleted
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    let spec = live_replicaset
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;
    let uid = metadata
        .get("uid")
        .and_then(|u| u.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing uid"))?;

    let replicas = spec.get("replicas").and_then(|r| r.as_i64()).unwrap_or(1) as usize;
    let template = spec
        .get("template")
        .ok_or_else(|| anyhow::anyhow!("Missing template"))?;
    let selector = spec
        .get("selector")
        .ok_or_else(|| anyhow::anyhow!("Missing selector"))?;
    let owned_by_deployment = replicaset_owned_by_deployment(metadata);

    // Fetch pods owned by this RS across every ownerReferences entry.
    let rs_owned = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;

    // Release pods that no longer match the selector.
    let mut owned_pods = Vec::new();
    for pod in rs_owned {
        if !pod_matches_selector_or_pending_hash_stamp(&pod.data, selector, owned_by_deployment) {
            let mut released_pod: Value = (*pod.data).clone();
            if crate::controllers::common::remove_owner_reference_by_uid(
                &mut released_pod,
                "ReplicaSet",
                uid,
            ) {
                let owner_refs = released_pod
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                pod_writer
                    .update_pod_owner_references(namespace, &pod.name, owner_refs)
                    .await?;
            }
        } else if pod_is_active(&pod.data) {
            owned_pods.push(pod);
        }
    }

    // Orphan adoption: full namespace scan only when we have fewer pods than desired.
    // This path is rare (only when pods exist with no controller owner that match our selector).
    if owned_pods.len() < replicas {
        let all_pods = pod_reader
            .list_pods(Some(namespace), None, None, None, None)
            .await?
            .items;
        for pod in all_pods {
            if pod_owned_by_replicaset(&pod.data, uid) {
                continue; // already in owned_pods
            }
            if pod_matches_selector(&pod.data, selector)
                && !pod_has_controller_owner(&pod.data)
                && pod_is_active(&pod.data)
            {
                let mut adopted_pod: Value = (*pod.data).clone();
                crate::controllers::common::append_owner_reference(
                    &mut adopted_pod,
                    common.build_owner_ref("apps/v1", "ReplicaSet", name, uid),
                );
                let owner_refs = adopted_pod
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                pod_writer
                    .update_pod_owner_references(namespace, &pod.name, owner_refs)
                    .await?;
                owned_pods.push(pod);
            }
        }
    }

    let current_replicas = owned_pods.len();

    // Create pods if we have fewer than desired replicas
    if current_replicas < replicas {
        let mut created_or_existing = current_replicas;
        while created_or_existing < replicas {
            // Re-check the live RS before each create. A concurrent Deployment
            // reconcile can lower spec.replicas while this loop is in flight;
            // continuing from the stale count would create excess pods.
            let Some(live_rs) = db
                .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
                .await?
            else {
                return Ok(());
            };
            if live_rs
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_some()
            {
                return Ok(());
            }
            let live_replicas = live_rs
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1)
                .max(0) as usize;
            if created_or_existing >= live_replicas {
                break;
            }
            create_pod(pod_writer, name, uid, namespace, node_name, template).await?;
            created_or_existing += 1;
        }
    }

    // Delete excess pods if we have more than desired replicas
    if current_replicas > replicas {
        let excess = current_replicas - replicas;
        let mut deleted = 0usize;
        for pod_resource in owned_pods.iter().rev().take(excess) {
            let Some(live_rs) = db
                .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
                .await?
            else {
                return Ok(());
            };
            if live_rs
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_some()
            {
                return Ok(());
            }
            let live_replicas = live_rs
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1)
                .max(0) as usize;
            if current_replicas.saturating_sub(deleted) <= live_replicas {
                break;
            }
            let pod_name = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let pod_ns = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            if !pod_name.is_empty() && !pod_ns.is_empty() {
                pod_writer.delete_pod(pod_ns, pod_name).await?;
                deleted += 1;
            }
        }
    }

    // Re-query owned pods to get fresh state (may have changed since the scale operations above)
    let current_owned_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let active_current_owned_pods: Vec<_> = current_owned_pods
        .iter()
        .filter(|p| pod_is_active(&p.data))
        .cloned()
        .collect();
    let Some(status_resource) = db
        .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
        .await?
    else {
        return Ok(());
    };
    let status_replicaset =
        crate::api::inject_resource_version(status_resource.data, status_resource.resource_version);
    let status_metadata = status_replicaset
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Count pods with Ready=True condition (not terminating)
    let ready_replicas = common.count_ready_pods(&active_current_owned_pods);
    let observed_generation = status_metadata
        .get("generation")
        .and_then(|g| g.as_u64())
        .unwrap_or(1);

    // Preserve conditions set via UpdateStatus — the RS controller only owns replica
    // count fields, not conditions (which are set by external callers or higher-level controllers).
    let existing_conditions = status_replicaset
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut status = json!({
        "replicas": active_current_owned_pods.len(),
        "readyReplicas": ready_replicas,
        "availableReplicas": ready_replicas,
        "fullyLabeledReplicas": active_current_owned_pods.len(),
        "observedGeneration": observed_generation,
    });
    if !existing_conditions.is_empty() {
        status["conditions"] = Value::Array(existing_conditions);
    }

    crate::controllers::common::write_status(db, &status_replicaset, &status).await?;

    Ok(())
}

fn pod_matches_selector(pod: &Value, selector: &Value) -> bool {
    let parsed = match crate::label_selector::LabelSelector::from_k8s_selector(selector) {
        Ok(p) => p,
        // Malformed selector → match nothing (a Pod with no labels still
        // wouldn't match an unparseable selector).
        Err(_) => return false,
    };
    parsed.matches_resource(pod)
}

fn pod_matches_selector_or_pending_hash_stamp(
    pod: &Value,
    selector: &Value,
    owned_by_deployment: bool,
) -> bool {
    if pod_matches_selector(pod, selector) {
        return true;
    }
    if !owned_by_deployment {
        return false;
    }

    let selector_has_hash = selector
        .pointer("/matchLabels/pod-template-hash")
        .and_then(|v| v.as_str())
        .is_some_and(|hash| !hash.is_empty());
    if !selector_has_hash {
        return false;
    }
    if pod
        .pointer("/metadata/labels/pod-template-hash")
        .and_then(|v| v.as_str())
        .is_some()
    {
        return false;
    }

    let mut selector_without_hash = selector.clone();
    let Some(match_labels) = selector_without_hash
        .pointer_mut("/matchLabels")
        .and_then(|v| v.as_object_mut())
    else {
        return false;
    };
    if match_labels.remove("pod-template-hash").is_none() {
        return false;
    }

    // Deployment adoption stamps pod-template-hash onto existing RS pods via
    // PodObjectWriter. In leader multinode mode that metadata write is queued
    // through the outbox, so an immediate scale-down reconcile can see the
    // ownerRef before the label. Treat only this missing injected hash as a
    // temporary match; all other selector drift still releases the pod.
    pod_matches_selector(pod, &selector_without_hash)
}

fn replicaset_owned_by_deployment(metadata: &Value) -> bool {
    metadata
        .pointer("/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|owner| {
                owner.get("apiVersion").and_then(|v| v.as_str()) == Some("apps/v1")
                    && owner.get("kind").and_then(|v| v.as_str()) == Some("Deployment")
                    && owner.get("controller").and_then(|v| v.as_bool()) == Some(true)
            })
        })
}

fn pod_owned_by_replicaset(pod: &Value, rs_uid: &str) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter().any(|owner| {
                owner.get("kind").and_then(|v| v.as_str()) == Some("ReplicaSet")
                    && owner.get("uid").and_then(|v| v.as_str()) == Some(rs_uid)
            })
        })
}

fn pod_is_terminating(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp").is_some()
}

fn pod_is_active(pod: &Value) -> bool {
    !pod_is_terminating(pod)
        && !matches!(
            pod.pointer("/status/phase").and_then(|v| v.as_str()),
            Some("Succeeded" | "Failed")
        )
}

fn pod_has_controller_owner(pod: &Value) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter()
                .any(|owner| owner.get("controller").and_then(|v| v.as_bool()) == Some(true))
        })
}

async fn create_pod(
    pod_writer: &dyn PodObjectWriter,
    rs_name: &str,
    rs_uid: &str,
    namespace: &str,
    node_name: &str,
    template: &Value,
) -> Result<()> {
    let pod_name = format!(
        "{}-{}",
        rs_name,
        uuid::Uuid::new_v4()
            .to_string()
            .chars()
            .take(5)
            .collect::<String>()
    );
    let pod = crate::controllers::common::build_child_pod(
        template,
        &pod_name,
        namespace,
        "",
        crate::controllers::common::OwnerInfo {
            api_version: "apps/v1",
            kind: "ReplicaSet",
            name: rs_name,
            uid: rs_uid,
        },
        &[],
        &[],
    )?;

    pod_writer
        .create_controller_pod(namespace, &pod_name, node_name, pod)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
