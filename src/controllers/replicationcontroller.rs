//! ReplicationController core reconcile logic
//!
//! RC is the legacy predecessor of ReplicaSet. Key differences:
//! - Uses simple selector (map of key=value) instead of matchLabels
//! - API version is v1 (core) not apps/v1
//! - Otherwise functionally identical to ReplicaSet

use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use crate::label_selector::LabelSelector;
use anyhow::{Context as _, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

type ReplicationControllerReconcileLocks = HashMap<String, Arc<tokio::sync::Mutex<()>>>;

const RC_SCALE_UP_PROGRESS_INTERVAL: usize = 10;

static REPLICATIONCONTROLLER_RECONCILE_LOCKS: LazyLock<
    tokio::sync::Mutex<ReplicationControllerReconcileLocks>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

async fn replicationcontroller_reconcile_lock(
    namespace: &str,
    name: &str,
) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = REPLICATIONCONTROLLER_RECONCILE_LOCKS.lock().await;
    locks
        .entry(format!("{namespace}/{name}"))
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn is_controller_owner_ref(owner_ref: &Value) -> bool {
    owner_ref
        .get("controller")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Reconcile a ReplicationController to match desired state
pub async fn reconcile_replicationcontroller(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
    rc: &Value,
    node_name: &str,
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    let rc_name = rc["metadata"]["name"].as_str().context("RC missing name")?;
    let namespace = rc["metadata"]["namespace"]
        .as_str()
        .context("RC missing namespace")?;
    let reconcile_lock = replicationcontroller_reconcile_lock(namespace, rc_name).await;
    let _reconcile_guard = reconcile_lock.lock().await;

    let live_resource = match db
        .get_resource("v1", "ReplicationController", Some(namespace), rc_name)
        .await?
    {
        Some(resource) => resource,
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
                .get_resource("v1", "ReplicationController", Some(namespace), rc_name)
                .await?
            {
                Some(resource) => resource,
                None => return Ok(()),
            }
        }
        _ => live_resource,
    };

    let rc =
        crate::api::inject_resource_version(live_resource.data, live_resource.resource_version);

    if rc.pointer("/metadata/deletionTimestamp").is_some() {
        return Ok(());
    }

    let rc_uid = rc["metadata"]["uid"].as_str().context("RC missing uid")?;

    // Extract spec
    let desired_replicas = rc["spec"]["replicas"].as_u64().unwrap_or(1) as usize;
    let selector_value = rc.get("spec").and_then(|s| s.get("selector"));
    let selector = match selector_value {
        Some(v) => match LabelSelector::from_flat_match_labels(v) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(namespace, name = rc_name, "RC selector parse error: {e:#}");
                return Ok(());
            }
        },
        None => {
            tracing::warn!(namespace, name = rc_name, "RC missing selector");
            return Ok(());
        }
    };
    let template = &rc["spec"]["template"];

    // Find all pods matching selector
    let all_pods_result = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?;

    let mut owned_pods = Vec::new();
    for pod in all_pods_result.items {
        let matches_selector = pod_matches_selector(&pod.data, &selector);

        // Check if pod is owned by this RC
        if let Some(owner_refs) = pod.data["metadata"]["ownerReferences"].as_array()
            && owner_refs.iter().any(|o| {
                is_controller_owner_ref(o)
                    && o["kind"] == "ReplicationController"
                    && o["name"] == rc_name
                    && o["uid"] == rc_uid
            })
        {
            if matches_selector {
                owned_pods.push(pod);
            } else {
                // Release pod when it no longer matches selector.
                let released_refs: Vec<Value> = pod
                    .data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|o| {
                        !(is_controller_owner_ref(o)
                            && o["kind"] == "ReplicationController"
                            && o["name"] == rc_name
                            && o["uid"] == rc_uid)
                    })
                    .collect();
                pod_writer
                    .update_pod_owner_references(namespace, &pod.name.clone(), released_refs)
                    .await?;
            }
            continue;
        }

        // Check if pod matches selector and can be adopted.
        // Only adopt truly orphan pods; never steal from another controller owner.
        if matches_selector && !pod_has_controller_owner(&pod.data) {
            // Adopt orphaned pod
            let owner_ref = common.build_owner_ref("v1", "ReplicationController", rc_name, rc_uid);
            let mut owner_refs: Vec<Value> = pod
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            owner_refs.push(owner_ref);

            pod_writer
                .update_pod_owner_references(namespace, &pod.name.clone(), owner_refs)
                .await?;

            owned_pods.push(pod);
        }
    }

    // Count non-terminating pods
    let active_pods = active_replicationcontroller_pods(&owned_pods);

    let current_replicas = active_pods.len();

    // Scale up or down.
    let mut creation_failure: Option<String> = None;
    if current_replicas < desired_replicas {
        let to_create = desired_replicas - current_replicas;
        let mut created_in_reconcile = 0usize;
        for _ in 0..to_create {
            let Some(live_rc) = db
                .get_resource("v1", "ReplicationController", Some(namespace), rc_name)
                .await?
            else {
                return Ok(());
            };
            if live_rc
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_some()
            {
                return Ok(());
            }
            let live_replicas = live_rc
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1)
                .max(0) as usize;
            if current_replicas + created_in_reconcile >= live_replicas {
                break;
            }
            match create_pod(pod_writer, rc_name, rc_uid, namespace, node_name, template).await {
                Ok(created) => {
                    owned_pods.push(created);
                    created_in_reconcile += 1;
                    if should_publish_scale_up_progress(
                        current_replicas,
                        desired_replicas,
                        created_in_reconcile,
                    ) {
                        update_replicationcontroller_status(
                            db,
                            rc_name,
                            namespace,
                            &owned_pods,
                            None,
                        )
                        .await?;
                    }
                }
                Err(err) => {
                    creation_failure = Some(err.to_string());
                    break;
                }
            }
        }
    } else if current_replicas > desired_replicas {
        let to_delete = current_replicas - desired_replicas;
        for (deleted, pod) in active_pods.iter().take(to_delete).enumerate() {
            let Some(live_rc) = db
                .get_resource("v1", "ReplicationController", Some(namespace), rc_name)
                .await?
            else {
                return Ok(());
            };
            if live_rc
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_some()
            {
                return Ok(());
            }
            let live_replicas = live_rc
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1)
                .max(0) as usize;
            if current_replicas.saturating_sub(deleted) <= live_replicas {
                break;
            }
            pod_writer.delete_pod(namespace, &pod.name.clone()).await?;
        }
    }

    // Re-query owned pods after scale operations to get fresh state.
    let current_owned_pods = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?
        .items
        .into_iter()
        .filter(|pod| {
            common.is_owned_by(&pod.data, rc_uid) && pod_matches_selector(&pod.data, &selector)
        })
        .collect::<Vec<_>>();

    // Update RC status, including the ReplicaFailure condition if any pod creation failed.
    update_replicationcontroller_status(
        db,
        rc_name,
        namespace,
        &current_owned_pods,
        creation_failure.as_deref(),
    )
    .await?;

    if let Some(msg) = creation_failure {
        return Err(anyhow::anyhow!(msg));
    }

    Ok(())
}

fn should_publish_scale_up_progress(
    starting_replicas: usize,
    desired_replicas: usize,
    created_in_reconcile: usize,
) -> bool {
    if created_in_reconcile == 0 {
        return false;
    }

    let observed_replicas = starting_replicas + created_in_reconcile;
    created_in_reconcile == 1
        || observed_replicas == desired_replicas
        || observed_replicas.is_multiple_of(RC_SCALE_UP_PROGRESS_INTERVAL)
}

/// Check if pod labels match RC selector. An empty selector matches nothing
/// for RC adoption safety — prevents mass-adoption of unlabeled pods.
fn pod_matches_selector(pod: &Value, selector: &LabelSelector) -> bool {
    if selector.requirements().is_empty() {
        return false;
    }
    selector.matches_resource(pod)
}

fn pod_has_controller_owner(pod: &Value) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .is_some_and(|refs| {
            refs.iter()
                .any(|owner| owner.get("controller").and_then(|v| v.as_bool()) == Some(true))
        })
}

fn active_replicationcontroller_pods(
    pods: &[crate::datastore::Resource],
) -> Vec<&crate::datastore::Resource> {
    pods.iter()
        .filter(|p| {
            p.data["metadata"]["deletionTimestamp"].is_null()
                && p.data["status"]["phase"].as_str() != Some("Succeeded")
                && p.data["status"]["phase"].as_str() != Some("Failed")
        })
        .collect()
}

/// Create a pod from RC template
async fn create_pod(
    pod_writer: &dyn PodObjectWriter,
    rc_name: &str,
    rc_uid: &str,
    namespace: &str,
    node_name: &str,
    template: &Value,
) -> Result<crate::datastore::Resource> {
    let pod_name = format!(
        "{}-{}",
        rc_name,
        uuid::Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("xxxxx")
    );
    let pod = crate::controllers::common::build_child_pod(
        template,
        &pod_name,
        namespace,
        "",
        crate::controllers::common::OwnerInfo {
            api_version: "v1",
            kind: "ReplicationController",
            name: rc_name,
            uid: rc_uid,
        },
        &[],
        &[],
    )?;

    let created = pod_writer
        .create_controller_pod(namespace, &pod_name, node_name, pod)
        .await?;

    Ok(created)
}

#[cfg(test)]
mod tests;

/// Update RC status
/// Update RC status.replicas/readyReplicas and publish/clear the
/// `ReplicaFailure` condition. K8s RC controller sets this condition
/// (type=ReplicaFailure, status=True, reason=FailedCreate) whenever a
/// pod creation attempt fails (quota exceeded, invalid spec, etc.), and
/// clears it when all desired replicas are running. Conformance test
/// P0-E2E-20260423-06 verifies the condition surfaces within the timeout.
async fn update_replicationcontroller_status(
    db: &dyn DatastoreBackend,
    name: &str,
    namespace: &str,
    owned_pods: &[crate::datastore::Resource],
    creation_failure: Option<&str>,
) -> Result<()> {
    // Get current RC first so status update can preserve condition history and
    // report the currently observed generation.
    let rc = db
        .get_resource("v1", "ReplicationController", Some(namespace), name)
        .await?
        .context("RC not found")?;

    let active_pods = active_replicationcontroller_pods(owned_pods);

    let ready_pods = active_pods
        .iter()
        .filter(|p| crate::controllers::common::is_pod_ready_value(&p.data))
        .count();

    // Preserve all non-ReplicaFailure conditions, then upsert ReplicaFailure
    // only while create failures are present. Kubernetes expects this
    // condition to be absent once healthy.
    let mut conditions = rc
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|c| c["type"] != "ReplicaFailure")
        .collect::<Vec<_>>();

    let now = crate::utils::k8s_time_now();
    if let Some(msg) = creation_failure {
        conditions.push(json!({
            "type": "ReplicaFailure",
            "status": "True",
            "reason": "FailedCreate",
            "message": msg,
            "lastTransitionTime": now
        }));
    }

    let observed_generation = rc
        .data
        .pointer("/metadata/generation")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let status = json!({
        "replicas": active_pods.len(),
        "fullyLabeledReplicas": active_pods.len(),
        "readyReplicas": ready_pods,
        "availableReplicas": ready_pods,
        "observedGeneration": observed_generation,
        "conditions": conditions
    });

    crate::controllers::common::write_status_for_resource(db, &rc, &status).await?;

    Ok(())
}

#[cfg(test)]
mod condition_tests;
