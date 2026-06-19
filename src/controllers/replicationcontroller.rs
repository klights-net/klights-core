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
use futures::future::{poll_fn, select_all};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::Poll;

type ReplicationControllerReconcileLocks = HashMap<String, Arc<tokio::sync::Mutex<()>>>;
type PodCreateFuture<'a> =
    Pin<Box<dyn Future<Output = Result<crate::datastore::Resource>> + Send + 'a>>;

enum ScaleUpPollResult {
    Status(Result<()>),
    Create(Result<crate::datastore::Resource>),
}

struct ScaleUpProgress<'state, 'future> {
    in_flight_creates: &'state mut Vec<PodCreateFuture<'future>>,
    owned_pods: &'state mut Vec<crate::datastore::Resource>,
    created_in_reconcile: &'state mut usize,
    creation_failure: &'state mut Option<String>,
}

const RC_SCALE_UP_PROGRESS_INTERVAL: usize = 10;
const RC_SCALE_UP_MAX_IN_FLIGHT: usize = 4;

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
        let mut created_in_reconcile = 0usize;
        let mut stop_starting_creates = false;
        let mut in_flight_creates: Vec<PodCreateFuture<'_>> = Vec::new();

        loop {
            if !stop_starting_creates && in_flight_creates.is_empty() {
                let create_batch_limit =
                    scale_up_create_concurrency_limit(db, namespace, template).await?;
                while in_flight_creates.len() < create_batch_limit
                    && current_replicas + created_in_reconcile + in_flight_creates.len()
                        < desired_replicas
                {
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
                    if current_replicas + created_in_reconcile + in_flight_creates.len()
                        >= live_replicas
                    {
                        stop_starting_creates = true;
                        break;
                    }
                    in_flight_creates.push(Box::pin(create_pod(
                        pod_writer, rc_name, rc_uid, namespace, node_name, template,
                    )));
                }
            }

            if in_flight_creates.is_empty() {
                break;
            }
            let (create_result, _completed_index, remaining_creates) =
                select_all(in_flight_creates).await;
            in_flight_creates = remaining_creates;

            match create_result {
                Ok(created) => {
                    owned_pods.push(created);
                    created_in_reconcile += 1;
                    if should_publish_scale_up_progress(
                        current_replicas,
                        desired_replicas,
                        created_in_reconcile,
                    ) {
                        let status_pods = owned_pods.clone();
                        update_replicationcontroller_status_while_polling_creates(
                            db,
                            rc_name,
                            namespace,
                            status_pods,
                            ScaleUpProgress {
                                in_flight_creates: &mut in_flight_creates,
                                owned_pods: &mut owned_pods,
                                created_in_reconcile: &mut created_in_reconcile,
                                creation_failure: &mut creation_failure,
                            },
                        )
                        .await?;
                        if creation_failure.is_some() {
                            stop_starting_creates = true;
                        }
                    }
                }
                Err(err) => {
                    creation_failure = Some(err.to_string());
                    stop_starting_creates = true;
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
    let mut current_owned_pods = pod_reader
        .list_pods(Some(namespace), None, None, None, None)
        .await?
        .items
        .into_iter()
        .filter(|pod| {
            common.is_owned_by(&pod.data, rc_uid) && pod_matches_selector(&pod.data, &selector)
        })
        .collect::<Vec<_>>();

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
    let live_desired_replicas = live_rc
        .data
        .pointer("/spec/replicas")
        .and_then(|r| r.as_i64())
        .unwrap_or(1)
        .max(0) as usize;
    let active_after_scale = active_replicationcontroller_pods(&current_owned_pods);
    if active_after_scale.len() > live_desired_replicas {
        let surplus = active_after_scale.len() - live_desired_replicas;
        let surplus_pod_names = active_after_scale
            .iter()
            .take(surplus)
            .map(|pod| pod.name.clone())
            .collect::<Vec<_>>();
        drop(active_after_scale);
        for pod_name in surplus_pod_names {
            pod_writer.delete_pod(namespace, &pod_name).await?;
        }
        current_owned_pods = pod_reader
            .list_pods(Some(namespace), None, None, None, None)
            .await?
            .items
            .into_iter()
            .filter(|pod| {
                common.is_owned_by(&pod.data, rc_uid) && pod_matches_selector(&pod.data, &selector)
            })
            .collect::<Vec<_>>();
    }

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

async fn scale_up_create_concurrency_limit(
    db: &dyn DatastoreBackend,
    namespace: &str,
    template: &Value,
) -> Result<usize> {
    let quota_list = db
        .list_resources(
            "v1",
            "ResourceQuota",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    if quota_list.items.is_empty() {
        return Ok(RC_SCALE_UP_MAX_IN_FLIGHT);
    }

    let quota_probe_pod = quota_probe_pod_from_template(namespace, template);
    let has_matching_pod_quota = quota_list
        .items
        .iter()
        .any(|quota| resource_quota_constrains_pod_creates(&quota.data, &quota_probe_pod));
    Ok(if has_matching_pod_quota {
        1
    } else {
        RC_SCALE_UP_MAX_IN_FLIGHT
    })
}

fn quota_probe_pod_from_template(namespace: &str, template: &Value) -> Value {
    json!({
        "metadata": {
            "namespace": namespace,
            "labels": template.pointer("/metadata/labels").cloned().unwrap_or_else(|| json!({}))
        },
        "spec": template.get("spec").cloned().unwrap_or_else(|| json!({}))
    })
}

fn resource_quota_constrains_pod_creates(quota: &Value, pod: &Value) -> bool {
    let Some(hard) = quota
        .pointer("/spec/hard")
        .and_then(|value| value.as_object())
    else {
        return false;
    };
    let scopes = quota
        .pointer("/spec/scopes")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if !scopes.is_empty() && !crate::controllers::resource_quota::pod_matches_scopes(pod, &scopes) {
        return false;
    }

    hard.keys().any(|key| pod_relevant_quota_key(key))
}

fn pod_relevant_quota_key(key: &str) -> bool {
    matches!(
        key,
        "pods" | "count/pods" | "cpu" | "memory" | "ephemeral-storage"
    ) || key
        .strip_prefix("requests.")
        .is_some_and(|resource| !resource.is_empty())
        || key
            .strip_prefix("limits.")
            .is_some_and(|resource| !resource.is_empty())
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

async fn update_replicationcontroller_status_while_polling_creates<'a>(
    db: &dyn DatastoreBackend,
    name: &str,
    namespace: &str,
    status_pods: Vec<crate::datastore::Resource>,
    progress: ScaleUpProgress<'_, 'a>,
) -> Result<()> {
    let ScaleUpProgress {
        in_flight_creates,
        owned_pods,
        created_in_reconcile,
        creation_failure,
    } = progress;
    let mut status_update = Box::pin(update_replicationcontroller_status(
        db,
        name,
        namespace,
        &status_pods,
        None,
    ));

    loop {
        match poll_fn(|cx| {
            let mut index = 0usize;
            while index < in_flight_creates.len() {
                match in_flight_creates[index].as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        let _completed = in_flight_creates.swap_remove(index);
                        return Poll::Ready(ScaleUpPollResult::Create(result));
                    }
                    Poll::Pending => index += 1,
                }
            }

            match status_update.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(ScaleUpPollResult::Status(result)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
        {
            ScaleUpPollResult::Status(result) => return result,
            ScaleUpPollResult::Create(Ok(created)) => {
                owned_pods.push(created);
                *created_in_reconcile += 1;
            }
            ScaleUpPollResult::Create(Err(err)) => {
                if creation_failure.is_none() {
                    *creation_failure = Some(err.to_string());
                }
            }
        }
    }
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
