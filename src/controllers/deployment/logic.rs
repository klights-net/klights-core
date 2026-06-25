use crate::datastore::{
    DatastoreBackend, PatchKind, Resource, ResourcePatchRequest, ResourcePreconditions,
};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use tracing;

use super::finalize::{apply_revision_and_gc, build_conditions_and_revision};
use super::helpers::{
    compute_pod_template_hash, count_deployment_pods, get_max_surge, get_max_unavailable,
    get_next_revision, labels_match_selector, templates_match,
};

const DESIRED_REPLICAS_ANNOTATION: &str = "deployment.kubernetes.io/desired-replicas";
const MAX_REPLICAS_ANNOTATION: &str = "deployment.kubernetes.io/max-replicas";

fn is_rolling_update_strategy(spec: &Value) -> bool {
    spec.get("strategy")
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("RollingUpdate")
        == "RollingUpdate"
}

fn max_replicas_for_annotations(spec: &Value, desired_replicas: i64) -> i64 {
    if desired_replicas <= 0 || !is_rolling_update_strategy(spec) {
        desired_replicas.max(0)
    } else {
        desired_replicas + get_max_surge(spec, desired_replicas)
    }
}

fn ensure_metadata_labels(value: &mut Value) -> Option<&mut serde_json::Map<String, Value>> {
    let obj = value.as_object_mut()?;
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| json!({}));
    if !metadata.is_object() {
        *metadata = json!({});
    }
    let labels = metadata
        .as_object_mut()?
        .entry("labels".to_string())
        .or_insert_with(|| json!({}));
    if !labels.is_object() {
        *labels = json!({});
    }
    labels.as_object_mut()
}

fn stamp_replicaset_pod_template_hash(replicaset: &mut Value, pod_template_hash: &str) {
    if let Some(labels) = ensure_metadata_labels(replicaset) {
        labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));
    }

    let Some(spec) = replicaset.get_mut("spec").and_then(|s| s.as_object_mut()) else {
        return;
    };

    let selector = spec
        .entry("selector".to_string())
        .or_insert_with(|| json!({}));
    if !selector.is_object() {
        *selector = json!({});
    }
    let match_labels = selector
        .as_object_mut()
        .unwrap()
        .entry("matchLabels".to_string())
        .or_insert_with(|| json!({}));
    if !match_labels.is_object() {
        *match_labels = json!({});
    }
    if let Some(labels) = match_labels.as_object_mut() {
        labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));
    }

    let template = spec
        .entry("template".to_string())
        .or_insert_with(|| json!({}));
    if !template.is_object() {
        *template = json!({});
    }
    if let Some(labels) = ensure_metadata_labels(template) {
        labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));
    }
}

async fn stamp_existing_replicaset_pods(
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    namespace: &str,
    rs_uid: &str,
    pod_template_hash: &str,
) -> Result<()> {
    let pods = pod_reader.list_pods_by_owner_uid(namespace, rs_uid).await?;
    for pod in pods {
        if pod
            .data
            .pointer("/metadata/labels/pod-template-hash")
            .and_then(|v| v.as_str())
            == Some(pod_template_hash)
        {
            continue;
        }
        pod_writer
            .merge_pod_labels(
                namespace,
                &pod.name,
                vec![(
                    "pod-template-hash".to_string(),
                    pod_template_hash.to_string(),
                )],
            )
            .await?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaSetScaleTarget {
    name: String,
    replicas: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DeploymentRolloutPlan {
    targets: Vec<ReplicaSetScaleTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaSetRolloutSnapshot {
    name: String,
    replicas: i64,
    available_replicas: i64,
    max_replicas_annotation: Option<i64>,
    revision: i64,
    is_new: bool,
}

fn replica_annotation_i64(replicaset: &Value, key: &str) -> Option<i64> {
    replicaset
        .pointer("/metadata/annotations")
        .and_then(|v| v.as_object())
        .and_then(|annotations| annotations.get(key))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|v| *v > 0)
}

fn active_pod_count(pods: &[Resource]) -> usize {
    pods.iter()
        .filter(|pod| {
            pod.data.pointer("/metadata/deletionTimestamp").is_none()
                && !matches!(
                    pod.data.pointer("/status/phase").and_then(|v| v.as_str()),
                    Some("Succeeded" | "Failed")
                )
        })
        .count()
}

async fn acknowledge_observed_generation(
    db: &dyn DatastoreBackend,
    deployment: &Value,
    metadata: &Value,
) -> Result<()> {
    let generation = metadata
        .get("generation")
        .and_then(|g| g.as_i64())
        .unwrap_or(1);
    if deployment
        .pointer("/status/observedGeneration")
        .and_then(|g| g.as_i64())
        .is_some_and(|observed| observed >= generation)
    {
        return Ok(());
    }

    let mut status = deployment
        .get("status")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !status.is_object() {
        status = json!({});
    }
    if let Some(status_obj) = status.as_object_mut() {
        status_obj.insert("observedGeneration".to_string(), json!(generation));
        for field in [
            "replicas",
            "readyReplicas",
            "updatedReplicas",
            "availableReplicas",
            "unavailableReplicas",
        ] {
            status_obj
                .entry(field.to_string())
                .or_insert_with(|| json!(0));
        }
    }

    crate::controllers::common::write_status(db, deployment, &status).await?;
    Ok(())
}

struct ZeroReplicaOldReplicaSetRedrive<'a> {
    db: &'a dyn DatastoreBackend,
    pod_reader: &'a dyn PodReader,
    pod_writer: &'a dyn PodObjectWriter,
    pod_delete_sink: &'a dyn crate::controllers::gc::GcPodDeleteSink,
    namespace: &'a str,
    deployment_uid: &'a str,
    current_template: &'a Value,
    node_name: &'a str,
}

async fn redrive_zero_replica_old_replicasets_with_live_pods(
    ctx: ZeroReplicaOldReplicaSetRedrive<'_>,
    owned_rs_list: &[Resource],
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    for rs in owned_rs_list {
        if !common.is_controlled_by(&rs.data, ctx.deployment_uid) {
            continue;
        }
        if rs
            .data
            .pointer("/spec/template")
            .is_some_and(|template| templates_match(template, ctx.current_template))
        {
            continue;
        }
        let desired_replicas = rs
            .data
            .pointer("/spec/replicas")
            .and_then(|r| r.as_i64())
            .unwrap_or(1);
        if desired_replicas > 0 {
            continue;
        }
        let Some(rs_uid) = rs.data.pointer("/metadata/uid").and_then(|u| u.as_str()) else {
            continue;
        };
        let pods = ctx
            .pod_reader
            .list_pods_by_owner_uid(ctx.namespace, rs_uid)
            .await?;
        if active_pod_count(&pods) == 0 {
            continue;
        }

        let rs_with_metadata =
            crate::api::inject_resource_version(rs.data.clone(), rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            ctx.db,
            ctx.pod_reader,
            ctx.pod_writer,
            ctx.pod_delete_sink,
            &rs_with_metadata,
            ctx.node_name,
        )
        .await?;
    }

    Ok(())
}

fn replica_set_proportion_from_snapshot(
    snapshot: &ReplicaSetRolloutSnapshot,
    desired_replicas: i64,
    max_surge: i64,
    fallback_max_replicas: i64,
    replicas_to_add: i64,
    replicas_added: i64,
) -> i64 {
    if snapshot.replicas == 0 || replicas_to_add == 0 || replicas_to_add == replicas_added {
        return 0;
    }
    if desired_replicas == 0 {
        return -snapshot.replicas;
    }

    let deployment_max_replicas = desired_replicas + max_surge;
    let max_before_scale = snapshot
        .max_replicas_annotation
        .unwrap_or(fallback_max_replicas);
    if max_before_scale == 0 {
        return 0;
    }

    let scaled = ((snapshot.replicas * deployment_max_replicas) as f64 / max_before_scale as f64)
        .round() as i64;
    let fraction = scaled - snapshot.replicas;
    let allowed = replicas_to_add - replicas_added;
    if replicas_to_add > 0 {
        fraction.min(allowed)
    } else {
        fraction.max(allowed)
    }
}

fn plan_rolling_update_once(
    snapshots: &[ReplicaSetRolloutSnapshot],
    desired_replicas: i64,
    max_surge: i64,
    max_unavailable: i64,
) -> DeploymentRolloutPlan {
    let mut working_snapshots = snapshots.to_vec();
    if !working_snapshots.iter().any(|rs| rs.is_new) {
        return DeploymentRolloutPlan {
            targets: Vec::new(),
        };
    }

    let active_old_replicas = working_snapshots
        .iter()
        .any(|rs| !rs.is_new && rs.replicas > 0);
    let current_total_replicas = working_snapshots.iter().map(|rs| rs.replicas).sum::<i64>();
    let target_total_replicas = if desired_replicas <= 0 {
        0
    } else {
        desired_replicas + max_surge
    };
    let replicas_to_add = target_total_replicas - current_total_replicas;
    let deployment_scale_changed = working_snapshots
        .iter()
        .filter(|rs| rs.replicas > 0)
        .filter_map(|rs| rs.max_replicas_annotation)
        .any(|max_replicas| max_replicas != target_total_replicas);
    let available_new_rs_can_take_scale_up = replicas_to_add > 0
        && working_snapshots
            .iter()
            .any(|rs| rs.is_new && rs.replicas > 0 && rs.available_replicas >= rs.replicas);
    if active_old_replicas
        && deployment_scale_changed
        && current_total_replicas > 0
        && current_total_replicas != target_total_replicas
        && !available_new_rs_can_take_scale_up
    {
        let mut active_indices: Vec<_> = working_snapshots
            .iter()
            .enumerate()
            .filter_map(|(idx, rs)| (rs.replicas > 0).then_some(idx))
            .collect();
        if replicas_to_add > 0 {
            active_indices.sort_by(|left_idx, right_idx| {
                let left = &working_snapshots[*left_idx];
                let right = &working_snapshots[*right_idx];
                right
                    .replicas
                    .cmp(&left.replicas)
                    .then_with(|| right.revision.cmp(&left.revision))
            });
        } else {
            active_indices.sort_by(|left_idx, right_idx| {
                let left = &working_snapshots[*left_idx];
                let right = &working_snapshots[*right_idx];
                right
                    .is_new
                    .cmp(&left.is_new)
                    .then_with(|| right.available_replicas.cmp(&left.available_replicas))
            });
        }

        let mut target_pairs = Vec::with_capacity(active_indices.len());
        let mut replicas_added = 0;
        for idx in &active_indices {
            let rs = &working_snapshots[*idx];
            let proportion = replica_set_proportion_from_snapshot(
                rs,
                desired_replicas,
                max_surge,
                current_total_replicas,
                replicas_to_add,
                replicas_added,
            );
            replicas_added += proportion;
            target_pairs.push((*idx, rs.replicas + proportion));
        }

        if let Some((_, first_target_replicas)) = target_pairs.first_mut() {
            let leftover = replicas_to_add - replicas_added;
            *first_target_replicas = (*first_target_replicas + leftover).max(0);
        }

        for (idx, replicas) in target_pairs {
            working_snapshots[idx].replicas = replicas.max(0);
        }
    }

    let Some(new_rs_idx) = working_snapshots.iter().position(|rs| rs.is_new) else {
        return DeploymentRolloutPlan {
            targets: Vec::new(),
        };
    };
    let new_rs = &working_snapshots[new_rs_idx];
    let total_old_replicas = working_snapshots
        .iter()
        .filter(|rs| !rs.is_new)
        .map(|rs| rs.replicas)
        .sum::<i64>();
    let target_new_replicas = if total_old_replicas == 0 {
        desired_replicas
    } else {
        let max_total_pods = desired_replicas + max_surge;
        let max_new_allowed_by_surge = std::cmp::max(0, max_total_pods - total_old_replicas);
        std::cmp::min(
            std::cmp::min(new_rs.replicas + max_surge, desired_replicas),
            max_new_allowed_by_surge,
        )
    }
    .max(0);

    let min_available = desired_replicas - max_unavailable;
    let all_pods_count = target_new_replicas + total_old_replicas;
    let new_rs_unavailable =
        std::cmp::max(0, target_new_replicas - new_rs.available_replicas.max(0));
    let max_scaled_down = all_pods_count - min_available - new_rs_unavailable;
    let mut remaining_old_scale_down =
        std::cmp::max(0, std::cmp::min(total_old_replicas, max_scaled_down));

    let mut final_snapshots = working_snapshots.clone();
    final_snapshots[new_rs_idx].replicas = target_new_replicas;

    let mut old_indices: Vec<_> = final_snapshots
        .iter()
        .enumerate()
        .filter_map(|(idx, rs)| (!rs.is_new).then_some(idx))
        .collect();
    old_indices.sort_by_key(|idx| final_snapshots[*idx].available_replicas);
    for old_idx in old_indices {
        let old_replicas = final_snapshots[old_idx].replicas;
        if remaining_old_scale_down <= 0 || old_replicas <= 0 {
            continue;
        }
        let scale_down = std::cmp::min(remaining_old_scale_down, old_replicas);
        let target_old_replicas = old_replicas - scale_down;
        remaining_old_scale_down -= scale_down;
        final_snapshots[old_idx].replicas = target_old_replicas;
    }

    let targets = final_snapshots
        .into_iter()
        .filter_map(|final_rs| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.name == final_rs.name)
                .and_then(|original| {
                    if original.replicas == final_rs.replicas {
                        None
                    } else {
                        Some(ReplicaSetScaleTarget {
                            name: final_rs.name,
                            replicas: final_rs.replicas.max(0),
                        })
                    }
                })
        })
        .collect();

    DeploymentRolloutPlan { targets }
}

async fn scale_replicaset_resource(
    db: &dyn DatastoreBackend,
    namespace: &str,
    rs: &Resource,
    target_replicas: i64,
    desired_replicas: i64,
    max_replicas: i64,
) -> Result<Resource> {
    if rs.data.pointer("/spec").is_none() {
        return Err(anyhow::anyhow!("ReplicaSet {} missing spec", rs.name));
    }

    let patch = json!({
        "metadata": {
            "annotations": {
                DESIRED_REPLICAS_ANNOTATION: desired_replicas.to_string(),
                MAX_REPLICAS_ANNOTATION: max_replicas.to_string()
            }
        },
        "spec": {
            "replicas": target_replicas.max(0)
        }
    });

    db.patch_resource_latest_with_preconditions(
        "apps/v1",
        "ReplicaSet",
        Some(namespace),
        &rs.name,
        ResourcePatchRequest::new(
            PatchKind::Merge,
            patch,
            ResourcePreconditions::uid(rs.uid.clone()),
        ),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("ReplicaSet {} disappeared during scale", rs.name))
}

pub async fn reconcile_deployment(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
    deployment: &Value,
    node_name: &str,
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    let input_metadata = deployment
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

    tracing::info!(
        target: "klights::deployment::reconcile",
        deploy = %name,
        ns = %namespace,
        "reconcile_deployment started"
    );

    // Preserve validation semantics for malformed reconcile payloads.
    // Controller tests expect missing spec to be rejected.
    deployment
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    // Re-read the live object from storage. Watch/retry queues can deliver stale
    // snapshots after a Deployment has already been deleted; reconciling that
    // stale payload must not recreate ReplicaSets/Pods.
    let live_deployment = match db
        .get_resource("apps/v1", "Deployment", Some(namespace), name)
        .await?
    {
        Some(r) => crate::api::inject_resource_version(r.data, r.resource_version),
        None => return Ok(()),
    };
    let deployment = &live_deployment;
    let metadata = deployment
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Skip reconciliation if the resource is being deleted
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    let spec = deployment
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    let uid = metadata
        .get("uid")
        .and_then(|u| u.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing uid"))?;

    let desired_replicas = spec.get("replicas").and_then(|r| r.as_i64()).unwrap_or(1);
    let max_replicas_annotation = max_replicas_for_annotations(spec, desired_replicas);

    // Validate required fields - if missing, set failure condition and return
    let selector = match spec.get("selector") {
        Some(s) => s,
        None => {
            let now = crate::utils::k8s_timestamp();
            let failure_condition = json!({
                "type": "ReplicaFailure",
                "status": "True",
                "lastTransitionTime": now,
                "lastUpdateTime": now,
                "reason": "InvalidSpec",
                "message": "Deployment spec is missing required field: selector"
            });

            let status = json!({
                "observedGeneration": metadata.get("generation").and_then(|g| g.as_i64()).unwrap_or(1),
                "replicas": 0,
                "readyReplicas": 0,
                "updatedReplicas": 0,
                "availableReplicas": 0,
                "unavailableReplicas": 0,
                "conditions": [failure_condition]
            });

            crate::controllers::common::write_status(db, deployment, &status).await?;

            return Ok(());
        }
    };

    let template = match spec.get("template") {
        Some(t) => t,
        None => {
            let now = crate::utils::k8s_timestamp();
            let failure_condition = json!({
                "type": "ReplicaFailure",
                "status": "True",
                "lastTransitionTime": now,
                "lastUpdateTime": now,
                "reason": "InvalidSpec",
                "message": "Deployment spec is missing required field: template"
            });

            let status = json!({
                "observedGeneration": metadata.get("generation").and_then(|g| g.as_i64()).unwrap_or(1),
                "replicas": 0,
                "readyReplicas": 0,
                "updatedReplicas": 0,
                "availableReplicas": 0,
                "unavailableReplicas": 0,
                "conditions": [failure_condition]
            });

            crate::controllers::common::write_status(db, deployment, &status).await?;

            return Ok(());
        }
    };

    acknowledge_observed_generation(db, deployment, metadata).await?;

    // Get all ReplicaSets owned by this deployment
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    let mut owned_rs_list = Vec::new();
    let deployment_owner_ref = common.build_owner_ref("apps/v1", "Deployment", name, uid);

    for rs_resource in rs_list.items {
        // Active ReplicaSets: controlled by this Deployment.
        if common.is_controlled_by(&rs_resource.data, uid) {
            owned_rs_list.push(rs_resource);
            continue;
        }

        // Repair legacy non-controller ownerRefs left by older klights builds.
        // Kubernetes keeps old ReplicaSets controlled during a rolling update so
        // their available pods continue to count toward Deployment availability.
        if common.is_owned_by(&rs_resource.data, uid) {
            let has_other_controller_owner = rs_resource
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|o| o.as_array())
                .is_some_and(|owners| {
                    owners.iter().any(|o| {
                        o.get("controller").and_then(|c| c.as_bool()) == Some(true)
                            && o.get("uid").and_then(|u| u.as_str()) != Some(uid)
                    })
                });
            let rs_labels = rs_resource
                .data
                .pointer("/metadata/labels")
                .and_then(|l| l.as_object());
            let selected = rs_labels
                .map(|labels| labels_match_selector(selector, labels))
                .unwrap_or(false);
            if has_other_controller_owner || !selected {
                continue;
            }

            let mut repaired: Value = (*rs_resource.data).clone();
            if let Some(owner_refs) = repaired
                .pointer_mut("/metadata/ownerReferences")
                .and_then(|v| v.as_array_mut())
            {
                for ref_ in owner_refs.iter_mut() {
                    if ref_.get("uid").and_then(|u| u.as_str()) == Some(uid) {
                        ref_["controller"] = json!(true);
                    }
                }
            }
            let updated = db
                .update_resource_with_preconditions(
                    "apps/v1",
                    "ReplicaSet",
                    Some(namespace),
                    &rs_resource.name.clone(),
                    repaired,
                    ResourcePreconditions::from_resource(&rs_resource),
                )
                .await?;
            owned_rs_list.push(updated);
            continue;
        }

        // Adopt orphan ReplicaSets selected by this Deployment.
        // This is required for rollover semantics where a Deployment takes over
        // pre-existing RSs with matching labels/selectors.
        let has_controller_owner = rs_resource
            .data
            .pointer("/metadata/ownerReferences")
            .and_then(|o| o.as_array())
            .is_some_and(|owners| {
                owners
                    .iter()
                    .any(|o| o.get("controller").and_then(|c| c.as_bool()) == Some(true))
            });
        if has_controller_owner {
            continue;
        }

        let rs_labels = rs_resource
            .data
            .pointer("/metadata/labels")
            .and_then(|l| l.as_object());
        let should_adopt = rs_labels
            .map(|labels| labels_match_selector(selector, labels))
            .unwrap_or(false);
        if !should_adopt {
            continue;
        }

        let pod_template_hash = rs_resource
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .and_then(|rs_name| rs_name.strip_prefix(&format!("{name}-")))
            .map(str::to_string)
            .unwrap_or_else(|| {
                rs_resource
                    .data
                    .pointer("/spec/template")
                    .map(compute_pod_template_hash)
                    .unwrap_or_else(|| compute_pod_template_hash(template))
            });

        let mut adopted: Value = (*rs_resource.data).clone();
        stamp_replicaset_pod_template_hash(&mut adopted, &pod_template_hash);
        if let Some(meta) = adopted.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            let owner_refs = meta
                .entry("ownerReferences".to_string())
                .or_insert_with(|| json!([]));
            if let Some(arr) = owner_refs.as_array_mut() {
                arr.push(deployment_owner_ref.clone());
            }
        }

        let updated = db
            .update_resource_with_preconditions(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                &rs_resource.name.clone(),
                adopted,
                ResourcePreconditions::from_resource(&rs_resource),
            )
            .await?;
        if let Some(rs_uid) = updated
            .data
            .pointer("/metadata/uid")
            .and_then(|u| u.as_str())
        {
            stamp_existing_replicaset_pods(
                pod_reader,
                pod_writer,
                namespace,
                rs_uid,
                &pod_template_hash,
            )
            .await?;
        }
        owned_rs_list.push(updated);
    }

    // Check for rollback annotation
    if let Some(annotations) = metadata.get("annotations").and_then(|a| a.as_object())
        && let Some(rollback_to) = annotations.get("deployment.kubernetes.io/rollback-to")
        && let Some(target_revision_str) = rollback_to.as_str()
        && let Ok(target_revision) = target_revision_str.parse::<i64>()
    {
        // Find the ReplicaSet with the target revision.
        // Re-query all RSes. Older klights builds may have left
        // non-controller ownerRefs behind, so rollback lookup uses
        // the broader ownership predicate.
        let all_rs = db
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let target_rs = all_rs.items.iter().find(|rs| {
            common.is_owned_by(&rs.data, uid) && {
                rs.data
                    .get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .and_then(|a| a.as_object())
                    .and_then(|ann| ann.get("deployment.kubernetes.io/revision"))
                    .and_then(|r| r.as_str())
                    .and_then(|r| r.parse::<i64>().ok())
                    == Some(target_revision)
            }
        });

        if let Some(target_rs) = target_rs {
            // Update deployment to use the target RS template
            let target_template = target_rs
                .data
                .get("spec")
                .and_then(|s| s.get("template"))
                .ok_or_else(|| anyhow::anyhow!("Target RS missing template"))?;

            let mut updated_deployment = deployment.clone();
            if let Some(deploy_spec) = updated_deployment
                .get_mut("spec")
                .and_then(|s| s.as_object_mut())
            {
                deploy_spec.insert("template".to_string(), target_template.clone());
            }

            // Remove rollback annotation
            if let Some(deploy_metadata) = updated_deployment
                .get_mut("metadata")
                .and_then(|m| m.as_object_mut())
                && let Some(deploy_annotations) = deploy_metadata
                    .get_mut("annotations")
                    .and_then(|a| a.as_object_mut())
            {
                deploy_annotations.remove("deployment.kubernetes.io/rollback-to");
                if deploy_annotations.is_empty() {
                    deploy_metadata.remove("annotations");
                }
            }

            // Update the deployment
            let current_rv = crate::utils::extract_resource_version(metadata);
            db.update_resource_with_preconditions(
                "apps/v1",
                "Deployment",
                Some(namespace),
                name,
                updated_deployment,
                ResourcePreconditions::from_metadata(metadata, current_rv)?,
            )
            .await?;

            // Rollback complete - return early
            return Ok(());
        }
    }

    // Calculate next revision number
    let next_revision = get_next_revision(&owned_rs_list);

    // Find the ReplicaSet that matches the current template
    let mut matching_rs = None;
    let mut old_rs_list = Vec::new();
    let mut created_rs_name: Option<String> = None;

    for rs in &owned_rs_list {
        let rs_template = rs.data.get("spec").and_then(|s| s.get("template"));

        if let Some(rs_tmpl) = rs_template {
            if templates_match(template, rs_tmpl) {
                matching_rs = Some(rs);
            } else {
                old_rs_list.push(rs);
            }
        }
    }

    if let Some(existing_rs) = matching_rs {
        // ReplicaSet with matching template exists — scale progressively.
        let rs_name = existing_rs
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing RS name"))?
            .to_string();

        let strategy_type = spec
            .get("strategy")
            .and_then(|s| s.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("RollingUpdate");

        if strategy_type == "Recreate" {
            // Recreate must not run old and new pods concurrently.
            // Scale all old RS to zero first, then allow the matching/new RS to scale up.
            for old_rs in &old_rs_list {
                let old_rs_name = old_rs
                    .data
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let old_rs_replicas = old_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0);
                if old_rs_replicas > 0 && !old_rs_name.is_empty() {
                    let updated = scale_replicaset_resource(
                        db,
                        namespace,
                        old_rs,
                        0,
                        desired_replicas,
                        max_replicas_annotation,
                    )
                    .await?;
                    let rs_with_metadata =
                        crate::api::inject_resource_version(updated.data, updated.resource_version);
                    crate::controllers::replicaset::reconcile_replicaset(
                        db,
                        pod_reader,
                        pod_writer,
                        pod_delete_sink,
                        &rs_with_metadata,
                        node_name,
                    )
                    .await?;
                }
            }

            // Re-read old RSes to determine whether scale-down has completed.
            let current_rs = db
                .list_resources(
                    "apps/v1",
                    "ReplicaSet",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?;
            let total_old_replicas = current_rs
                .items
                .iter()
                .filter(|rs| rs.name != rs_name && common.is_owned_by(&rs.data, uid))
                .map(|rs| {
                    rs.data
                        .pointer("/spec/replicas")
                        .and_then(|r| r.as_i64())
                        .unwrap_or(0)
                })
                .sum::<i64>();

            let target_new_replicas = if total_old_replicas == 0 {
                desired_replicas
            } else {
                0
            };

            let current_new_replicas = existing_rs
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1);
            if current_new_replicas != target_new_replicas {
                let updated = scale_replicaset_resource(
                    db,
                    namespace,
                    existing_rs,
                    target_new_replicas,
                    desired_replicas,
                    max_replicas_annotation,
                )
                .await?;
                let rs_with_metadata =
                    crate::api::inject_resource_version(updated.data, updated.resource_version);
                crate::controllers::replicaset::reconcile_replicaset(
                    db,
                    pod_reader,
                    pod_writer,
                    pod_delete_sink,
                    &rs_with_metadata,
                    node_name,
                )
                .await?;
            }
        } else if strategy_type == "RollingUpdate" && !old_rs_list.is_empty() {
            // Compute one rollout step from current state and exit. Later
            // ReplicaSet/Pod watch events requeue the Deployment for the next step.
            let max_surge = get_max_surge(spec, desired_replicas);
            let max_unavailable = get_max_unavailable(spec, desired_replicas);

            let all_rs = db
                .list_resources(
                    "apps/v1",
                    "ReplicaSet",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?;
            let current_new_rs = all_rs
                .items
                .iter()
                .find(|rs| rs.name == rs_name)
                .ok_or_else(|| anyhow::anyhow!("New RS {} disappeared during rollout", rs_name))?;
            let new_rs_uid = current_new_rs
                .data
                .pointer("/metadata/uid")
                .and_then(|u| u.as_str())
                .ok_or_else(|| anyhow::anyhow!("New RS {} missing uid", rs_name))?;
            let new_rs_pods = pod_reader
                .list_pods_by_owner_uid(namespace, new_rs_uid)
                .await?;
            let new_rs_live_available = common.count_ready_pods(&new_rs_pods) as i64;
            let new_rs_status_available = current_new_rs
                .data
                .pointer("/status/availableReplicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(0)
                .max(0);
            let mut snapshots = Vec::new();
            let mut old_rs_active_pod_counts = HashMap::new();
            snapshots.push(ReplicaSetRolloutSnapshot {
                name: current_new_rs.name.clone(),
                replicas: current_new_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0),
                available_replicas: new_rs_live_available.max(new_rs_status_available),
                max_replicas_annotation: replica_annotation_i64(
                    &current_new_rs.data,
                    MAX_REPLICAS_ANNOTATION,
                ),
                revision: replica_annotation_i64(
                    &current_new_rs.data,
                    "deployment.kubernetes.io/revision",
                )
                .unwrap_or(0),
                is_new: true,
            });
            for old_rs in all_rs
                .items
                .iter()
                .filter(|rs| rs.name != rs_name && common.is_controlled_by(&rs.data, uid))
            {
                let Some(rs_uid) = old_rs
                    .data
                    .pointer("/metadata/uid")
                    .and_then(|u| u.as_str())
                else {
                    continue;
                };
                let pods = pod_reader.list_pods_by_owner_uid(namespace, rs_uid).await?;
                let live_available = common.count_ready_pods(&pods) as i64;
                old_rs_active_pod_counts.insert(old_rs.name.clone(), active_pod_count(&pods));
                let status_available = old_rs
                    .data
                    .pointer("/status/availableReplicas")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    .max(0);
                snapshots.push(ReplicaSetRolloutSnapshot {
                    name: old_rs.name.clone(),
                    replicas: old_rs
                        .data
                        .pointer("/spec/replicas")
                        .and_then(|r| r.as_i64())
                        .unwrap_or(0),
                    available_replicas: live_available.max(status_available),
                    max_replicas_annotation: replica_annotation_i64(
                        &old_rs.data,
                        MAX_REPLICAS_ANNOTATION,
                    ),
                    revision: replica_annotation_i64(
                        &old_rs.data,
                        "deployment.kubernetes.io/revision",
                    )
                    .unwrap_or(0),
                    is_new: false,
                });
            }

            let plan =
                plan_rolling_update_once(&snapshots, desired_replicas, max_surge, max_unavailable);
            let mut reconciled_rs_names = std::collections::HashSet::new();
            for target in plan.targets {
                let rs = all_rs
                    .items
                    .iter()
                    .find(|rs| rs.name == target.name)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "ReplicaSet {} disappeared before rollout plan apply",
                            target.name
                        )
                    })?;
                let current_replicas = rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0);
                if current_replicas == target.replicas {
                    continue;
                }
                let updated = scale_replicaset_resource(
                    db,
                    namespace,
                    rs,
                    target.replicas,
                    desired_replicas,
                    max_replicas_annotation,
                )
                .await?;
                reconciled_rs_names.insert(target.name.clone());
                let rs_with_metadata =
                    crate::api::inject_resource_version(updated.data, updated.resource_version);
                crate::controllers::replicaset::reconcile_replicaset(
                    db,
                    pod_reader,
                    pod_writer,
                    pod_delete_sink,
                    &rs_with_metadata,
                    node_name,
                )
                .await?;
            }

            for old_rs in all_rs
                .items
                .iter()
                .filter(|rs| rs.name != rs_name && common.is_controlled_by(&rs.data, uid))
            {
                if reconciled_rs_names.contains(&old_rs.name) {
                    continue;
                }
                let desired_old_replicas = old_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0)
                    .max(0) as usize;
                let active_old_pods = old_rs_active_pod_counts
                    .get(&old_rs.name)
                    .copied()
                    .unwrap_or(0);
                if active_old_pods <= desired_old_replicas {
                    continue;
                }

                let rs_with_metadata = crate::api::inject_resource_version(
                    old_rs.data.clone(),
                    old_rs.resource_version,
                );
                crate::controllers::replicaset::reconcile_replicaset(
                    db,
                    pod_reader,
                    pod_writer,
                    pod_delete_sink,
                    &rs_with_metadata,
                    node_name,
                )
                .await?;
            }
        } else {
            // No old RSes (pure scale operation or Recreate) or rollout complete — set desired directly
            let current_new_replicas = existing_rs
                .data
                .pointer("/spec/replicas")
                .and_then(|r| r.as_i64())
                .unwrap_or(1);
            if current_new_replicas != desired_replicas {
                let updated = scale_replicaset_resource(
                    db,
                    namespace,
                    existing_rs,
                    desired_replicas,
                    desired_replicas,
                    max_replicas_annotation,
                )
                .await?;
                let rs_with_metadata =
                    crate::api::inject_resource_version(updated.data, updated.resource_version);
                crate::controllers::replicaset::reconcile_replicaset(
                    db,
                    pod_reader,
                    pod_writer,
                    pod_delete_sink,
                    &rs_with_metadata,
                    node_name,
                )
                .await?;
            }
        }
    } else {
        // No matching ReplicaSet - create new one
        let strategy_type = spec
            .get("strategy")
            .and_then(|s| s.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("RollingUpdate");

        // For Recreate strategy: scale all old RS to 0 before creating new RS
        let mut recreate_has_old_replicas = false;
        if strategy_type == "Recreate" {
            for old_rs in &old_rs_list {
                let old_rs_name = old_rs
                    .data
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let old_rs_replicas = old_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0);
                if old_rs_replicas > 0 && !old_rs_name.is_empty() {
                    recreate_has_old_replicas = true;
                    let updated = scale_replicaset_resource(
                        db,
                        namespace,
                        old_rs,
                        0,
                        desired_replicas,
                        max_replicas_annotation,
                    )
                    .await?;
                    let rs_with_metadata =
                        crate::api::inject_resource_version(updated.data, updated.resource_version);
                    crate::controllers::replicaset::reconcile_replicaset(
                        db,
                        pod_reader,
                        pod_writer,
                        pod_delete_sink,
                        &rs_with_metadata,
                        node_name,
                    )
                    .await?;
                }
            }
        }

        // Compute deterministic pod-template-hash from the deployment's template
        let pod_template_hash = compute_pod_template_hash(template);
        let rs_name = format!("{}-{}", name, pod_template_hash);

        let mut old_replicas_after_plan: HashMap<String, i64> = HashMap::new();
        let mut planned_new_replicas: Option<i64> = None;
        if strategy_type == "RollingUpdate" && !old_rs_list.is_empty() {
            let max_surge = get_max_surge(spec, desired_replicas);
            let max_unavailable = get_max_unavailable(spec, desired_replicas);
            let mut snapshots = Vec::with_capacity(old_rs_list.len() + 1);
            for old_rs in &old_rs_list {
                let replicas = old_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0);
                let Some(old_rs_uid) = old_rs
                    .data
                    .pointer("/metadata/uid")
                    .and_then(|u| u.as_str())
                else {
                    continue;
                };
                let pods = pod_reader
                    .list_pods_by_owner_uid(namespace, old_rs_uid)
                    .await?;
                let live_available = common.count_ready_pods(&pods) as i64;
                let status_available = old_rs
                    .data
                    .pointer("/status/availableReplicas")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    .max(0);
                old_replicas_after_plan.insert(old_rs.name.clone(), replicas);
                snapshots.push(ReplicaSetRolloutSnapshot {
                    name: old_rs.name.clone(),
                    replicas,
                    available_replicas: live_available.max(status_available),
                    max_replicas_annotation: replica_annotation_i64(
                        &old_rs.data,
                        MAX_REPLICAS_ANNOTATION,
                    ),
                    revision: replica_annotation_i64(
                        &old_rs.data,
                        "deployment.kubernetes.io/revision",
                    )
                    .unwrap_or(0),
                    is_new: false,
                });
            }
            snapshots.push(ReplicaSetRolloutSnapshot {
                name: rs_name.clone(),
                replicas: 0,
                available_replicas: 0,
                max_replicas_annotation: Some(max_replicas_annotation),
                revision: next_revision,
                is_new: true,
            });

            let plan =
                plan_rolling_update_once(&snapshots, desired_replicas, max_surge, max_unavailable);
            for target in plan.targets {
                if target.name == rs_name {
                    planned_new_replicas = Some(target.replicas);
                    continue;
                }
                let Some(old_rs) = old_rs_list.iter().find(|rs| rs.name == target.name) else {
                    continue;
                };
                let current_replicas = old_rs
                    .data
                    .pointer("/spec/replicas")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(0);
                if current_replicas == target.replicas {
                    old_replicas_after_plan.insert(target.name, target.replicas);
                    continue;
                }
                let updated = scale_replicaset_resource(
                    db,
                    namespace,
                    old_rs,
                    target.replicas,
                    desired_replicas,
                    max_replicas_annotation,
                )
                .await?;
                old_replicas_after_plan.insert(target.name, target.replicas);
                let rs_with_metadata =
                    crate::api::inject_resource_version(updated.data, updated.resource_version);
                crate::controllers::replicaset::reconcile_replicaset(
                    db,
                    pod_reader,
                    pod_writer,
                    pod_delete_sink,
                    &rs_with_metadata,
                    node_name,
                )
                .await?;
            }
        }

        // Calculate initial replica count based on strategy
        let initial_new_replicas = if strategy_type == "Recreate" {
            // Recreate must wait for old RS scale-down completion before new pods are started.
            if recreate_has_old_replicas {
                0
            } else {
                desired_replicas
            }
        } else if strategy_type == "RollingUpdate" && !old_rs_list.is_empty() {
            let max_total_pods = desired_replicas + get_max_surge(spec, desired_replicas);
            let total_old_after_plan = old_replicas_after_plan.values().sum::<i64>();
            planned_new_replicas
                .unwrap_or_else(|| {
                    desired_replicas.min((max_total_pods - total_old_after_plan).max(0))
                })
                .max(0)
        } else {
            desired_replicas
        };

        // Build RS labels: deployment's selector matchLabels + pod-template-hash.
        // matchLabels (not matchExpressions) harvests here because RS *labels*
        // need concrete key/value pairs — set-based requirements have no
        // analogue in the labels field.
        let mut rs_labels = serde_json::Map::new();
        if let Some(match_labels) = selector.pointer("/matchLabels").and_then(|m| m.as_object()) {
            for (k, v) in match_labels {
                rs_labels.insert(k.clone(), v.clone());
            }
        }
        rs_labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));

        // Build RS selector: deployment's selector + pod-template-hash in matchLabels
        let mut rs_selector = selector.clone();
        if let Some(match_labels) = rs_selector
            .pointer_mut("/matchLabels")
            .and_then(|m| m.as_object_mut())
        {
            match_labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));
        }

        // Build RS template: inject pod-template-hash into template labels
        let mut rs_template = template.clone();
        if let Some(labels) = rs_template
            .pointer_mut("/metadata/labels")
            .and_then(|l| l.as_object_mut())
        {
            labels.insert("pod-template-hash".to_string(), json!(pod_template_hash));
        } else {
            // Template has no labels — create metadata.labels with pod-template-hash
            if let Some(meta) = rs_template
                .get_mut("metadata")
                .and_then(|m| m.as_object_mut())
            {
                meta.insert(
                    "labels".to_string(),
                    json!({"pod-template-hash": pod_template_hash}),
                );
            }
        }

        let replicaset = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": rs_name,
                "namespace": namespace,
                "labels": rs_labels,
                "annotations": {
                    "deployment.kubernetes.io/revision": next_revision.to_string(),
                    DESIRED_REPLICAS_ANNOTATION: desired_replicas.to_string(),
                    MAX_REPLICAS_ANNOTATION: max_replicas_annotation.to_string()
                },
                "ownerReferences": [common.build_owner_ref("apps/v1", "Deployment", name, uid)]
            },
            "spec": {
                "replicas": initial_new_replicas,
                "selector": rs_selector,
                "template": rs_template
            }
        });

        let created_rs = match db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                &rs_name,
                replicaset.clone(),
            )
            .await
        {
            Ok(rs) => rs,
            Err(e) if e.to_string().contains("already exists") => {
                // RS with this name already exists — concurrent reconcile created it first.
                // Adopt it if it's owned by this deployment (idempotent create).
                match db
                    .get_resource("apps/v1", "ReplicaSet", Some(namespace), &rs_name)
                    .await?
                {
                    Some(existing) if common.is_owned_by(&existing.data, uid) => {
                        tracing::debug!(
                            "RS {}/{} already exists and is owned by this deployment — adopting",
                            namespace,
                            rs_name
                        );
                        existing
                    }
                    Some(_) => {
                        return Err(anyhow::anyhow!(
                            "ReplicaSet {}/{} already exists and is owned by a different controller",
                            namespace,
                            rs_name
                        ));
                    }
                    None => {
                        return Err(anyhow::anyhow!(
                            "ReplicaSet {}/{} reported as existing but could not be retrieved",
                            namespace,
                            rs_name
                        ));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create ReplicaSet for Deployment {}/{}: {} \
                     — error returned to controller workqueue for exponential-backoff retry",
                    namespace,
                    name,
                    e
                );
                return Err(e).with_context(|| {
                    format!(
                        "Failed to create ReplicaSet for Deployment {}/{}",
                        namespace, name
                    )
                });
            }
        };
        created_rs_name = Some(rs_name.clone());

        if crate::controllers::gc::reconcile_owner_references(
            db,
            created_rs.clone(),
            pod_delete_sink,
        )
        .await?
            == crate::controllers::gc::OwnerReferenceReconcile::Deleted
        {
            return Ok(());
        }

        // Reconcile the ReplicaSet to create pods
        let rs_with_metadata =
            crate::api::inject_resource_version(created_rs.data, created_rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            db,
            pod_reader,
            pod_writer,
            pod_delete_sink,
            &rs_with_metadata,
            node_name,
        )
        .await?;
    }

    // Re-query owned RS list so any RS created/updated above is included in the count.
    // The original owned_rs_list was captured before RS creation, so it would miss new RS pods.
    let fresh_rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let fresh_owned_rs_list: Vec<_> = fresh_rs_list
        .items
        .into_iter()
        .filter(|rs| common.is_controlled_by(&rs.data, uid))
        .collect();
    redrive_zero_replica_old_replicasets_with_live_pods(
        ZeroReplicaOldReplicaSetRedrive {
            db,
            pod_reader,
            pod_writer,
            pod_delete_sink,
            namespace,
            deployment_uid: uid,
            current_template: template,
            node_name,
        },
        &fresh_owned_rs_list,
    )
    .await?;

    // Count actual pods to update status
    let (total_pods, ready_pods, updated_pods, available_pods) =
        count_deployment_pods(pod_reader, namespace, &fresh_owned_rs_list, template).await?;

    let (conditions, current_revision) = build_conditions_and_revision(
        available_pods,
        updated_pods,
        desired_replicas,
        &created_rs_name,
        &matching_rs,
        next_revision,
    );

    // Write revision annotation FIRST — metadata mutation bumps RV once.
    // Then re-read to pick up the fresh RV for the status write.
    apply_revision_and_gc(
        db,
        namespace,
        name,
        spec,
        &fresh_owned_rs_list,
        template,
        current_revision,
    )
    .await?;

    // Re-read the Deployment to get a fresh RV after the revision annotation
    // write. The `deployment` variable was read at the top of reconcile and is
    // stale after RS scaling, pod operations, and the revision annotation write.
    let fresh_deployment = match db
        .get_resource("apps/v1", "Deployment", Some(namespace), name)
        .await?
    {
        Some(r) => crate::api::inject_resource_version(r.data, r.resource_version),
        None => return Ok(()),
    };

    let observed_generation = fresh_deployment
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(|g| g.as_i64())
        .unwrap_or(1);

    let unavailable_replicas = (desired_replicas - available_pods).max(0);
    let status = json!({
        "observedGeneration": observed_generation,
        "replicas": total_pods,
        "readyReplicas": ready_pods,
        "updatedReplicas": updated_pods,
        "availableReplicas": available_pods,
        "unavailableReplicas": unavailable_replicas,
        "conditions": conditions
    });

    crate::controllers::common::write_status(db, &fresh_deployment, &status).await?;

    Ok(())
}
