use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{PodListRequest, PodOwnerListRequest, PodQuery};
use klights_reconcile_api::{ControllerStoreResult, GcPodDeleteSink};
use serde_json::{Value, json};

#[async_trait]
pub trait ReplicaSetStore: crate::controllers::gc::GcResourceStore + Send + Sync {
    async fn get_replicaset(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn update_replicaset_status(
        &self,
        resource: &Resource,
        status: Value,
    ) -> ControllerStoreResult<()>;
}

#[async_trait]
pub trait ReplicaSetPodMutation: Send + Sync {
    async fn create_replicaset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: Value,
    ) -> ControllerStoreResult<Resource>;
    async fn replace_replicaset_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> ControllerStoreResult<Resource>;
}

pub(crate) async fn reconcile_replicaset(
    db: &(impl ReplicaSetStore + ?Sized),
    pod_reader: &(impl PodQuery + ?Sized),
    pod_writer: &(impl ReplicaSetPodMutation + ?Sized),
    pod_delete_sink: &dyn GcPodDeleteSink,
    non_pod_finalization: &dyn klights_reconcile_api::GcNonPodFinalizationPort,
    replicaset: &Value,
    reconcile_context: klights_controllers::ControllerReconcileContext<'_>,
) -> Result<()> {
    let coordination = reconcile_context.coordination;
    let node_name = reconcile_context.node_name;
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
    let reconcile_lock = coordination.reconcile_lock(
        klights_controllers::CoordinatedControllerKind::ReplicaSet,
        namespace,
        name,
    );
    let _reconcile_guard = reconcile_lock.lock().await;

    // Preserve validation semantics for malformed reconcile payloads.
    // Controller tests expect missing spec to be rejected.
    replicaset
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    // Re-read the live object from storage. Watch/retry queues can deliver stale
    // snapshots after a ReplicaSet has already been deleted; reconciling that
    // stale payload must not recreate pods.
    let live_resource = match db.get_replicaset(namespace, name).await? {
        Some(r) => r,
        None => return Ok(()),
    };

    let live_resource = match crate::controllers::gc::reconcile_owner_references(
        db,
        live_resource.clone(),
        pod_delete_sink,
        non_pod_finalization,
        coordination,
    )
    .await?
    {
        crate::controllers::gc::OwnerReferenceReconcile::Deleted => return Ok(()),
        crate::controllers::gc::OwnerReferenceReconcile::OwnerReferencesUpdated => {
            match db.get_replicaset(namespace, name).await? {
                Some(r) => r,
                None => return Ok(()),
            }
        }
        _ => live_resource,
    };

    let live_replicaset = crate::controllers::resource_projection::with_resource_version(
        live_resource.data,
        live_resource.resource_version,
        reconcile_context.wall_time,
    );

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
    let rs_owned = pod_reader
        .list_pods_by_owner_uid(PodOwnerListRequest::try_new(namespace, uid)?)
        .await?;

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
                    .replace_replicaset_pod_owner_references(namespace, &pod.name, owner_refs)
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
            .list_pods(PodListRequest::try_new(
                Some(namespace.to_string()),
                None,
                None,
                None,
                None,
            )?)
            .await?
            .into_parts()
            .0;
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
                    .replace_replicaset_pod_owner_references(namespace, &pod.name, owner_refs)
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
            let Some(live_rs) = db.get_replicaset(namespace, name).await? else {
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
            let Some(live_rs) = db.get_replicaset(namespace, name).await? else {
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
            let pod_uid = pod_resource
                .data
                .pointer("/metadata/uid")
                .and_then(|uid| uid.as_str())
                .unwrap_or("");
            if !pod_name.is_empty() && !pod_ns.is_empty() && !pod_uid.is_empty() {
                pod_delete_sink
                    .request_gc_pod_delete(klights_reconcile_api::GcPodDeleteRequest::new(
                        klights_types::PodIdentity::new(pod_ns, pod_name, pod_uid),
                    ))
                    .await?;
                deleted += 1;
            }
        }
    }

    // Re-query owned pods to get fresh state (may have changed since the scale operations above)
    let current_owned_pods = pod_reader
        .list_pods_by_owner_uid(PodOwnerListRequest::try_new(namespace, uid)?)
        .await?;
    let active_current_owned_pods: Vec<_> = current_owned_pods
        .iter()
        .filter(|p| pod_is_active(&p.data))
        .cloned()
        .collect();

    // Count pods with Ready=True condition (not terminating).
    let ready_replicas = common.count_ready_pods(&active_current_owned_pods);

    // Re-read the latest status-bearing snapshot before writing status so a
    // concurrent status-only write is not clobbered by reconcile from a stale
    // payload.
    let latest_status_resource = db.get_replicaset(namespace, name).await?;
    let latest_status_resource = match latest_status_resource {
        Some(resource) => resource,
        None => return Ok(()),
    };
    let observed_generation = latest_status_resource
        .data
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?
        .get("generation")
        .and_then(|g| g.as_u64())
        .unwrap_or(1);

    let existing_conditions = latest_status_resource
        .data
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

    db.update_replicaset_status(&latest_status_resource, status)
        .await?;

    Ok(())
}

fn pod_matches_selector(pod: &Value, selector: &Value) -> bool {
    let parsed = match klights_types::LabelSelector::from_k8s_selector(selector) {
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

const GENERATED_POD_CREATE_MAX_ATTEMPTS: usize = 8;

async fn create_pod(
    pod_writer: &(impl ReplicaSetPodMutation + ?Sized),
    rs_name: &str,
    rs_uid: &str,
    namespace: &str,
    node_name: &str,
    template: &Value,
) -> Result<()> {
    let prefix = format!("{rs_name}-");
    create_pod_with_name_generator(
        pod_writer,
        rs_name,
        rs_uid,
        namespace,
        node_name,
        template,
        || crate::resource_name::generate(&prefix),
    )
    .await
}

async fn create_pod_with_name_generator(
    pod_writer: &(impl ReplicaSetPodMutation + ?Sized),
    rs_name: &str,
    rs_uid: &str,
    namespace: &str,
    node_name: &str,
    template: &Value,
    mut generate_name: impl FnMut() -> String,
) -> Result<()> {
    let mut final_collision = None;
    for _ in 0..GENERATED_POD_CREATE_MAX_ATTEMPTS {
        let pod_name = generate_name();
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

        match pod_writer
            .create_replicaset_pod(namespace, &pod_name, node_name, pod)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.is_already_exists() => final_collision = Some(error),
            Err(error) => return Err(error.into()),
        }
    }

    Err(final_collision
        .expect("generated Pod retry budget is non-zero")
        .into())
}

#[cfg(test)]
mod tests;
