use crate::datastore::{DatastoreBackend, Resource};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::Result;
use serde_json::{Value, json};

pub fn compute_statefulset_update_revision(name: &str, template: &Value) -> String {
    let canonical = serde_json::to_string(template).unwrap_or_default();
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("{}-{:x}", name, hasher.finish())
}

fn derive_current_revision_from_pods(
    active_pods: &[&Resource],
    update_revision: &str,
) -> Option<String> {
    use std::collections::HashMap;

    let mut non_update_counts: HashMap<&str, usize> = HashMap::new();
    let mut any_update = false;

    for pod in active_pods {
        if let Some(rev) = pod
            .data
            .pointer("/metadata/labels/controller-revision-hash")
            .and_then(|v| v.as_str())
        {
            if rev == update_revision {
                any_update = true;
            } else {
                *non_update_counts.entry(rev).or_insert(0) += 1;
            }
        }
    }

    if let Some((rev, _)) = non_update_counts
        .into_iter()
        .max_by_key(|(_, count)| *count)
    {
        return Some(rev.to_string());
    }
    if any_update {
        return Some(update_revision.to_string());
    }
    None
}

fn statefulset_pod_ordinal(pod: &Resource, statefulset_name: &str) -> Option<usize> {
    pod.data
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())?
        .strip_prefix(&format!("{}-", statefulset_name))
        .and_then(|s| s.parse::<usize>().ok())
}

/// Check if a pod is Ready by fetching it through the pod repository.
async fn is_pod_ready(pod_reader: &dyn PodReader, namespace: &str, pod_name: &str) -> Result<bool> {
    let common = crate::controllers::common::controller_common();
    let pod = pod_reader.get_pod(namespace, pod_name).await?;

    match pod {
        Some(p) => Ok(common.is_pod_ready(&p.data)),
        None => Ok(false),
    }
}

async fn live_statefulset_replicas(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
) -> Result<Option<usize>> {
    let Some(resource) = db
        .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
        .await?
    else {
        return Ok(None);
    };
    if resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .is_some()
    {
        return Ok(None);
    }
    Ok(Some(
        resource
            .data
            .pointer("/spec/replicas")
            .and_then(|r| r.as_i64())
            .unwrap_or(1)
            .max(0) as usize,
    ))
}

pub async fn reconcile_statefulset(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
    statefulset: &Value,
    node_name: &str,
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    let input_metadata = statefulset
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

    let live_resource = match db
        .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
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
                .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
                .await?
            {
                Some(resource) => resource,
                None => return Ok(()),
            }
        }
        _ => live_resource,
    };
    let statefulset =
        crate::api::inject_resource_version(live_resource.data, live_resource.resource_version);

    let metadata = statefulset
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Skip reconciliation if the resource is being deleted
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    let spec = statefulset
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
    let service_name = spec
        .get("serviceName")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let pod_management_policy = spec
        .get("podManagementPolicy")
        .and_then(|p| p.as_str())
        .unwrap_or("OrderedReady");
    let update_revision = compute_statefulset_update_revision(name, template);

    // List existing pods owned by this StatefulSet
    let owned_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;

    // Filter out pods that are being deleted (have deletionTimestamp set)
    // These pods still exist in the database but should not count toward current replicas
    let active_pods: Vec<&Resource> = owned_pods
        .iter()
        .filter(|pod| {
            pod.data
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_none()
        })
        .collect();

    for pod in &active_pods {
        if pod.data.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Failed") {
            if let Some(pod_name) = pod.data.pointer("/metadata/name").and_then(|v| v.as_str()) {
                pod_writer.delete_pod(namespace, pod_name).await?;
            }
            return Ok(());
        }
    }

    let current_replicas = active_pods.len();
    let current_revision_for_create = statefulset
        .pointer("/status/currentRevision")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| derive_current_revision_from_pods(&active_pods, &update_revision))
        .unwrap_or_else(|| update_revision.clone());
    let update_strategy_for_create = spec
        .get("updateStrategy")
        .and_then(|u| u.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("RollingUpdate");
    let partition_for_create = if update_strategy_for_create == "RollingUpdate" {
        spec.pointer("/updateStrategy/rollingUpdate/partition")
            .and_then(|p| p.as_i64())
            .unwrap_or(0) as usize
    } else {
        0
    };

    // Create pods if we have fewer than desired replicas
    // StatefulSet creates pods with ordinal names: {name}-0, {name}-1, etc.
    if current_replicas < replicas {
        let active_ordinals: std::collections::BTreeSet<usize> = active_pods
            .iter()
            .filter_map(|pod| statefulset_pod_ordinal(pod, name))
            .collect();
        let occupied_ordinals: std::collections::BTreeSet<usize> = owned_pods
            .iter()
            .filter_map(|pod| statefulset_pod_ordinal(pod, name))
            .collect();
        let missing_ordinals: Vec<usize> = (0..replicas)
            .filter(|ordinal| {
                !active_ordinals.contains(ordinal) && !occupied_ordinals.contains(ordinal)
            })
            .collect();
        let ordinals_to_create: Vec<usize> = if pod_management_policy == "Parallel" {
            missing_ordinals
        } else {
            let next_ordinal = missing_ordinals
                .into_iter()
                .find(|ordinal| (0..*ordinal).all(|lower| active_ordinals.contains(&lower)));
            match next_ordinal {
                Some(ordinal) => {
                    let mut predecessors_ready = true;
                    for lower in 0..ordinal {
                        if !is_pod_ready(pod_reader, namespace, &format!("{}-{}", name, lower))
                            .await?
                        {
                            predecessors_ready = false;
                            break;
                        }
                    }
                    if predecessors_ready {
                        vec![ordinal]
                    } else {
                        vec![]
                    }
                }
                None => vec![],
            }
        };

        for i in ordinals_to_create {
            let Some(live_replicas) = live_statefulset_replicas(db, namespace, name).await? else {
                return Ok(());
            };
            if i >= live_replicas {
                break;
            }
            let pod_name = format!("{}-{}", name, i);
            let target_revision = if update_strategy_for_create == "RollingUpdate"
                && i < partition_for_create
                && current_revision_for_create != update_revision
            {
                current_revision_for_create.as_str()
            } else {
                update_revision.as_str()
            };
            let template_for_ordinal = if target_revision == current_revision_for_create
                && target_revision != update_revision
            {
                active_pods
                    .iter()
                    .find(|pod| {
                        pod.data
                            .pointer("/metadata/labels/controller-revision-hash")
                            .and_then(|v| v.as_str())
                            == Some(target_revision)
                    })
                    .and_then(|pod| pod.data.get("spec"))
                    .map(|spec| {
                        let mut old_template = template.clone();
                        if let Some(obj) = old_template.as_object_mut() {
                            obj.insert("spec".to_string(), spec.clone());
                        }
                        old_template
                    })
                    .unwrap_or_else(|| template.clone())
            } else {
                template.clone()
            };

            let mut pod = crate::controllers::common::build_child_pod(
                &template_for_ordinal,
                &pod_name,
                namespace,
                node_name,
                crate::controllers::common::OwnerInfo {
                    api_version: "apps/v1",
                    kind: "StatefulSet",
                    name,
                    uid,
                },
                &[("controller-revision-hash", target_revision)],
                &[],
            )?;
            // StatefulSet pods get hostname and subdomain for stable network
            // identity — controller-specific extension on top of the canonical
            // template.
            if let Some(spec_obj) = pod.get_mut("spec").and_then(|s| s.as_object_mut()) {
                spec_obj.insert("hostname".to_string(), json!(pod_name));
                spec_obj.insert("subdomain".to_string(), json!(service_name));
            }

            pod_writer
                .create_controller_pod(namespace, &pod_name, node_name, pod)
                .await?;
        }
    }

    // Handle rolling update with partition (canary updates)
    let update_strategy = spec
        .get("updateStrategy")
        .and_then(|u| u.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("RollingUpdate");

    if update_strategy == "RollingUpdate" {
        let partition = spec
            .pointer("/updateStrategy/rollingUpdate/partition")
            .and_then(|p| p.as_i64())
            .unwrap_or(0) as usize;

        // StatefulSet rolling updates are ordered highest-ordinal first and
        // advance one pod at a time when higher ordinals are ready on the
        // update revision.
        let mut pods_with_ordinal: Vec<(usize, &Resource)> = active_pods
            .iter()
            .filter_map(|pod| {
                let pod_name = pod
                    .data
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let ordinal = pod_name
                    .strip_prefix(&format!("{}-", name))
                    .and_then(|s| s.parse::<usize>().ok())?;
                Some((ordinal, *pod))
            })
            .collect();
        pods_with_ordinal.sort_by_key(|(ordinal, _)| std::cmp::Reverse(*ordinal));

        for (ordinal, pod) in &pods_with_ordinal {
            if *ordinal < partition {
                continue;
            }

            let pod_name = pod
                .data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("");
            if pod_name.is_empty() {
                continue;
            }

            // Check if pod's template matches current template by comparing container images
            let pod_containers = pod
                .data
                .pointer("/spec/containers")
                .and_then(|c| c.as_array());
            let template_containers = template
                .pointer("/spec/containers")
                .and_then(|c| c.as_array());

            let needs_update = match (pod_containers, template_containers) {
                (Some(pc), Some(tc)) => {
                    if pc.len() != tc.len() {
                        true
                    } else {
                        pc.iter().zip(tc.iter()).any(|(p, t)| {
                            p.get("image") != t.get("image")
                                || p.get("name") != t.get("name")
                                || p.get("command") != t.get("command")
                                || p.get("args") != t.get("args")
                                || p.get("env") != t.get("env")
                        })
                    }
                }
                _ => false,
            };

            let pod_revision = pod
                .data
                .pointer("/metadata/labels/controller-revision-hash")
                .and_then(|v| v.as_str());
            let needs_revision_update = pod_revision != Some(update_revision.as_str());
            let needs_update = needs_update || needs_revision_update;
            if !needs_update {
                continue;
            }

            let higher_ordinals_ready_on_update = ((*ordinal + 1)..replicas).all(|higher| {
                if higher < partition {
                    return true;
                }
                pods_with_ordinal
                    .iter()
                    .find(|(candidate, _)| *candidate == higher)
                    .is_some_and(|(_, higher_pod)| {
                        let higher_rev = higher_pod
                            .data
                            .pointer("/metadata/labels/controller-revision-hash")
                            .and_then(|v| v.as_str());
                        higher_rev == Some(update_revision.as_str())
                            && common.is_pod_ready(&higher_pod.data)
                    })
            });
            if !higher_ordinals_ready_on_update || !common.is_pod_ready(&pod.data) {
                break;
            }

            // Delete a single ordinal per reconcile. The replacement has the
            // same Pod name, so creation must wait until the lifecycle actor
            // finalizes the old UID and frees the datastore name slot.
            pod_writer.delete_pod(namespace, pod_name).await?;
            break;
        }
    }

    // Re-fetch active pods after potential rolling update changes
    let owned_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let active_pods: Vec<&Resource> = owned_pods
        .iter()
        .filter(|pod| {
            pod.data
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_none()
        })
        .collect();
    let current_replicas = active_pods.len();

    // Delete excess pods if we have more than desired replicas
    // StatefulSet deletes in reverse ordinal order (highest first)
    if current_replicas > replicas {
        // Sort by ordinal and delete highest condemned ordinals first.
        let mut pods_with_ordinal: Vec<(usize, String, String)> = active_pods
            .iter()
            .filter_map(|p| {
                let pod_name = p
                    .data
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let pod_ns = p
                    .data
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string();
                let ordinal = pod_name
                    .strip_prefix(&format!("{name}-"))
                    .and_then(|s| s.parse::<usize>().ok())?;
                Some((ordinal, pod_name, pod_ns))
            })
            .collect();

        pods_with_ordinal.sort_by_key(|entry| std::cmp::Reverse(entry.0));

        let condemned_pods: Vec<(usize, String, String)> = pods_with_ordinal
            .into_iter()
            .filter(|(ordinal, _, _)| *ordinal >= replicas)
            .collect();

        if pod_management_policy != "Parallel"
            && owned_pods
                .iter()
                .any(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
        {
            tracing::debug!(
                "StatefulSet {}/{} scale-down halted: a higher ordinal pod is still terminating",
                namespace,
                name
            );
        } else if pod_management_policy == "Parallel" {
            for (ordinal, pod_name, pod_ns) in
                condemned_pods.iter().take(current_replicas - replicas)
            {
                if !pod_name.is_empty() && !pod_ns.is_empty() {
                    let Some(live_replicas) =
                        live_statefulset_replicas(db, namespace, name).await?
                    else {
                        return Ok(());
                    };
                    if *ordinal < live_replicas {
                        continue;
                    }
                    pod_writer.delete_pod(pod_ns, pod_name).await?;
                }
            }
        } else if let Some((target_ordinal, pod_name, pod_ns)) = condemned_pods.first() {
            // OrderedReady scale-down only requires predecessors of the target
            // ordinal to be Ready; the target pod itself may be Pending or
            // otherwise unhealthy and still must be deleted.
            let predecessors_ready = active_pods
                .iter()
                .filter_map(|pod| statefulset_pod_ordinal(pod, name).map(|ordinal| (ordinal, pod)))
                .filter(|(ordinal, _)| *ordinal < *target_ordinal)
                .all(|(_, pod)| common.is_pod_ready(&pod.data));

            if predecessors_ready {
                if !pod_name.is_empty() && !pod_ns.is_empty() {
                    let Some(live_replicas) =
                        live_statefulset_replicas(db, namespace, name).await?
                    else {
                        return Ok(());
                    };
                    if *target_ordinal < live_replicas {
                        return Ok(());
                    }
                    pod_writer.delete_pod(pod_ns, pod_name).await?;
                }
            } else {
                tracing::debug!(
                    "StatefulSet {}/{} scale-down halted: a predecessor of ordinal {} is not Ready",
                    namespace,
                    name,
                    target_ordinal
                );
            }
        }
    }

    // Preserve currentRevision from existing status.
    // If status is absent (for example an update payload omitted status),
    // infer a stable current revision from existing pods first.
    let current_revision = statefulset
        .pointer("/status/currentRevision")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| derive_current_revision_from_pods(&active_pods, &update_revision))
        .unwrap_or_else(|| update_revision.clone());

    // Re-fetch pods for accurate counting after updates
    let final_pods = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let final_active: Vec<&Resource> = final_pods
        .iter()
        .filter(|pod| {
            pod.data
                .get("metadata")
                .and_then(|m| m.get("deletionTimestamp"))
                .is_none()
        })
        .collect();

    // Count updated/current replicas by controller-revision-hash label.
    let updated_replicas = final_active
        .iter()
        .filter(|pod| {
            pod.data
                .pointer("/metadata/labels/controller-revision-hash")
                .and_then(|v| v.as_str())
                == Some(update_revision.as_str())
        })
        .count();
    let current_replicas = final_active
        .iter()
        .filter(|pod| {
            pod.data
                .pointer("/metadata/labels/controller-revision-hash")
                .and_then(|v| v.as_str())
                == Some(current_revision.as_str())
        })
        .count();

    // Count ready pods
    let mut ready_count = 0usize;
    for pod in &final_active {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        if is_pod_ready(pod_reader, namespace, pod_name).await? {
            ready_count += 1;
        }
    }

    // Advance currentRevision only after a full, ready rollout completes.
    let final_current_revision = if current_revision == update_revision
        || (updated_replicas == replicas && ready_count == replicas)
    {
        update_revision.clone()
    } else {
        current_revision
    };

    // Preserve conditions set via UpdateStatus — the STS controller only owns replica
    // count and revision fields, not conditions set by external callers.
    let existing_conditions = statefulset
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Update StatefulSet status
    let mut status = json!({
        "replicas": final_active.len(),
        "readyReplicas": ready_count,
        "currentReplicas": current_replicas,
        "updatedReplicas": updated_replicas,
        "currentRevision": final_current_revision,
        "updateRevision": update_revision,
        "observedGeneration": metadata.get("generation").and_then(|g| g.as_i64()).unwrap_or(0)
    });
    if !existing_conditions.is_empty() {
        status["conditions"] = Value::Array(existing_conditions);
    }

    // Re-read the StatefulSet to get a fresh RV for status CAS. The
    // `statefulset` parameter was read at the top of reconcile and is
    // stale after pod create/delete operations.
    let fresh_sts = match db
        .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
        .await?
    {
        Some(r) => crate::api::inject_resource_version(r.data, r.resource_version),
        None => return Ok(()),
    };
    // Update observedGeneration from the fresh read
    status["observedGeneration"] = json!(
        fresh_sts
            .get("metadata")
            .and_then(|m| m.get("generation"))
            .and_then(|g| g.as_i64())
            .unwrap_or(0)
    );

    crate::controllers::common::write_status(db, &fresh_sts, &status).await?;

    Ok(())
}

#[cfg(test)]
mod tests;
