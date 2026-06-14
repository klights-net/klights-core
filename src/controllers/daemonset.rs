use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use anyhow::Result;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracing;

/// Compute a deterministic hash of the pod template for change detection.
/// Uses SHA256 which is stable across process restarts.
fn compute_value_hash(value: &Value) -> String {
    let template_str = serde_json::to_string(value).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(template_str.as_bytes());
    let result = hasher.finalize();
    // Use first 10 hex chars
    let hex_string = result
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    hex_string[..10].to_string()
}

fn daemonset_template_patch(template: &Value) -> Value {
    let mut patch_template = template.clone();
    prune_go_json_omitempty_zero_values(&mut patch_template);
    if let Some(obj) = patch_template.as_object_mut() {
        obj.insert("$patch".to_string(), json!("replace"));
        if let Some(containers) = obj
            .get_mut("spec")
            .and_then(|spec| spec.get_mut("containers"))
            .and_then(|containers| containers.as_array_mut())
        {
            for container in containers {
                if let Some(container) = container.as_object_mut() {
                    container
                        .entry("resources".to_string())
                        .or_insert_with(|| json!({}));
                }
            }
        }
    }
    json!({"spec": {"template": patch_template}})
}

struct ResolvedControllerRevision {
    name: String,
    hash: String,
}

async fn resolve_controller_revision_for_template(
    db: &dyn DatastoreBackend,
    namespace: &str,
    daemonset_name: &str,
    daemonset_uid: &str,
    template: &Value,
) -> Result<ResolvedControllerRevision> {
    let data = daemonset_template_patch(template);
    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    tracing::info!(
        target: "klights::daemonset::controller_revision",
        ds = %daemonset_name,
        ns = %namespace,
        existing_revisions = revisions.items.len(),
        "resolve_controller_revision: listed existing revisions"
    );
    for revision in revisions.items {
        if !crate::controllers::common::is_owned_by(&revision.data, daemonset_uid) {
            continue;
        }
        if revision.data.get("data") == Some(&data) {
            let hash = revision
                .data
                .pointer("/metadata/labels/controller-revision-hash")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| compute_value_hash(&data));
            tracing::info!(
                target: "klights::daemonset::controller_revision",
                ds = %daemonset_name,
                name = %revision.name,
                hash = %hash,
                "reusing existing ControllerRevision"
            );
            return Ok(ResolvedControllerRevision {
                name: revision.name,
                hash,
            });
        }
    }

    let hash = compute_value_hash(&data);
    tracing::info!(
        target: "klights::daemonset::controller_revision",
        ds = %daemonset_name,
        hash = %hash,
        name = format!("{}-{}", daemonset_name, &hash[..8]),
        "resolved new ControllerRevision (no existing match)"
    );
    Ok(ResolvedControllerRevision {
        name: format!("{}-{}", daemonset_name, &hash[..8]),
        hash,
    })
}

async fn controller_revision_names_by_hash(
    db: &dyn DatastoreBackend,
    namespace: &str,
    daemonset_uid: &str,
) -> Result<std::collections::HashMap<String, String>> {
    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let mut by_hash = std::collections::HashMap::new();
    for revision in revisions.items {
        if !crate::controllers::common::is_owned_by(&revision.data, daemonset_uid) {
            continue;
        }
        let hash = revision
            .data
            .pointer("/metadata/labels/controller-revision-hash")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| revision.data.get("data").map(compute_value_hash));
        if let Some(hash) = hash {
            by_hash.insert(hash, revision.name);
        }
    }
    Ok(by_hash)
}

fn pod_revision_hash(pod: &Value) -> Option<&str> {
    pod.pointer("/metadata/labels/controller-revision-hash")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/metadata/annotations/klights.dev~1template-hash")
                .and_then(|v| v.as_str())
        })
}

fn pod_is_deleting(pod: &Value) -> bool {
    pod.pointer("/metadata/deletionTimestamp").is_some()
}

fn deleting_pod_blocks_daemonset_replacement(pod: &Value, current_template_hash: &str) -> bool {
    if !pod_is_deleting(pod) {
        return false;
    }

    // For non-surge rollouts, an old-revision terminating pod still occupies
    // its node until the lifecycle actor finishes runtime cleanup and removes
    // the API row. Blocking only old-revision rows keeps failed/current
    // revision replacement fast while preventing rollback observers from
    // treating a stale terminating old pod as a preserved instance.
    pod_revision_hash(pod)
        .map(|hash| hash != current_template_hash)
        .unwrap_or(true)
}

#[derive(Debug)]
struct DaemonSetPodChoice {
    name: String,
    hash_matches: bool,
    not_terminating: bool,
    ready: bool,
    running: bool,
    creation_timestamp: String,
}

#[derive(Debug)]
struct DaemonSetPodOnNode {
    name: String,
    hash: String,
    available: bool,
}

fn pod_ready(pod: &Value) -> bool {
    pod.pointer("/status/conditions")
        .and_then(|conditions| conditions.as_array())
        .map(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
            })
        })
        .unwrap_or(false)
}

fn duplicate_daemonset_pods_to_delete(
    pods: &[Resource],
    matching_node_names: &std::collections::HashSet<String>,
    current_template_hash: &str,
) -> Vec<String> {
    let mut pods_by_node: std::collections::HashMap<String, Vec<DaemonSetPodChoice>> =
        std::collections::HashMap::new();

    for pod in pods {
        if pod_is_deleting(&pod.data) {
            continue;
        }
        let Some(node_name) = pod
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .filter(|node_name| matching_node_names.contains(*node_name))
        else {
            continue;
        };
        let Some(pod_name) = pod.data.pointer("/metadata/name").and_then(|v| v.as_str()) else {
            continue;
        };

        let phase = pod
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        pods_by_node
            .entry(node_name.to_string())
            .or_default()
            .push(DaemonSetPodChoice {
                name: pod_name.to_string(),
                hash_matches: pod_revision_hash(&pod.data) == Some(current_template_hash),
                not_terminating: pod.data.pointer("/metadata/deletionTimestamp").is_none(),
                ready: pod_ready(&pod.data),
                running: phase == "Running",
                creation_timestamp: pod
                    .data
                    .pointer("/metadata/creationTimestamp")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            });
    }

    let mut delete_names = Vec::new();
    for candidates in pods_by_node.values_mut() {
        if candidates.len() <= 1 {
            continue;
        }
        candidates.sort_by(|a, b| {
            b.hash_matches
                .cmp(&a.hash_matches)
                .then_with(|| b.not_terminating.cmp(&a.not_terminating))
                .then_with(|| b.ready.cmp(&a.ready))
                .then_with(|| b.running.cmp(&a.running))
                .then_with(|| a.creation_timestamp.cmp(&b.creation_timestamp))
                .then_with(|| a.name.cmp(&b.name))
        });
        delete_names.extend(candidates.iter().skip(1).map(|pod| pod.name.clone()));
    }
    delete_names.sort();
    delete_names
}

/// Compare two ControllerRevision JSON values while ignoring server-set
/// metadata fields (uid, creationTimestamp, generation, resourceVersion)
/// that are injected by create_resource but not included in the `desired`
/// JSON constructed by ensure_controller_revision.
fn controller_revision_data_equal(a: &Value, b: &Value) -> bool {
    let a = strip_server_metadata(a);
    let b = strip_server_metadata(b);
    a == b
}

fn strip_server_metadata(v: &Value) -> Value {
    let mut v = v.clone();
    if let Some(meta) = v.pointer_mut("/metadata")
        && let Some(obj) = meta.as_object_mut()
    {
        obj.remove("uid");
        obj.remove("creationTimestamp");
        obj.remove("generation");
        obj.remove("resourceVersion");
        obj.remove("selfLink");
        obj.remove("managedFields");
    }
    // Also strip resourceVersion from list metadata
    if let Some(meta) = v.pointer_mut("/metadata")
        && let Some(obj) = meta.as_object_mut()
    {
        obj.remove("resourceVersion");
    }
    v
}

fn prune_go_json_omitempty_zero_values(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for child in map.values_mut() {
                prune_go_json_omitempty_zero_values(child);
            }
            map.retain(|_, child| !is_go_json_omitempty_zero_value(child));
        }
        Value::Array(items) => {
            for item in items {
                prune_go_json_omitempty_zero_values(item);
            }
        }
        _ => {}
    }
}

fn is_go_json_omitempty_zero_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(false) => true,
        Value::Number(n) => n.as_i64() == Some(0) || n.as_u64() == Some(0),
        Value::String(s) => s.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}

struct DaemonSetRevisionInput<'a> {
    namespace: &'a str,
    daemonset_name: &'a str,
    daemonset_uid: &'a str,
    revision_name: &'a str,
    revision_hash: &'a str,
    previous_update_revision: Option<&'a str>,
    template: &'a Value,
}

async fn ensure_controller_revision(
    db: &dyn DatastoreBackend,
    input: DaemonSetRevisionInput<'_>,
) -> Result<()> {
    let data = daemonset_template_patch(input.template);
    let existing = db
        .get_resource(
            "apps/v1",
            "ControllerRevision",
            Some(input.namespace),
            input.revision_name,
        )
        .await?;
    tracing::info!(
        target: "klights::daemonset::controller_revision",
        ds = %input.daemonset_name,
        ns = %input.namespace,
        revision_name = %input.revision_name,
        hash = %input.revision_hash,
        exists = %existing.is_some(),
        prev_update = ?input.previous_update_revision,
        "ensure_controller_revision called"
    );
    if existing.is_none()
        && input.previous_update_revision == Some(input.revision_name)
        && has_any_controller_revision(db, input.namespace, input.daemonset_uid).await?
    {
        tracing::info!(
            target: "klights::daemonset::controller_revision",
            ds = %input.daemonset_name,
            "ControllerRevision already has previous update revision; skipping create"
        );
        return Ok(());
    }

    let revision = if let Some(existing) = &existing {
        if existing.data.get("data") == Some(&data) {
            existing
                .data
                .get("revision")
                .and_then(|v| v.as_i64())
                .unwrap_or(1)
        } else {
            next_controller_revision_number(db, input.namespace, input.daemonset_uid).await?
        }
    } else {
        next_controller_revision_number(db, input.namespace, input.daemonset_uid).await?
    };

    let mut labels = input
        .template
        .pointer("/metadata/labels")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    labels.insert(
        "controller-revision-hash".to_string(),
        json!(input.revision_hash),
    );
    labels
        .entry("daemonset-name".to_string())
        .or_insert_with(|| json!(input.daemonset_name));

    let desired = json!({
        "apiVersion": "apps/v1",
        "kind": "ControllerRevision",
        "metadata": {
            "name": input.revision_name,
            "namespace": input.namespace,
            "labels": labels,
            "ownerReferences": [crate::controllers::common::build_owner_ref(
                "apps/v1",
                "DaemonSet",
                input.daemonset_name,
                input.daemonset_uid,
            )]
        },
        "data": data,
        "revision": revision,
    });

    match existing {
        Some(existing) => {
            // Compare ignoring server-set metadata fields (uid, creationTimestamp,
            // generation, resourceVersion) that create_resource injects but we
            // don't include in `desired`. Otherwise the comparison is always
            // false and we keep rewriting the ControllerRevision on every
            // reconcile (e.g. Node heartbeat → daemonset_node_reconcile storm).
            let data_changed = !controller_revision_data_equal(&existing.data, &desired);
            if data_changed {
                tracing::info!(
                    target: "klights::daemonset::controller_revision",
                    ds = %input.daemonset_name,
                    revision_name = %input.revision_name,
                    "updating existing ControllerRevision"
                );
                db.update_resource_with_preconditions(
                    "apps/v1",
                    "ControllerRevision",
                    Some(input.namespace),
                    input.revision_name,
                    desired,
                    ResourcePreconditions::from_resource(&existing),
                )
                .await?;
            } else {
                tracing::debug!(
                    target: "klights::daemonset::controller_revision",
                    ds = %input.daemonset_name,
                    revision_name = %input.revision_name,
                    "ControllerRevision unchanged; skipping update"
                );
            }
        }
        None => {
            tracing::info!(
                target: "klights::daemonset::controller_revision",
                ds = %input.daemonset_name,
                revision_name = %input.revision_name,
                revision = %revision,
                "creating new ControllerRevision"
            );
            match db
                .create_resource(
                    "apps/v1",
                    "ControllerRevision",
                    Some(input.namespace),
                    input.revision_name,
                    desired,
                )
                .await
            {
                Ok(resource) => {
                    tracing::info!(
                        target: "klights::daemonset::controller_revision",
                        ds = %input.daemonset_name,
                        revision_name = %input.revision_name,
                        rv = %resource.resource_version,
                        "ControllerRevision created successfully"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "klights::daemonset::controller_revision",
                        ds = %input.daemonset_name,
                        revision_name = %input.revision_name,
                        error = %e,
                        "ControllerRevision create FAILED"
                    );
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

async fn has_any_controller_revision(
    db: &dyn DatastoreBackend,
    namespace: &str,
    daemonset_uid: &str,
) -> Result<bool> {
    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    Ok(revisions
        .items
        .iter()
        .any(|revision| crate::controllers::common::is_owned_by(&revision.data, daemonset_uid)))
}

async fn next_controller_revision_number(
    db: &dyn DatastoreBackend,
    namespace: &str,
    daemonset_uid: &str,
) -> Result<i64> {
    let revisions = db
        .list_resources(
            "apps/v1",
            "ControllerRevision",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let max_revision = revisions
        .items
        .iter()
        .filter(|revision| crate::controllers::common::is_owned_by(&revision.data, daemonset_uid))
        .filter_map(|revision| revision.data.get("revision").and_then(|v| v.as_i64()))
        .max()
        .unwrap_or(0);
    Ok(max_revision + 1)
}

fn node_matches_template_selector(node: &Value, template: &Value) -> bool {
    let Some(selector) = template
        .pointer("/spec/nodeSelector")
        .and_then(|selector| selector.as_object())
    else {
        return true;
    };

    let labels = node
        .pointer("/metadata/labels")
        .and_then(|labels| labels.as_object());
    selector.iter().all(|(key, expected)| {
        labels
            .and_then(|labels| labels.get(key))
            .map(|actual| actual == expected)
            .unwrap_or(false)
    })
}

async fn live_daemonset_active(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
) -> Result<bool> {
    let Some(resource) = db
        .get_resource("apps/v1", "DaemonSet", Some(namespace), name)
        .await?
    else {
        return Ok(false);
    };
    Ok(resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .is_none())
}

/// Reconcile a DaemonSet by ensuring one pod exists per node.
/// Phase 1 (single-node): creates exactly one pod for the local node.
pub async fn reconcile_daemonset(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn PodReader,
    pod_writer: &dyn PodObjectWriter,
    pod_delete_sink: &dyn crate::controllers::gc::GcPodDeleteSink,
    daemonset: &Value,
) -> Result<()> {
    let common = crate::controllers::common::controller_common();
    let input_metadata = daemonset
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
        .get_resource("apps/v1", "DaemonSet", Some(namespace), name)
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
                .get_resource("apps/v1", "DaemonSet", Some(namespace), name)
                .await?
            {
                Some(resource) => resource,
                None => return Ok(()),
            }
        }
        _ => live_resource,
    };
    let daemonset =
        crate::api::inject_resource_version(live_resource.data, live_resource.resource_version);

    let metadata = daemonset
        .get("metadata")
        .ok_or_else(|| anyhow::anyhow!("Missing metadata"))?;

    // Skip reconciliation if the resource is being deleted
    if metadata.get("deletionTimestamp").is_some() {
        return Ok(());
    }

    let spec = daemonset
        .get("spec")
        .ok_or_else(|| anyhow::anyhow!("Missing spec"))?;

    let uid = metadata
        .get("uid")
        .and_then(|u| u.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing uid"))?;

    tracing::info!(
        target: "klights::daemonset::reconcile",
        ds = %name,
        ns = %namespace,
        uid = %uid,
        "reconcile_daemonset started"
    );

    let template = spec
        .get("template")
        .ok_or_else(|| anyhow::anyhow!("Missing template"))?;

    // Get update strategy (default: RollingUpdate)
    let update_strategy = spec
        .get("updateStrategy")
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("RollingUpdate");

    let max_unavailable = if update_strategy == "RollingUpdate" {
        spec.get("updateStrategy")
            .and_then(|s| s.get("rollingUpdate"))
            .and_then(|r| r.get("maxUnavailable"))
            .and_then(|m| m.as_i64())
            .unwrap_or(1) as usize
    } else {
        0
    };

    // Resolve the target ControllerRevision by patch data so rollback to an
    // earlier equivalent template reuses the existing revision and hash.
    let resolved_revision =
        resolve_controller_revision_for_template(db, namespace, name, uid, template).await?;
    let current_template_hash = resolved_revision.hash;
    let current_revision = resolved_revision.name;
    let observed_generation = metadata
        .get("generation")
        .and_then(|g| g.as_i64())
        .unwrap_or(1);
    let previous_update_revision = daemonset
        .pointer("/status/updateRevision")
        .and_then(|v| v.as_str());
    tracing::info!(
        target: "klights::daemonset::reconcile",
        ds = %name,
        revision = %current_revision,
        hash = %current_template_hash,
        prev_update = ?previous_update_revision,
        "calling ensure_controller_revision"
    );
    ensure_controller_revision(
        db,
        DaemonSetRevisionInput {
            namespace,
            daemonset_name: name,
            daemonset_uid: uid,
            revision_name: &current_revision,
            revision_hash: &current_template_hash,
            previous_update_revision,
            template,
        },
    )
    .await?;
    tracing::info!(
        target: "klights::daemonset::reconcile",
        ds = %name,
        "ensure_controller_revision completed"
    );

    // List all nodes, then restrict scheduling to nodes that match the
    // DaemonSet pod template's nodeSelector. Kubernetes DaemonSets do not
    // create pods on nodes that cannot run the template.
    let node_list = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let matching_nodes: Vec<_> = node_list
        .items
        .iter()
        .filter(|node| node_matches_template_selector(&node.data, template))
        .collect();
    let matching_node_names: std::collections::HashSet<String> = matching_nodes
        .iter()
        .filter_map(|node| {
            node.data
                .pointer("/metadata/name")
                .and_then(|name| name.as_str())
                .map(str::to_string)
        })
        .collect();

    // List existing pods owned by this DaemonSet
    let owned_pod_resources = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;

    // Build map: node_name -> current pod state used for non-surge rolling updates.
    let mut node_to_pod: std::collections::HashMap<String, DaemonSetPodOnNode> =
        std::collections::HashMap::new();
    for pod_resource in &owned_pod_resources {
        if pod_is_deleting(&pod_resource.data) {
            continue;
        }
        if let Some(node_name) = pod_resource
            .data
            .get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
        {
            let pod_name = pod_resource
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();

            let pod_hash = pod_revision_hash(&pod_resource.data)
                .unwrap_or("")
                .to_string();
            let available = crate::controllers::common::is_pod_ready_value(&pod_resource.data);

            node_to_pod.insert(
                node_name.to_string(),
                DaemonSetPodOnNode {
                    name: pod_name,
                    hash: pod_hash,
                    available,
                },
            );
        }
    }

    // Handle update strategy: delete pods with outdated template
    if update_strategy == "RollingUpdate" {
        let mut num_unavailable = 0usize;
        let mut allowed_replacement_pods = Vec::new();
        let mut candidate_pods_to_delete = Vec::new();

        for node_name in &matching_node_names {
            match node_to_pod.get(node_name) {
                None => {
                    num_unavailable += 1;
                }
                Some(pod) if pod.hash == current_template_hash => {
                    if !pod.available {
                        num_unavailable += 1;
                    }
                }
                Some(pod) => {
                    if !pod.available {
                        allowed_replacement_pods.push(pod.name.clone());
                        num_unavailable += 1;
                    } else {
                        candidate_pods_to_delete.push(pod.name.clone());
                    }
                }
            }
        }

        let remaining_unavailable = max_unavailable
            .saturating_sub(num_unavailable)
            .min(candidate_pods_to_delete.len());
        let old_pods_to_delete = allowed_replacement_pods.into_iter().chain(
            candidate_pods_to_delete
                .into_iter()
                .take(remaining_unavailable),
        );

        for pod_name in old_pods_to_delete {
            pod_writer.delete_pod(namespace, &pod_name).await?;
        }
    }
    // OnDelete strategy: do nothing, users must manually delete pods

    // Re-list pods after potential deletions
    let owned_pod_resources_updated = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;

    // Delete Failed/Succeeded pods so replacements can be created
    for pod_resource in &owned_pod_resources_updated {
        if pod_is_deleting(&pod_resource.data) {
            continue;
        }
        let phase = pod_resource
            .data
            .pointer("/status/phase")
            .and_then(|p| p.as_str())
            .unwrap_or("");
        if phase == "Failed" || phase == "Succeeded" {
            let pod_name = pod_resource
                .data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("");
            if !pod_name.is_empty() {
                // Surface delete failures so the controller's retry loop can
                // re-attempt cleanup; silently dropping the error leaves the
                // Failed/Succeeded pod in place without observability.
                pod_writer.delete_pod(namespace, pod_name).await?;
            }
        }
    }

    // Re-list again after deleting terminated pods. Delete pods that are now
    // misscheduled because node labels or the template nodeSelector changed;
    // replacements are created below only for matching nodes.
    let mut owned_pod_resources_active = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    for pod_resource in &owned_pod_resources_active {
        if pod_is_deleting(&pod_resource.data) {
            continue;
        }
        let Some(node_name) = pod_resource
            .data
            .pointer("/spec/nodeName")
            .and_then(|n| n.as_str())
        else {
            continue;
        };
        if !matching_node_names.contains(node_name)
            && let Some(pod_name) = pod_resource
                .data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
        {
            pod_writer.delete_pod(namespace, pod_name).await?;
        }
    }

    owned_pod_resources_active = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let duplicate_pod_names = duplicate_daemonset_pods_to_delete(
        &owned_pod_resources_active,
        &matching_node_names,
        &current_template_hash,
    );
    for pod_name in &duplicate_pod_names {
        tracing::info!(
            target: "klights::daemonset::reconcile",
            ds = %name,
            ns = %namespace,
            pod = %pod_name,
            "deleting duplicate DaemonSet pod on node"
        );
        pod_writer.delete_pod(namespace, pod_name).await?;
    }
    if !duplicate_pod_names.is_empty() {
        owned_pod_resources_active = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    }

    let mut current_node_pods: std::collections::HashSet<String> = std::collections::HashSet::new();
    for pod_resource in &owned_pod_resources_active {
        if pod_is_deleting(&pod_resource.data)
            && !deleting_pod_blocks_daemonset_replacement(
                &pod_resource.data,
                &current_template_hash,
            )
        {
            continue;
        }
        if let Some(node_name) = pod_resource
            .data
            .get("spec")
            .and_then(|s| s.get("nodeName"))
            .and_then(|n| n.as_str())
            .filter(|node_name| matching_node_names.contains(*node_name))
        {
            current_node_pods.insert(node_name.to_string());
        }
    }

    // Create a pod for each matching node that doesn't already have one
    for node_resource in &matching_nodes {
        let node_name = node_resource
            .data
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("");

        if node_name.is_empty() || current_node_pods.contains(node_name) {
            continue;
        }
        if !live_daemonset_active(db, namespace, name).await? {
            return Ok(());
        }

        let pod_name = format!(
            "{}-{}",
            name,
            uuid::Uuid::new_v4()
                .to_string()
                .chars()
                .take(5)
                .collect::<String>()
        );

        let mut pod = crate::controllers::common::build_child_pod(
            template,
            &pod_name,
            namespace,
            node_name,
            crate::controllers::common::OwnerInfo {
                api_version: "apps/v1",
                kind: "DaemonSet",
                name,
                uid,
            },
            &[],
            &[("klights.dev/template-hash", current_template_hash.as_str())],
        )?;
        if let Some(labels) = pod
            .pointer_mut("/metadata/labels")
            .and_then(|labels| labels.as_object_mut())
        {
            labels.insert(
                "controller-revision-hash".to_string(),
                json!(current_template_hash),
            );
        }

        pod_writer
            .create_controller_pod(namespace, &pod_name, node_name, pod)
            .await?;
    }

    // Update DaemonSet status
    let desired = matching_nodes.len() as i64;
    let final_pod_count = pod_reader.list_pods_by_owner_uid(namespace, uid).await?;
    let active_final_pods: Vec<_> = final_pod_count
        .iter()
        .filter(|pod| !pod_is_deleting(&pod.data))
        .cloned()
        .collect();
    let scheduled_pods: Vec<_> = active_final_pods
        .iter()
        .filter(|pod| {
            pod.data
                .pointer("/spec/nodeName")
                .and_then(|node| node.as_str())
                .map(|node| matching_node_names.contains(node))
                .unwrap_or(false)
        })
        .cloned()
        .collect();
    let misscheduled = active_final_pods.len().saturating_sub(scheduled_pods.len()) as i64;
    let current = scheduled_pods.len() as i64;

    // Count ready and available pods from actual pod status
    let number_ready = common.count_ready_pods(&scheduled_pods) as i64;
    let number_available = number_ready;
    let updated_number_scheduled = scheduled_pods
        .iter()
        .filter(|pod| pod_revision_hash(&pod.data) == Some(current_template_hash.as_str()))
        .count() as i64;
    let revision_names_by_hash = controller_revision_names_by_hash(db, namespace, uid).await?;
    let mut scheduled_hashes: Vec<String> = scheduled_pods
        .iter()
        .filter_map(|pod| pod_revision_hash(&pod.data).map(str::to_string))
        .collect();
    scheduled_hashes.sort();
    scheduled_hashes.dedup();
    let current_status_revision = scheduled_hashes
        .iter()
        .find(|hash| hash.as_str() != current_template_hash.as_str())
        .or_else(|| scheduled_hashes.first())
        .and_then(|hash| revision_names_by_hash.get(hash))
        .cloned()
        .unwrap_or_else(|| current_revision.clone());

    let mut status = json!({
        "currentNumberScheduled": current,
        "desiredNumberScheduled": desired,
        "numberAvailable": number_available,
        "numberReady": number_ready,
        "numberMisscheduled": misscheduled,
        "updatedNumberScheduled": updated_number_scheduled,
        "observedGeneration": observed_generation,
        "currentRevision": current_status_revision,
        "updateRevision": current_revision
    });
    if let Some(conditions) = daemonset.pointer("/status/conditions").cloned() {
        status["conditions"] = conditions;
    }

    // Re-read the DaemonSet to get a fresh RV for status CAS. The
    // `daemonset` parameter was read at the top of reconcile and is
    // stale after pod create/delete operations.
    let fresh_ds = match db
        .get_resource("apps/v1", "DaemonSet", Some(namespace), name)
        .await?
    {
        Some(r) => crate::api::inject_resource_version(r.data, r.resource_version),
        None => return Ok(()),
    };
    // Update observedGeneration from the fresh read
    status["observedGeneration"] = json!(
        fresh_ds
            .get("metadata")
            .and_then(|m| m.get("generation"))
            .and_then(|g| g.as_i64())
            .unwrap_or(0)
    );

    crate::controllers::common::write_status(db, &fresh_ds, &status).await?;

    tracing::info!(
        target: "klights::daemonset::reconcile",
        ds = %name,
        ns = %namespace,
        current_revision = %current_status_revision,
        update_revision = %current_revision,
        "reconcile_daemonset completed successfully"
    );

    Ok(())
}

#[cfg(test)]
mod tests;
