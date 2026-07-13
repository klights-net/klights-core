use crate::datastore::command::StorageCommand;
use crate::datastore::{DatastoreBackend, ResourcePreconditions};
use crate::kubelet::node_role_labels::prune_klights_managed_node_role_labels;
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
use crate::kubelet::outbox::{
    Outbox, OutboxCommand, OutboxSendPlanner, OutboxSendRoute, OutboxSubject,
};
use crate::utils::k8s_time_now;
use anyhow::{Context, Result};
use std::sync::Arc;

pub(crate) use crate::kubelet::node_registration::register_node_snapshot;
pub use crate::kubelet::node_registration::{
    NodeRegistrationAddresses, NodeRegistrationHostFacts, NodeRegistrationSnapshot, register_node,
    register_node_at_addresses, register_node_with_outbox, register_node_with_outbox_at_addresses,
};
pub use crate::kubelet::node_role_labels::role_label_keys_for_shape;
pub use crate::node_heartbeat::{
    LeaseRenewClient, NodeLeaseRenewClient, run_heartbeat_with_lease_client,
};

/// Get total memory in KiB from /proc/meminfo. Cached process-wide;
/// total memory does not change during the kubelet's lifetime, and the
/// previous per-call read was a sync FS hit on async pod startup paths.
pub fn memory_ki() -> u64 {
    static MEMORY_KI: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *MEMORY_KI.get_or_init(|| {
        crate::utils::read_utf8_file("/proc/meminfo")
            .ok()
            .and_then(|content| parse_memory_ki(&content))
            .unwrap_or(8 * 1024 * 1024) // Default 8GB if unable to read
    })
}

pub(super) fn parse_memory_ki(content: &str) -> Option<u64> {
    content
        .lines()
        .find(|line| line.starts_with("MemTotal:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())
}

/// The two network-related Node conditions (`Ready` + `NetworkUnavailable`)
/// derived from the local dataplane health. Shared by initial registration and
/// the event-driven readiness reconciler so both encode health identically.
pub(super) struct NodeNetworkConditions {
    pub(super) ready_status: &'static str,
    pub(super) ready_reason: &'static str,
    pub(super) ready_message: String,
    pub(super) net_unavail_status: &'static str,
    pub(super) net_unavail_reason: &'static str,
    pub(super) net_unavail_message: String,
}

impl NodeNetworkConditions {
    pub(super) fn from_health(
        dataplane_health: Option<&crate::networking::dataplane_health::DataplaneHealth>,
    ) -> Self {
        use crate::networking::dataplane_health::DataplaneHealthStatus;
        match dataplane_health.map(|health| health.status()) {
            None | Some(DataplaneHealthStatus::Healthy) => Self {
                ready_status: "True",
                ready_reason: "KubeletReady",
                ready_message: "klights is ready".to_string(),
                net_unavail_status: "False",
                net_unavail_reason: "RouteCreated",
                net_unavail_message: "RouteController created a route".to_string(),
            },
            Some(DataplaneHealthStatus::Unavailable { reason }) => Self {
                ready_status: "False",
                ready_reason: "NetworkUnavailable",
                ready_message: reason.clone(),
                net_unavail_status: "True",
                net_unavail_reason: "DataplaneNotReady",
                net_unavail_message: reason,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeNetworkRefreshResult {
    Missing,
    Unchanged,
    Updated,
}

/// Update one Node condition in place to match the desired status/reason/message.
/// Returns true if anything changed (so callers can skip a no-op write and keep
/// the node idle-silent). `lastTransitionTime` is refreshed only when the
/// `status` value itself flips, per the K8s condition contract.
fn set_node_condition(
    node: &mut serde_json::Value,
    cond_type: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> bool {
    let Some(conditions) = node
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return false;
    };
    if let Some(existing) = conditions
        .iter_mut()
        .find(|cond| cond.get("type").and_then(|t| t.as_str()) == Some(cond_type))
    {
        let status_changed = existing.get("status").and_then(|v| v.as_str()) != Some(status);
        let reason_changed = existing.get("reason").and_then(|v| v.as_str()) != Some(reason);
        let message_changed = existing.get("message").and_then(|v| v.as_str()) != Some(message);
        if !status_changed && !reason_changed && !message_changed {
            return false;
        }
        existing["status"] = serde_json::json!(status);
        existing["reason"] = serde_json::json!(reason);
        existing["message"] = serde_json::json!(message);
        if status_changed {
            existing["lastTransitionTime"] = serde_json::json!(k8s_time_now());
        }
        true
    } else {
        conditions.push(serde_json::json!({
            "type": cond_type,
            "status": status,
            "reason": reason,
            "message": message,
            "lastTransitionTime": k8s_time_now(),
        }));
        true
    }
}

/// Apply the `Ready` + `NetworkUnavailable` conditions to a Node object in
/// place. Returns true if either condition actually changed.
fn apply_network_conditions(
    node: &mut serde_json::Value,
    conditions: &NodeNetworkConditions,
) -> bool {
    let ready_changed = set_node_condition(
        node,
        "Ready",
        conditions.ready_status,
        conditions.ready_reason,
        &conditions.ready_message,
    );
    let net_changed = set_node_condition(
        node,
        "NetworkUnavailable",
        conditions.net_unavail_status,
        conditions.net_unavail_reason,
        &conditions.net_unavail_message,
    );
    ready_changed || net_changed
}

/// Re-evaluate and persist the local node's `Ready`/`NetworkUnavailable`
/// conditions from the current dataplane health. Event-driven: called by the
/// peer-route watcher when peer connectivity changes, so a node stops reporting
/// Ready as soon as a Ready peer becomes unreachable and recovers once the
/// WireGuard route is installed.
///
/// Writes go through the outbox when provided (mandatory on non-leader nodes,
/// which must not originate local cluster.db writes); the direct path is only
/// for the leader. Returns true if a write was issued.
pub async fn refresh_node_network_conditions(
    db: &dyn DatastoreBackend,
    outbox: Option<&Outbox>,
    node_name: &str,
    dataplane_health: &crate::networking::dataplane_health::DataplaneHealth,
) -> Result<NodeNetworkRefreshResult> {
    let Some(existing) = db.get_resource("v1", "Node", None, node_name).await? else {
        return Ok(NodeNetworkRefreshResult::Missing);
    };
    let conditions = NodeNetworkConditions::from_health(Some(dataplane_health));
    let mut node = existing.data.as_ref().clone();
    let commit_changed = stamp_current_git_commit_annotation(&mut node);
    let status_changed = apply_network_conditions(&mut node, &conditions);

    if let Some(outbox) = outbox {
        if !status_changed {
            return Ok(NodeNetworkRefreshResult::Unchanged);
        }
        let status = node
            .get("status")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        send_node_command(
            Some(outbox),
            OutboxOperation::NodeStatus,
            node_name,
            existing.uid.as_str(),
            StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: node_name.to_string(),
                status,
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some(existing.uid.clone()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            },
        )
        .await
        .context("Failed to enqueue Node network condition refresh")?;
    } else {
        if !(commit_changed || status_changed) {
            return Ok(NodeNetworkRefreshResult::Unchanged);
        }
        db.update_resource_with_preconditions(
            "v1",
            "Node",
            None,
            node_name,
            node,
            ResourcePreconditions::from_resource(&existing),
        )
        .await
        .context("Failed to update Node network conditions")?;
    }
    Ok(NodeNetworkRefreshResult::Updated)
}

pub async fn refresh_current_git_commit_annotation_via_leader(
    cluster_api: &dyn crate::control_plane::client::LeaderApiClient,
    node_name: &str,
) -> Result<()> {
    let command = current_git_commit_annotation_patch_command(node_name);
    let payload = OutboxPayload::from_command(command)
        .encode_protobuf()
        .context("Failed to encode Node git-commit annotation patch")?;
    let idempotency_key = format!(
        "NodeGitCommitRefresh:v1/Node/{node_name}:{}:{}",
        crate::version::GIT_COMMIT_SHORT,
        uuid::Uuid::new_v4()
    );
    cluster_api
        .apply_outbox(
            &idempotency_key,
            OutboxOperation::NodeStatus,
            bytes::Bytes::from(payload),
            node_name,
            0,
            0,
        )
        .await
        .map(|_| ())
        .map_err(|err| anyhow::anyhow!("leader rejected Node git-commit annotation patch: {err}"))
}

fn current_git_commit_annotation_patch_command(node_name: &str) -> StorageCommand {
    use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;
    StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: node_name.to_string(),
        patch_kind: crate::datastore::types::PatchKind::Merge,
        patch: serde_json::json!({
            "metadata": {
                "annotations": {
                    GIT_COMMIT_ANNOTATION: crate::version::GIT_COMMIT_SHORT,
                }
            }
        }),
        preconditions: ResourcePreconditions::default(),
        strict_resource_version: false,
    }
}

pub(super) async fn send_node_command(
    outbox: Option<&Outbox>,
    operation: OutboxOperation,
    node_name: &str,
    node_uid: &str,
    command: StorageCommand,
) -> Result<OutboxSendRoute> {
    let subject_key = if node_uid.is_empty() {
        format!("v1/Node/{node_name}")
    } else {
        format!("v1/Node/{node_name}/{node_uid}")
    };
    OutboxSendPlanner::new(outbox)
        .route(OutboxCommand {
            idempotency_key: format!(
                "{}:{}:{}",
                operation.as_str(),
                subject_key,
                uuid::Uuid::new_v4()
            ),
            operation,
            subject: OutboxSubject {
                key: subject_key,
                namespace: None,
                name: node_name.to_string(),
                uid: (!node_uid.is_empty()).then(|| node_uid.to_string()),
            },
            pod_uid: String::new(),
            command,
            now_ms: epoch_ms(),
        })
        .await
}

fn node_address_json(address_type: &str, address: &str) -> serde_json::Value {
    serde_json::json!({"type": address_type, "address": address})
}

pub fn set_node_external_ip(node: &mut serde_json::Value, external_ip: &str) -> bool {
    let external_ip = external_ip.trim();
    if external_ip.is_empty() {
        return false;
    }

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let status = node_object
        .entry("status")
        .or_insert_with(|| serde_json::json!({}));
    let Some(status_object) = status.as_object_mut() else {
        *status = serde_json::json!({});
        let Some(status_object) = status.as_object_mut() else {
            return false;
        };
        return set_node_external_ip_in_status(status_object, external_ip);
    };
    set_node_external_ip_in_status(status_object, external_ip)
}

pub fn set_node_external_ip_from_dataplane_annotation(node: &mut serde_json::Value) -> bool {
    let endpoint = node
        .pointer("/metadata/annotations")
        .and_then(|value| value.as_object())
        .and_then(|annotations| {
            annotations
                .get(crate::controllers::annotations::DATAPLANE_ENDPOINT_ANNOTATION)
                .and_then(|value| value.as_str())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let Some(endpoint) = endpoint else {
        return false;
    };
    set_node_external_ip(node, &endpoint)
}

pub async fn update_existing_node_external_ip_if_changed(
    db: &dyn DatastoreBackend,
    node_name: &str,
    external_ip: &str,
) -> Result<()> {
    let external_ip = external_ip.trim();
    if external_ip.is_empty() {
        return Ok(());
    }
    let Some(resource) = db.get_resource("v1", "Node", None, node_name).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !set_node_external_ip(&mut data, external_ip) {
        return Ok(());
    }
    db.update_resource_with_preconditions(
        "v1",
        "Node",
        None,
        node_name,
        data,
        ResourcePreconditions::from_resource(&resource),
    )
    .await?;
    Ok(())
}

pub async fn stamp_node_routing_metadata_from_store(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_node_routing_metadata_from_store_impl(db, node_name, node, false).await
}

pub async fn stamp_node_routing_metadata_and_external_ip_from_store(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
) -> Result<bool> {
    stamp_node_routing_metadata_from_store_impl(db, node_name, node, true).await
}

async fn stamp_node_routing_metadata_from_store_impl(
    db: &dyn DatastoreBackend,
    node_name: &str,
    node: &mut serde_json::Value,
    publish_external_ip: bool,
) -> Result<bool> {
    let mut changed = false;
    if let Some(subnet) = db.get_node_subnet(node_name).await? {
        changed |= set_node_pod_cidr(node, &subnet.subnet.to_string());
    }
    if let Some(metadata) = db.get_node_dataplane(node_name).await? {
        if publish_external_ip {
            changed |= set_node_external_ip(node, &metadata.endpoint.to_string());
        }
        changed |= set_node_dataplane_annotations(node, &metadata);
    }
    Ok(changed)
}

pub fn set_node_pod_cidr(node: &mut serde_json::Value, pod_cidr: &str) -> bool {
    let pod_cidr = pod_cidr.trim();
    if pod_cidr.is_empty() {
        return false;
    }
    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let spec = node_object
        .entry("spec")
        .or_insert_with(|| serde_json::json!({}));
    if !spec.is_object() {
        *spec = serde_json::json!({});
    }
    let Some(spec_object) = spec.as_object_mut() else {
        return false;
    };

    let mut changed = set_json_string_field(spec_object, "podCIDR", pod_cidr);
    let desired = serde_json::json!([pod_cidr]);
    if spec_object.get("podCIDRs") != Some(&desired) {
        spec_object.insert("podCIDRs".to_string(), desired);
        changed = true;
    }
    changed
}

pub fn set_node_dataplane_annotations(
    node: &mut serde_json::Value,
    metadata: &crate::networking::wireguard::DataplanePeerMetadata,
) -> bool {
    use crate::controllers::annotations::{
        DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION, DATAPLANE_MODE_ANNOTATION,
        DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION,
    };

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let metadata_object = node_object
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    if !metadata_object.is_object() {
        *metadata_object = serde_json::json!({});
    }
    let Some(metadata_object) = metadata_object.as_object_mut() else {
        return false;
    };
    let annotations = metadata_object
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    if !annotations.is_object() {
        *annotations = serde_json::json!({});
    }
    let Some(annotations) = annotations.as_object_mut() else {
        return false;
    };

    let mut changed = false;
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_ENDPOINT_ANNOTATION,
        &metadata.endpoint.to_string(),
    );
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_MODE_ANNOTATION,
        metadata.mode.as_str(),
    );
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_ENCRYPTION_ANNOTATION,
        metadata.encryption.as_str(),
    );
    if let Some(port) = metadata.port {
        changed |= set_json_string_field(annotations, DATAPLANE_PORT_ANNOTATION, &port.to_string());
    } else {
        changed |= annotations.remove(DATAPLANE_PORT_ANNOTATION).is_some();
    }
    if let Some(public_key) = metadata.public_key.as_ref() {
        changed |= set_json_string_field(
            annotations,
            DATAPLANE_PUBLIC_KEY_ANNOTATION,
            &public_key.to_string(),
        );
    } else {
        changed |= annotations
            .remove(DATAPLANE_PUBLIC_KEY_ANNOTATION)
            .is_some();
    }
    changed
}

fn set_json_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: &str,
) -> bool {
    if object.get(key).and_then(|existing| existing.as_str()) == Some(value) {
        return false;
    }
    object.insert(key.to_string(), serde_json::json!(value));
    true
}

fn set_node_external_ip_in_status(
    status: &mut serde_json::Map<String, serde_json::Value>,
    external_ip: &str,
) -> bool {
    let addresses = status
        .entry("addresses")
        .or_insert_with(|| serde_json::json!([]));
    if !addresses.is_array() {
        *addresses = serde_json::json!([]);
    }

    let Some(addresses) = addresses.as_array_mut() else {
        return false;
    };
    let mut changed = false;
    let mut found_external = false;
    for address in addresses.iter_mut() {
        if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
            found_external = true;
            if address.get("address").and_then(|value| value.as_str()) != Some(external_ip) {
                address["address"] = serde_json::json!(external_ip);
                changed = true;
            }
        }
    }
    if !found_external {
        addresses.push(node_address_json("ExternalIP", external_ip));
        changed = true;
    }
    changed
}

pub fn merge_existing_node_mutable_fields(
    desired: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let desired_labels = desired.pointer("/metadata/labels").cloned();
    let desired_annotations = desired.pointer("/metadata/annotations").cloned();
    let desired_creation_timestamp = desired.pointer("/metadata/creationTimestamp").cloned();
    let desired_has_external_ip = node_status_external_ip(desired).is_some();
    let existing_external_ip = node_status_external_ip(existing).map(str::to_string);

    if let Some(existing_metadata) = existing.get("metadata").cloned() {
        desired["metadata"] = existing_metadata;
    }
    prune_klights_managed_node_role_labels(desired);
    merge_metadata_object_field(desired, "labels", desired_labels.as_ref());
    merge_metadata_object_field(desired, "annotations", desired_annotations.as_ref());
    if let Some(creation_timestamp) = desired_creation_timestamp
        && let Some(metadata) = desired
            .get_mut("metadata")
            .and_then(|metadata| metadata.as_object_mut())
    {
        metadata.insert("creationTimestamp".to_string(), creation_timestamp);
    }

    if let Some(existing_spec) = existing.get("spec").cloned() {
        desired["spec"] = existing_spec;
    }
    if !desired_has_external_ip && let Some(existing_external_ip) = existing_external_ip {
        set_node_external_ip(desired, &existing_external_ip);
    }
    // `status.conditions` is co-authored: the worker posts its
    // dataplane-derived `Ready`/`NetworkUnavailable` via this forwarded update,
    // while the leader's node_lifecycle controller writes `Ready=Unknown` on
    // lease expiry via CAS (node_lifecycle.rs). This forwarded path drops the
    // RV precondition (apply_against_latest), so an unconditionally
    // overwriting merge lets a stale worker snapshot revert the leader's
    // fresher Unknown (lost update — the worker's queued Ready=True, retried
    // after the leader marked Unknown, would clobber it for a heartbeat
    // window and let the scheduler place Pods on an unhealthy Node). Merge
    // conditions per type by `lastTransitionTime` (newest wins, K8s
    // condition contract): a stale worker snapshot has an older transition
    // time and loses, while a genuine recovery transition (Unknown->True)
    // stamps a newer time and wins.
    merge_node_status_conditions(desired, existing);
}

/// Merge an incoming Node `/status` update with the live Node's co-authored
/// conditions, preserving fresher leader transitions while allowing newer
/// worker dataplane recovery transitions to apply.
pub fn merge_node_status_for_update(
    incoming_status: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let mut desired = serde_json::json!({ "status": incoming_status.clone() });
    merge_node_status_conditions(&mut desired, existing);
    if let Some(status) = desired.get("status").cloned() {
        *incoming_status = status;
    }
}

pub(super) fn preserve_existing_network_conditions(
    desired: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let Some(desired_conditions) = desired
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    let Some(existing_conditions) = existing
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
    else {
        return;
    };

    for cond_type in ["Ready", "NetworkUnavailable"] {
        let Some(existing_condition) = condition_by_type(existing_conditions, cond_type).cloned()
        else {
            continue;
        };
        match desired_conditions.iter().position(|condition| {
            condition.get("type").and_then(|value| value.as_str()) == Some(cond_type)
        }) {
            Some(index) => desired_conditions[index] = existing_condition,
            None => desired_conditions.push(existing_condition),
        }
    }
}

/// Merge `status.conditions` from `existing` into `desired`, per condition
/// `type`, keeping the freshest by `lastTransitionTime`. See
/// [`merge_existing_node_mutable_fields`] for the lost-update rationale.
fn merge_node_status_conditions(desired: &mut serde_json::Value, existing: &serde_json::Value) {
    let Some(desired_conditions) = desired
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        // Worker snapshot carries no conditions: nothing to merge.
        return;
    };
    let Some(existing_conditions) = existing
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
    else {
        // No leader conditions to preserve.
        return;
    };
    let desired_network_pair_should_replace =
        network_condition_pair_should_replace(desired_conditions, existing_conditions);
    let existing_network_pair_should_preserve =
        network_condition_pair_should_preserve_existing(desired_conditions, existing_conditions);
    for existing_cond in existing_conditions {
        let Some(cond_type) = existing_cond.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        match desired_conditions
            .iter()
            .position(|c| c.get("type").and_then(|v| v.as_str()) == Some(cond_type))
        {
            None => {
                // Leader-owned condition type absent from the worker snapshot:
                // preserve it instead of letting the forwarded update drop it.
                desired_conditions.push(existing_cond.clone());
            }
            Some(idx) => {
                if desired_network_pair_should_replace && is_network_condition_type(cond_type) {
                    continue;
                }
                if existing_network_pair_should_preserve && is_network_condition_type(cond_type) {
                    desired_conditions[idx] = existing_cond.clone();
                    continue;
                }
                // Both writers authored this condition type: keep the worker's
                // (desired) only when it is strictly newer; on a tie or older,
                // prefer the leader's (existing) so a retried worker snapshot
                // cannot revert an authoritative write.
                if !condition_is_strictly_newer(&desired_conditions[idx], existing_cond) {
                    desired_conditions[idx] = existing_cond.clone();
                }
            }
        }
    }
}

fn is_network_condition_type(cond_type: &str) -> bool {
    matches!(cond_type, "Ready" | "NetworkUnavailable")
}

fn network_condition_pair_has_newer_transition(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    if !desired_network_pair_is_coherent(desired_ready, desired_network) {
        return false;
    }

    let desired_ready_is_newer = desired_ready.is_some_and(|desired| {
        existing_ready.is_some_and(|existing| condition_is_strictly_newer(desired, existing))
    });
    if desired_ready_is_newer {
        return true;
    }

    // `Ready` is the scheduling gate and the stable ordering anchor for a
    // worker-authored network pair. A stale startup snapshot can flip only
    // `NetworkUnavailable` to a newer timestamp while carrying an older/tied
    // `Ready=False`; do not let that override a fresher healthy pair. Healthy
    // recovery keeps the existing tie rule below because the peer route itself
    // can be the condition that transitioned within the same K8s second.
    network_condition_pair_is_healthy(desired_ready, desired_network)
        && desired_network.is_some_and(|desired| {
            existing_network.is_some_and(|existing| condition_is_strictly_newer(desired, existing))
        })
}

fn network_condition_pair_should_replace(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    if network_condition_pair_has_newer_transition(desired_conditions, existing_conditions) {
        return true;
    }

    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    network_condition_pair_is_healthy(desired_ready, desired_network)
        && network_condition_pair_is_unavailable(existing_ready, existing_network)
        && network_condition_pair_timestamps_tie(
            desired_ready,
            desired_network,
            existing_ready,
            existing_network,
        )
}

fn network_condition_pair_should_preserve_existing(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    desired_network_pair_is_coherent(desired_ready, desired_network)
        && desired_network_pair_is_coherent(existing_ready, existing_network)
        && !network_condition_pair_should_replace(desired_conditions, existing_conditions)
}

fn desired_network_pair_is_coherent(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("True"), Some("False")) | (Some("False"), Some("True"))
    )
}

fn network_condition_pair_is_healthy(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("True"), Some("False"))
    )
}

fn network_condition_pair_is_unavailable(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("False"), Some("True"))
    )
}

fn network_condition_pair_timestamps_tie(
    desired_ready: Option<&serde_json::Value>,
    desired_network: Option<&serde_json::Value>,
    existing_ready: Option<&serde_json::Value>,
    existing_network: Option<&serde_json::Value>,
) -> bool {
    condition_transition_time(desired_ready).is_some()
        && condition_transition_time(desired_ready) == condition_transition_time(existing_ready)
        && condition_transition_time(desired_network).is_some()
        && condition_transition_time(desired_network) == condition_transition_time(existing_network)
}

fn condition_by_type<'a>(
    conditions: &'a [serde_json::Value],
    cond_type: &str,
) -> Option<&'a serde_json::Value> {
    conditions
        .iter()
        .find(|condition| condition.get("type").and_then(|value| value.as_str()) == Some(cond_type))
}

fn condition_status(condition: Option<&serde_json::Value>) -> Option<&str> {
    condition
        .and_then(|condition| condition.get("status"))
        .and_then(|value| value.as_str())
}

fn condition_transition_time(condition: Option<&serde_json::Value>) -> Option<&str> {
    condition
        .and_then(|condition| condition.get("lastTransitionTime"))
        .and_then(|value| value.as_str())
}

/// True when condition `a` has a strictly newer `lastTransitionTime` than `b`.
/// Falls back to lexicographic RFC3339 comparison when the timestamps do not
/// parse, and to `false` (keep `b`) when either side lacks a timestamp —
/// conservative against reverting an authoritative condition.
fn condition_is_strictly_newer(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let a_time = a.get("lastTransitionTime").and_then(|v| v.as_str());
    let b_time = b.get("lastTransitionTime").and_then(|v| v.as_str());
    match (a_time, b_time) {
        (Some(a_str), Some(b_str)) => match (parse_rfc3339_utc(a_str), parse_rfc3339_utc(b_str)) {
            (Some(a_dt), Some(b_dt)) => a_dt > b_dt,
            // Unparseable but present: lexicographic RFC3339-UTC is chronological.
            _ => a_str > b_str,
        },
        // Missing a timestamp: do not claim newer; let the caller keep `b`.
        _ => false,
    }
}

fn parse_rfc3339_utc(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

fn stamp_current_git_commit_annotation(node: &mut serde_json::Value) -> bool {
    use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let metadata = node_object
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    if !metadata.is_object() {
        *metadata = serde_json::json!({});
    }
    let Some(metadata_object) = metadata.as_object_mut() else {
        return false;
    };
    let annotations = metadata_object
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    if !annotations.is_object() {
        *annotations = serde_json::json!({});
    }
    let Some(annotations_object) = annotations.as_object_mut() else {
        return false;
    };
    let current = annotations_object
        .get(GIT_COMMIT_ANNOTATION)
        .and_then(|value| value.as_str());
    if current == Some(crate::version::GIT_COMMIT_SHORT) {
        return false;
    }
    annotations_object.insert(
        GIT_COMMIT_ANNOTATION.to_string(),
        serde_json::json!(crate::version::GIT_COMMIT_SHORT),
    );
    true
}

fn node_status_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
                    address.get("address").and_then(|value| value.as_str())
                } else {
                    None
                }
            })
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn merge_metadata_object_field(
    desired: &mut serde_json::Value,
    field: &str,
    desired_overlay: Option<&serde_json::Value>,
) {
    let Some(overlay) = desired_overlay.and_then(|v| v.as_object()) else {
        return;
    };
    let Some(metadata) = desired.get_mut("metadata").and_then(|v| v.as_object_mut()) else {
        return;
    };
    let entry = metadata
        .entry(field.to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !entry.is_object() {
        *entry = serde_json::json!({});
    }
    if let Some(entry_obj) = entry.as_object_mut() {
        for (key, value) in overlay {
            entry_obj.insert(key.clone(), value.clone());
        }
    }
}

/// Remove stale `node-role.kubernetes.io/leader` labels from every node
/// except the current local node. The local leader election is responsible
/// for stamping its own leader label; this keeps old leader labels from a
/// previous leader visible only until the leader changes and the new leader
/// has observed that transition.
pub(crate) async fn clear_leader_label_from_other_nodes(
    db: &dyn DatastoreBackend,
    local_node_name: &str,
) -> Result<()> {
    let nodes = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for node in nodes.items {
        if node.name == local_node_name {
            continue;
        }
        let mut data = Arc::unwrap_or_clone(node.data.clone());
        let Some(labels) = data
            .pointer_mut("/metadata/labels")
            .and_then(|labels| labels.as_object_mut())
        else {
            continue;
        };
        if labels.remove("node-role.kubernetes.io/leader").is_none() {
            continue;
        }
        if let Err(err) = db
            .update_resource_with_preconditions(
                "v1",
                "Node",
                None,
                &node.name,
                data,
                ResourcePreconditions::from_resource(&node),
            )
            .await
        {
            tracing::warn!(
                error = %err,
                node_name = %node.name,
                local_node_name,
                "failed to clear stale node leader label"
            );
        }
    }
    Ok(())
}

fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::networking::dataplane_health::DataplaneHealth;
    use crate::node_heartbeat::{
        NODE_HEARTBEAT_INTERVAL, build_lease, run_heartbeat_with_interval,
    };
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    fn node_condition_status<'a>(node: &'a serde_json::Value, cond_type: &str) -> Option<&'a str> {
        node.pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conds| {
                conds.iter().find_map(|c| {
                    if c.get("type").and_then(|t| t.as_str()) == Some(cond_type) {
                        c.get("status").and_then(|s| s.as_str())
                    } else {
                        None
                    }
                })
            })
    }

    fn node_with_ready_condition(
        status: &str,
        reason: &str,
        last_transition: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": status, "reason": reason, "message": "m", "lastTransitionTime": last_transition}
                ]
            }
        })
    }

    // Issue #3: a forwarded worker Node update (apply_against_latest, no RV
    // precondition) must not let a stale worker status.conditions revert the
    // leader's fresher authoritative condition.
    #[test]
    fn merge_node_fields_keeps_leader_unknown_against_stale_worker_ready() {
        // Leader marked Ready=Unknown at 11:00 (lease expiry). The worker's
        // queued snapshot still carries Ready=True from 10:00 (before the blip;
        // the status never transitioned so lastTransitionTime is stale).
        let mut desired = node_with_ready_condition("True", "KubeletReady", "2026-06-18T10:00:00Z");
        let existing =
            node_with_ready_condition("Unknown", "NodeStatusUnknown", "2026-06-18T11:00:00Z");
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("Unknown"),
            "a stale worker Ready=True must not revert the leader's fresher Ready=Unknown"
        );
    }

    #[test]
    fn merge_node_fields_lets_worker_recovery_transition_win() {
        // Worker genuinely recovered: Ready transitioned Unknown->True at 12:00,
        // stamping a lastTransitionTime newer than the leader's 11:00 Unknown.
        let mut desired = node_with_ready_condition("True", "KubeletReady", "2026-06-18T12:00:00Z");
        let existing =
            node_with_ready_condition("Unknown", "NodeStatusUnknown", "2026-06-18T11:00:00Z");
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a genuine recovery transition (newer lastTransitionTime) must win"
        );
    }

    #[test]
    fn merge_node_fields_accepts_coherent_network_recovery_when_ready_timestamp_ties() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T07:44:57Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T07:44:56Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a coherent network recovery must not leave Ready=False when NetworkUnavailable=False is newer"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_accepts_network_recovery_when_pair_timestamps_tie() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a same-second network recovery pair must not be discarded as a stale tie"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_rejects_stale_unavailable_when_ready_is_not_newer() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T10:03:12Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T10:03:12Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T10:03:13Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:44:08Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a stale pending-connectivity snapshot must not override a fresher Ready=True condition"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_preserves_leader_condition_absent_from_worker() {
        // Worker snapshot lacks a condition the leader authored; the merge must
        // preserve it rather than let the forwarded update drop it.
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "m", "lastTransitionTime": "2026-06-18T10:00:00Z"}
            ]}
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "m", "lastTransitionTime": "2026-06-18T10:00:00Z"},
                {"type": "MemoryPressure", "status": "False", "reason": "KubeletHasSufficientMemory", "message": "m", "lastTransitionTime": "2026-06-18T09:00:00Z"}
            ]}
        });
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "MemoryPressure"),
            Some("False"),
            "a leader-owned condition absent from the worker snapshot must be preserved"
        );
    }

    async fn create_ready_node(db: &dyn DatastoreBackend, name: &str) {
        db.create_resource(
            "v1",
            "Node",
            None,
            name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "annotations": {
                        crate::controllers::annotations::GIT_COMMIT_ANNOTATION: crate::version::GIT_COMMIT_SHORT
                    }
                },
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": k8s_time_now()},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": k8s_time_now()}
                    ]
                }
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn refresh_marks_node_not_ready_when_peers_disconnected() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        let health = DataplaneHealth::new_healthy();
        health.set_peers_disconnected("1 of 1 ready peer unreachable".to_string());

        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .expect("refresh must succeed");
        assert_eq!(
            wrote,
            NodeNetworkRefreshResult::Updated,
            "a Ready->NotReady transition must be written"
        );

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node_condition_status(&node.data, "Ready"), Some("False"));
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("True")
        );
    }

    #[tokio::test]
    async fn refresh_recovers_node_ready_when_peers_reconnect() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        let health = DataplaneHealth::new_healthy();
        health.set_peers_disconnected("unreachable".to_string());
        refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();

        // Peer becomes reachable again.
        health.set_peers_connected();
        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();
        assert_eq!(
            wrote,
            NodeNetworkRefreshResult::Updated,
            "a NotReady->Ready recovery must be written"
        );

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node_condition_status(&node.data, "Ready"), Some("True"));
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[tokio::test]
    async fn refresh_network_conditions_stamps_current_git_commit() {
        use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;

        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "node-a",
                    "annotations": {
                        GIT_COMMIT_ANNOTATION: "oldcommit"
                    }
                },
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "waiting", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                        {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "waiting", "lastTransitionTime": "2026-06-19T07:44:56Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let health = DataplaneHealth::new_healthy();
        health.set_peers_connected();
        refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node.data
                .pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|value| value.as_str()),
            Some(crate::version::GIT_COMMIT_SHORT),
            "network status refresh must not forward a stale build commit from the local Node cache"
        );
    }

    #[tokio::test]
    async fn git_commit_refresh_via_leader_patches_existing_node_annotation() {
        use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;

        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "node-a",
                    "annotations": {
                        GIT_COMMIT_ANNOTATION: "oldcommit"
                    }
                },
                "status": {
                    "conditions": []
                }
            }),
        )
        .await
        .unwrap();
        let client = crate::control_plane::client::local::LocalApiClient::new(
            std::sync::Arc::new(db.clone()),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        refresh_current_git_commit_annotation_via_leader(&client, "node-a")
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .expect("Node must still exist");
        assert_eq!(
            node.data
                .pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|value| value.as_str()),
            Some(crate::version::GIT_COMMIT_SHORT),
            "leader-applied self patch must publish the caller's current build commit"
        );
    }

    #[tokio::test]
    async fn register_node_without_dataplane_health_preserves_network_unavailable() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                        {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "node-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node_condition_status(&node.data, "Ready"),
            Some("False"),
            "a registration refresh without dataplane health must not declare peer connectivity ready"
        );
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("True")
        );
    }

    #[tokio::test]
    async fn register_node_refresh_preserves_existing_network_conditions() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T10:03:13Z"},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:44:08Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending();
        register_node(
            &db,
            "node-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            Some(&health),
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node_condition_status(&node.data, "Ready"),
            Some("True"),
            "existing Node registration refresh must not own dataplane readiness"
        );
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[tokio::test]
    async fn refresh_is_noop_when_conditions_unchanged() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        // Health already Healthy => same conditions already present => no write.
        let health = DataplaneHealth::new_healthy();
        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .expect("refresh must succeed");
        assert!(
            matches!(wrote, NodeNetworkRefreshResult::Unchanged),
            "unchanged conditions must not write (keep the node idle-silent)"
        );
    }

    async fn wait_for_lease_resource_version(
        db: &dyn DatastoreBackend,
        node_name: &str,
        min_rv: i64,
        timeout: Duration,
    ) -> Option<i64> {
        let start = std::time::Instant::now();
        loop {
            let lease = db
                .get_resource(
                    "coordination.k8s.io/v1",
                    "Lease",
                    Some("kube-node-lease"),
                    node_name,
                )
                .await
                .ok()
                .flatten();

            if let Some(resource) = lease
                && resource.resource_version >= min_rv
            {
                return Some(resource.resource_version);
            }

            if start.elapsed() >= timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn test_build_lease_renew_time_uses_canonical_microtime_format() {
        // Lease renewTime is metav1.MicroTime — must serialize as
        // YYYY-MM-DDTHH:MM:SS.ffffffZ (exactly 6 microsecond digits + Z).
        // P0-E2E-20260423-12 regression: prior bug emitted second-precision Z
        // or (via the protobuf round-trip) `+00:00` offsets.
        let lease = build_lease("dp");
        let renew = lease
            .pointer("/spec/renewTime")
            .and_then(|v| v.as_str())
            .expect("renewTime present");
        assert!(
            renew.ends_with("Z"),
            "renewTime must end with Z, got: {renew}"
        );
        assert!(
            !renew.contains("+"),
            "renewTime must not contain offset, got: {renew}"
        );
        assert_eq!(
            renew.len(),
            27,
            "MicroTime is exactly 27 chars, got: {renew}"
        );
        // ".ffffffZ" — period at -8 from the end means there are 6 fractional digits.
        assert_eq!(&renew[19..20], ".", "period at index 19, got: {renew}");
    }

    #[test]
    fn build_lease_uses_canonical_lease_duration() {
        let lease = build_lease("dp");
        assert_eq!(
            lease
                .pointer("/spec/leaseDurationSeconds")
                .and_then(|value| value.as_i64()),
            Some(crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS),
            "advertised leaseDurationSeconds must derive from the canonical node-lease constant"
        );
    }

    #[test]
    fn heartbeat_default_interval_derives_from_canonical_cadence() {
        // No literal pin: the renewal timer must equal the single canonical
        // node-lease cadence so the timer and the staleness grace cannot
        // drift apart. (Value itself is owned by node_lease_tracker.)
        assert_eq!(
            NODE_HEARTBEAT_INTERVAL,
            Duration::from_secs(
                crate::node_lease_tracker::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS as u64
            ),
        );
    }

    #[tokio::test]
    async fn test_register_node_creates_node_resource() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap();
        assert!(
            node.is_some(),
            "Node resource should exist after register_node"
        );

        let data = node.unwrap().data;
        assert_eq!(data["apiVersion"], "v1");
        assert_eq!(data["kind"], "Node");
        assert_eq!(data["metadata"]["name"], "test-node");
        assert_eq!(data["metadata"]["labels"]["kubernetes.io/os"], "linux");
        assert_eq!(
            data["metadata"]["labels"]["kubernetes.io/hostname"],
            "test-node"
        );
        let version = data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        assert_eq!(
            version,
            crate::version::git_version(),
            "root-mode kubeletVersion should use the shared klights version"
        );
        assert_eq!(data["status"]["nodeInfo"]["operatingSystem"], "linux");
        assert!(
            data["status"]["nodeInfo"]["osImage"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.osImage must be populated for kubectl wide output"
        );
        assert!(
            data["status"]["nodeInfo"]["kernelVersion"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.kernelVersion must be populated for kubectl wide output"
        );
        assert!(
            data["status"]["nodeInfo"]["containerRuntimeVersion"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.containerRuntimeVersion must be populated for kubectl wide output"
        );
    }

    #[tokio::test]
    async fn typed_registration_snapshot_preserves_remote_host_facts_exactly() {
        let db = crate::datastore::test_support::in_memory().await;
        let snapshot = NodeRegistrationSnapshot {
            node_name: "remote-cp".to_string(),
            node_mode: crate::controllers::annotations::NodePeerMode::Root,
            node_role: crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints: vec!["https://192.0.2.10:7679".to_string()],
                token: None,
                skip_ca: false,
                as_learner: false,
            },
            addresses: NodeRegistrationAddresses::new(
                "172.31.50.2".to_string(),
                Some("192.0.2.50".to_string()),
            ),
            raft_shape: Some(crate::datastore::raft::types::RaftShape {
                voter_count: 2,
                is_leader: false,
                is_learner: false,
            }),
            grpc_port: Some(7679),
            host: NodeRegistrationHostFacts {
                cpu_count: 37,
                memory_ki: 98_765_432,
                architecture: "remote-arch".to_string(),
                operating_system: "linux".to_string(),
                os_image: "Remote Linux 42".to_string(),
                kernel_version: "9.8.7-remote".to_string(),
                container_runtime_version: "containerd://9.9.9".to_string(),
                kubelet_version: "v1.34.0-klights.remote".to_string(),
                git_commit: "joinerabc".to_string(),
            },
        };

        register_node_snapshot(&db, None, None, None, &snapshot)
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "remote-cp")
            .await
            .unwrap()
            .expect("remote Node must be registered");
        assert_eq!(node.data["status"]["capacity"]["cpu"], "37");
        assert_eq!(node.data["status"]["capacity"]["memory"], "98765432Ki");
        assert_eq!(node.data["status"]["allocatable"]["cpu"], "37");
        assert_eq!(node.data["status"]["allocatable"]["memory"], "98765432Ki");
        assert_eq!(node.data["metadata"]["labels"]["kubernetes.io/os"], "linux");
        assert_eq!(
            node.data["metadata"]["labels"]["kubernetes.io/arch"],
            "remote-arch"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["architecture"],
            "remote-arch"
        );
        assert_eq!(node.data["status"]["nodeInfo"]["operatingSystem"], "linux");
        assert_eq!(
            node.data["status"]["nodeInfo"]["osImage"],
            "Remote Linux 42"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["kernelVersion"],
            "9.8.7-remote"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["containerRuntimeVersion"],
            "containerd://9.9.9"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["kubeletVersion"],
            "v1.34.0-klights.remote"
        );
        assert_eq!(
            node.data["metadata"]["annotations"]["klights.io/git-commit"],
            "joinerabc"
        );
    }

    #[tokio::test]
    async fn test_seed_leader_register_node_omits_self_dataplane_endpoint_external_ip() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            Some("203.0.113.10"),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        let addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(
            !addresses.iter().any(|address| {
                address["type"] == "ExternalIP" && address["address"] == "203.0.113.10"
            }),
            "seed leader must not publish a self-authored dataplane endpoint as ExternalIP: {addresses:?}"
        );
    }

    #[tokio::test]
    async fn register_node_at_addresses_separates_internal_and_external_ip_for_worker() {
        let db = crate::datastore::test_support::in_memory().await;
        let addresses = NodeRegistrationAddresses::new(
            "172.31.11.2".to_string(),
            Some("10.99.0.11".to_string()),
        );

        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            &addresses,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "mn-worker")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        let node_addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(node_addresses.iter().any(|address| {
            address["type"] == "InternalIP" && address["address"] == "172.31.11.2"
        }));
        assert!(node_addresses.iter().any(|address| {
            address["type"] == "ExternalIP" && address["address"] == "10.99.0.11"
        }));
        assert!(
            !node_addresses.iter().any(|address| {
                address["type"] == "InternalIP" && address["address"] == "10.99.0.11"
            }),
            "external endpoint must not overwrite Kubernetes InternalIP: {node_addresses:?}"
        );
    }

    #[tokio::test]
    async fn register_node_at_addresses_preserves_existing_external_ip_when_refresh_has_none() {
        let db = crate::datastore::test_support::in_memory().await;
        let role = crate::bootstrap::NodeRole::Worker {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };
        let observed_addresses = NodeRegistrationAddresses::new(
            "172.31.11.2".to_string(),
            Some("10.99.0.11".to_string()),
        );
        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &role,
            None,
            &observed_addresses,
        )
        .await
        .unwrap();

        let refresh_addresses = NodeRegistrationAddresses::new("172.31.11.2".to_string(), None);
        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &role,
            None,
            &refresh_addresses,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "mn-worker")
            .await
            .unwrap()
            .expect("Node resource should exist after refresh");
        let node_addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(
            node_addresses.iter().any(|address| {
                address["type"] == "ExternalIP" && address["address"] == "10.99.0.11"
            }),
            "registration refresh must preserve peer-observed ExternalIP: {node_addresses:?}"
        );
    }

    #[tokio::test]
    async fn test_register_node_publishes_allocated_pod_cidr() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("test-node", "10.50.0.0/16", "192.0.2.10")
            .await
            .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            Some("203.0.113.10"),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        assert_eq!(
            node.data.pointer("/spec/podCIDR").and_then(|v| v.as_str()),
            Some("10.50.0.0/24")
        );
        assert_eq!(
            node.data
                .pointer("/spec/podCIDRs/0")
                .and_then(|v| v.as_str()),
            Some("10.50.0.0/24")
        );
    }

    #[tokio::test]
    async fn test_register_node_refreshes_existing_node_internal_ip() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {"example.com/preserve": "true"},
                    "annotations": {"example.com/preserve": "true"}
                },
                "spec": {"unschedulable": true},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "test-node"},
                        {"type": "InternalIP", "address": "127.0.0.1"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let internal_ip = node.data["status"]["addresses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|addr| addr["type"] == "InternalIP")
            .and_then(|addr| addr["address"].as_str())
            .unwrap();

        assert_ne!(
            internal_ip, "127.0.0.1",
            "restart registration must refresh stale loopback InternalIP"
        );
        assert_eq!(
            node.data["metadata"]["labels"]["example.com/preserve"], "true",
            "register_node must not erase user-managed labels on restart"
        );
        assert_eq!(
            node.data["metadata"]["annotations"]["example.com/preserve"], "true",
            "register_node must not erase user-managed annotations on restart"
        );
        assert_eq!(
            node.data["spec"]["unschedulable"], true,
            "register_node must not uncordon an existing node"
        );
    }

    #[tokio::test]
    async fn test_register_node_refreshes_creation_timestamp_on_restart() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "creationTimestamp": "2026-01-01T00:00:00Z"
                },
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let creation_timestamp = node
            .data
            .pointer("/metadata/creationTimestamp")
            .and_then(|v| v.as_str())
            .unwrap();
        assert_ne!(
            creation_timestamp, "2026-01-01T00:00:00Z",
            "register_node must refresh Node creationTimestamp so kubectl AGE reflects this process start"
        );
    }

    #[tokio::test]
    async fn test_register_node_sets_capacity_and_allocatable() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let data = node.data;

        // capacity and allocatable must both exist with cpu, memory, pods
        for field in ["capacity", "allocatable"] {
            let section = &data["status"][field];
            assert!(
                section["cpu"].as_str().is_some(),
                "{}.cpu should be a string",
                field
            );
            let cpu: usize = section["cpu"].as_str().unwrap().parse().unwrap();
            assert!(cpu >= 1, "{}.cpu should be >= 1", field);

            assert!(
                section["memory"].as_str().unwrap().ends_with("Ki"),
                "{}.memory should end with Ki",
                field
            );
            assert_eq!(section["pods"], "110");
        }

        // capacity and allocatable should match (klights doesn't reserve resources)
        assert_eq!(
            data["status"]["capacity"]["cpu"],
            data["status"]["allocatable"]["cpu"]
        );
        assert_eq!(
            data["status"]["capacity"]["memory"],
            data["status"]["allocatable"]["memory"]
        );
    }

    #[tokio::test]
    async fn test_register_node_sets_ready_condition() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let conditions = node.data["status"]["conditions"].as_array().unwrap();

        // Should have 5 conditions: Ready, MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable
        assert_eq!(conditions.len(), 5);

        let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], "True");
        assert_eq!(ready["reason"], "KubeletReady");
        assert!(
            ready.get("lastHeartbeatTime").is_none(),
            "registered Node status must not persist the churny lastHeartbeatTime field"
        );
        assert!(ready.get("lastTransitionTime").is_some());

        // Negative conditions should all be False
        for cond_type in [
            "MemoryPressure",
            "DiskPressure",
            "PIDPressure",
            "NetworkUnavailable",
        ] {
            let cond = conditions.iter().find(|c| c["type"] == cond_type).unwrap();
            assert_eq!(cond["status"], "False", "{} should be False", cond_type);
        }

        // NetworkUnavailable should have specific reason
        let network_cond = conditions
            .iter()
            .find(|c| c["type"] == "NetworkUnavailable")
            .unwrap();
        assert_eq!(network_cond["reason"], "RouteCreated");
    }

    #[tokio::test]
    async fn test_register_node_has_leader_role_label() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        // kubectl derives ROLES column from node-role.kubernetes.io/* labels
        assert!(
            labels.contains_key("node-role.kubernetes.io/leader"),
            "leader node must have leader role label for kubectl ROLES column"
        );
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/controlplane"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_has_worker_role_label() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7979".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        assert!(labels.contains_key("node-role.kubernetes.io/worker"));
        assert!(!labels.contains_key("node-role.kubernetes.io/leader"));
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/controlplane"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_prunes_stale_klights_role_labels_on_refresh() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {
                        "node-role.kubernetes.io/master": "",
                        "node-role.kubernetes.io/controlplane": "",
                        "example.com/preserve": "true"
                    }
                },
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7979".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        assert_eq!(
            labels.get("example.com/preserve").and_then(|v| v.as_str()),
            Some("true")
        );
        assert!(labels.contains_key("node-role.kubernetes.io/worker"));
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_version_format() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();

        let version = node.data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        // kubectl displays this in the VERSION column
        assert!(
            version.starts_with("v"),
            "kubeletVersion must start with 'v' for kubectl VERSION column, got: {}",
            version
        );
    }

    #[tokio::test]
    async fn test_register_node_rootless_appends_rootless_to_kubelet_version() {
        let db = crate::datastore::test_support::in_memory().await;
        let mode = crate::bootstrap::NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: std::path::PathBuf::from("/proc/self/ns/net"),
        };

        register_node(
            &db,
            "rootless-node",
            &mode,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "rootless-node")
            .await
            .unwrap()
            .unwrap();

        let version = node.data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        assert_eq!(
            version,
            format!("{} (rootless)", crate::version::git_version())
        );
    }

    #[tokio::test]
    async fn test_register_node_sets_daemon_endpoints_kubelet_port() {
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();

        let port = node
            .data
            .pointer("/status/daemonEndpoints/kubeletEndpoint/Port")
            .and_then(|v| v.as_i64());
        assert_eq!(
            port,
            Some(10250),
            "Node must have status.daemonEndpoints.kubeletEndpoint.Port = 10250"
        );
        assert!(
            node.data
                .pointer("/status/daemonEndpoints/kubeletEndpoint/port")
                .is_none(),
            "Node must not use non-Kubernetes lowercase daemon endpoint port"
        );
    }

    struct RecordingLeaseRenewClient {
        calls: StdArc<Mutex<Vec<String>>>,
    }

    impl RecordingLeaseRenewClient {
        fn new() -> Self {
            Self {
                calls: StdArc::new(Mutex::new(Vec::new())),
            }
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .expect("recording mutex must remain lockable")
                .len()
        }
    }

    #[async_trait::async_trait]
    impl NodeLeaseRenewClient for RecordingLeaseRenewClient {
        async fn renew_node_lease(
            &self,
            node_name: &str,
            _lease: &serde_json::Value,
        ) -> Result<()> {
            self.calls
                .lock()
                .expect("recording mutex must remain lockable")
                .push(node_name.to_string());
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_heartbeat_with_interval_never_writes_lease_to_db() {
        // T6: the production heartbeat is memory-only. It renews via the lease
        // client (worker -> leader RPC / leader-local tracker) and must never
        // write a Lease row to cluster.db.
        let db = crate::datastore::test_support::in_memory().await;
        let client = std::sync::Arc::new(RecordingLeaseRenewClient::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = tokio::spawn(run_heartbeat_with_interval(
            std::sync::Arc::new(db.clone()),
            client.clone(),
            "test-node".to_string(),
            cancel.clone(),
            std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
                crate::task_supervisor::TaskCategoryConfig::default(),
            )),
            Duration::from_millis(25),
        ));

        // Over several heartbeat intervals, no Lease row may appear in cluster.db.
        let lease_rv =
            wait_for_lease_resource_version(&db, "test-node", 1, Duration::from_millis(300)).await;
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

        assert!(
            lease_rv.is_none(),
            "memory-only heartbeat must not write a Lease to cluster.db"
        );
        assert!(
            client.call_count() > 0,
            "heartbeat must renew via the lease client"
        );
    }

    #[tokio::test]
    async fn lease_renew_client_switches_to_remote_when_not_leader() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let tracker = std::sync::Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new());
        let remote = std::sync::Arc::new(RecordingLeaseRenewClient::new());
        let client = LeaseRenewClient::new(tracker.clone(), remote.clone(), rx);

        let lease = build_lease("test-node");
        client
            .renew_node_lease("test-node", &lease)
            .await
            .expect("initial remote renew should succeed");
        assert_eq!(
            remote.call_count(),
            1,
            "non-leader should renew via remote client"
        );
        assert!(
            tracker.observed("test-node").await.is_none(),
            "non-leader renew should not touch local tracker"
        );

        tx.send(true).expect("leadership watch should update");
        client
            .renew_node_lease("test-node", &lease)
            .await
            .expect("leader renew should succeed");
        assert_eq!(
            remote.call_count(),
            1,
            "leader renew should continue using local tracker"
        );
        assert!(
            tracker.observed("test-node").await.is_some(),
            "leader renew should update local tracker"
        );
    }

    /// F2-05 closing gate: rootless boot publishes both mode and hostport-range
    /// annotations so peers can project this node as `NodePeerMode::Rootless`.
    #[tokio::test]
    async fn node_status_publishes_mode_annotation() {
        use crate::controllers::annotations::{
            DEFAULT_HOSTPORT_RANGE, HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION,
        };
        let db = crate::datastore::test_support::in_memory().await;
        let mode = crate::bootstrap::NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: std::path::PathBuf::from("/proc/self/ns/net"),
        };
        register_node(
            &db,
            "rootless-node-x",
            &mode,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "rootless-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        assert_eq!(
            annotations
                .get(NODE_MODE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some("rootless"),
        );
        assert_eq!(
            annotations
                .get(HOSTPORT_RANGE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some(DEFAULT_HOSTPORT_RANGE),
        );
    }

    /// F2-05 closing gate: root-mode publishes mode=root with an empty
    /// hostport-range so mixed clusters see a uniform annotation shape
    /// without implying root mode has a rootless host-port graft range.
    #[tokio::test]
    async fn node_status_root_publishes_empty_hostport_annotation() {
        use crate::controllers::annotations::{HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION};
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "root-node-x",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "root-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        assert_eq!(
            annotations
                .get(NODE_MODE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some("root"),
        );
        assert_eq!(
            annotations
                .get(HOSTPORT_RANGE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some(""),
            "root mode must publish an empty hostport-range for shape consistency"
        );
    }

    /// F2-05 DRY gate: the Node publisher and the projector consume shared
    /// annotation constants. If a future refactor introduces a duplicate
    /// string, this test fails the symbol equality.
    #[test]
    fn annotation_key_constants_are_shared() {
        use crate::controllers::annotations::{HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION};
        assert_eq!(NODE_MODE_ANNOTATION, "klights.io/mode");
        assert_eq!(HOSTPORT_RANGE_ANNOTATION, "klights.io/hostport-range");
    }

    /// `register_node` publishes the short git commit hash so the wide-only
    /// `COMMIT` column in `kubectl get nodes -o wide` can surface version skew
    /// across nodes in a multinode cluster.
    #[tokio::test]
    async fn node_status_publishes_git_commit_annotation() {
        use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "commit-node-x",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "commit-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        let commit = annotations
            .get(GIT_COMMIT_ANNOTATION)
            .and_then(|v| v.as_str())
            .expect("Node must publish klights.io/git-commit annotation");
        assert_eq!(
            commit,
            crate::version::GIT_COMMIT_SHORT,
            "published git-commit annotation must match the build-time short hash"
        );
        assert!(
            !commit.is_empty(),
            "git-commit annotation must not be empty"
        );
    }
}
