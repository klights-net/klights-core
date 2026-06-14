use crate::api::AppError;
use crate::watch::{EventType, WatchEvent};
use axum::{
    Json,
    body::Body,
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde_json::Value;
use std::sync::Arc;

pub fn prefers_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/vnd.kubernetes.protobuf"))
        .unwrap_or(false)
}

pub fn wants_table_format(headers: &HeaderMap) -> Result<bool, AppError> {
    let accept = match headers.get("accept").and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return Ok(false),
    };

    if !accept.contains("as=Table") {
        return Ok(false);
    }

    let has_version = accept.contains("v=");
    let has_group = accept.contains("g=");

    if has_version && !accept.contains("v=v1") {
        return Err(AppError::NotAcceptable(
            "only v1 Table format is supported".to_string(),
        ));
    }
    if has_group && !accept.contains("g=meta.k8s.io") {
        return Err(AppError::NotAcceptable(
            "only meta.k8s.io Table group is supported".to_string(),
        ));
    }

    Ok(true)
}

pub struct K8sResponse {
    value: Value,
    use_protobuf: bool,
}

impl K8sResponse {
    pub fn new(value: Value, headers: &HeaderMap) -> Self {
        K8sResponse {
            value,
            use_protobuf: prefers_protobuf(headers),
        }
    }
}

impl IntoResponse for K8sResponse {
    fn into_response(self) -> Response {
        if self.use_protobuf {
            let kind = self
                .value
                .get("kind")
                .and_then(|k| k.as_str())
                .unwrap_or("unknown");
            match crate::protobuf::encode_protobuf(&self.value) {
                Ok(bytes) => {
                    let mut response = Response::new(Body::from(bytes));
                    response.headers_mut().insert(
                        "content-type",
                        "application/vnd.kubernetes.protobuf".parse().unwrap(),
                    );
                    response
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to encode protobuf response for kind={}: {}",
                        kind,
                        e
                    );
                    Json(self.value).into_response()
                }
            }
        } else {
            Json(self.value).into_response()
        }
    }
}

pub fn format_age(creation_timestamp: &str) -> String {
    if let Ok(created) = chrono::DateTime::parse_from_rfc3339(creation_timestamp) {
        let duration =
            chrono::Utc::now().signed_duration_since(created.with_timezone(&chrono::Utc));
        if duration.num_days() > 0 {
            format!("{}d", duration.num_days())
        } else if duration.num_hours() > 0 {
            format!("{}h", duration.num_hours())
        } else if duration.num_minutes() > 0 {
            format!("{}m", duration.num_minutes())
        } else {
            format!("{}s", duration.num_seconds())
        }
    } else {
        "<unknown>".to_string()
    }
}

fn table_string_or_none(value: Option<&str>) -> String {
    match value.filter(|s| !s.is_empty()) {
        Some(value) => value.to_string(),
        None => "<none>".to_string(),
    }
}

fn pod_table_ip(pod: &Value) -> String {
    let pod_ip = pod["status"]["podIPs"]
        .as_array()
        .and_then(|ips| ips.first())
        .and_then(|ip| ip["ip"].as_str())
        .or_else(|| pod["status"]["podIP"].as_str());
    table_string_or_none(pod_ip)
}

fn pod_table_readiness_gates(pod: &Value) -> String {
    let Some(readiness_gates) = pod["spec"]["readinessGates"].as_array() else {
        return "<none>".to_string();
    };
    if readiness_gates.is_empty() {
        return "<none>".to_string();
    }

    let conditions = pod["status"]["conditions"].as_array();
    let true_conditions = readiness_gates
        .iter()
        .filter(|readiness_gate| {
            let Some(condition_type) = readiness_gate["conditionType"].as_str() else {
                return false;
            };
            conditions
                .map(|conditions| {
                    conditions.iter().any(|condition| {
                        condition["type"].as_str() == Some(condition_type)
                            && condition["status"].as_str() == Some("True")
                    })
                })
                .unwrap_or(false)
        })
        .count();

    format!("{}/{}", true_conditions, readiness_gates.len())
}

fn pod_table_column_definitions() -> Value {
    serde_json::json!([
        {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
        {"name": "Ready", "type": "string", "description": "The aggregate readiness state of this pod for accepting traffic", "priority": 0},
        {"name": "Status", "type": "string", "description": "The aggregate state of the containers in this pod", "priority": 0},
        {"name": "Restarts", "type": "integer", "description": "The number of times the containers in this pod have been restarted and when the last container in this pod has restarted", "priority": 0},
        {"name": "Age", "type": "string", "description": "CreationTimestamp is a timestamp representing the server time when this object was created. It is represented in RFC3339 form and is in UTC.", "priority": 0},
        {"name": "IP", "type": "string", "description": "IP address of the pod", "priority": 1},
        {"name": "Node", "type": "string", "description": "Node name of the pod", "priority": 1},
        {"name": "Nominated Node", "type": "string", "description": "Node nominated by the scheduler for this pod", "priority": 1},
        {"name": "Readiness Gates", "type": "string", "description": "Readiness gates configured on the pod", "priority": 1},
    ])
}

pub fn pod_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|pod| {
            // Build cells from borrows of `pod`, then move `pod` into the
            // row's `"object"` field below.  Using `into_iter` (vs `iter`)
            // and moving here avoids the per-row `Value` clone that
            // `json!({"object": &pod})` would force.
            let name = pod["metadata"]["name"].as_str().unwrap_or("").to_string();
            let creation_timestamp = pod["metadata"]["creationTimestamp"]
                .as_str()
                .unwrap_or("")
                .to_string();

            let age = format_age(&creation_timestamp);

            let container_statuses = pod["status"]["containerStatuses"].as_array();
            let init_container_statuses = pod["status"]["initContainerStatuses"].as_array();

            let ready_containers = container_statuses
                .map(|cs| {
                    cs.iter()
                        .filter(|c| c["ready"].as_bool().unwrap_or(false))
                        .count()
                })
                .unwrap_or(0);
            // Total containers come from spec.containers, not
            // status.containerStatuses (which is empty for Pending pods,
            // causing "0/0" display).
            let total_containers = pod["spec"]["containers"]
                .as_array()
                .map(|cs| cs.len())
                .unwrap_or(0);
            let ready = format!("{}/{}", ready_containers, total_containers);

            // STATUS — kubectl-equivalent derivation from container states.
            // Priority: terminated/waiting reason > pod status reason > phase.
            let status = {
                let phase = pod["status"]["phase"].as_str().unwrap_or("Unknown");
                let mut derived = pod["status"]["reason"]
                    .as_str()
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or(phase)
                    .to_string();
                if let Some(cs) = container_statuses {
                    for c in cs {
                        if let Some(waiting) = c.pointer("/state/waiting/reason")
                            && let Some(reason) = waiting.as_str()
                        {
                            derived = reason.to_string();
                            break;
                        }
                        if let Some(terminated) = c.pointer("/state/terminated/reason")
                            && let Some(reason) = terminated.as_str()
                        {
                            derived = reason.to_string();
                        }
                    }
                }
                derived
            };

            let restarts: i64 = container_statuses
                .map(|cs| {
                    cs.iter()
                        .map(|c| c["restartCount"].as_i64().unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0);

            let init_status = init_container_statuses
                .and_then(|ics| {
                    ics.iter()
                        .find(|ic| !ic["ready"].as_bool().unwrap_or(false))
                })
                .map(|_| "Init:");

            let display_status = if let Some(prefix) = init_status {
                format!("{}{}", prefix, status)
            } else {
                status
            };

            let pod_ip = pod_table_ip(&pod);
            let node_name = table_string_or_none(pod["spec"]["nodeName"].as_str());
            let nominated_node_name =
                table_string_or_none(pod["status"]["nominatedNodeName"].as_str());
            let readiness_gates = pod_table_readiness_gates(&pod);

            serde_json::json!({
                "cells": [
                    name,
                    ready,
                    display_status,
                    restarts,
                    age,
                    pod_ip,
                    node_name,
                    nominated_node_name,
                    readiness_gates
                ],
                "object": pod,
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {
            "resourceVersion": resource_version,
        },
        "columnDefinitions": pod_table_column_definitions(),
        "rows": rows,
    })
}

pub fn node_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|node| {
            // Build cells from borrows of `node`, then move `node` into the
            // row's `"object"` field below.  See `pod_list_to_table` for why.
            let name = node["metadata"]["name"].as_str().unwrap_or("").to_string();
            let creation_timestamp = node["metadata"]["creationTimestamp"]
                .as_str()
                .unwrap_or("")
                .to_string();

            let age = format_age(&creation_timestamp);

            let conditions = node["status"]["conditions"].as_array();
            let ready_condition = conditions
                .and_then(|conds| conds.iter().find(|c| c["type"].as_str() == Some("Ready")));
            let mut status = if let Some(cond) = ready_condition {
                if cond["status"].as_str() == Some("True") {
                    "Ready".to_string()
                } else {
                    "NotReady".to_string()
                }
            } else {
                "Unknown".to_string()
            };
            if node
                .pointer("/spec/unschedulable")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
            {
                status.push_str(",SchedulingDisabled");
            }

            let roles = node_roles_for_table(&node);

            let version = node["status"]["nodeInfo"]["kubeletVersion"]
                .as_str()
                .unwrap_or("<unknown>")
                .to_string();
            let internal_ip = node_address_for_table(&node, "InternalIP");
            let external_ip = node_address_for_table(&node, "ExternalIP");
            let os_image = node_info_for_table(&node, "osImage");
            let kernel_version = node_info_for_table(&node, "kernelVersion");
            let container_runtime = node_info_for_table(&node, "containerRuntimeVersion");
            let commit = node_commit_for_table(&node);

            serde_json::json!({
                "cells": [
                    name,
                    status,
                    roles,
                    age,
                    version,
                    internal_ip,
                    external_ip,
                    os_image,
                    kernel_version,
                    container_runtime,
                    commit
                ],
                "object": node,
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {
            "resourceVersion": resource_version,
        },
        "columnDefinitions": node_table_column_definitions(),
        "rows": rows,
    })
}

fn node_table_column_definitions() -> Value {
    serde_json::json!([
        {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
        {"name": "Status", "type": "string", "description": "The status of the node", "priority": 0},
        {"name": "Roles", "type": "string", "description": "The roles assigned to the node", "priority": 0},
        {"name": "Age", "type": "string", "description": "CreationTimestamp is a timestamp representing the server time when this object was created", "priority": 0},
        {"name": "Version", "type": "string", "description": "Kubelet version reported by the node", "priority": 0},
        {"name": "Internal-IP", "type": "string", "description": "Internal IP address of the node", "priority": 1},
        {"name": "External-IP", "type": "string", "description": "External IP address of the node", "priority": 1},
        {"name": "OS-Image", "type": "string", "description": "Operating system image reported by the node", "priority": 1},
        {"name": "Kernel-Version", "type": "string", "description": "Kernel version reported by the node", "priority": 1},
        {"name": "Container-Runtime", "type": "string", "description": "Container runtime reported by the node", "priority": 1},
        {"name": "Commit", "type": "string", "description": "Short git commit hash of the klights binary running on the node", "priority": 1}
    ])
}

fn node_commit_for_table(node: &Value) -> String {
    node.pointer("/metadata/annotations/klights.io~1git-commit")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("<unknown>")
        .to_string()
}

fn node_roles_for_table(node: &Value) -> String {
    let Some(labels) = node
        .pointer("/metadata/labels")
        .and_then(|labels| labels.as_object())
    else {
        return "<none>".to_string();
    };

    let mut roles: Vec<&str> = labels
        .keys()
        .filter_map(|key| key.strip_prefix("node-role.kubernetes.io/"))
        .filter(|role| !role.is_empty())
        .collect();
    roles.sort_unstable();

    if roles.is_empty() {
        "<none>".to_string()
    } else {
        roles.join(",")
    }
}

fn node_address_for_table(node: &Value, address_type: &str) -> String {
    node["status"]["addresses"]
        .as_array()
        .and_then(|addresses| {
            addresses
                .iter()
                .find(|address| address["type"].as_str() == Some(address_type))
        })
        .and_then(|address| address["address"].as_str())
        .filter(|address| !address.is_empty())
        .unwrap_or("<none>")
        .to_string()
}

fn node_info_for_table(node: &Value, field: &str) -> String {
    node["status"]["nodeInfo"][field]
        .as_str()
        .filter(|value| !value.is_empty())
        .unwrap_or("<unknown>")
        .to_string()
}

/// ReplicaSet table: NAME DESIRED CURRENT READY AGE
pub fn replicaset_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|rs| {
            let name = rs["metadata"]["name"].as_str().unwrap_or("").to_string();
            let creation_timestamp = rs["metadata"]["creationTimestamp"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let age = format_age(&creation_timestamp);

            let desired = rs["spec"]["replicas"].as_i64().unwrap_or(0);
            let current = rs["status"]["replicas"].as_i64().unwrap_or(0);
            let ready = rs["status"]["readyReplicas"].as_i64().unwrap_or(0);

            serde_json::json!({
                "cells": [name, desired, current, ready, age],
                "object": rs,
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {
            "resourceVersion": resource_version,
        },
        "columnDefinitions": [
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Desired", "type": "integer", "description": "Number of desired pods", "priority": 0},
            {"name": "Current", "type": "integer", "description": "Number of created pods", "priority": 0},
            {"name": "Ready", "type": "integer", "description": "Number of ready pods", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ],
        "rows": rows,
    })
}

/// Deployment table: NAME READY UP-TO-DATE AVAILABLE AGE
pub fn deployment_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|dep| {
            let name = dep["metadata"]["name"].as_str().unwrap_or("").to_string();
            let creation_timestamp = dep["metadata"]["creationTimestamp"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let age = format_age(&creation_timestamp);

            let ready = dep["status"]["readyReplicas"].as_i64().unwrap_or(0);
            let updated = dep["status"]["updatedReplicas"].as_i64().unwrap_or(0);
            let available = dep["status"]["availableReplicas"].as_i64().unwrap_or(0);

            serde_json::json!({
                "cells": [name, ready, updated, available, age],
                "object": dep,
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {
            "resourceVersion": resource_version,
        },
        "columnDefinitions": [
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Ready", "type": "integer", "description": "Number of ready replicas", "priority": 0},
            {"name": "Up-to-date", "type": "integer", "description": "Number of updated replicas", "priority": 0},
            {"name": "Available", "type": "integer", "description": "Number of available replicas", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ],
        "rows": rows,
    })
}

/// StatefulSet table: NAME READY AGE
pub fn statefulset_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|sts| {
            let name = sts["metadata"]["name"].as_str().unwrap_or("").to_string();
            let creation_timestamp = sts["metadata"]["creationTimestamp"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let age = format_age(&creation_timestamp);

            let ready_replicas = sts["status"]["readyReplicas"].as_i64().unwrap_or(0);
            let desired_replicas = sts["spec"]["replicas"].as_i64().unwrap_or(1);
            let ready = format!("{}/{}", ready_replicas, desired_replicas);

            serde_json::json!({
                "cells": [name, ready, age],
                "object": sts,
            })
        })
        .collect();

    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {
            "resourceVersion": resource_version,
        },
        "columnDefinitions": [
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Ready", "type": "string", "description": "Number of ready replicas", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ],
        "rows": rows,
    })
}

pub fn watch_event_to_table(event: WatchEvent, kind: &str) -> WatchEvent {
    let resource_version = event
        .object
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|rv| rv.as_str())
        .unwrap_or("0")
        .to_string();

    // For BOOKMARK events, return a minimal Table with no rows.
    // Only the initial-events-end BOOKMARK gets columnDefinitions (kubectl prints headers).
    // All other BOOKMARKs omit columnDefinitions to prevent duplicate headers.
    if event.event_type == EventType::Bookmark {
        let is_initial_events_end = event
            .object
            .pointer("/metadata/annotations/k8s.io~1initial-events-end")
            .is_some();

        let mut bookmark_table = serde_json::json!({
            "apiVersion": "meta.k8s.io/v1",
            "kind": "Table",
            "metadata": {
                "resourceVersion": resource_version,
                "annotations": event.object.get("metadata")
                    .and_then(|m| m.get("annotations"))
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            },
            "rows": [],
        });

        if is_initial_events_end {
            let column_defs = match kind {
                "Pod" => pod_table_column_definitions(),
                "Node" => node_table_column_definitions(),
                "ReplicaSet" => serde_json::json!([
                    {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
                    {"name": "Desired", "type": "integer", "description": "Number of desired pods", "priority": 0},
                    {"name": "Current", "type": "integer", "description": "Number of created pods", "priority": 0},
                    {"name": "Ready", "type": "integer", "description": "Number of ready pods", "priority": 0},
                    {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0}
                ]),
                "Deployment" => serde_json::json!([
                    {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
                    {"name": "Ready", "type": "integer", "description": "Number of ready replicas", "priority": 0},
                    {"name": "Up-to-date", "type": "integer", "description": "Number of updated replicas", "priority": 0},
                    {"name": "Available", "type": "integer", "description": "Number of available replicas", "priority": 0},
                    {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0}
                ]),
                "StatefulSet" => serde_json::json!([
                    {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
                    {"name": "Ready", "type": "string", "description": "Number of ready replicas", "priority": 0},
                    {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0}
                ]),
                _ => table_column_definitions_for_kind(kind),
            };
            bookmark_table["columnDefinitions"] = column_defs;
        }

        return WatchEvent {
            event_type: EventType::Bookmark,
            object: Arc::new(bookmark_table),
            encoded_payload: None,
        };
    }

    let object_val = Arc::try_unwrap(event.object).unwrap_or_else(|arc| (*arc).clone());
    let mut table = match kind {
        "Pod" => pod_list_to_table(vec![object_val], resource_version),
        "Node" => node_list_to_table(vec![object_val], resource_version),
        "ReplicaSet" => replicaset_list_to_table(vec![object_val], resource_version),
        "Deployment" => deployment_list_to_table(vec![object_val], resource_version),
        "StatefulSet" => statefulset_list_to_table(vec![object_val], resource_version),
        _ => {
            let cells = table_row_cells_for_kind(kind, &object_val);
            serde_json::json!({
                "apiVersion": "meta.k8s.io/v1",
                "kind": "Table",
                "metadata": {
                    "resourceVersion": resource_version,
                },
                "columnDefinitions": table_column_definitions_for_kind(kind),
                "rows": [{
                    "cells": cells,
                    "object": object_val,
                }],
            })
        }
    };

    // Strip columnDefinitions from ALL non-BOOKMARK watch events.
    // Headers come only from the initial-events-end BOOKMARK (handled above).
    // kubectl caches columns from that first event and never needs them again.
    if let Some(obj) = table.as_object_mut() {
        obj.remove("columnDefinitions");
    }

    WatchEvent {
        event_type: event.event_type,
        object: Arc::new(table),
        encoded_payload: None,
    }
}

// ---------------------------------------------------------------------------
// Generic server-side table printing for resources without a dedicated
// converter. Mirrors kubectl's human-readable columns so `kubectl get <kind>`
// shows the same columns as upstream Kubernetes. Resources with no specific
// printer fall back to the upstream default (NAME + CREATED AT).
//
// Following the established klights model, time columns are pre-formatted
// server-side: AGE cells carry the relative age string (format_age) and the
// CREATED AT cell carries the raw RFC3339 timestamp; kubectl prints cells
// verbatim.
// ---------------------------------------------------------------------------

fn default_table_column_definitions() -> Value {
    serde_json::json!([
        {"name": "Name", "type": "string", "format": "name", "description": "Name must be unique within a namespace.", "priority": 0},
        {"name": "Created At", "type": "date", "description": "CreationTimestamp is a timestamp representing the server time when this object was created.", "priority": 0},
    ])
}

/// Column definitions for `kind`, matching kubectl's default human-readable
/// output. Falls back to NAME + CREATED AT for kinds without a custom printer.
pub fn table_column_definitions_for_kind(kind: &str) -> Value {
    match kind {
        "Service" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Type", "type": "string", "description": "The type of this service", "priority": 0},
            {"name": "Cluster-IP", "type": "string", "description": "The cluster IP of this service", "priority": 0},
            {"name": "External-IP", "type": "string", "description": "External IPs of this service", "priority": 0},
            {"name": "Port(s)", "type": "string", "description": "The ports exposed by this service", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
            {"name": "Selector", "type": "string", "description": "The label selector of this service", "priority": 1},
        ]),
        "ConfigMap" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Data", "type": "integer", "description": "Number of data entries", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        "Secret" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Type", "type": "string", "description": "The type of the secret", "priority": 0},
            {"name": "Data", "type": "integer", "description": "Number of data entries", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        "Namespace" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Status", "type": "string", "description": "The status of the namespace", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        "ServiceAccount" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Secrets", "type": "integer", "description": "Number of secrets", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        "Endpoints" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Endpoints", "type": "string", "description": "The endpoints of this resource", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        "ClusterRoleBinding" | "RoleBinding" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Role", "type": "string", "description": "The role being referenced", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
            {"name": "Users", "type": "string", "description": "Users in the binding", "priority": 1},
            {"name": "Groups", "type": "string", "description": "Groups in the binding", "priority": 1},
            {"name": "ServiceAccounts", "type": "string", "description": "ServiceAccounts in the binding", "priority": 1},
        ]),
        "PersistentVolumeClaim" => serde_json::json!([
            {"name": "Name", "type": "string", "format": "name", "description": "Name", "priority": 0},
            {"name": "Status", "type": "string", "description": "The phase of the claim", "priority": 0},
            {"name": "Volume", "type": "string", "description": "The bound volume", "priority": 0},
            {"name": "Capacity", "type": "string", "description": "The capacity of the bound volume", "priority": 0},
            {"name": "Access Modes", "type": "string", "description": "The access modes of the bound volume", "priority": 0},
            {"name": "Storageclass", "type": "string", "description": "The storage class of the claim", "priority": 0},
            {"name": "Age", "type": "string", "description": "CreationTimestamp", "priority": 0},
        ]),
        _ => default_table_column_definitions(),
    }
}

/// Row cells for an `item` of `kind`, aligned 1:1 with
/// [`table_column_definitions_for_kind`].
pub fn table_row_cells_for_kind(kind: &str, item: &Value) -> Vec<Value> {
    let name = item["metadata"]["name"].as_str().unwrap_or("").to_string();
    let created_at = item["metadata"]["creationTimestamp"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let age = format_age(&created_at);

    match kind {
        "Service" => vec![
            Value::String(name),
            Value::String(table_service_type(item)),
            Value::String(table_service_cluster_ip(item)),
            Value::String(table_service_external_ip(item)),
            Value::String(table_service_ports(item)),
            Value::String(age),
            Value::String(table_service_selector(item)),
        ],
        "ConfigMap" => vec![
            Value::String(name),
            Value::from(
                table_count_map_keys(item, "data") + table_count_map_keys(item, "binaryData"),
            ),
            Value::String(age),
        ],
        "Secret" => vec![
            Value::String(name),
            Value::String(item["type"].as_str().unwrap_or("Opaque").to_string()),
            Value::from(table_count_map_keys(item, "data")),
            Value::String(age),
        ],
        "Namespace" => vec![
            Value::String(name),
            Value::String(item["status"]["phase"].as_str().unwrap_or("").to_string()),
            Value::String(age),
        ],
        "ServiceAccount" => vec![
            Value::String(name),
            Value::from(table_count_array(item, "secrets")),
            Value::String(age),
        ],
        "Endpoints" => vec![
            Value::String(name),
            Value::String(table_format_endpoints(item)),
            Value::String(age),
        ],
        "ClusterRoleBinding" | "RoleBinding" => vec![
            Value::String(name),
            Value::String(table_role_ref(item)),
            Value::String(age),
            Value::String(String::new()),
            Value::String(String::new()),
            Value::String(String::new()),
        ],
        "PersistentVolumeClaim" => vec![
            Value::String(name),
            Value::String(item["status"]["phase"].as_str().unwrap_or("").to_string()),
            Value::String(
                item["spec"]["volumeName"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            Value::String(
                item["status"]["capacity"]["storage"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            Value::String(table_access_modes(&item["spec"]["accessModes"])),
            Value::String(
                item["spec"]["storageClassName"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            Value::String(age),
        ],
        // Upstream default convertor: NAME + CREATED AT (raw timestamp).
        _ => vec![Value::String(name), Value::String(created_at)],
    }
}

/// Build a Table for a list of items of `kind` using the kind's column set.
pub fn generic_list_to_table(kind: &str, items: Vec<Value>, resource_version: String) -> Value {
    let rows: Vec<Value> = items
        .into_iter()
        .map(|item| {
            let cells = table_row_cells_for_kind(kind, &item);
            serde_json::json!({"cells": cells, "object": item})
        })
        .collect();
    serde_json::json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "Table",
        "metadata": {"resourceVersion": resource_version},
        "columnDefinitions": table_column_definitions_for_kind(kind),
        "rows": rows,
    })
}

fn table_service_type(svc: &Value) -> String {
    svc["spec"]["type"]
        .as_str()
        .unwrap_or("ClusterIP")
        .to_string()
}

fn table_service_cluster_ip(svc: &Value) -> String {
    let ip = svc["spec"]["clusterIP"].as_str().unwrap_or("");
    if ip.is_empty() {
        "<none>".to_string()
    } else {
        ip.to_string()
    }
}

fn table_service_external_ip(svc: &Value) -> String {
    let svc_type = svc["spec"]["type"].as_str().unwrap_or("ClusterIP");
    let mut ips: Vec<String> = Vec::new();
    if let Some(arr) = svc["spec"]["externalIPs"].as_array() {
        for v in arr {
            if let Some(s) = v.as_str() {
                ips.push(s.to_string());
            }
        }
    }
    match svc_type {
        "ExternalName" => svc["spec"]["externalName"]
            .as_str()
            .unwrap_or("<none>")
            .to_string(),
        "LoadBalancer" => {
            if let Some(ingress) = svc["status"]["loadBalancer"]["ingress"].as_array() {
                for v in ingress {
                    if let Some(s) = v["ip"].as_str().filter(|s| !s.is_empty()) {
                        ips.push(s.to_string());
                    } else if let Some(s) = v["hostname"].as_str().filter(|s| !s.is_empty()) {
                        ips.push(s.to_string());
                    }
                }
            }
            if ips.is_empty() {
                "<pending>".to_string()
            } else {
                ips.join(",")
            }
        }
        _ => {
            if ips.is_empty() {
                "<none>".to_string()
            } else {
                ips.join(",")
            }
        }
    }
}

fn table_service_ports(svc: &Value) -> String {
    let Some(ports) = svc["spec"]["ports"].as_array() else {
        return "<none>".to_string();
    };
    if ports.is_empty() {
        return "<none>".to_string();
    }
    let parts: Vec<String> = ports
        .iter()
        .map(|p| {
            let port = p["port"].as_i64().unwrap_or(0);
            let protocol = p["protocol"].as_str().unwrap_or("TCP");
            let node_port = p["nodePort"].as_i64().unwrap_or(0);
            if node_port > 0 {
                format!("{}:{}/{}", port, node_port, protocol)
            } else {
                format!("{}/{}", port, protocol)
            }
        })
        .collect();
    parts.join(",")
}

fn table_service_selector(svc: &Value) -> String {
    match svc["spec"]["selector"].as_object() {
        Some(map) if !map.is_empty() => {
            let mut parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect();
            parts.sort();
            parts.join(",")
        }
        _ => "<none>".to_string(),
    }
}

fn table_count_map_keys(item: &Value, field: &str) -> i64 {
    item[field].as_object().map(|m| m.len() as i64).unwrap_or(0)
}

fn table_count_array(item: &Value, field: &str) -> i64 {
    item[field].as_array().map(|a| a.len() as i64).unwrap_or(0)
}

fn table_format_endpoints(endpoints: &Value) -> String {
    const MAX: usize = 3;
    let mut entries: Vec<String> = Vec::new();
    let mut more = 0usize;
    if let Some(subsets) = endpoints["subsets"].as_array() {
        for subset in subsets {
            let ports = subset["ports"].as_array();
            let addresses = subset["addresses"].as_array();
            if let (Some(ports), Some(addresses)) = (ports, addresses) {
                for addr in addresses {
                    let Some(ip) = addr["ip"].as_str() else {
                        continue;
                    };
                    for port in ports {
                        if let Some(p) = port["port"].as_i64() {
                            if entries.len() < MAX {
                                entries.push(format!("{}:{}", ip, p));
                            } else {
                                more += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    if entries.is_empty() {
        return "<none>".to_string();
    }
    if more > 0 {
        format!("{} + {} more...", entries.join(","), more)
    } else {
        entries.join(",")
    }
}

fn table_role_ref(binding: &Value) -> String {
    let kind = binding["roleRef"]["kind"].as_str().unwrap_or("");
    let name = binding["roleRef"]["name"].as_str().unwrap_or("");
    if kind.is_empty() && name.is_empty() {
        String::new()
    } else {
        format!("{}/{}", kind, name)
    }
}

fn table_access_modes(modes: &Value) -> String {
    let Some(arr) = modes.as_array() else {
        return String::new();
    };
    let mut seen: Vec<&str> = Vec::new();
    for m in arr {
        let abbrev = match m.as_str().unwrap_or("") {
            "ReadWriteOnce" => "RWO",
            "ReadOnlyMany" => "ROX",
            "ReadWriteMany" => "RWX",
            "ReadWriteOncePod" => "RWOP",
            _ => continue,
        };
        if !seen.contains(&abbrev) {
            seen.push(abbrev);
        }
    }
    seen.join(",")
}

#[cfg(test)]
mod table_printer_tests {
    use super::*;
    use serde_json::json;

    fn col_names(kind: &str) -> Vec<String> {
        table_column_definitions_for_kind(kind)
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect()
    }

    fn cell_strings(kind: &str, item: &Value) -> Vec<String> {
        table_row_cells_for_kind(kind, item)
            .iter()
            .map(|c| match c {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect()
    }

    #[test]
    fn default_columns_are_name_and_created_at() {
        // Resources without a dedicated printer (e.g. ClusterRole, Role) use
        // the upstream default: NAME + CREATED AT, with the raw timestamp.
        for kind in ["ClusterRole", "Role", "SomeUnknownKind"] {
            assert_eq!(col_names(kind), vec!["Name", "Created At"], "kind={kind}");
            let item =
                json!({"metadata": {"name": "admin", "creationTimestamp": "2025-10-22T14:00:03Z"}});
            assert_eq!(
                cell_strings(kind, &item),
                vec!["admin", "2025-10-22T14:00:03Z"],
                "kind={kind}"
            );
        }
    }

    #[test]
    fn service_columns_and_cells_match_kubectl() {
        assert_eq!(
            col_names("Service"),
            vec![
                "Name",
                "Type",
                "Cluster-IP",
                "External-IP",
                "Port(s)",
                "Age",
                "Selector"
            ]
        );
        let svc = json!({
            "metadata": {"name": "kubernetes", "creationTimestamp": "2025-01-01T00:00:00Z"},
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "100.113.64.1",
                "ports": [{"port": 443, "protocol": "TCP"}],
                "selector": {"app": "x"}
            }
        });
        let cells = cell_strings("Service", &svc);
        assert_eq!(cells[0], "kubernetes");
        assert_eq!(cells[1], "ClusterIP");
        assert_eq!(cells[2], "100.113.64.1");
        assert_eq!(cells[3], "<none>");
        assert_eq!(cells[4], "443/TCP");
        assert_eq!(cells[6], "app=x");
    }

    #[test]
    fn service_nodeport_and_loadbalancer_external_ip() {
        let np = json!({
            "metadata": {"name": "np"},
            "spec": {"type": "NodePort", "clusterIP": "10.0.0.1",
                     "ports": [{"port": 80, "nodePort": 30080, "protocol": "TCP"}]}
        });
        assert_eq!(cell_strings("Service", &np)[4], "80:30080/TCP");

        let lb_pending = json!({
            "metadata": {"name": "lb"},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.0.0.2",
                     "ports": [{"port": 80, "protocol": "TCP"}]},
            "status": {"loadBalancer": {}}
        });
        assert_eq!(cell_strings("Service", &lb_pending)[3], "<pending>");

        let lb_ready = json!({
            "metadata": {"name": "lb"},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.0.0.2",
                     "ports": [{"port": 80, "protocol": "TCP"}]},
            "status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.4"}]}}
        });
        assert_eq!(cell_strings("Service", &lb_ready)[3], "192.0.2.4");

        let ext = json!({
            "metadata": {"name": "ext"},
            "spec": {"type": "ExternalName", "externalName": "example.com"}
        });
        assert_eq!(cell_strings("Service", &ext)[3], "example.com");
    }

    #[test]
    fn configmap_and_secret_data_counts() {
        assert_eq!(col_names("ConfigMap"), vec!["Name", "Data", "Age"]);
        let cm = json!({"metadata": {"name": "cm"}, "data": {"a": "1", "b": "2"}, "binaryData": {"c": "x"}});
        let cells = table_row_cells_for_kind("ConfigMap", &cm);
        assert_eq!(cells[0], json!("cm"));
        assert_eq!(cells[1], json!(3));

        assert_eq!(col_names("Secret"), vec!["Name", "Type", "Data", "Age"]);
        let secret = json!({"metadata": {"name": "s"}, "type": "kubernetes.io/tls", "data": {"tls.crt": "x", "tls.key": "y"}});
        let cells = table_row_cells_for_kind("Secret", &secret);
        assert_eq!(cells[1], json!("kubernetes.io/tls"));
        assert_eq!(cells[2], json!(2));
    }

    #[test]
    fn namespace_and_serviceaccount_columns() {
        assert_eq!(col_names("Namespace"), vec!["Name", "Status", "Age"]);
        let ns = json!({"metadata": {"name": "default"}, "status": {"phase": "Active"}});
        assert_eq!(cell_strings("Namespace", &ns)[1], "Active");

        assert_eq!(col_names("ServiceAccount"), vec!["Name", "Secrets", "Age"]);
        let sa = json!({"metadata": {"name": "default"}, "secrets": [{"name": "a"}]});
        assert_eq!(table_row_cells_for_kind("ServiceAccount", &sa)[1], json!(1));
    }

    #[test]
    fn endpoints_formatting_truncates() {
        assert_eq!(col_names("Endpoints"), vec!["Name", "Endpoints", "Age"]);
        let ep = json!({
            "metadata": {"name": "svc"},
            "subsets": [{
                "addresses": [{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}, {"ip": "10.0.0.3"}, {"ip": "10.0.0.4"}],
                "ports": [{"port": 8080}]
            }]
        });
        let cells = cell_strings("Endpoints", &ep);
        assert_eq!(
            cells[1],
            "10.0.0.1:8080,10.0.0.2:8080,10.0.0.3:8080 + 1 more..."
        );

        let empty = json!({"metadata": {"name": "svc"}, "subsets": []});
        assert_eq!(cell_strings("Endpoints", &empty)[1], "<none>");
    }

    #[test]
    fn rolebindings_show_role_ref() {
        for kind in ["ClusterRoleBinding", "RoleBinding"] {
            let names = col_names(kind);
            assert_eq!(&names[0..3], &["Name", "Role", "Age"], "kind={kind}");
            let b = json!({
                "metadata": {"name": "b"},
                "roleRef": {"kind": "ClusterRole", "name": "cluster-admin"}
            });
            assert_eq!(
                cell_strings(kind, &b)[1],
                "ClusterRole/cluster-admin",
                "kind={kind}"
            );
        }
    }

    #[test]
    fn pvc_columns_and_access_modes() {
        assert_eq!(
            col_names("PersistentVolumeClaim"),
            vec![
                "Name",
                "Status",
                "Volume",
                "Capacity",
                "Access Modes",
                "Storageclass",
                "Age"
            ]
        );
        let pvc = json!({
            "metadata": {"name": "data"},
            "spec": {"volumeName": "pv-1", "accessModes": ["ReadWriteOnce", "ReadOnlyMany"], "storageClassName": "standard"},
            "status": {"phase": "Bound", "capacity": {"storage": "1Gi"}}
        });
        let cells = cell_strings("PersistentVolumeClaim", &pvc);
        assert_eq!(cells[1], "Bound");
        assert_eq!(cells[2], "pv-1");
        assert_eq!(cells[3], "1Gi");
        assert_eq!(cells[4], "RWO,ROX");
        assert_eq!(cells[5], "standard");
    }

    #[test]
    fn generic_list_to_table_shape() {
        let items = vec![
            json!({"metadata": {"name": "kubernetes", "creationTimestamp": "2025-01-01T00:00:00Z"},
            "spec": {"type": "ClusterIP", "clusterIP": "10.0.0.1", "ports": [{"port": 443, "protocol": "TCP"}]}}),
        ];
        let table = generic_list_to_table("Service", items, "123".to_string());
        assert_eq!(table["kind"], "Table");
        assert_eq!(table["metadata"]["resourceVersion"], "123");
        assert_eq!(table["columnDefinitions"][0]["name"], "Name");
        assert_eq!(table["rows"][0]["cells"][0], "kubernetes");
        // The full object is preserved for kubectl -o wide / passthrough.
        assert_eq!(table["rows"][0]["object"]["metadata"]["name"], "kubernetes");
    }
}
