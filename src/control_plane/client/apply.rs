use bytes::Bytes;
use klights_leader_api::{
    OutboxDeliveryError as OutboxApplyError, OutboxDeliveryResult as OutboxApplyResult,
};

use crate::datastore::DatastoreBackend;
use crate::datastore::ResourcePreconditions;
use crate::datastore::command::StorageCommand;
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::log_apply::OutboxStreamWatermark;

#[cfg(test)]
pub async fn apply_outbox_transactionally(
    db: &dyn crate::datastore::DatastoreBackend,
    idempotency_key: &str,
    operation: crate::kubelet::outbox::payload::OutboxOperation,
    payload: &[u8],
    authoring_node: &str,
) -> std::result::Result<
    crate::kubelet::outbox::OutboxApplyResult,
    crate::kubelet::outbox::OutboxApplyError,
> {
    // Run UID-mismatch check here (allowed file for Pod DB calls)
    let decoded = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(payload)
        .map_err(|e| OutboxApplyError::Retryable(e.to_string()))?;
    reject_pod_uid_mismatch(db, &decoded.command).await?;

    db.apply_outbox_transactionally(idempotency_key, operation.as_str(), payload, authoring_node)
        .await
}

/// Run GC on the applied_outbox idempotency ledger. Prunes all entries older
/// than `ttl_ms`; node-local outbox resend is bounded by the same ceiling.
pub async fn gc_applied_outbox(
    db: &dyn crate::datastore::DatastoreBackend,
    now_ms: i64,
    ttl_ms: i64,
) -> Result<usize, crate::kubelet::outbox::OutboxApplyError> {
    db.gc_applied_outbox(now_ms, ttl_ms)
        .await
        .map_err(|e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()))
}

pub fn outbox_stream_watermark(
    client_id: &str,
    stream_id: i64,
    stream_seq: i64,
) -> Option<OutboxStreamWatermark> {
    (stream_seq > 0).then(|| OutboxStreamWatermark {
        client_id: client_id.to_string(),
        stream_id,
        stream_seq,
    })
}

/// Durably consume one authenticated, assigned outbox position after the
/// leader has made a terminal decision before the worker payload can be
/// proposed normally (for example, malformed protobuf or failed
/// NodeRestriction validation).
///
/// The existing UID-bound stale-Pod protocol materializes this sentinel as a
/// ledger-and-watermark-only raft commit. Its deliberately invalid Kubernetes
/// namespace cannot collide with an API-created Pod, and no resource mutation
/// or public watch event is produced.
pub async fn consume_terminal_outbox_sequence(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    authoring_node: &str,
    watermark: Option<OutboxStreamWatermark>,
) -> std::result::Result<(), OutboxApplyError> {
    let assigned_sequence = watermark.is_some();
    let payload = crate::kubelet::outbox::payload::terminal_decision_payload(idempotency_key)
        .map(Bytes::from)
        .map_err(|error| {
            OutboxApplyError::Retryable(format!(
                "failed to encode terminal outbox decision: {error}"
            ))
        })?;
    match apply_outbox_to_local_leader_with_node_operation(
        db,
        idempotency_key,
        operation,
        payload,
        authoring_node,
        watermark,
    )
    .await
    {
        Ok(_) => Ok(()),
        // With an assigned position, the deliberately nonexistent sentinel
        // is materialized as a fresh ledger-and-watermark commit before this
        // exact typed result is returned. Unassigned attempts never receive
        // this acknowledgement because they cannot prove durable consumption.
        Err(OutboxApplyError::NotFound(message))
            if assigned_sequence
                && message == "Pod __klights-terminal-outbox__/decision not found" =>
        {
            Ok(())
        }
        // A pre-existing terminal ledger row does not persist its originating
        // stream identity, so it cannot safely authorize advancement of this
        // request's watermark. Keep the row retryable instead of consuming a
        // potentially unrelated stream.
        Err(error) if error.is_terminal() => Err(OutboxApplyError::Retryable(format!(
            "terminal outbox ledger does not prove this stream position: {error}"
        ))),
        Err(error) => Err(error),
    }
}

pub async fn apply_outbox_to_local_leader(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
    watermark: Option<OutboxStreamWatermark>,
) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
    apply_outbox_to_local_leader_with_node_operation(
        db,
        idempotency_key,
        operation,
        payload,
        authoring_node,
        watermark,
    )
    .await
}

async fn apply_outbox_to_local_leader_with_node_operation(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
    watermark: Option<OutboxStreamWatermark>,
) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
    Ok(
        crate::datastore::raft::state_machine::propose_outbox_on_backend_with_watermark(
            db,
            idempotency_key,
            operation,
            payload,
            authoring_node,
            watermark,
        )
        .await?
        .result,
    )
}

pub async fn reject_pod_uid_mismatch(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
) -> std::result::Result<(), OutboxApplyError> {
    let Some((namespace, name, preconditions)) = pod_target(command) else {
        return Ok(());
    };
    let Some(expected) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
        return Ok(());
    };
    let live = db
        .get_resource("v1", "Pod", Some(namespace), name)
        .await
        .map_err(|err| OutboxApplyError::Retryable(err.to_string()))?;
    let Some(live) = live else {
        if matches!(command, StorageCommand::DeleteResource { .. }) {
            tracing::warn!(
                namespace = %namespace,
                pod = %name,
                expected_uid = %expected,
                "leader apply_outbox rejected actor-owned Pod delete because the Pod is already absent"
            );
        }
        return Err(OutboxApplyError::NotFound(format!(
            "Pod {namespace}/{name} not found"
        )));
    };
    if live.uid == expected {
        return Ok(());
    }
    if matches!(command, StorageCommand::DeleteResource { .. }) {
        tracing::warn!(
            namespace = %namespace,
            pod = %name,
            expected_uid = %expected,
            actual_uid = %live.uid,
            "leader apply_outbox rejected actor-owned Pod delete due to UID mismatch"
        );
    }
    Err(OutboxApplyError::UidMismatch {
        expected: expected.to_string(),
        actual: live.uid,
    })
}

/// Bind a structurally valid PodMetadata delivery to the authenticated
/// worker's live Pod. The trusted node may update only its assigned Pod and
/// may finalize deletion only after the actor-observed terminal conditions
/// already required by the lifecycle path are present.
pub(crate) async fn authorize_live_pod_metadata_command(
    db: &dyn DatastoreBackend,
    command: &StorageCommand,
    authenticated_node: &str,
) -> std::result::Result<(), klights_leader_api::OutboxDeliveryError> {
    let Some((namespace, name, preconditions)) = pod_target(command) else {
        return Err(klights_leader_api::OutboxDeliveryError::conflict(
            "PodMetadata delivery must target one namespaced v1/Pod",
        ));
    };
    let Some(expected_uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
        return Err(klights_leader_api::OutboxDeliveryError::conflict(
            "PodMetadata delivery must carry a Pod UID precondition",
        ));
    };
    let live = db
        .get_resource("v1", "Pod", Some(namespace), name)
        .await
        .map_err(|error| klights_leader_api::OutboxDeliveryError::unavailable(error.to_string()))?
        .ok_or_else(|| {
            klights_leader_api::OutboxDeliveryError::not_found(format!(
                "Pod {namespace}/{name} not found"
            ))
        })?;

    if live.uid != expected_uid {
        return Err(klights_leader_api::OutboxDeliveryError::uid_mismatch(
            expected_uid,
            live.uid,
        ));
    }

    let assigned_node = live
        .data
        .pointer("/spec/nodeName")
        .and_then(serde_json::Value::as_str)
        .filter(|node_name| !node_name.is_empty());
    if assigned_node != Some(authenticated_node) {
        return Err(klights_leader_api::OutboxDeliveryError::conflict(format!(
            "PodMetadata delivery for {namespace}/{name} is restricted to its assigned node"
        )));
    }

    if matches!(command, StorageCommand::DeleteResource { .. }) {
        let has_finalizers = live
            .data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|finalizers| !finalizers.is_empty());
        if has_finalizers {
            return Err(klights_leader_api::OutboxDeliveryError::conflict(format!(
                "PodMetadata actor finalization for {namespace}/{name} is blocked by finalizers"
            )));
        }

        let terminating = live.data.pointer("/metadata/deletionTimestamp").is_some()
            || (live
                .data
                .pointer("/status/phase")
                .and_then(serde_json::Value::as_str)
                == Some("Failed")
                && live
                    .data
                    .pointer("/status/reason")
                    .and_then(serde_json::Value::as_str)
                    == Some("NodeLost"));
        if !terminating {
            return Err(klights_leader_api::OutboxDeliveryError::conflict(format!(
                "PodMetadata actor finalization requires terminating Pod {namespace}/{name}"
            )));
        }
    }

    Ok(())
}

fn pod_target(command: &StorageCommand) -> Option<(&str, &str, &ResourcePreconditions)> {
    match command {
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } if api_version == "v1" && kind == "Pod" => Some((
            namespace.as_deref().unwrap_or("default"),
            name,
            preconditions,
        )),
        _ => None,
    }
}

pub fn reject_node_author_mismatch(
    command: &StorageCommand,
    authoring_node: &str,
) -> std::result::Result<(), OutboxApplyError> {
    let Some(target_node) = node_scoped_outbox_target(command) else {
        return Ok(());
    };
    if target_node == authoring_node {
        return Ok(());
    }
    Err(OutboxApplyError::ConflictTerminal(format!(
        "node-scoped outbox command for node \"{target_node}\" may not be authored by node \"{authoring_node}\""
    )))
}

fn node_scoped_outbox_target(command: &StorageCommand) -> Option<&str> {
    if let Some((api_version, kind, namespace, name)) = resource_command_target(command) {
        if api_version == "v1" && kind == "Node" && namespace.is_none() {
            return Some(name);
        }
        if api_version == "coordination.k8s.io/v1"
            && kind == "Lease"
            && namespace == Some("kube-node-lease")
        {
            return Some(name);
        }
    }

    match command {
        StorageCommand::AllocateNodeSubnet { node_name, .. }
        | StorageCommand::UpdateNodePeerAttributes { node_name, .. }
        | StorageCommand::UpdateNodeDataplane { node_name, .. }
        | StorageCommand::DeleteNodeSubnet { node_name } => Some(node_name),

        _ => None,
    }
}

/// Authorize the worker-owned command carried by a durable outbox delivery.
///
/// This check is deliberately structural and must run before any datastore,
/// Raft, or side-effect work. `StorageCommand` is a broad cluster-internal
/// command language; the worker delivery port is allowed to expose only the
/// small, operation-specific subset below.
pub(crate) fn authorize_outbox_command(
    operation: klights_leader_api::OutboxDeliveryOperation,
    command: &StorageCommand,
    authenticated_node: &str,
) -> std::result::Result<(), klights_leader_api::OutboxDeliveryError> {
    use klights_leader_api::OutboxDeliveryOperation;

    if authenticated_node.is_empty() {
        return Err(klights_leader_api::OutboxDeliveryError::ConflictTerminal(
            "durable outbox delivery requires an authenticated node identity".to_string(),
        ));
    }

    let authorized = match operation {
        OutboxDeliveryOperation::PodStatus
        | OutboxDeliveryOperation::RuntimeReconcile
        | OutboxDeliveryOperation::ProbeReadiness
        | OutboxDeliveryOperation::DeadlineExceeded
        | OutboxDeliveryOperation::ContainerStatusSnapshot
        | OutboxDeliveryOperation::EphemeralContainerStatuses => is_uid_bound_pod_status(command),
        OutboxDeliveryOperation::PodMetadata => is_uid_bound_pod_metadata(command),
        OutboxDeliveryOperation::NodeRegistration => {
            is_authenticated_node_registration(command, authenticated_node)
        }
        OutboxDeliveryOperation::NodeStatus => {
            is_authenticated_node_status(command, authenticated_node)
        }
        OutboxDeliveryOperation::NodeDataplane => matches!(
            command,
            StorageCommand::UpdateNodeDataplane { node_name, .. }
                if node_name == authenticated_node
        ),
        OutboxDeliveryOperation::EventCreate => {
            is_authenticated_event_create(command, authenticated_node)
        }
    };

    if authorized {
        Ok(())
    } else {
        Err(klights_leader_api::OutboxDeliveryError::ConflictTerminal(
            format!(
                "outbox operation {} does not authorize {} for authenticated node {authenticated_node}",
                operation.as_wire_name(),
                command.variant_name(),
            ),
        ))
    }
}

fn is_uid_bound_pod_status(command: &StorageCommand) -> bool {
    matches!(
        command,
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        } if api_version == "v1"
            && kind == "Pod"
            && !namespace.is_empty()
            && !name.is_empty()
            && preconditions.uid.as_deref().is_some_and(|uid| !uid.is_empty())
    )
}

fn is_uid_bound_pod_metadata(command: &StorageCommand) -> bool {
    match command {
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            patch_kind: crate::datastore::PatchKind::Merge,
            patch,
            preconditions,
            strict_resource_version,
        } => {
            api_version == "v1"
                && kind == "Pod"
                && !namespace.is_empty()
                && !name.is_empty()
                && preconditions
                    .uid
                    .as_deref()
                    .is_some_and(|uid| !uid.is_empty())
                && is_exact_pod_metadata_patch(
                    patch,
                    preconditions.resource_version,
                    *strict_resource_version,
                )
        }
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
        } => {
            api_version == "v1"
                && kind == "Pod"
                && !namespace.is_empty()
                && !name.is_empty()
                && preconditions
                    .uid
                    .as_deref()
                    .is_some_and(|uid| !uid.is_empty())
                && preconditions.resource_version.is_none()
        }
        _ => false,
    }
}

fn is_exact_pod_metadata_patch(
    patch: &serde_json::Value,
    resource_version: Option<i64>,
    strict_resource_version: bool,
) -> bool {
    let Some(root) = patch.as_object().filter(|root| root.len() == 1) else {
        return false;
    };
    let Some(metadata) = root.get("metadata").and_then(serde_json::Value::as_object) else {
        return false;
    };

    if metadata.len() == 2 {
        return !strict_resource_version
            && resource_version.is_none()
            && metadata
                .get("deletionTimestamp")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|timestamp| !timestamp.is_empty())
            && metadata
                .get("deletionGracePeriodSeconds")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|seconds| seconds >= 0);
    }

    if metadata.len() != 1 || !strict_resource_version || resource_version.is_none_or(|rv| rv <= 0)
    {
        return false;
    }

    match metadata
        .iter()
        .next()
        .map(|(key, value)| (key.as_str(), value))
    {
        Some(("ownerReferences", owner_references)) => owner_references.is_array(),
        Some(("labels", labels)) => labels
            .as_object()
            .is_some_and(|labels| labels.values().all(|value| value.as_str().is_some())),
        Some(("annotations", annotations)) => annotations.as_object().is_some_and(|annotations| {
            annotations.len() == 1
                && annotations
                    .get("klights.dev/sandbox-id")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        }),
        _ => false,
    }
}

fn is_authenticated_node_registration(command: &StorageCommand, authenticated_node: &str) -> bool {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        } => {
            api_version == "v1"
                && kind == "Node"
                && namespace.is_none()
                && name == authenticated_node
                && resource_body_matches(data, api_version, kind, None, name, None)
        }
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
            ..
        } => {
            let Some(uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
                return false;
            };
            api_version == "v1"
                && kind == "Node"
                && namespace.is_none()
                && name == authenticated_node
                && resource_body_matches(data, api_version, kind, None, name, Some(uid))
        }
        _ => false,
    }
}

fn is_authenticated_node_status(command: &StorageCommand, authenticated_node: &str) -> bool {
    matches!(
        command,
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        } if api_version == "v1"
            && kind == "Node"
            && namespace.is_none()
            && name == authenticated_node
            && preconditions.uid.as_deref().is_some_and(|uid| !uid.is_empty())
    )
}

fn is_authenticated_event_create(command: &StorageCommand, authenticated_node: &str) -> bool {
    let StorageCommand::CreateResource {
        api_version,
        kind,
        namespace: Some(namespace),
        name,
        data,
    } = command
    else {
        return false;
    };
    if !matches!(api_version.as_str(), "v1" | "events.k8s.io/v1")
        || kind != "Event"
        || namespace.is_empty()
        || name.is_empty()
        || !resource_body_matches(data, api_version, kind, Some(namespace), name, None)
    {
        return false;
    }

    match api_version.as_str() {
        "v1" => {
            data.pointer("/source/host")
                .and_then(serde_json::Value::as_str)
                == Some(authenticated_node)
        }
        "events.k8s.io/v1" => {
            data.get("reportingInstance")
                .and_then(serde_json::Value::as_str)
                == Some(authenticated_node)
        }
        _ => false,
    }
}

fn resource_body_matches(
    data: &serde_json::Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    uid: Option<&str>,
) -> bool {
    data.get("apiVersion").and_then(serde_json::Value::as_str) == Some(api_version)
        && data.get("kind").and_then(serde_json::Value::as_str) == Some(kind)
        && data
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            == Some(name)
        && match namespace {
            Some(namespace) => {
                data.pointer("/metadata/namespace")
                    .and_then(serde_json::Value::as_str)
                    == Some(namespace)
            }
            None => data.pointer("/metadata/namespace").is_none(),
        }
        && match uid {
            Some(uid) => {
                data.pointer("/metadata/uid")
                    .and_then(serde_json::Value::as_str)
                    == Some(uid)
            }
            None => true,
        }
}

#[cfg(test)]
mod delivery_authorization_tests {
    use klights_leader_api::{OutboxDeliveryError, OutboxDeliveryOperation};
    use serde_json::json;

    use super::*;

    fn pod_status(uid: Option<&str>) -> StorageCommand {
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: uid.map(str::to_owned),
                resource_version: None,
            },
            observed_status_stamp: Some(41),
        }
    }

    fn node_status(name: &str, uid: Option<&str>) -> StorageCommand {
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: name.to_string(),
            status: json!({"conditions": []}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: uid.map(str::to_owned),
                resource_version: None,
            },
            observed_status_stamp: None,
        }
    }

    #[test]
    fn authorization_admits_only_the_operation_specific_worker_command() {
        for operation in [
            OutboxDeliveryOperation::PodStatus,
            OutboxDeliveryOperation::RuntimeReconcile,
            OutboxDeliveryOperation::ProbeReadiness,
            OutboxDeliveryOperation::DeadlineExceeded,
            OutboxDeliveryOperation::ContainerStatusSnapshot,
            OutboxDeliveryOperation::EphemeralContainerStatuses,
        ] {
            authorize_outbox_command(operation, &pod_status(Some("pod-uid")), "worker-a")
                .expect("UID-bound Pod status is authorized");
        }

        let pod_delete = StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            preconditions: ResourcePreconditions::uid("pod-uid"),
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::PodMetadata,
            &pod_delete,
            "worker-a",
        )
        .expect("actor-originated UID-bound Pod deletion remains deliverable");

        let pod_update = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            patch_kind: crate::datastore::PatchKind::Merge,
            patch: json!({"metadata": {"labels": {"app": "web"}}}),
            preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
            strict_resource_version: true,
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::PodMetadata,
            &pod_update,
            "worker-a",
        )
        .expect("UID-bound exact Pod labels patch is authorized");

        let pod_patch = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            patch_kind: crate::datastore::PatchKind::Merge,
            patch: json!({
                "metadata": {
                    "deletionTimestamp": "2026-07-18T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                }
            }),
            preconditions: ResourcePreconditions::uid("pod-uid"),
            strict_resource_version: false,
        };
        authorize_outbox_command(OutboxDeliveryOperation::PodMetadata, &pod_patch, "worker-a")
            .expect("UID-bound exact actor delete-mark patch is authorized");

        authorize_outbox_command(
            OutboxDeliveryOperation::NodeStatus,
            &node_status("worker-a", Some("node-uid")),
            "worker-a",
        )
        .expect("a node may update only its own UID-bound status");

        let node_registration = StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            data: json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
            }),
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::NodeRegistration,
            &node_registration,
            "worker-a",
        )
        .expect("a node may register only its own identity");

        let dataplane = StorageCommand::UpdateNodeDataplane {
            node_name: "worker-a".to_string(),
            mode: "root".to_string(),
            encryption: "wireguard".to_string(),
            public_key: None,
            endpoint: "192.0.2.10".to_string(),
            port: Some(7679),
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::NodeDataplane,
            &dataplane,
            "worker-a",
        )
        .expect("a node may publish only its own dataplane");

        let event = StorageCommand::CreateResource {
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "started.123".to_string(),
            data: json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {"namespace": "default", "name": "started.123"},
                "reportingInstance": "worker-a",
            }),
        };
        authorize_outbox_command(OutboxDeliveryOperation::EventCreate, &event, "worker-a")
            .expect("a node may create only an Event attributed to its identity");
    }

    #[test]
    fn pod_metadata_authorization_rejects_full_objects_and_unfocused_mutations() {
        let rejected = [
            (
                StorageCommand::UpdateResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    data: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "web",
                            "uid": "pod-uid",
                            "labels": {"app": "web"}
                        },
                        "spec": {"nodeName": "worker-a", "containers": []}
                    }),
                    expected_rv: 7,
                    preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                },
                "full Pod replacement",
            ),
            (
                StorageCommand::PatchResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    patch_kind: crate::datastore::PatchKind::Merge,
                    patch: json!({"spec": {"nodeName": "worker-b"}}),
                    preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                    strict_resource_version: true,
                },
                "Pod spec patch",
            ),
            (
                StorageCommand::PatchResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    patch_kind: crate::datastore::PatchKind::Merge,
                    patch: json!({"metadata": {"annotations": {"unowned.example/key": "value"}}}),
                    preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                    strict_resource_version: true,
                },
                "unowned annotation patch",
            ),
            (
                StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                },
                "actor finalization delete with an unowned RV precondition",
            ),
        ];

        for (command, reason) in rejected {
            assert!(
                matches!(
                    authorize_outbox_command(
                        OutboxDeliveryOperation::PodMetadata,
                        &command,
                        "worker-a",
                    ),
                    Err(OutboxDeliveryError::ConflictTerminal(_))
                ),
                "{reason} must be terminally rejected by the focused Pod metadata capability"
            );
        }
    }

    #[tokio::test]
    async fn pod_metadata_live_authorization_is_uid_node_and_finalizer_bound() {
        let (_datastore, db) = crate::datastore::test_support::in_memory_with_handle().await;
        for (name, uid, node_name, terminating, finalizers, phase, reason) in [
            (
                "ready",
                "uid-ready",
                "worker-a",
                true,
                json!([]),
                "Running",
                "",
            ),
            (
                "held",
                "uid-held",
                "worker-a",
                true,
                json!(["example.com/hold"]),
                "Running",
                "",
            ),
            (
                "live",
                "uid-live",
                "worker-a",
                false,
                json!([]),
                "Running",
                "",
            ),
            (
                "replacement",
                "uid-replacement",
                "worker-b",
                true,
                json!([]),
                "Running",
                "",
            ),
            (
                "node-lost",
                "uid-node-lost",
                "worker-a",
                false,
                json!([]),
                "Failed",
                "NodeLost",
            ),
        ] {
            let mut metadata = json!({
                "namespace": "default",
                "name": name,
                "uid": uid,
                "finalizers": finalizers,
            });
            if terminating {
                metadata["deletionTimestamp"] = json!("2026-07-18T00:00:00Z");
                metadata["deletionGracePeriodSeconds"] = json!(0);
            }
            db.create_resource(
                "v1",
                "Pod",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": metadata,
                    "spec": {"nodeName": node_name, "containers": []},
                    "status": {"phase": phase, "reason": reason},
                }),
            )
            .await
            .expect("seed live Pod");
        }

        let delete = |name: &str, uid: &str| StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            preconditions: ResourcePreconditions::uid(uid),
        };

        authorize_live_pod_metadata_command(db.as_ref(), &delete("ready", "uid-ready"), "worker-a")
            .await
            .expect("the actor may finalize its terminating, finalizer-free Pod");
        authorize_live_pod_metadata_command(
            db.as_ref(),
            &delete("node-lost", "uid-node-lost"),
            "worker-a",
        )
        .await
        .expect("the actor preserves the existing NodeLost terminal finalization path");

        for (command, author, expected, reason) in [
            (
                delete("ready", "stale-uid"),
                "worker-a",
                "uid mismatch",
                "stale actor UID",
            ),
            (
                delete("replacement", "uid-replacement"),
                "worker-a",
                "assigned node",
                "different assigned node",
            ),
            (
                delete("held", "uid-held"),
                "worker-a",
                "finalizers",
                "held finalizer",
            ),
            (
                delete("live", "uid-live"),
                "worker-a",
                "terminating",
                "non-terminating live Pod",
            ),
            (
                delete("absent", "uid-absent"),
                "worker-a",
                "not found",
                "absent Pod",
            ),
        ] {
            let error = authorize_live_pod_metadata_command(db.as_ref(), &command, author)
                .await
                .expect_err(reason);
            assert!(
                error.to_string().to_ascii_lowercase().contains(expected),
                "{reason} returned unexpected error: {error}"
            );
        }
    }

    #[test]
    fn authorization_is_default_deny_for_broad_cross_node_and_uidless_commands() {
        let rejected = [
            (
                OutboxDeliveryOperation::PodStatus,
                pod_status(None),
                "missing Pod UID",
            ),
            (
                OutboxDeliveryOperation::NodeStatus,
                node_status("worker-b", Some("node-uid")),
                "cross-node Node status",
            ),
            (
                OutboxDeliveryOperation::NodeStatus,
                node_status("worker-a", None),
                "missing Node UID",
            ),
            (
                OutboxDeliveryOperation::PodStatus,
                StorageCommand::CreateResource {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "smuggled".to_string(),
                    data: json!({}),
                },
                "generic resource command",
            ),
            (
                OutboxDeliveryOperation::PodMetadata,
                StorageCommand::ApplyResourceBatch {
                    operations: Vec::new(),
                },
                "batch command",
            ),
            (
                OutboxDeliveryOperation::PodMetadata,
                StorageCommand::CreateNamespace {
                    name: "smuggled".to_string(),
                    data: json!({}),
                },
                "namespace command",
            ),
            (
                OutboxDeliveryOperation::NodeDataplane,
                StorageCommand::AllocateNodeSubnet {
                    node_name: "worker-a".to_string(),
                    subnet: "10.42.0.0/16".to_string(),
                    node_ip: "192.0.2.10".to_string(),
                },
                "network allocation command",
            ),
            (
                OutboxDeliveryOperation::EventCreate,
                StorageCommand::CreateResource {
                    api_version: "events.k8s.io/v1".to_string(),
                    kind: "Event".to_string(),
                    namespace: Some("default".to_string()),
                    name: "spoofed.123".to_string(),
                    data: json!({
                        "apiVersion": "events.k8s.io/v1",
                        "kind": "Event",
                        "metadata": {"namespace": "default", "name": "spoofed.123"},
                        "reportingInstance": "worker-b",
                    }),
                },
                "cross-node Event author",
            ),
            (
                OutboxDeliveryOperation::PodMetadata,
                StorageCommand::MovePodToCleanupIntent {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "web".to_string(),
                    pod_uid: "pod-uid".to_string(),
                    reason: "smuggled".to_string(),
                },
                "dormant cleanup-intent command",
            ),
            (
                OutboxDeliveryOperation::PodMetadata,
                StorageCommand::SetKlightsMeta {
                    key: "smuggled".to_string(),
                    value: "true".to_string(),
                },
                "cluster meta command",
            ),
        ];

        for (operation, command, reason) in rejected {
            assert!(
                matches!(
                    authorize_outbox_command(operation, &command, "worker-a"),
                    Err(OutboxDeliveryError::ConflictTerminal(_))
                ),
                "{reason} must be terminally rejected"
            );
        }
    }
}

fn resource_command_target(command: &StorageCommand) -> Option<(&str, &str, Option<&str>, &str)> {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        } => Some((api_version, kind, namespace.as_deref(), name)),
        _ => None,
    }
}

pub fn classify_apply_error(err: anyhow::Error) -> OutboxApplyError {
    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    if lower.contains("uid mismatch") || lower.contains("uid precondition failed") {
        return OutboxApplyError::UidMismatch {
            expected: "<unknown>".to_string(),
            actual: "<unknown>".to_string(),
        };
    }
    if lower.contains("not found") {
        return OutboxApplyError::NotFound(message);
    }
    if lower.contains("conflict") || lower.contains("precondition failed") {
        return OutboxApplyError::ConflictTerminal(message);
    }
    OutboxApplyError::Retryable(message)
}

pub(crate) fn classify_apply_error_for_command(
    command: &StorageCommand,
    err: OutboxApplyError,
) -> OutboxApplyError {
    match err {
        OutboxApplyError::Retryable(message) => {
            if is_pod_stale_precondition_miss(command, &message) {
                OutboxApplyError::ConflictTerminal(message)
            } else {
                classify_apply_error(anyhow::anyhow!(message))
            }
        }
        other => other,
    }
}

fn is_pod_stale_precondition_miss(command: &StorageCommand, message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("query returned no rows")
        && matches!(
            command,
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                ..
            }
            | StorageCommand::UpdateResource {
                api_version,
                kind,
                ..
            }
            | StorageCommand::PatchResource {
                api_version,
                kind,
                ..
            }
            | StorageCommand::DeleteResource {
                api_version,
                kind,
                ..
            } if api_version == "v1" && kind == "Pod"
        )
}

pub fn subject_key_for_command(command: &StorageCommand) -> String {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            ..
        } => resource_subject_key(api_version, kind, namespace.as_deref(), name, data),
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        } => resource_key_parts(
            api_version,
            kind,
            namespace.as_deref(),
            name,
            preconditions.uid.as_deref(),
        ),
        StorageCommand::CreateNamespace { name, data }
        | StorageCommand::UpdateNamespace { name, data, .. } => {
            resource_subject_key("v1", "Namespace", None, name, data)
        }
        StorageCommand::DeleteNamespace { name }
        | StorageCommand::DeleteNamespaceContents { name } => {
            resource_key_parts("v1", "Namespace", None, name, None)
        }
        other => other.variant_name().to_string(),
    }
}

fn resource_subject_key(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: &serde_json::Value,
) -> String {
    resource_key_parts(
        api_version,
        kind,
        namespace,
        name,
        data.pointer("/metadata/uid").and_then(|uid| uid.as_str()),
    )
}

fn resource_key_parts(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    uid: Option<&str>,
) -> String {
    let mut key = match namespace {
        Some(namespace) => format!("{api_version}/{kind}/{namespace}/{name}"),
        None => format!("{api_version}/{kind}/{name}"),
    };
    if let Some(uid) = uid.filter(|uid| !uid.is_empty()) {
        key.push('/');
        key.push_str(uid);
    }
    key
}
