use bytes::Bytes;

use crate::datastore::DatastoreBackend;
use crate::datastore::ResourcePreconditions;
use crate::datastore::command::StorageCommand;
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
use crate::replication::protocol::ForwardedResource;

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

pub async fn apply_outbox_to_local_leader(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
    Ok(apply_outbox_to_local_leader_with_resource(
        db,
        idempotency_key,
        operation,
        payload,
        authoring_node,
    )
    .await?
    .result)
}

pub struct LocalOutboxApply {
    pub result: OutboxApplyResult,
    pub resource: Option<ForwardedResource>,
    /// Set when the apply newly persisted a mutation (Applied result). None
    /// for AlreadyApplied — the side-effect dispatcher must not re-fire on
    /// duplicate applies.
    pub command: Option<StorageCommand>,
}

pub async fn apply_outbox_to_local_leader_with_resource(
    db: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: OutboxOperation,
    payload: Bytes,
    authoring_node: &str,
) -> std::result::Result<LocalOutboxApply, OutboxApplyError> {
    let applied = crate::datastore::raft::state_machine::propose_outbox_on_backend(
        db,
        idempotency_key,
        operation,
        payload,
        authoring_node,
    )
    .await?;
    Ok(LocalOutboxApply {
        result: applied.result,
        resource: applied.resource,
        command: applied.command,
    })
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
        return Err(OutboxApplyError::NotFound(format!(
            "Pod {namespace}/{name} not found"
        )));
    };
    if live.uid == expected {
        return Ok(());
    }
    Err(OutboxApplyError::UidMismatch {
        expected: expected.to_string(),
        actual: live.uid,
    })
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
        | StorageCommand::UpdateNodeVtepMac { node_name, .. }
        | StorageCommand::UpdateNodePeerAttributes { node_name, .. }
        | StorageCommand::UpdateNodeDataplane { node_name, .. }
        | StorageCommand::DeleteNodeSubnet { node_name } => Some(node_name),

        _ => None,
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
    if lower.contains("uid mismatch") {
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
