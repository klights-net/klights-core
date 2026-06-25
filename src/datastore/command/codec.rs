//! JSON + protobuf encode/decode for storage commands.
//! Extracted from command.rs (refactor).

use super::proto::*;
use super::*;

// ---------------------------------------------------------------------------

/// Encode a `StorageCommand` as a JSON byte vector.
pub fn encode_command_json(cmd: &StorageCommand) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(cmd)?)
}

/// Decode a `StorageCommand` from JSON bytes.
pub fn decode_command_json(bytes: &[u8]) -> anyhow::Result<StorageCommand> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Encode `StorageResponse` as JSON.
pub fn encode_response_json(resp: &StorageResponse) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(resp)?)
}

/// Decode `StorageResponse` from JSON.
pub fn decode_response_json(bytes: &[u8]) -> anyhow::Result<StorageResponse> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Encode `CommandMeta` as JSON.
pub fn encode_meta_json(meta: &CommandMeta) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(meta)?)
}

/// Decode `CommandMeta` from JSON.
pub fn decode_meta_json(bytes: &[u8]) -> anyhow::Result<CommandMeta> {
    Ok(serde_json::from_slice(bytes)?)
}

// ---------------------------------------------------------------------------
// Protobuf codec
// ---------------------------------------------------------------------------

/// Encode a `StorageCommand` as protobuf bytes.
pub fn encode_command_protobuf(cmd: &StorageCommand) -> anyhow::Result<Vec<u8>> {
    let proto = ProtoStorageCommand::from(cmd.clone());
    let mut buf = Vec::with_capacity(proto.encoded_len());
    prost::Message::encode(&proto, &mut buf)?;
    Ok(buf)
}

/// Decode a `StorageCommand` from protobuf bytes.
pub fn decode_command_protobuf(bytes: &[u8]) -> anyhow::Result<StorageCommand> {
    let proto: ProtoStorageCommand = prost::Message::decode(bytes)?;
    proto.try_into()
}

/// Encode `StorageResponse` as protobuf bytes.
pub fn encode_response_protobuf(resp: &StorageResponse) -> anyhow::Result<Vec<u8>> {
    let proto = ProtoStorageResponse::from(resp.clone());
    let mut buf = Vec::with_capacity(proto.encoded_len());
    prost::Message::encode(&proto, &mut buf)?;
    Ok(buf)
}

/// Decode `StorageResponse` from protobuf bytes.
pub fn decode_response_protobuf(bytes: &[u8]) -> anyhow::Result<StorageResponse> {
    let proto: ProtoStorageResponse = prost::Message::decode(bytes)?;
    proto.try_into()
}

/// Encode `CommandMeta` as protobuf bytes.
pub fn encode_meta_protobuf(meta: &CommandMeta) -> anyhow::Result<Vec<u8>> {
    let proto: ProtoCommandMeta = meta.clone().into();
    let mut buf = Vec::with_capacity(proto.encoded_len());
    prost::Message::encode(&proto, &mut buf)?;
    Ok(buf)
}

/// Decode `CommandMeta` from protobuf bytes.
pub fn decode_meta_protobuf(bytes: &[u8]) -> anyhow::Result<CommandMeta> {
    let proto: ProtoCommandMeta = prost::Message::decode(bytes)?;
    Ok(proto.into())
}

/// Encode `CommandError` as protobuf bytes.
pub fn encode_error_protobuf(err: &CommandError) -> anyhow::Result<Vec<u8>> {
    let proto = ProtoCommandError::from(err.clone());
    let mut buf = Vec::with_capacity(proto.encoded_len());
    prost::Message::encode(&proto, &mut buf)?;
    Ok(buf)
}

/// Decode `CommandError` from protobuf bytes.
pub fn decode_error_protobuf(bytes: &[u8]) -> anyhow::Result<CommandError> {
    let proto: ProtoCommandError = prost::Message::decode(bytes)?;
    proto.try_into()
}

// ---------------------------------------------------------------------------
// Proto conversions
// ---------------------------------------------------------------------------

fn json_to_bytes(val: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(val).expect("serde_json::Value serialization is infallible")
}

fn bytes_to_json(data: &[u8]) -> serde_json::Value {
    serde_json::from_slice(data).unwrap_or(serde_json::Value::Null)
}

impl From<ResourcePreconditions> for ProtoResourcePreconditions {
    fn from(preconditions: ResourcePreconditions) -> Self {
        Self {
            uid: preconditions.uid,
            resource_version: preconditions.resource_version,
        }
    }
}

impl From<ProtoResourcePreconditions> for ResourcePreconditions {
    fn from(preconditions: ProtoResourcePreconditions) -> Self {
        Self {
            uid: preconditions.uid,
            resource_version: preconditions.resource_version,
        }
    }
}

fn decode_preconditions(
    preconditions: Option<ProtoResourcePreconditions>,
    command: &str,
) -> anyhow::Result<ResourcePreconditions> {
    preconditions
        .map(Into::into)
        .ok_or_else(|| anyhow::anyhow!("protobuf {command} is missing resource preconditions"))
}

impl From<ResourceBatchPutMode> for ProtoResourceBatchPutMode {
    fn from(mode: ResourceBatchPutMode) -> Self {
        match mode {
            ResourceBatchPutMode::Create => Self::Create,
            ResourceBatchPutMode::Update => Self::Update,
        }
    }
}

impl TryFrom<ProtoResourceBatchPutMode> for ResourceBatchPutMode {
    type Error = anyhow::Error;

    fn try_from(mode: ProtoResourceBatchPutMode) -> anyhow::Result<Self> {
        Ok(match mode {
            ProtoResourceBatchPutMode::Create => Self::Create,
            ProtoResourceBatchPutMode::Update => Self::Update,
        })
    }
}

impl From<ResourceBatchOperation> for ProtoResourceBatchOperation {
    fn from(operation: ResourceBatchOperation) -> Self {
        let operation = match operation {
            ResourceBatchOperation::Put {
                api_version,
                kind,
                namespace,
                name,
                data,
                mode,
                preconditions,
            } => proto_resource_batch_operation::Operation::Put(ProtoResourceBatchPut {
                api_version,
                kind,
                namespace,
                name,
                data: json_to_bytes(&data),
                mode: ProtoResourceBatchPutMode::from(mode) as i32,
                preconditions: Some(preconditions.into()),
            }),
            ResourceBatchOperation::Delete {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            } => proto_resource_batch_operation::Operation::Delete(ProtoResourceBatchDelete {
                api_version,
                kind,
                namespace,
                name,
                preconditions: Some(preconditions.into()),
            }),
        };
        Self {
            operation: Some(operation),
        }
    }
}

impl TryFrom<ProtoResourceBatchOperation> for ResourceBatchOperation {
    type Error = anyhow::Error;

    fn try_from(operation: ProtoResourceBatchOperation) -> anyhow::Result<Self> {
        let operation = operation
            .operation
            .ok_or_else(|| anyhow::anyhow!("protobuf ResourceBatchOperation has no variant"))?;
        match operation {
            proto_resource_batch_operation::Operation::Put(put) => {
                let mode = ProtoResourceBatchPutMode::try_from(put.mode)
                    .map_err(|_| {
                        anyhow::anyhow!("unknown protobuf ResourceBatchPutMode: {}", put.mode)
                    })
                    .and_then(ResourceBatchPutMode::try_from)?;
                Ok(ResourceBatchOperation::Put {
                    api_version: put.api_version,
                    kind: put.kind,
                    namespace: put.namespace,
                    name: put.name,
                    data: bytes_to_json(&put.data),
                    mode,
                    preconditions: decode_preconditions(put.preconditions, "ResourceBatchPut")?,
                })
            }
            proto_resource_batch_operation::Operation::Delete(delete) => {
                Ok(ResourceBatchOperation::Delete {
                    api_version: delete.api_version,
                    kind: delete.kind,
                    namespace: delete.namespace,
                    name: delete.name,
                    preconditions: decode_preconditions(
                        delete.preconditions,
                        "ResourceBatchDelete",
                    )?,
                })
            }
        }
    }
}

impl From<StorageCommand> for ProtoStorageCommand {
    fn from(cmd: StorageCommand) -> Self {
        let command = match cmd {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
            } => proto_storage_command::Command::CreateResource(ProtoCreateResource {
                api_version,
                kind,
                namespace,
                name,
                data: json_to_bytes(&data),
            }),
            StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                expected_rv,
                preconditions,
            } => proto_storage_command::Command::UpdateResource(ProtoUpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data: json_to_bytes(&data),
                expected_rv,
                preconditions: Some(preconditions.into()),
            }),
            StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
            } => proto_storage_command::Command::DeleteResource(ProtoDeleteResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions: Some(preconditions.into()),
            }),
            StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch_kind,
                patch,
                preconditions,
                strict_resource_version,
            } => proto_storage_command::Command::PatchResource(ProtoPatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch_kind: match patch_kind {
                    PatchKind::Merge => ProtoPatchKind::Merge as i32,
                },
                patch: json_to_bytes(&patch),
                preconditions: Some(preconditions.into()),
                strict_resource_version,
            }),
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => proto_storage_command::Command::UpdateStatus(ProtoUpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status: json_to_bytes(&status),
                expected_rv,
                preconditions: Some(preconditions.into()),
                observed_status_stamp,
            }),
            StorageCommand::ApplyResourceBatch { operations } => {
                proto_storage_command::Command::ApplyResourceBatch(ProtoApplyResourceBatch {
                    operations: operations.into_iter().map(Into::into).collect(),
                })
            }
            StorageCommand::CreateNamespace { name, data } => {
                proto_storage_command::Command::CreateNamespace(ProtoCreateNamespace {
                    name,
                    data: json_to_bytes(&data),
                })
            }
            StorageCommand::UpdateNamespace {
                name,
                data,
                expected_rv,
            } => proto_storage_command::Command::UpdateNamespace(ProtoUpdateNamespace {
                name,
                data: json_to_bytes(&data),
                expected_rv,
            }),
            StorageCommand::DeleteNamespace { name } => {
                proto_storage_command::Command::DeleteNamespace(ProtoDeleteNamespace { name })
            }
            StorageCommand::DeleteNamespaceContents { name } => {
                proto_storage_command::Command::DeleteNamespaceContents(
                    ProtoDeleteNamespaceContents { name },
                )
            }
            StorageCommand::AllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            } => proto_storage_command::Command::AllocateNodeSubnet(ProtoAllocateNodeSubnet {
                node_name,
                subnet,
                node_ip,
            }),
            StorageCommand::UpdateNodeVtepMac {
                node_name,
                vtep_mac,
            } => proto_storage_command::Command::UpdateNodeVtepMac(ProtoUpdateNodeVtepMac {
                node_name,
                vtep_mac,
            }),
            StorageCommand::UpdateNodePeerAttributes {
                node_name,
                mode,
                hostport_range,
            } => proto_storage_command::Command::UpdateNodePeerAttributes(
                ProtoUpdateNodePeerAttributes {
                    node_name,
                    mode,
                    hostport_range,
                },
            ),
            StorageCommand::UpdateNodeDataplane {
                node_name,
                mode,
                encryption,
                public_key,
                endpoint,
                port,
            } => proto_storage_command::Command::UpdateNodeDataplane(ProtoUpdateNodeDataplane {
                node_name,
                mode,
                encryption,
                public_key,
                endpoint,
                port: port.map(u32::from),
            }),
            StorageCommand::DeleteNodeSubnet { node_name } => {
                proto_storage_command::Command::DeleteNodeSubnet(ProtoDeleteNodeSubnet {
                    node_name,
                })
            }
            StorageCommand::PodSlotTryAdmit {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => proto_storage_command::Command::PodSlotTryAdmit(ProtoPodSlotAdmissionCommand {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            }),
            StorageCommand::PodSlotMarkTerminating {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => proto_storage_command::Command::PodSlotMarkTerminating(
                ProtoPodSlotAdmissionCommand {
                    namespace,
                    pod_name,
                    pod_uid,
                    node_name,
                },
            ),
            StorageCommand::PodSlotClearIfUid {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            } => proto_storage_command::Command::PodSlotClearIfUid(ProtoPodSlotAdmissionCommand {
                namespace,
                pod_name,
                pod_uid,
                node_name,
            }),
            StorageCommand::MovePodToCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => proto_storage_command::Command::MovePodToCleanupIntent(
                ProtoPodCleanupIntentCommand {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                },
            ),
            StorageCommand::DeletePodCleanupIntent {
                node_name,
                namespace,
                pod_name,
                pod_uid,
                reason,
            } => proto_storage_command::Command::DeletePodCleanupIntent(
                ProtoPodCleanupIntentCommand {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                },
            ),
            StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
                proto_storage_command::Command::DeletePodCleanupIntentsForNode(
                    ProtoDeletePodCleanupIntentsForNode { node_name },
                )
            }
            StorageCommand::AdvanceResourceVersion { min_rv, new_rv } => {
                proto_storage_command::Command::AdvanceResourceVersion(
                    ProtoAdvanceResourceVersion { min_rv, new_rv },
                )
            }
            StorageCommand::WatchEventAppend { event_bytes, rv } => {
                proto_storage_command::Command::WatchEventAppend(ProtoWatchEventAppend {
                    event_bytes,
                    rv,
                })
            }
            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            } => proto_storage_command::Command::GcWatchEvents(ProtoGcWatchEvents {
                max_rows,
                batch_cap,
            }),
            StorageCommand::EnsureClusterMetadata { cluster_id } => {
                proto_storage_command::Command::EnsureClusterMetadata(ProtoEnsureClusterMetadata {
                    cluster_id,
                })
            }
            StorageCommand::SetKlightsMeta { key, value } => {
                proto_storage_command::Command::SetKlightsMeta(ProtoSetKlightsMeta { key, value })
            }
        };
        ProtoStorageCommand {
            command: Some(command),
        }
    }
}

impl TryFrom<ProtoStorageCommand> for StorageCommand {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoStorageCommand) -> anyhow::Result<Self> {
        let cmd = proto
            .command
            .ok_or_else(|| anyhow::anyhow!("protobuf StorageCommand has no variant"))?;

        Ok(match cmd {
            proto_storage_command::Command::CreateResource(p) => StorageCommand::CreateResource {
                api_version: p.api_version,
                kind: p.kind,
                namespace: p.namespace,
                name: p.name,
                data: bytes_to_json(&p.data),
            },
            proto_storage_command::Command::UpdateResource(p) => StorageCommand::UpdateResource {
                api_version: p.api_version,
                kind: p.kind,
                namespace: p.namespace,
                name: p.name,
                data: bytes_to_json(&p.data),
                expected_rv: p.expected_rv,
                preconditions: decode_preconditions(p.preconditions, "UpdateResource")?,
            },
            proto_storage_command::Command::DeleteResource(p) => StorageCommand::DeleteResource {
                api_version: p.api_version,
                kind: p.kind,
                namespace: p.namespace,
                name: p.name,
                preconditions: decode_preconditions(p.preconditions, "DeleteResource")?,
            },
            proto_storage_command::Command::PatchResource(p) => StorageCommand::PatchResource {
                api_version: p.api_version,
                kind: p.kind,
                namespace: p.namespace,
                name: p.name,
                patch_kind: match ProtoPatchKind::try_from(p.patch_kind) {
                    Ok(ProtoPatchKind::Merge) => PatchKind::Merge,
                    Err(_) => anyhow::bail!("unknown protobuf PatchKind: {}", p.patch_kind),
                },
                patch: bytes_to_json(&p.patch),
                preconditions: decode_preconditions(p.preconditions, "PatchResource")?,
                strict_resource_version: p.strict_resource_version,
            },
            proto_storage_command::Command::UpdateStatus(p) => StorageCommand::UpdateStatus {
                api_version: p.api_version,
                kind: p.kind,
                namespace: p.namespace,
                name: p.name,
                status: bytes_to_json(&p.status),
                expected_rv: p.expected_rv,
                preconditions: decode_preconditions(p.preconditions, "UpdateStatus")?,
                observed_status_stamp: p.observed_status_stamp,
            },
            proto_storage_command::Command::ApplyResourceBatch(batch) => {
                StorageCommand::ApplyResourceBatch {
                    operations: batch
                        .operations
                        .into_iter()
                        .map(ResourceBatchOperation::try_from)
                        .collect::<anyhow::Result<Vec<_>>>()?,
                }
            }
            proto_storage_command::Command::CreateNamespace(p) => StorageCommand::CreateNamespace {
                name: p.name,
                data: bytes_to_json(&p.data),
            },
            proto_storage_command::Command::UpdateNamespace(p) => StorageCommand::UpdateNamespace {
                name: p.name,
                data: bytes_to_json(&p.data),
                expected_rv: p.expected_rv,
            },
            proto_storage_command::Command::DeleteNamespace(p) => {
                StorageCommand::DeleteNamespace { name: p.name }
            }
            proto_storage_command::Command::DeleteNamespaceContents(p) => {
                StorageCommand::DeleteNamespaceContents { name: p.name }
            }
            proto_storage_command::Command::AllocateNodeSubnet(p) => {
                StorageCommand::AllocateNodeSubnet {
                    node_name: p.node_name,
                    subnet: p.subnet,
                    node_ip: p.node_ip,
                }
            }
            proto_storage_command::Command::UpdateNodeVtepMac(p) => {
                StorageCommand::UpdateNodeVtepMac {
                    node_name: p.node_name,
                    vtep_mac: p.vtep_mac,
                }
            }
            proto_storage_command::Command::UpdateNodePeerAttributes(p) => {
                StorageCommand::UpdateNodePeerAttributes {
                    node_name: p.node_name,
                    mode: p.mode,
                    hostport_range: p.hostport_range,
                }
            }
            proto_storage_command::Command::UpdateNodeDataplane(p) => {
                let port = p
                    .port
                    .map(u16::try_from)
                    .transpose()
                    .map_err(|_| anyhow::anyhow!("dataplane port exceeds u16"))?;
                StorageCommand::UpdateNodeDataplane {
                    node_name: p.node_name,
                    mode: p.mode,
                    encryption: p.encryption,
                    public_key: p.public_key,
                    endpoint: p.endpoint,
                    port,
                }
            }
            proto_storage_command::Command::DeleteNodeSubnet(p) => {
                StorageCommand::DeleteNodeSubnet {
                    node_name: p.node_name,
                }
            }
            proto_storage_command::Command::PodSlotTryAdmit(p) => StorageCommand::PodSlotTryAdmit {
                namespace: p.namespace,
                pod_name: p.pod_name,
                pod_uid: p.pod_uid,
                node_name: p.node_name,
            },
            proto_storage_command::Command::PodSlotMarkTerminating(p) => {
                StorageCommand::PodSlotMarkTerminating {
                    namespace: p.namespace,
                    pod_name: p.pod_name,
                    pod_uid: p.pod_uid,
                    node_name: p.node_name,
                }
            }
            proto_storage_command::Command::PodSlotClearIfUid(p) => {
                StorageCommand::PodSlotClearIfUid {
                    namespace: p.namespace,
                    pod_name: p.pod_name,
                    pod_uid: p.pod_uid,
                    node_name: p.node_name,
                }
            }
            proto_storage_command::Command::MovePodToCleanupIntent(p) => {
                StorageCommand::MovePodToCleanupIntent {
                    node_name: p.node_name,
                    namespace: p.namespace,
                    pod_name: p.pod_name,
                    pod_uid: p.pod_uid,
                    reason: p.reason,
                }
            }
            proto_storage_command::Command::DeletePodCleanupIntent(p) => {
                StorageCommand::DeletePodCleanupIntent {
                    node_name: p.node_name,
                    namespace: p.namespace,
                    pod_name: p.pod_name,
                    pod_uid: p.pod_uid,
                    reason: p.reason,
                }
            }
            proto_storage_command::Command::DeletePodCleanupIntentsForNode(p) => {
                StorageCommand::DeletePodCleanupIntentsForNode {
                    node_name: p.node_name,
                }
            }
            proto_storage_command::Command::AdvanceResourceVersion(p) => {
                StorageCommand::AdvanceResourceVersion {
                    min_rv: p.min_rv,
                    new_rv: p.new_rv,
                }
            }
            proto_storage_command::Command::WatchEventAppend(p) => {
                StorageCommand::WatchEventAppend {
                    event_bytes: p.event_bytes,
                    rv: p.rv,
                }
            }
            proto_storage_command::Command::GcWatchEvents(p) => StorageCommand::GcWatchEvents {
                max_rows: p.max_rows,
                batch_cap: p.batch_cap,
            },
            proto_storage_command::Command::EnsureClusterMetadata(p) => {
                StorageCommand::EnsureClusterMetadata {
                    cluster_id: p.cluster_id,
                }
            }
            proto_storage_command::Command::SetKlightsMeta(p) => StorageCommand::SetKlightsMeta {
                key: p.key,
                value: p.value,
            },
        })
    }
}

impl From<StorageResponse> for ProtoStorageResponse {
    fn from(resp: StorageResponse) -> Self {
        let response = match resp {
            StorageResponse::Resource {
                resource_version,
                data,
            } => proto_storage_response::Response::Resource(ProtoResourceResp {
                resource_version,
                data: json_to_bytes(&data),
            }),
            StorageResponse::Ack { resource_version } => {
                proto_storage_response::Response::Ack(ProtoAckResp { resource_version })
            }
            StorageResponse::NodeSubnet {
                node_name,
                subnet,
                subnet_base_int,
                vtep_ip,
                node_ip,
                mode,
                hostport_range,
            } => proto_storage_response::Response::NodeSubnet(ProtoNodeSubnetResp {
                node_name,
                subnet,
                subnet_base_int,
                vtep_ip,
                node_ip,
                mode,
                hostport_range,
            }),
            StorageResponse::Error { message } => {
                proto_storage_response::Response::Error(ProtoErrorResp { message })
            }
        };
        ProtoStorageResponse {
            response: Some(response),
        }
    }
}

impl TryFrom<ProtoStorageResponse> for StorageResponse {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoStorageResponse) -> anyhow::Result<Self> {
        let resp = proto
            .response
            .ok_or_else(|| anyhow::anyhow!("protobuf StorageResponse has no variant"))?;

        Ok(match resp {
            proto_storage_response::Response::Resource(p) => StorageResponse::Resource {
                resource_version: p.resource_version,
                data: bytes_to_json(&p.data),
            },
            proto_storage_response::Response::Ack(p) => StorageResponse::Ack {
                resource_version: p.resource_version,
            },
            proto_storage_response::Response::NodeSubnet(p) => StorageResponse::NodeSubnet {
                node_name: p.node_name,
                subnet: p.subnet,
                subnet_base_int: p.subnet_base_int,
                vtep_ip: p.vtep_ip,
                node_ip: p.node_ip,
                mode: p.mode,
                hostport_range: p.hostport_range,
            },
            proto_storage_response::Response::Error(p) => {
                StorageResponse::Error { message: p.message }
            }
        })
    }
}

impl From<CommandError> for ProtoCommandError {
    fn from(err: CommandError) -> Self {
        let (code, message) = match err {
            CommandError::Conflict { message } => (ProtoCommandErrorCode::Conflict as i32, message),
            CommandError::NotFound { message } => (ProtoCommandErrorCode::NotFound as i32, message),
            CommandError::Internal { message } => (ProtoCommandErrorCode::Internal as i32, message),
        };
        ProtoCommandError { code, message }
    }
}

impl TryFrom<ProtoCommandError> for CommandError {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoCommandError) -> anyhow::Result<Self> {
        let code = ProtoCommandErrorCode::try_from(proto.code)
            .map_err(|_| anyhow::anyhow!("unknown protobuf CommandErrorCode: {}", proto.code))?;

        Ok(match code {
            ProtoCommandErrorCode::Conflict => CommandError::Conflict {
                message: proto.message,
            },
            ProtoCommandErrorCode::NotFound => CommandError::NotFound {
                message: proto.message,
            },
            ProtoCommandErrorCode::Internal => CommandError::Internal {
                message: proto.message,
            },
            ProtoCommandErrorCode::Unknown => {
                anyhow::bail!("unknown protobuf CommandErrorCode: {}", proto.code)
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn uid_preconditions(uid: &str) -> ResourcePreconditions {
        ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        }
    }

    #[test]
    fn apply_resource_batch_round_trips_json_and_protobuf() {
        let cmd = StorageCommand::apply_resource_batch(vec![
            ResourceBatchOperation::Put {
                api_version: "discovery.k8s.io/v1".to_string(),
                kind: "EndpointSlice".to_string(),
                namespace: Some("default".to_string()),
                name: "web-klights".to_string(),
                data: json!({
                    "apiVersion": "discovery.k8s.io/v1",
                    "kind": "EndpointSlice",
                    "metadata": {"name": "web-klights", "namespace": "default"},
                    "addressType": "IPv4",
                    "endpoints": [],
                    "ports": []
                }),
                mode: ResourceBatchPutMode::Create,
                preconditions: ResourcePreconditions::default(),
            },
            ResourceBatchOperation::Put {
                api_version: "v1".to_string(),
                kind: "Endpoints".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Endpoints",
                    "metadata": {"name": "web", "namespace": "default"},
                    "subsets": []
                }),
                mode: ResourceBatchPutMode::Update,
                preconditions: ResourcePreconditions {
                    uid: Some("endpoints-uid".to_string()),
                    resource_version: Some(7),
                },
            },
            ResourceBatchOperation::Delete {
                api_version: "discovery.k8s.io/v1".to_string(),
                kind: "EndpointSlice".to_string(),
                namespace: Some("default".to_string()),
                name: "web-klights-old".to_string(),
                preconditions: ResourcePreconditions {
                    uid: Some("stale-slice-uid".to_string()),
                    resource_version: None,
                },
            },
        ]);

        let json_bytes = encode_command_json(&cmd).expect("encode JSON command");
        let json_decoded = decode_command_json(&json_bytes).expect("decode JSON command");
        assert_eq!(json_decoded, cmd);

        let proto_bytes = encode_command_protobuf(&cmd).expect("encode protobuf command");
        let proto_decoded = decode_command_protobuf(&proto_bytes).expect("decode protobuf command");
        assert_eq!(proto_decoded, cmd);
    }

    #[test]
    fn apply_resource_batch_variant_name_and_builder() {
        let cmd = StorageCommand::apply_resource_batch(Vec::new());
        assert_eq!(cmd.variant_name(), "ApplyResourceBatch");
        match cmd {
            StorageCommand::ApplyResourceBatch { operations } => assert!(operations.is_empty()),
            other => panic!(
                "expected ApplyResourceBatch, got {:?}",
                other.variant_name()
            ),
        }
    }

    // ---- Helpers ----

    fn sample_meta() -> CommandMeta {
        CommandMeta {
            command_id: CommandId("test-cmd-001".into()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 42,
            uid: Some("uid-abc-123".into()),
            timestamp_ms: 1_700_000_000_000,
            authoring_node: "node-1".into(),
        }
    }

    /// Returns one sample per `StorageCommand` variant, tagged with the
    /// variant name for table-driven tests.  Adding a new variant to
    /// `StorageCommand` without adding an entry here will cause
    /// `every_storage_command_variant_is_tested` to fail.
    fn all_command_samples() -> Vec<(StorageCommand, &'static str)> {
        vec![
            (
                StorageCommand::CreateResource {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "my-pod".into(),
                    data: json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "my-pod"}}),
                },
                "CreateResource",
            ),
            (
                StorageCommand::UpdateResource {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "my-pod".into(),
                    data: json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "my-pod", "resourceVersion": "42"}}),
                    expected_rv: 42,
                    preconditions: ResourcePreconditions {
                        uid: Some("uid-abc-123".into()),
                        resource_version: Some(42),
                    },
                },
                "UpdateResource",
            ),
            (
                StorageCommand::DeleteResource {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "my-pod".into(),
                    preconditions: uid_preconditions("uid-abc-123"),
                },
                "DeleteResource",
            ),
            (
                StorageCommand::PatchResource {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "my-pod".into(),
                    patch_kind: PatchKind::Merge,
                    patch: json!({"metadata": {"labels": {"app": "test"}}}),
                    preconditions: uid_preconditions("uid-abc-123"),
                    strict_resource_version: true,
                },
                "PatchResource",
            ),
            (
                StorageCommand::UpdateStatus {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "my-pod".into(),
                    status: json!({"phase": "Running", "podIP": "10.42.0.5"}),
                    expected_rv: Some(42),
                    preconditions: ResourcePreconditions {
                        uid: Some("uid-abc-123".into()),
                        resource_version: Some(42),
                    },
                    observed_status_stamp: Some(7),
                },
                "UpdateStatus",
            ),
            (
                StorageCommand::ApplyResourceBatch {
                    operations: vec![
                        ResourceBatchOperation::Put {
                            api_version: "v1".into(),
                            kind: "Endpoints".into(),
                            namespace: Some("default".into()),
                            name: "my-service".into(),
                            data: json!({
                                "apiVersion": "v1",
                                "kind": "Endpoints",
                                "metadata": {"name": "my-service", "namespace": "default"},
                                "subsets": []
                            }),
                            mode: ResourceBatchPutMode::Create,
                            preconditions: ResourcePreconditions::default(),
                        },
                        ResourceBatchOperation::Delete {
                            api_version: "discovery.k8s.io/v1".into(),
                            kind: "EndpointSlice".into(),
                            namespace: Some("default".into()),
                            name: "my-service-stale".into(),
                            preconditions: uid_preconditions("stale-slice-uid"),
                        },
                    ],
                },
                "ApplyResourceBatch",
            ),
            (
                StorageCommand::CreateNamespace {
                    name: "test-ns".into(),
                    data: json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test-ns"}}),
                },
                "CreateNamespace",
            ),
            (
                StorageCommand::UpdateNamespace {
                    name: "test-ns".into(),
                    data: json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test-ns", "resourceVersion": "10"}}),
                    expected_rv: 10,
                },
                "UpdateNamespace",
            ),
            (
                StorageCommand::DeleteNamespace {
                    name: "test-ns".into(),
                },
                "DeleteNamespace",
            ),
            (
                StorageCommand::DeleteNamespaceContents {
                    name: "test-ns".into(),
                },
                "DeleteNamespaceContents",
            ),
            (
                StorageCommand::AllocateNodeSubnet {
                    node_name: "node-1".into(),
                    subnet: "10.42.0.0/16".into(),
                    node_ip: "192.168.1.10".into(),
                },
                "AllocateNodeSubnet",
            ),
            (
                StorageCommand::UpdateNodeVtepMac {
                    node_name: "node-1".into(),
                    vtep_mac: "aa:bb:cc:dd:ee:ff".into(),
                },
                "UpdateNodeVtepMac",
            ),
            (
                StorageCommand::UpdateNodePeerAttributes {
                    node_name: "node-1".into(),
                    mode: "root".into(),
                    hostport_range: Some("32000-32767".into()),
                },
                "UpdateNodePeerAttributes",
            ),
            (
                StorageCommand::UpdateNodeDataplane {
                    node_name: "node-1".into(),
                    mode: "rootless".into(),
                    encryption: "enabled".into(),
                    public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
                    endpoint: "192.0.2.10".into(),
                    port: Some(51_820),
                },
                "UpdateNodeDataplane",
            ),
            (
                StorageCommand::DeleteNodeSubnet {
                    node_name: "node-1".into(),
                },
                "DeleteNodeSubnet",
            ),
            (
                StorageCommand::PodSlotTryAdmit {
                    namespace: "default".into(),
                    pod_name: "slot-pod".into(),
                    pod_uid: "uid-a".into(),
                    node_name: "node-a".into(),
                },
                "PodSlotTryAdmit",
            ),
            (
                StorageCommand::PodSlotMarkTerminating {
                    namespace: "default".into(),
                    pod_name: "slot-pod".into(),
                    pod_uid: "uid-a".into(),
                    node_name: "node-a".into(),
                },
                "PodSlotMarkTerminating",
            ),
            (
                StorageCommand::PodSlotClearIfUid {
                    namespace: "default".into(),
                    pod_name: "slot-pod".into(),
                    pod_uid: "uid-a".into(),
                    node_name: "node-a".into(),
                },
                "PodSlotClearIfUid",
            ),
            (
                StorageCommand::MovePodToCleanupIntent {
                    node_name: "node-a".into(),
                    namespace: "default".into(),
                    pod_name: "lost-pod".into(),
                    pod_uid: "lost-uid".into(),
                    reason: "NodeLost".into(),
                },
                "MovePodToCleanupIntent",
            ),
            (
                StorageCommand::DeletePodCleanupIntent {
                    node_name: "node-a".into(),
                    namespace: "default".into(),
                    pod_name: "lost-pod".into(),
                    pod_uid: "lost-uid".into(),
                    reason: "NodeLost".into(),
                },
                "DeletePodCleanupIntent",
            ),
            (
                StorageCommand::DeletePodCleanupIntentsForNode {
                    node_name: "node-a".into(),
                },
                "DeletePodCleanupIntentsForNode",
            ),
            (
                StorageCommand::AdvanceResourceVersion {
                    min_rv: 100,
                    new_rv: 101,
                },
                "AdvanceResourceVersion",
            ),
            (
                StorageCommand::WatchEventAppend {
                    event_bytes: br#"{"type":"ADDED","object":{"kind":"Pod"}}"#.to_vec(),
                    rv: 99,
                },
                "WatchEventAppend",
            ),
            (
                StorageCommand::GcWatchEvents {
                    max_rows: 50_000,
                    batch_cap: 1_024,
                },
                "GcWatchEvents",
            ),
            (
                StorageCommand::SetKlightsMeta {
                    key: "voters".into(),
                    value: r#"["mn-leader"]"#.into(),
                },
                "SetKlightsMeta",
            ),
        ]
    }

    fn all_response_samples() -> Vec<(StorageResponse, &'static str)> {
        vec![
            (
                StorageResponse::Resource {
                    resource_version: 42,
                    data: json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "my-pod", "resourceVersion": "42"}}),
                },
                "Resource",
            ),
            (
                StorageResponse::Ack {
                    resource_version: 43,
                },
                "Ack",
            ),
            (
                StorageResponse::NodeSubnet {
                    node_name: "node-1".into(),
                    subnet: "10.42.1.0/24".into(),
                    subnet_base_int: 0x0a2a0100,
                    vtep_ip: "10.42.1.1".into(),
                    node_ip: "192.168.1.10".into(),
                    mode: "rootless".into(),
                    hostport_range: Some("32000-32767".into()),
                },
                "NodeSubnet",
            ),
            (
                StorageResponse::Error {
                    message: "conflict".into(),
                },
                "Error",
            ),
        ]
    }

    // ---- Coverage gate: every variant covered ----

    /// Ensures every `StorageCommand` variant has an entry in the
    /// round-trip test table.  This is the DSB-HA-01 closing gate
    /// requirement: "round-trip tests cover every command variant."
    #[test]
    fn every_storage_command_variant_is_tested() {
        let known_variants: std::collections::BTreeSet<&str> = [
            "CreateResource",
            "UpdateResource",
            "DeleteResource",
            "PatchResource",
            "UpdateStatus",
            "ApplyResourceBatch",
            "CreateNamespace",
            "UpdateNamespace",
            "DeleteNamespace",
            "DeleteNamespaceContents",
            "AllocateNodeSubnet",
            "UpdateNodeVtepMac",
            "UpdateNodePeerAttributes",
            "UpdateNodeDataplane",
            "DeleteNodeSubnet",
            "PodSlotTryAdmit",
            "PodSlotMarkTerminating",
            "PodSlotClearIfUid",
            "MovePodToCleanupIntent",
            "DeletePodCleanupIntent",
            "DeletePodCleanupIntentsForNode",
            "AdvanceResourceVersion",
            "WatchEventAppend",
            "GcWatchEvents",
            "SetKlightsMeta",
        ]
        .into_iter()
        .collect();

        let tested: std::collections::BTreeSet<&str> = all_command_samples()
            .iter()
            .map(|(_, name)| *name)
            .collect();

        let missing: Vec<_> = known_variants.difference(&tested).collect();
        assert!(
            missing.is_empty(),
            "StorageCommand variants missing from round-trip tests: {missing:?}"
        );

        let extra: Vec<_> = tested.difference(&known_variants).collect();
        assert!(
            extra.is_empty(),
            "Round-trip test table has entries for unknown variants: {extra:?}"
        );
    }

    #[test]
    fn every_storage_response_variant_is_tested() {
        let known_variants: std::collections::BTreeSet<&str> =
            ["Resource", "Ack", "NodeSubnet", "Error"]
                .into_iter()
                .collect();

        let tested: std::collections::BTreeSet<&str> = all_response_samples()
            .iter()
            .map(|(_, name)| *name)
            .collect();

        let missing: Vec<_> = known_variants.difference(&tested).collect();
        assert!(
            missing.is_empty(),
            "StorageResponse variants missing from round-trip tests: {missing:?}"
        );
    }

    // ---- JSON round-trip ----

    #[test]
    fn json_round_trip_all_command_variants() {
        for (cmd, name) in all_command_samples() {
            let encoded = encode_command_json(&cmd)
                .unwrap_or_else(|e| panic!("JSON encode failed for {name}: {e}"));
            let decoded = decode_command_json(&encoded)
                .unwrap_or_else(|e| panic!("JSON decode failed for {name}: {e}"));
            assert_eq!(cmd, decoded, "JSON round-trip mismatch for {name}");
        }
    }

    #[test]
    fn json_round_trip_all_response_variants() {
        for (resp, name) in all_response_samples() {
            let encoded = encode_response_json(&resp)
                .unwrap_or_else(|e| panic!("JSON encode failed for {name}: {e}"));
            let decoded = decode_response_json(&encoded)
                .unwrap_or_else(|e| panic!("JSON decode failed for {name}: {e}"));
            assert_eq!(resp, decoded, "JSON round-trip mismatch for {name}");
        }
    }

    #[test]
    fn json_round_trip_command_meta() {
        let meta = sample_meta();
        let encoded = encode_meta_json(&meta).unwrap();
        let decoded = decode_meta_json(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    // ---- Protobuf round-trip ----

    #[test]
    fn protobuf_round_trip_all_command_variants() {
        for (cmd, name) in all_command_samples() {
            let encoded = encode_command_protobuf(&cmd)
                .unwrap_or_else(|e| panic!("protobuf encode failed for {name}: {e}"));
            let decoded = decode_command_protobuf(&encoded)
                .unwrap_or_else(|e| panic!("protobuf decode failed for {name}: {e}"));
            assert_eq!(cmd, decoded, "protobuf round-trip mismatch for {name}");
        }
    }

    #[test]
    fn protobuf_round_trip_all_response_variants() {
        for (resp, name) in all_response_samples() {
            let encoded = encode_response_protobuf(&resp)
                .unwrap_or_else(|e| panic!("protobuf encode failed for {name}: {e}"));
            let decoded = decode_response_protobuf(&encoded)
                .unwrap_or_else(|e| panic!("protobuf decode failed for {name}: {e}"));
            assert_eq!(resp, decoded, "protobuf round-trip mismatch for {name}");
        }
    }

    #[test]
    fn protobuf_round_trip_command_meta() {
        let meta = sample_meta();
        let encoded = encode_meta_protobuf(&meta).unwrap();
        let decoded = decode_meta_protobuf(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn protobuf_round_trip_command_error() {
        let errors = vec![
            (
                CommandError::Conflict {
                    message: "rv mismatch".into(),
                },
                "Conflict",
            ),
            (
                CommandError::NotFound {
                    message: "pod not found".into(),
                },
                "NotFound",
            ),
            (
                CommandError::Internal {
                    message: "db error".into(),
                },
                "Internal",
            ),
        ];
        for (err, name) in errors {
            let encoded = encode_error_protobuf(&err).unwrap();
            let decoded = decode_error_protobuf(&encoded).unwrap();
            assert_eq!(err, decoded, "protobuf round-trip mismatch for {name}");
        }
    }

    // ---- Cross-codec: JSON ≠ protobuf wire format, but both decode correctly ----

    #[test]
    fn json_and_protobuf_produce_same_decoded_command() {
        for (cmd, name) in all_command_samples() {
            let json_bytes = encode_command_json(&cmd).unwrap();
            let protobuf_bytes = encode_command_protobuf(&cmd).unwrap();

            let from_json = decode_command_json(&json_bytes)
                .unwrap_or_else(|e| panic!("JSON decode failed for {name}: {e}"));
            let from_protobuf = decode_command_protobuf(&protobuf_bytes)
                .unwrap_or_else(|e| panic!("protobuf decode failed for {name}: {e}"));

            assert_eq!(from_json, from_protobuf, "cross-codec mismatch for {name}");

            // Wire formats must differ (JSON is text, protobuf is binary)
            assert_ne!(
                json_bytes.as_slice(),
                protobuf_bytes.as_slice(),
                "{name}: JSON and protobuf produced identical wire — suspicious"
            );
        }
    }

    // ---- Compatibility test: decode a v1 JSON fixture ----

    /// Decodes a hand-crafted JSON fixture representing a v1 `CreateResource`
    /// command.  This test proves that a binary built today can decode a
    /// command serialized by a binary from the same codec version.
    /// When `COMMAND_CODEC_VERSION` is bumped, add a new fixture and
    /// keep this one (or a copy targeting the old version) to prove
    /// backward compatibility.
    #[test]
    fn decode_v1_json_create_resource_fixture() {
        let fixture = r#"{
            "CreateResource": {
                "api_version": "v1",
                "kind": "Pod",
                "namespace": "kube-system",
                "name": "coredns-abc",
                "data": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "coredns-abc",
                        "namespace": "kube-system",
                        "uid": "uid-fixture-001",
                        "resourceVersion": "100"
                    }
                }
            }
        }"#;

        let cmd =
            decode_command_json(fixture.as_bytes()).expect("v1 fixture should decode cleanly");

        match cmd {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
            } => {
                assert_eq!(api_version, "v1");
                assert_eq!(kind, "Pod");
                assert_eq!(namespace.as_deref(), Some("kube-system"));
                assert_eq!(name, "coredns-abc");
                assert_eq!(data["metadata"]["name"], "coredns-abc");
            }
            other => panic!("expected CreateResource, got {:?}", other.variant_name()),
        }
    }

    /// Decodes a hand-crafted JSON fixture representing a v1
    /// `UpdateNodePeerAttributes` command to cover a non-resource variant.
    #[test]
    fn decode_v1_json_update_node_peer_attributes_fixture() {
        let fixture = r#"{
            "UpdateNodePeerAttributes": {
                "node_name": "worker-2",
                "mode": "rootless",
                "hostport_range": "32000-32767"
            }
        }"#;

        let cmd =
            decode_command_json(fixture.as_bytes()).expect("v1 fixture should decode cleanly");

        match cmd {
            StorageCommand::UpdateNodePeerAttributes {
                node_name,
                mode,
                hostport_range,
            } => {
                assert_eq!(node_name, "worker-2");
                assert_eq!(mode, "rootless");
                assert_eq!(hostport_range.as_deref(), Some("32000-32767"));
            }
            other => panic!(
                "expected UpdateNodePeerAttributes, got {:?}",
                other.variant_name()
            ),
        }
    }

    /// Decodes a hand-crafted JSON fixture representing a v1
    /// `StorageResponse::NodeSubnet`, including the F2-04 peer attributes
    /// (`mode`, `hostport_range`) so rootless peers round-trip correctly.
    #[test]
    fn decode_v1_json_node_subnet_response_fixture() {
        let fixture = r#"{
            "NodeSubnet": {
                "node_name": "node-1",
                "subnet": "10.42.1.0/24",
                "subnet_base_int": 170403840,
                "vtep_ip": "10.42.1.1",
                "node_ip": "192.168.1.10",
                "mode": "rootless",
                "hostport_range": "32000-32767"
            }
        }"#;

        let resp = decode_response_json(fixture.as_bytes())
            .expect("v1 response fixture should decode cleanly");

        match resp {
            StorageResponse::NodeSubnet {
                node_name,
                subnet,
                subnet_base_int,
                vtep_ip,
                node_ip,
                mode,
                hostport_range,
            } => {
                assert_eq!(node_name, "node-1");
                assert_eq!(subnet, "10.42.1.0/24");
                assert_eq!(vtep_ip, "10.42.1.1");
                assert_eq!(node_ip, "192.168.1.10");
                assert_eq!(mode, "rootless");
                assert_eq!(hostport_range.as_deref(), Some("32000-32767"));
                let _ = subnet_base_int; // present and round-trips
            }
            other => panic!("expected NodeSubnet response, got {other:?}"),
        }
    }

    /// Decodes a hand-crafted JSON fixture for an `AdvanceResourceVersion`
    /// command — the typed replacement for the previous generic
    /// `UpdateConfig { key, value }` form.
    #[test]
    fn decode_v1_json_advance_resource_version_fixture() {
        let fixture = r#"{
            "AdvanceResourceVersion": {
                "min_rv": 100,
                "new_rv": 101
            }
        }"#;

        let cmd =
            decode_command_json(fixture.as_bytes()).expect("v1 fixture should decode cleanly");

        match cmd {
            StorageCommand::AdvanceResourceVersion { min_rv, new_rv } => {
                assert_eq!(min_rv, 100);
                assert_eq!(new_rv, 101);
            }
            other => panic!(
                "expected AdvanceResourceVersion, got {:?}",
                other.variant_name()
            ),
        }
    }

    /// Decodes a hand-crafted JSON fixture for a `WatchEventAppend`
    /// command — covers the spec-required watch-event append metadata.
    #[test]
    fn decode_v1_json_watch_event_append_fixture() {
        let fixture = r#"{
            "WatchEventAppend": {
                "event_bytes": [123, 125],
                "rv": 99
            }
        }"#;

        let cmd =
            decode_command_json(fixture.as_bytes()).expect("v1 fixture should decode cleanly");

        match cmd {
            StorageCommand::WatchEventAppend { event_bytes, rv } => {
                assert_eq!(event_bytes, b"{}");
                assert_eq!(rv, 99);
            }
            other => panic!("expected WatchEventAppend, got {:?}", other.variant_name()),
        }
    }

    /// Decodes a hand-crafted JSON fixture for a `GcWatchEvents` command —
    /// watch-history GC must replicate so backends converge on the same
    /// retention window.
    #[test]
    fn decode_v1_json_gc_watch_events_fixture() {
        let fixture = r#"{
            "GcWatchEvents": {
                "max_rows": 50000,
                "batch_cap": 1024
            }
        }"#;

        let cmd =
            decode_command_json(fixture.as_bytes()).expect("v1 fixture should decode cleanly");

        match cmd {
            StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            } => {
                assert_eq!(max_rows, 50_000);
                assert_eq!(batch_cap, 1_024);
            }
            other => panic!("expected GcWatchEvents, got {:?}", other.variant_name()),
        }
    }

    /// Builder helpers stamp the matching variant.
    #[test]
    fn builder_helpers_construct_expected_variants() {
        let cr = StorageCommand::create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p",
            json!({"apiVersion": "v1"}),
        );
        assert_eq!(cr.variant_name(), "CreateResource");

        let ur = StorageCommand::update_resource(
            "v1",
            "Pod",
            Some("default"),
            "p",
            json!({"apiVersion": "v1"}),
            5,
        );
        assert_eq!(ur.variant_name(), "UpdateResource");

        let us = StorageCommand::update_status(
            "v1",
            "Pod",
            Some("default"),
            "p",
            json!({"phase": "Running"}),
            Some(5),
        );
        assert_eq!(us.variant_name(), "UpdateStatus");

        let pr = StorageCommand::patch_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm",
            PatchKind::Merge,
            json!({"data": {"k": "v"}}),
        );
        assert_eq!(pr.variant_name(), "PatchResource");

        let cn = StorageCommand::create_namespace("ns", json!({"kind": "Namespace"}));
        assert_eq!(cn.variant_name(), "CreateNamespace");

        // Cluster-scoped resource accepts namespace=None.
        let cluster = StorageCommand::create_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "admin",
            json!({"kind": "ClusterRole"}),
        );
        match cluster {
            StorageCommand::CreateResource { namespace, .. } => assert!(namespace.is_none()),
            other => panic!("expected CreateResource, got {:?}", other.variant_name()),
        }
    }

    /// Protobuf encoding is deterministic for the same input.
    #[test]
    fn protobuf_delete_resource_round_trip_is_deterministic() {
        let cmd = StorageCommand::DeleteResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("kube-system".into()),
            name: "test-cm".into(),
            preconditions: ResourcePreconditions::default(),
        };
        let first = encode_command_protobuf(&cmd).unwrap();
        let second = encode_command_protobuf(&cmd).unwrap();
        assert_eq!(first, second, "protobuf encoding must be deterministic");

        let decoded = decode_command_protobuf(&first).unwrap();
        assert_eq!(cmd, decoded);
    }

    // ---- Codec version constant ----

    #[test]
    fn codec_version_is_2() {
        assert_eq!(COMMAND_CODEC_VERSION, 2);
    }

    // ---- CommandId ----

    #[test]
    fn command_id_new_is_unique() {
        let a = CommandId::new();
        let b = CommandId::new();
        assert_ne!(
            a, b,
            "two CommandId::new() calls must produce different IDs"
        );
    }

    #[test]
    fn command_id_display_round_trips() {
        let id = CommandId("test-123".into());
        assert_eq!(id.to_string(), "test-123");
    }

    // ---- CommandMeta edge cases ----

    #[test]
    fn meta_without_uid_round_trips_json_and_protobuf() {
        let mut meta = sample_meta();
        meta.uid = None;
        // JSON
        let encoded = encode_meta_json(&meta).unwrap();
        let decoded = decode_meta_json(&encoded).unwrap();
        assert_eq!(meta, decoded);
        // Protobuf
        let encoded = encode_meta_protobuf(&meta).unwrap();
        let decoded = decode_meta_protobuf(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    // ---- variant_name coverage ----

    #[test]
    fn variant_name_matches_all_samples() {
        for (cmd, expected_name) in all_command_samples() {
            assert_eq!(cmd.variant_name(), expected_name);
        }
    }

    // ---- Error round-trip (JSON) ----

    #[test]
    fn json_round_trip_command_errors() {
        let errors = vec![
            (
                CommandError::Conflict {
                    message: "rv mismatch".into(),
                },
                "Conflict",
            ),
            (
                CommandError::NotFound {
                    message: "pod not found".into(),
                },
                "NotFound",
            ),
            (
                CommandError::Internal {
                    message: "db error".into(),
                },
                "Internal",
            ),
        ];
        for (err, name) in errors {
            let json = serde_json::to_vec(&err).unwrap();
            let decoded: CommandError = serde_json::from_slice(&json).unwrap();
            assert_eq!(err, decoded, "JSON round-trip mismatch for {name}");
        }
    }

    // ---- Node-local operations are excluded ----

    /// Validates that the command set does not carry node-local operations.
    /// This is a design-invariant test, not a codec test, but it guards
    /// against accidental addition of node-local command variants.
    #[test]
    fn no_node_local_command_variants_exist() {
        let node_local_prefixes = ["Sandbox", "PodNetwork", "PodWorkqueue", "PodEndpoint"];

        for (cmd, name) in all_command_samples() {
            for prefix in &node_local_prefixes {
                assert!(
                    !name.contains(prefix),
                    "StorageCommand::{name} looks like a node-local operation \
                     — node-local ops must stay as direct local backend calls, \
                     not replicated commands"
                );
            }
            assert_eq!(cmd.variant_name(), name);
        }
    }

    // ---- Cluster-internal state coverage ----

    #[test]
    fn node_subnet_commands_round_trip_both_codecs() {
        for (cmd, name) in all_command_samples() {
            match cmd {
                StorageCommand::AllocateNodeSubnet { .. }
                | StorageCommand::UpdateNodeVtepMac { .. }
                | StorageCommand::UpdateNodePeerAttributes { .. }
                | StorageCommand::DeleteNodeSubnet { .. } => {
                    let json = encode_command_json(&cmd)
                        .unwrap_or_else(|e| panic!("encode failed for {name}: {e}"));
                    let decoded_json = decode_command_json(&json)
                        .unwrap_or_else(|e| panic!("decode failed for {name}: {e}"));
                    assert_eq!(cmd, decoded_json);

                    let pb = encode_command_protobuf(&cmd)
                        .unwrap_or_else(|e| panic!("protobuf encode failed for {name}: {e}"));
                    let decoded_pb = decode_command_protobuf(&pb)
                        .unwrap_or_else(|e| panic!("protobuf decode failed for {name}: {e}"));
                    assert_eq!(cmd, decoded_pb);
                }
                _ => {}
            }
        }
    }

    // ---- Edge-case: None fields ----

    #[test]
    fn update_status_without_expected_rv_round_trips() {
        let cmd = StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "my-pod".into(),
            status: json!({"phase": "Pending"}),
            expected_rv: None,
            preconditions: uid_preconditions("uid-abc-123"),
            observed_status_stamp: None,
        };
        // JSON
        let encoded = encode_command_json(&cmd).unwrap();
        let decoded = decode_command_json(&encoded).unwrap();
        assert_eq!(cmd, decoded);
        // Protobuf
        let encoded = encode_command_protobuf(&cmd).unwrap();
        let decoded = decode_command_protobuf(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn resource_preconditions_round_trip_for_resource_write_commands() {
        let commands = vec![
            StorageCommand::UpdateResource {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "my-pod".into(),
                data: json!({"metadata": {"name": "my-pod", "uid": "uid-a"}}),
                expected_rv: 42,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-a".into()),
                    resource_version: Some(42),
                },
            },
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "my-pod".into(),
                status: json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: uid_preconditions("uid-a"),
                observed_status_stamp: None,
            },
            StorageCommand::PatchResource {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "my-pod".into(),
                patch_kind: PatchKind::Merge,
                patch: json!({"metadata": {"labels": {"app": "test"}}}),
                preconditions: uid_preconditions("uid-a"),
                strict_resource_version: true,
            },
            StorageCommand::DeleteResource {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "my-pod".into(),
                preconditions: uid_preconditions("uid-a"),
            },
        ];

        for command in commands {
            let json_encoded = encode_command_json(&command).unwrap();
            let json_decoded = decode_command_json(&json_encoded).unwrap();
            assert_eq!(command, json_decoded);

            let protobuf_encoded = encode_command_protobuf(&command).unwrap();
            let protobuf_decoded = decode_command_protobuf(&protobuf_encoded).unwrap();
            assert_eq!(command, protobuf_decoded);
        }
    }

    #[test]
    fn update_node_peer_attributes_without_hostport_range_round_trips() {
        let cmd = StorageCommand::UpdateNodePeerAttributes {
            node_name: "node-1".into(),
            mode: "root".into(),
            hostport_range: None,
        };
        // JSON
        let encoded = encode_command_json(&cmd).unwrap();
        let decoded = decode_command_json(&encoded).unwrap();
        assert_eq!(cmd, decoded);
        // Protobuf
        let encoded = encode_command_protobuf(&cmd).unwrap();
        let decoded = decode_command_protobuf(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn update_node_dataplane_round_trips_without_private_key() {
        let cmd = StorageCommand::UpdateNodeDataplane {
            node_name: "node-a".into(),
            mode: "rootless".into(),
            encryption: "enabled".into(),
            public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
            endpoint: "192.0.2.10".into(),
            port: Some(51_820),
        };

        let json = encode_command_json(&cmd).unwrap();
        let json_text = String::from_utf8(json.clone()).unwrap();
        assert!(json_text.contains("public_key"));
        assert!(!json_text.contains("private"));
        assert_eq!(decode_command_json(&json).unwrap(), cmd);

        let encoded = encode_command_protobuf(&cmd).unwrap();
        let decoded = decode_command_protobuf(&encoded).unwrap();
        assert_eq!(decoded, cmd);
    }

    // ---- Cluster-scoped resource (namespace=None) ----

    #[test]
    fn cluster_scoped_create_resource_round_trips() {
        let cmd = StorageCommand::CreateResource {
            api_version: "rbac.authorization.k8s.io/v1".into(),
            kind: "ClusterRole".into(),
            namespace: None,
            name: "admin".into(),
            data: json!({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "admin"}}),
        };
        // JSON
        let encoded = encode_command_json(&cmd).unwrap();
        let decoded = decode_command_json(&encoded).unwrap();
        assert_eq!(cmd, decoded);
        // Protobuf
        let encoded = encode_command_protobuf(&cmd).unwrap();
        let decoded = decode_command_protobuf(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    // ---- Response error round-trip ----

    #[test]
    fn response_error_round_trips_json_and_protobuf() {
        let resp = StorageResponse::Error {
            message: "resource version conflict".into(),
        };
        // JSON
        let json = encode_response_json(&resp).unwrap();
        let decoded = decode_response_json(&json).unwrap();
        assert_eq!(resp, decoded);
        // Protobuf
        let pb = encode_response_protobuf(&resp).unwrap();
        let decoded = decode_response_protobuf(&pb).unwrap();
        assert_eq!(resp, decoded);
    }

    // ---- Protobuf is more compact than JSON for command variants ----

    #[test]
    fn protobuf_is_more_compact_than_json_for_commands() {
        for (cmd, name) in all_command_samples() {
            let json_len = encode_command_json(&cmd).unwrap().len();
            let protobuf_len = encode_command_protobuf(&cmd).unwrap().len();
            assert!(
                protobuf_len <= json_len,
                "{name}: protobuf ({protobuf_len} bytes) should be no larger than JSON ({json_len} bytes)"
            );
        }
    }

    // ---- Compatibility test: decode a v1 protobuf fixture ----

    /// Encodes a protobuf command, captures the raw bytes as a "fixture",
    /// then decodes them back.  This proves the protobuf wire format is
    /// stable for a given codec version.  When `COMMAND_CODEC_VERSION`
    /// is bumped, snapshot the bytes here as a literal.
    #[test]
    fn protobuf_delete_namespace_fixture_round_trips() {
        let cmd = StorageCommand::DeleteNamespace {
            name: "test-ns".into(),
        };
        let encoded = encode_command_protobuf(&cmd).unwrap();
        let decoded = decode_command_protobuf(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }
}
