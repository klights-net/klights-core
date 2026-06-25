//! Pure helpers used by the apply path.
//!
//! Nothing in this file does I/O. No `await`, no `dyn DatastoreBackend`,
//! no network. Each function takes plain values and returns plain values
//! so that unit tests in `#[cfg(test)] mod tests` can exercise them
//! without booting a datastore, supervisor, or gRPC stack.
//!
//! Keep this module as pure apply logic: zero `await`, zero
//! `DatastoreBackend` references, and no runtime side effects.
use crate::datastore::command::{
    COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand, StorageResponse,
};
use crate::datastore::types::{Resource, ResourceBatchOperation};
use crate::replication::protocol::{
    ForwardedNodeSubnet, ForwardedPodSlotAdmission, ForwardedResource, ReplicationEntry,
};

/// Outcome of applying a forwarded command on this replica.
///
/// Holds at most one of `entry`/`resource`/`node_subnet`/`pod_slot_admission`
/// populated for the variant that was applied; `already_applied = true`
/// signals an idempotent replay (the leader's outbox row was already
/// present), in which case all other fields are `None`.
#[derive(Debug)]
pub struct ForwardedApply {
    pub entry: Option<ReplicationEntry>,
    pub resource: Option<ForwardedResource>,
    pub node_subnet: Option<ForwardedNodeSubnet>,
    pub pod_slot_admission: Option<ForwardedPodSlotAdmission>,
    pub already_applied: bool,
}

impl ForwardedApply {
    pub(super) fn already_applied() -> Self {
        Self {
            entry: None,
            resource: None,
            node_subnet: None,
            pod_slot_admission: None,
            already_applied: true,
        }
    }
}

/// Pull the resource_version out of an applied result, looking through the
/// three carriers in priority order: replication entry meta, the resource
/// itself, then the pod-slot admission. Returns `None` only when the
/// command produced no observable RV (e.g. blocked admission).
pub(super) fn applied_resource_version(applied: &ForwardedApply) -> Option<i64> {
    applied
        .entry
        .as_ref()
        .map(|entry| entry.meta.resource_version)
        .or_else(|| {
            applied
                .resource
                .as_ref()
                .map(|resource| resource.resource_version)
        })
        .or_else(|| {
            applied
                .pod_slot_admission
                .as_ref()
                .map(|admission| admission.resource_version)
        })
}

/// Project a `ForwardedApply` into the wire-shaped `StorageResponse` the
/// leader expects to ack the request. `Resource` and `NodeSubnet` carry
/// rich payloads; everything else collapses to a bare `Ack { rv }`.
pub(super) fn storage_response_for_apply(
    applied: &ForwardedApply,
    applied_rv: Option<i64>,
) -> StorageResponse {
    if let Some(resource) = applied.resource.as_ref() {
        return StorageResponse::Resource {
            resource_version: resource.resource_version,
            data: resource.data.clone(),
        };
    }
    if let Some(node_subnet) = applied.node_subnet.as_ref() {
        return StorageResponse::NodeSubnet {
            node_name: node_subnet.node_name.clone(),
            subnet: node_subnet.subnet.clone(),
            subnet_base_int: node_subnet.subnet_base_int,
            vtep_ip: node_subnet.vtep_ip.clone(),
            node_ip: node_subnet.node_ip.clone(),
            mode: node_subnet.mode.clone(),
            hostport_range: node_subnet.hostport_range.clone(),
        };
    }
    StorageResponse::Ack {
        resource_version: applied_rv.unwrap_or(0),
    }
}

/// Subject key used for outbox dedup: a stable identifier for the
/// targeted resource (or a variant name for non-resource commands).
/// Resource keys include the UID when known so that recreate-after-delete
/// of the same name is treated as a different subject.
pub(super) fn subject_key_for_command(command: &StorageCommand) -> String {
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
        StorageCommand::ApplyResourceBatch { operations } => match operations.first() {
            Some(ResourceBatchOperation::Put {
                api_version,
                kind,
                namespace,
                name,
                ..
            })
            | Some(ResourceBatchOperation::Delete {
                api_version,
                kind,
                namespace,
                name,
                ..
            }) => format!(
                "batch:{api_version}/{kind}/{}/{}",
                namespace.as_deref().unwrap_or(""),
                name
            ),
            None => "batch:empty".to_string(),
        },
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

/// Build a replication entry whose meta is sourced from a freshly-mutated
/// `Resource` (post-create / post-update / post-status path).
pub(super) fn entry_for_resource(
    command: StorageCommand,
    resource: &Resource,
    authoring_node: String,
) -> ReplicationEntry {
    ReplicationEntry {
        command,
        meta: meta_for_rv(
            resource.resource_version,
            Some(resource.uid.clone()),
            authoring_node,
        ),
    }
}

/// Build an "applied, no resource" outcome for commands that mutate
/// state without producing a Resource (delete, node-subnet ops, pod-slot
/// admin, gc, etc.).
pub(super) fn ack_apply(
    command: StorageCommand,
    rv: i64,
    authoring_node: String,
) -> ForwardedApply {
    ForwardedApply {
        entry: Some(ReplicationEntry {
            command,
            meta: meta_for_rv(rv, None, authoring_node),
        }),
        resource: None,
        node_subnet: None,
        pod_slot_admission: None,
        already_applied: false,
    }
}

/// Construct `CommandMeta` for a known resource_version. Generates a
/// fresh idempotency key and timestamps it with the current epoch.
pub(super) fn meta_for_rv(
    resource_version: i64,
    uid: Option<String>,
    authoring_node: String,
) -> CommandMeta {
    CommandMeta {
        command_id: CommandId::new(),
        codec_version: COMMAND_CODEC_VERSION,
        resource_version,
        uid,
        timestamp_ms: current_epoch_millis(),
        authoring_node,
    }
}

/// Wall-clock millis since UNIX_EPOCH, clamped to `i64::MAX`. Returns 0
/// only on the (unreachable on Linux) `SystemTime` error path.
pub(super) fn current_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::command::{COMMAND_CODEC_VERSION, CommandId};
    use crate::datastore::types::ResourcePreconditions;
    use crate::replication::protocol::{
        ForwardedNodeSubnet, ForwardedPodSlotAdmission, ForwardedResource,
    };
    use serde_json::json;

    fn meta(rv: i64, uid: Option<&str>) -> CommandMeta {
        CommandMeta {
            command_id: CommandId("k".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: rv,
            uid: uid.map(str::to_string),
            timestamp_ms: 0,
            authoring_node: "n".to_string(),
        }
    }

    fn forwarded_resource(rv: i64) -> ForwardedResource {
        ForwardedResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "x".into(),
            resource_version: rv,
            data: json!({"metadata": {"name": "x"}}),
        }
    }

    fn forwarded_node_subnet() -> ForwardedNodeSubnet {
        ForwardedNodeSubnet {
            node_name: "n1".into(),
            subnet: "10.244.0.0/24".into(),
            subnet_base_int: 0,
            vtep_ip: "10.244.0.1".into(),
            vtep_mac: None,
            node_ip: "192.168.1.1".into(),
            mode: "Root".into(),
            hostport_range: None,
        }
    }

    fn forwarded_pod_slot_admitted(rv: i64) -> ForwardedPodSlotAdmission {
        ForwardedPodSlotAdmission {
            admitted: true,
            blocking_uid: None,
            blocking_node: None,
            state: None,
            resource_version: rv,
        }
    }

    // ---- subject_key_for_command ---------------------------------------

    #[test]
    fn subject_key_create_resource_namespaced_includes_uid() {
        let cmd = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            data: json!({"metadata": {"uid": "u-1"}}),
        };
        assert_eq!(
            subject_key_for_command(&cmd),
            "v1/ConfigMap/default/foo/u-1"
        );
    }

    #[test]
    fn subject_key_create_resource_cluster_omits_namespace() {
        let cmd = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: "node-1".into(),
            data: json!({"metadata": {"uid": "u-2"}}),
        };
        assert_eq!(subject_key_for_command(&cmd), "v1/Node/node-1/u-2");
    }

    #[test]
    fn subject_key_create_resource_no_uid_omits_uid_segment() {
        let cmd = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            data: json!({"metadata": {}}),
        };
        assert_eq!(subject_key_for_command(&cmd), "v1/ConfigMap/default/foo");
    }

    #[test]
    fn subject_key_update_resource_takes_uid_from_data() {
        let cmd = StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            data: json!({"metadata": {"uid": "u-3"}}),
            expected_rv: 0,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: None,
            },
        };
        assert_eq!(
            subject_key_for_command(&cmd),
            "v1/ConfigMap/default/foo/u-3"
        );
    }

    #[test]
    fn subject_key_update_status_takes_uid_from_preconditions() {
        let cmd = StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "p".into(),
            status: json!({}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("u-4".into()),
                resource_version: Some(7),
            },
            observed_status_stamp: None,
        };
        assert_eq!(subject_key_for_command(&cmd), "v1/Pod/default/p/u-4");
    }

    #[test]
    fn subject_key_delete_resource_uses_precondition_uid() {
        let cmd = StorageCommand::DeleteResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            preconditions: ResourcePreconditions {
                uid: Some("u-5".into()),
                resource_version: None,
            },
        };
        assert_eq!(
            subject_key_for_command(&cmd),
            "v1/ConfigMap/default/foo/u-5"
        );
    }

    #[test]
    fn subject_key_patch_resource_uses_precondition_uid() {
        let cmd = StorageCommand::PatchResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            patch_kind: crate::datastore::types::PatchKind::Merge,
            patch: json!({}),
            preconditions: ResourcePreconditions {
                uid: Some("u-6".into()),
                resource_version: None,
            },
            strict_resource_version: false,
        };
        assert_eq!(
            subject_key_for_command(&cmd),
            "v1/ConfigMap/default/foo/u-6"
        );
    }

    #[test]
    fn subject_key_create_namespace_uses_v1_namespace_path() {
        let cmd = StorageCommand::CreateNamespace {
            name: "foo".into(),
            data: json!({"metadata": {"uid": "u-7"}}),
        };
        assert_eq!(subject_key_for_command(&cmd), "v1/Namespace/foo/u-7");
    }

    #[test]
    fn subject_key_delete_namespace_omits_uid() {
        let cmd = StorageCommand::DeleteNamespace { name: "foo".into() };
        assert_eq!(subject_key_for_command(&cmd), "v1/Namespace/foo");
    }

    #[test]
    fn subject_key_delete_namespace_contents_omits_uid() {
        let cmd = StorageCommand::DeleteNamespaceContents { name: "foo".into() };
        assert_eq!(subject_key_for_command(&cmd), "v1/Namespace/foo");
    }

    #[test]
    fn subject_key_other_commands_use_variant_name() {
        let cmd = StorageCommand::AllocateNodeSubnet {
            node_name: "n1".into(),
            subnet: "10.244.0.0/24".into(),
            node_ip: "192.168.1.1".into(),
        };
        assert_eq!(subject_key_for_command(&cmd), cmd.variant_name());
    }

    // ---- applied_resource_version --------------------------------------

    #[test]
    fn applied_rv_prefers_entry_meta() {
        let applied = ForwardedApply {
            entry: Some(ReplicationEntry {
                command: StorageCommand::DeleteNamespace { name: "x".into() },
                meta: meta(42, None),
            }),
            resource: Some(forwarded_resource(99)),
            node_subnet: None,
            pod_slot_admission: None,
            already_applied: false,
        };
        assert_eq!(applied_resource_version(&applied), Some(42));
    }

    #[test]
    fn applied_rv_falls_back_to_resource() {
        let applied = ForwardedApply {
            entry: None,
            resource: Some(forwarded_resource(33)),
            node_subnet: None,
            pod_slot_admission: None,
            already_applied: false,
        };
        assert_eq!(applied_resource_version(&applied), Some(33));
    }

    #[test]
    fn applied_rv_falls_back_to_pod_slot_admission() {
        let applied = ForwardedApply {
            entry: None,
            resource: None,
            node_subnet: None,
            pod_slot_admission: Some(forwarded_pod_slot_admitted(7)),
            already_applied: false,
        };
        assert_eq!(applied_resource_version(&applied), Some(7));
    }

    #[test]
    fn applied_rv_returns_none_when_no_carrier() {
        let applied = ForwardedApply::already_applied();
        assert_eq!(applied_resource_version(&applied), None);
    }

    // ---- storage_response_for_apply ------------------------------------

    #[test]
    fn storage_response_resource_carries_data_and_rv() {
        let applied = ForwardedApply {
            entry: None,
            resource: Some(forwarded_resource(11)),
            node_subnet: None,
            pod_slot_admission: None,
            already_applied: false,
        };
        match storage_response_for_apply(&applied, Some(11)) {
            StorageResponse::Resource {
                resource_version,
                data,
            } => {
                assert_eq!(resource_version, 11);
                assert_eq!(data["metadata"]["name"], "x");
            }
            other => panic!("expected Resource, got {:?}", other),
        }
    }

    #[test]
    fn storage_response_node_subnet_passes_fields_through() {
        let applied = ForwardedApply {
            entry: None,
            resource: None,
            node_subnet: Some(forwarded_node_subnet()),
            pod_slot_admission: None,
            already_applied: false,
        };
        match storage_response_for_apply(&applied, Some(0)) {
            StorageResponse::NodeSubnet {
                node_name, subnet, ..
            } => {
                assert_eq!(node_name, "n1");
                assert_eq!(subnet, "10.244.0.0/24");
            }
            other => panic!("expected NodeSubnet, got {:?}", other),
        }
    }

    #[test]
    fn storage_response_falls_back_to_ack() {
        let applied = ForwardedApply {
            entry: Some(ReplicationEntry {
                command: StorageCommand::DeleteNamespace { name: "x".into() },
                meta: meta(5, None),
            }),
            resource: None,
            node_subnet: None,
            pod_slot_admission: None,
            already_applied: false,
        };
        match storage_response_for_apply(&applied, Some(5)) {
            StorageResponse::Ack { resource_version } => assert_eq!(resource_version, 5),
            other => panic!("expected Ack, got {:?}", other),
        }
    }

    #[test]
    fn storage_response_ack_uses_zero_when_rv_missing() {
        let applied = ForwardedApply::already_applied();
        match storage_response_for_apply(&applied, None) {
            StorageResponse::Ack { resource_version } => assert_eq!(resource_version, 0),
            other => panic!("expected Ack(0), got {:?}", other),
        }
    }

    // ---- ack_apply / meta_for_rv ---------------------------------------

    #[test]
    fn ack_apply_builds_entry_with_no_resource() {
        let cmd = StorageCommand::DeleteNamespace { name: "x".into() };
        let applied = ack_apply(cmd, 99, "leader-1".to_string());
        assert!(applied.resource.is_none());
        assert!(applied.node_subnet.is_none());
        assert!(applied.pod_slot_admission.is_none());
        assert!(!applied.already_applied);
        let entry = applied.entry.expect("entry present");
        assert_eq!(entry.meta.resource_version, 99);
        assert_eq!(entry.meta.authoring_node, "leader-1");
        assert!(entry.meta.uid.is_none());
    }

    #[test]
    fn meta_for_rv_uses_supplied_fields_and_codec_version() {
        let m = meta_for_rv(42, Some("u-1".into()), "node-a".into());
        assert_eq!(m.resource_version, 42);
        assert_eq!(m.uid.as_deref(), Some("u-1"));
        assert_eq!(m.authoring_node, "node-a");
        assert_eq!(m.codec_version, COMMAND_CODEC_VERSION);
        assert!(!m.command_id.0.is_empty(), "command_id is generated");
    }
}
