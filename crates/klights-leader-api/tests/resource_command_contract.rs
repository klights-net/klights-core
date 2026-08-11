use std::sync::Arc;

use klights_cluster_core::{
    PatchKind, Resource, ResourcePreconditions, StorageCommand, StorageResponse,
};
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandError, ResourceCommandFuture, ResourceCommandRequest,
    ResourceCommandResult,
};
use serde_json::json;

fn config_map_command() -> StorageCommand {
    StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "settings".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "settings", "namespace": "default"},
            "data": {"mode": "strict"}
        }),
    }
}

#[test]
fn request_admits_exact_generic_resource_commands_without_rewriting_them() {
    let commands = [
        config_map_command(),
        StorageCommand::UpdateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: json!({"data": {"mode": "relaxed"}}),
            expected_rv: 41,
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-a", 41),
            preserve_status: false,
        },
        StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            patch_kind: PatchKind::Merge,
            patch: json!({"data": {"mode": "strict"}}),
            preconditions: ResourcePreconditions::uid("uid-a"),
            strict_resource_version: true,
        },
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "node-a".to_string(),
            status: json!({"phase": "Ready"}),
            expected_rv: Some(8),
            preconditions: ResourcePreconditions::default(),
            observed_status_stamp: None,
        },
        StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            preconditions: ResourcePreconditions::uid("uid-a"),
        },
        StorageCommand::DeleteResourceWithTombstone {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-a", 41),
            grace_seconds: 30,
        },
    ];

    for command in commands {
        let request = ResourceCommandRequest::try_new(command.clone()).expect("admitted command");
        assert_eq!(request.command(), &command);
        assert_eq!(request.into_command(), command);
    }
}

#[test]
fn request_admits_node_cleanup_intent_bulk_delete_without_rewriting_it() {
    let command = StorageCommand::DeletePodCleanupIntentsForNode {
        node_name: "e2e-fake-node".to_string(),
    };

    assert_eq!(
        ResourceCommandRequest::try_new(command.clone())
            .expect("node cleanup must route through the leader command port")
            .into_command(),
        command
    );
}

#[test]
fn request_rejects_pod_hard_delete_commands() {
    for command in [
        StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            preconditions: ResourcePreconditions::uid("pod-uid"),
        },
        StorageCommand::DeleteResourceWithTombstone {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            preconditions: ResourcePreconditions::uid("pod-uid"),
            grace_seconds: 0,
        },
    ] {
        assert!(matches!(
            ResourceCommandRequest::try_new(command),
            Err(ResourceCommandError::PodDeletionForbidden)
        ));
    }
}

#[test]
fn request_accepts_uid_and_rv_bound_pod_actor_compatibility_command() {
    let command = StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        preconditions: ResourcePreconditions {
            uid: Some("pod-uid".to_string()),
            resource_version: Some(17),
        },
    };
    assert_eq!(
        ResourceCommandRequest::try_new(command.clone())
            .expect("actor-qualified Pod delete request")
            .into_command(),
        command
    );
}

#[test]
fn request_validates_identity_and_tombstone_grace() {
    let missing_name = StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: String::new(),
        preconditions: ResourcePreconditions::default(),
    };
    assert!(matches!(
        ResourceCommandRequest::try_new(missing_name),
        Err(ResourceCommandError::InvalidRequest {
            field: "resource.name",
            ..
        })
    ));

    let negative_grace = StorageCommand::DeleteResourceWithTombstone {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "settings".to_string(),
        preconditions: ResourcePreconditions::default(),
        grace_seconds: -1,
    };
    assert!(matches!(
        ResourceCommandRequest::try_new(negative_grace),
        Err(ResourceCommandError::InvalidRequest {
            field: "delete.grace_seconds",
            ..
        })
    ));
}

#[test]
fn response_accepts_only_well_formed_resource_or_ack_results() {
    let data = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "settings",
            "namespace": "default",
            "uid": "uid-a",
            "resourceVersion": "42"
        }
    });
    let resource = Resource::try_from_data(Arc::new(data.clone())).expect("resource identity");
    assert_eq!(
        ResourceCommandResult::try_from_response(StorageResponse::Resource {
            resource_version: 42,
            data,
        })
        .expect("resource response"),
        ResourceCommandResult::Resource(resource)
    );
    assert_eq!(
        ResourceCommandResult::try_from_response(StorageResponse::Ack {
            resource_version: 43,
        })
        .expect("ack response"),
        ResourceCommandResult::Ack {
            resource_version: 43
        }
    );
    assert!(matches!(
        ResourceCommandResult::try_from_response(StorageResponse::NodeSubnet {
            node_name: "node-a".to_string(),
            subnet: "10.42.0.0/24".to_string(),
            subnet_base_int: 0,
            gateway_ip: "10.42.0.1".to_string(),
            node_ip: "192.0.2.10".to_string(),
            mode: "wireguard".to_string(),
            hostport_range: None,
        }),
        Err(ResourceCommandError::CorruptResponse { .. })
    ));
}

struct ObjectSafeCommandClient;

impl LeaderResourceCommand for ObjectSafeCommandClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        Box::pin(async move {
            let _ = request;
            Ok(ResourceCommandResult::Ack {
                resource_version: 1,
            })
        })
    }
}

#[test]
fn command_capability_is_object_safe() {
    let client: &dyn LeaderResourceCommand = &ObjectSafeCommandClient;
    let future = client.submit_resource_command(
        ResourceCommandRequest::try_new(config_map_command()).expect("valid request"),
    );
    drop(future);
}
