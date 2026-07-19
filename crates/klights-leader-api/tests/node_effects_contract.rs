use klights_cluster_core::{ResourcePreconditions, StorageCommand};
use klights_leader_api::{
    LeaderNodeLeaseRenewal, LeaderNodeLifecycleStatus, LeaderNodeSelfStatus, NodeLeaseRenewalError,
    NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeLifecycleStatusError, NodeLifecycleStatusFuture, NodeLifecycleStatusRequest,
    NodeLifecycleStatusResult, NodeSelfStatusError, NodeSelfStatusFuture, NodeSelfStatusRequest,
    NodeSelfStatusResult,
};
use serde_json::json;

fn self_status_command(name: &str, uid: &str) -> StorageCommand {
    StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: name.to_string(),
        status: json!({"conditions": [{"type": "Ready", "status": "True"}]}),
        expected_rv: None,
        preconditions: ResourcePreconditions::uid(uid),
        observed_status_stamp: None,
    }
}

fn lifecycle_status_command(name: &str, uid: &str, rv: i64) -> StorageCommand {
    StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: name.to_string(),
        status: json!({"conditions": [{"type": "Ready", "status": "Unknown"}]}),
        expected_rv: Some(rv),
        preconditions: ResourcePreconditions::uid_and_resource_version(uid, rv),
        observed_status_stamp: None,
    }
}

#[test]
fn lease_request_is_owned_and_rejects_nonpositive_duration() {
    let request = NodeLeaseRenewalRequest::try_new("worker-a", "2026-07-18T12:34:56.000000Z", 50)
        .expect("valid lease renewal");
    assert_eq!(request.node_name(), "worker-a");
    assert_eq!(request.renew_time(), "2026-07-18T12:34:56.000000Z");
    assert_eq!(request.lease_duration_seconds(), 50);

    for duration in [0, -1] {
        assert!(matches!(
            NodeLeaseRenewalRequest::try_new("worker-a", "2026-07-18T12:34:56Z", duration),
            Err(NodeLeaseRenewalError::InvalidRequest {
                field: "lease.lease_duration_seconds",
                ..
            })
        ));
    }
    assert!(matches!(
        NodeLeaseRenewalRequest::try_new("", "2026-07-18T12:34:56Z", 50),
        Err(NodeLeaseRenewalError::InvalidRequest {
            field: "lease.node_name",
            ..
        })
    ));
    assert!(matches!(
        NodeLeaseRenewalRequest::try_new("worker-a", "", 50),
        Err(NodeLeaseRenewalError::InvalidRequest {
            field: "lease.renew_time",
            ..
        })
    ));
}

#[test]
fn self_status_admits_only_exact_uid_node_status_without_rv_authority() {
    let command = self_status_command("worker-a", "uid-a");
    let request = NodeSelfStatusRequest::try_new(command.clone()).expect("valid self status");
    assert_eq!(request.node_name(), "worker-a");
    assert_eq!(request.node_uid(), "uid-a");
    assert_eq!(request.command(), &command);
    assert_eq!(request.into_command(), command);

    let invalid = [
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "worker-a".to_string(),
            status: json!({}),
            expected_rv: None,
            preconditions: ResourcePreconditions::uid("uid-a"),
            observed_status_stamp: None,
        },
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            status: json!({}),
            expected_rv: Some(7),
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-a", 7),
            observed_status_stamp: None,
        },
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            status: json!({}),
            expected_rv: None,
            preconditions: ResourcePreconditions::default(),
            observed_status_stamp: None,
        },
        StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: json!({"metadata": {"labels": {"smuggled": "true"}}}),
            preconditions: ResourcePreconditions::uid("uid-a"),
            strict_resource_version: false,
        },
    ];
    for command in invalid {
        assert!(matches!(
            NodeSelfStatusRequest::try_new(command),
            Err(NodeSelfStatusError::InvalidRequest { .. })
                | Err(NodeSelfStatusError::UnsupportedCommand { .. })
        ));
    }
}

#[test]
fn lifecycle_status_requires_matching_exact_uid_and_positive_rv_cas() {
    let command = lifecycle_status_command("worker-a", "uid-a", 41);
    let request =
        NodeLifecycleStatusRequest::try_new(command.clone()).expect("valid lifecycle status");
    assert_eq!(request.node_name(), "worker-a");
    assert_eq!(request.node_uid(), "uid-a");
    assert_eq!(request.resource_version(), 41);
    assert_eq!(request.command(), &command);
    assert_eq!(request.into_command(), command);

    for rv in [0, -1] {
        assert!(matches!(
            NodeLifecycleStatusRequest::try_new(lifecycle_status_command("worker-a", "uid-a", rv)),
            Err(NodeLifecycleStatusError::InvalidRequest {
                field: "status.resource_version",
                ..
            })
        ));
    }

    let mismatched_rv = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "worker-a".to_string(),
        status: json!({}),
        expected_rv: Some(41),
        preconditions: ResourcePreconditions::uid_and_resource_version("uid-a", 42),
        observed_status_stamp: None,
    };
    assert!(matches!(
        NodeLifecycleStatusRequest::try_new(mismatched_rv),
        Err(NodeLifecycleStatusError::InvalidRequest {
            field: "status.resource_version",
            ..
        })
    ));
}

struct ObjectSafeNodeEffects;

impl LeaderNodeLeaseRenewal for ObjectSafeNodeEffects {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        Box::pin(async move {
            let _ = request;
            Ok(NodeLeaseRenewalResult::Renewed)
        })
    }
}

impl LeaderNodeSelfStatus for ObjectSafeNodeEffects {
    fn submit_node_self_status(
        &self,
        request: NodeSelfStatusRequest,
    ) -> NodeSelfStatusFuture<'_, NodeSelfStatusResult> {
        Box::pin(async move {
            let _ = request;
            Ok(NodeSelfStatusResult::Enqueued)
        })
    }
}

impl LeaderNodeLifecycleStatus for ObjectSafeNodeEffects {
    fn submit_node_lifecycle_status(
        &self,
        request: NodeLifecycleStatusRequest,
    ) -> NodeLifecycleStatusFuture<'_, NodeLifecycleStatusResult> {
        Box::pin(async move {
            let _ = request;
            Ok(NodeLifecycleStatusResult::Updated {
                resource_version: 42,
            })
        })
    }
}

#[test]
fn node_effect_capabilities_are_independently_object_safe() {
    let lease: &dyn LeaderNodeLeaseRenewal = &ObjectSafeNodeEffects;
    drop(
        lease.renew_node_lease(
            NodeLeaseRenewalRequest::try_new("worker-a", "2026-07-18T12:34:56Z", 50)
                .expect("valid lease"),
        ),
    );

    let self_status: &dyn LeaderNodeSelfStatus = &ObjectSafeNodeEffects;
    drop(
        self_status.submit_node_self_status(
            NodeSelfStatusRequest::try_new(self_status_command("worker-a", "uid-a"))
                .expect("valid self status"),
        ),
    );

    let lifecycle: &dyn LeaderNodeLifecycleStatus = &ObjectSafeNodeEffects;
    drop(
        lifecycle.submit_node_lifecycle_status(
            NodeLifecycleStatusRequest::try_new(lifecycle_status_command("worker-a", "uid-a", 41))
                .expect("valid lifecycle status"),
        ),
    );
}
