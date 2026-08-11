use std::collections::BTreeSet;

use klights_cluster_core::{OutboxOperation, ResourcePreconditions, StorageCommand};

#[test]
fn member_join_gate_requires_exact_codec_v3() {
    assert!(klights_replication::join::validate_command_codec_v3_join(0).is_err());
    assert!(klights_replication::join::validate_command_codec_v3_join(2).is_err());
    assert!(klights_replication::join::validate_command_codec_v3_join(3).is_ok());
    assert!(klights_replication::join::validate_command_codec_v3_join(4).is_err());
}

#[test]
fn direct_node_resource_update_is_not_classified_as_node_status() {
    let command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "mn-controlplane1".to_string(),
        data: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "mn-controlplane1", "labels": {}},
            "spec": {},
            "status": {}
        }),
        expected_rv: 1,
        preconditions: ResourcePreconditions::resource_version(1),
        preserve_status: false,
    };

    assert_ne!(
        klights_replication::proposal::derive_operation_label(&command),
        OutboxOperation::NodeStatus,
        "direct API Node updates must not use the kubelet NodeStatus outbox operation"
    );
}

#[test]
fn local_commit_materialization_allows_solo_self_voter_before_leader_metric() {
    let voter_ids = BTreeSet::from([10]);
    assert!(
        klights_replication::proposal::local_commit_materialization_allowed(10, None, &voter_ids),
        "solo seed bootstrap may propose before current_leader is published"
    );
}

#[test]
fn local_commit_materialization_rejects_no_leader_multi_voter_reconfig_window() {
    let voter_ids = BTreeSet::from([10, 20]);
    assert!(
        !klights_replication::proposal::local_commit_materialization_allowed(10, None, &voter_ids),
        "no-leader local materialization carve-out must only apply to N=1 membership"
    );
}

#[test]
fn local_commit_materialization_rejects_no_leader_when_self_is_not_solo_voter() {
    let voter_ids = BTreeSet::from([20]);
    assert!(
        !klights_replication::proposal::local_commit_materialization_allowed(10, None, &voter_ids),
        "a node outside the solo voter set must not self-authorize local materialization"
    );
}

#[test]
fn local_commit_materialization_rejects_self_leader_metric_when_self_is_not_voter() {
    let voter_ids = BTreeSet::from([20]);
    assert!(
        !klights_replication::proposal::local_commit_materialization_allowed(
            10,
            Some(10),
            &voter_ids,
        ),
        "learner/replica must not materialize proposals even if metrics are inconsistent"
    );
}

#[test]
fn local_commit_materialization_rejects_known_other_leader() {
    let voter_ids = BTreeSet::from([10, 20]);
    assert!(
        !klights_replication::proposal::local_commit_materialization_allowed(
            10,
            Some(20),
            &voter_ids,
        ),
        "known non-self leader must reject local materialization"
    );
}

#[test]
fn outbox_priority_permit_classifier_is_explicit() {
    for (operation, expected) in [
        (OutboxOperation::NodeRegistration, true),
        (OutboxOperation::NodeDataplane, true),
        (OutboxOperation::NodeStatus, true),
        (OutboxOperation::PodStatus, false),
        (OutboxOperation::EventCreate, false),
    ] {
        assert_eq!(
            klights_replication::proposal::outbox_operation_uses_priority_permit(
                operation.as_str()
            ),
            expected,
            "{operation:?} priority classification mismatch"
        );
    }
}

#[test]
fn outbox_waiting_permit_classifier_is_explicit() {
    for (operation, expected) in [
        (OutboxOperation::PodStatus, true),
        (OutboxOperation::RuntimeReconcile, true),
        (OutboxOperation::ProbeReadiness, true),
        (OutboxOperation::DeadlineExceeded, true),
        (OutboxOperation::ContainerStatusSnapshot, true),
        (OutboxOperation::EphemeralContainerStatuses, true),
        (OutboxOperation::PodMetadata, true),
        (OutboxOperation::NodeStatus, false),
        (OutboxOperation::EventCreate, false),
    ] {
        assert_eq!(
            klights_replication::proposal::outbox_operation_waits_for_permit(operation.as_str()),
            expected,
            "{operation:?} waiting classification mismatch"
        );
    }
}

#[test]
fn raft_flow_control_cap_is_decoupled_from_payload_entries() {
    use klights_replication::node::RAFT_MAX_PAYLOAD_ENTRIES;
    use klights_replication::proposal::RAFT_MAX_INFLIGHT_PROPOSALS;

    assert!(RAFT_MAX_PAYLOAD_ENTRIES <= 16);
    assert!((8..=32).contains(&RAFT_MAX_INFLIGHT_PROPOSALS));
    assert_ne!(
        RAFT_MAX_INFLIGHT_PROPOSALS as u64, RAFT_MAX_PAYLOAD_ENTRIES,
        "in-flight proposal gate must be decoupled from payload entries"
    );
}
