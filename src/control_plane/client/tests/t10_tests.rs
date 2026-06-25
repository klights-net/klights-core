use std::sync::Arc;

use crate::control_plane::client::apply::{apply_outbox_transactionally, gc_applied_outbox};
use crate::datastore::ResourcePreconditions;
use crate::datastore::command::StorageCommand;
use crate::kubelet::outbox::OutboxApplyResult;
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};

fn pod_status_payload(uid: &str) -> Vec<u8> {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    OutboxPayload::from_command(command)
        .encode_protobuf()
        .expect("encode payload")
}

fn pod_status_payload_with_rv(uid: &str, expected_rv: i64, status: serde_json::Value) -> Vec<u8> {
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status,
        expected_rv: Some(expected_rv),
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: Some(expected_rv),
        },
        observed_status_stamp: None,
    };
    OutboxPayload::from_command(command)
        .encode_protobuf()
        .expect("encode payload")
}

fn encode_outbox_command(command: StorageCommand) -> Vec<u8> {
    OutboxPayload::from_command(command)
        .encode_protobuf()
        .expect("encode payload")
}

#[tokio::test]
async fn outbox_apply_records_ledger_in_same_transaction_as_mutation() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let result = apply_outbox_transactionally(
        db.as_ref(),
        "txn-key-1",
        OutboxOperation::PodStatus,
        &pod_status_payload("uid-1"),
        "node-a",
    )
    .await
    .expect("apply outbox transactionally");

    assert!(matches!(result, OutboxApplyResult::Applied { .. }));

    let record = db
        .get_applied_outbox("txn-key-1")
        .await
        .expect("get ledger")
        .expect("ledger row exists");
    assert_eq!(record.idempotency_key, "txn-key-1");

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod")
        .expect("pod exists");
    assert_eq!(
        pod.data.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
}

#[tokio::test]
async fn pod_status_outbox_applies_stale_rv_snapshot_to_same_uid_live_pod() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {
                    "phase": "Pending",
                    "podIP": "10.50.1.9",
                    "podIPs": [{"ip": "10.50.1.9"}]
                }
            }),
        )
        .await
        .expect("create pod");

    let mut leader_changed_pod = (*created.data).clone();
    leader_changed_pod["metadata"]["annotations"] =
        serde_json::json!({"leader.example/kept": "true"});
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "web",
        leader_changed_pod,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("leader advances pod RV");

    let result = apply_outbox_transactionally(
        db.as_ref(),
        "stale-pod-status-rv-key",
        OutboxOperation::PodStatus,
        &pod_status_payload_with_rv(
            "uid-1",
            created.resource_version,
            serde_json::json!({
                "phase": "Running",
                "podIP": "10.50.1.9",
                "podIPs": [{"ip": "10.50.1.9"}],
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-06-14T09:05:17Z"}}
                }],
                "conditions": [
                    {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-06-14T09:05:17Z"},
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-14T09:05:17Z"}
                ]
            }),
        ),
        "worker-a",
    )
    .await
    .expect("stale-RV PodStatus should apply against same-UID live Pod");

    assert!(matches!(result, OutboxApplyResult::Applied { .. }));

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod")
        .expect("pod exists");
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
    assert_eq!(
        stored
            .data
            .pointer("/metadata/annotations/leader.example~1kept")
            .and_then(|v| v.as_str()),
        Some("true"),
        "status apply must not roll back leader-owned metadata/spec"
    );
}

#[tokio::test]
async fn pod_status_outbox_stale_rv_still_rejects_same_name_different_uid() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "new-uid"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create replacement pod");

    let err = apply_outbox_transactionally(
        db.as_ref(),
        "stale-pod-status-wrong-uid-key",
        OutboxOperation::PodStatus,
        &pod_status_payload_with_rv(
            "old-uid",
            created.resource_version.saturating_sub(1).max(1),
            serde_json::json!({"phase": "Running"}),
        ),
        "worker-a",
    )
    .await
    .expect_err("stale status for a different UID must be rejected");

    assert!(
        matches!(
            err,
            crate::kubelet::outbox::OutboxApplyError::UidMismatch { .. }
        ),
        "same-name replacement must remain protected by UID precondition, got: {err:?}"
    );
}

#[tokio::test]
async fn transactional_worker_lease_renew_does_not_touch_cluster_db() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let created = db
        .create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "local-worker",
            serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": "local-worker",
                    "namespace": "kube-node-lease",
                    "uid": "lease-uid-1"
                },
                "spec": {
                    "holderIdentity": "local-worker",
                    "leaseDurationSeconds": 50,
                    "renewTime": "2026-05-22T19:26:19.000000Z"
                }
            }),
        )
        .await
        .expect("create lease");
    let mut leader_changed_lease = (*created.data).clone();
    leader_changed_lease["spec"]["renewTime"] = serde_json::json!("2026-05-22T19:26:29.000000Z");
    db.update_resource_with_preconditions(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "local-worker",
        leader_changed_lease,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("leader updates lease");

    let mut stale_worker_lease = (*created.data).clone();
    stale_worker_lease["spec"]["renewTime"] = serde_json::json!("2026-05-22T19:27:35.000000Z");
    let payload = encode_outbox_command(StorageCommand::UpdateResource {
        api_version: "coordination.k8s.io/v1".to_string(),
        kind: "Lease".to_string(),
        namespace: Some("kube-node-lease".to_string()),
        name: "local-worker".to_string(),
        data: stale_worker_lease,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
    });

    let result = apply_outbox_transactionally(
        db.as_ref(),
        "lease-rv-stale-key",
        OutboxOperation::LeaseRenew,
        &payload,
        "local-worker",
    )
    .await
    .expect("legacy LeaseRenew outbox should be accepted as a cluster-db no-op");
    assert!(matches!(
        result,
        OutboxApplyResult::Applied { applied_rv: 0 }
    ));

    let stored = db
        .get_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "local-worker",
        )
        .await
        .expect("get lease")
        .expect("lease exists");
    assert_eq!(
        stored
            .data
            .pointer("/spec/renewTime")
            .and_then(|v| v.as_str()),
        Some("2026-05-22T19:26:29.000000Z")
    );
    assert!(
        db.get_applied_outbox("lease-rv-stale-key")
            .await
            .expect("get applied_outbox")
            .is_none(),
        "LeaseRenew must not create applied_outbox rows"
    );
}

#[tokio::test]
async fn transactional_worker_node_status_ignores_stale_rv_and_updates_commit() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "local-worker",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "local-worker",
                    "uid": "node-uid-1",
                    "annotations": {
                        "klights.io/git-commit": "380f96e1"
                    }
                },
                "spec": {
                    "podCIDR": "10.43.1.0/24",
                    "unschedulable": false
                },
                "status": {
                    "conditions": [
                        {
                            "type": "Ready",
                            "status": "False",
                            "reason": "NetworkUnavailable",
                            "lastTransitionTime": "2026-06-19T07:44:56Z"
                        },
                        {
                            "type": "NetworkUnavailable",
                            "status": "True",
                            "reason": "DataplaneNotReady",
                            "lastTransitionTime": "2026-06-19T07:44:56Z"
                        }
                    ]
                }
            }),
        )
        .await
        .expect("create node");
    let mut leader_changed_node = (*created.data).clone();
    leader_changed_node["spec"]["unschedulable"] = serde_json::json!(true);
    db.update_resource_with_preconditions(
        "v1",
        "Node",
        None,
        "local-worker",
        leader_changed_node,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("leader updates node");

    let worker_status = serde_json::json!({
        "conditions": [
            {
                "type": "Ready",
                "status": "True",
                "reason": "KubeletReady",
                "lastTransitionTime": "2026-06-19T07:44:57Z"
            },
            {
                "type": "NetworkUnavailable",
                "status": "False",
                "reason": "RouteCreated",
                "lastTransitionTime": "2026-06-19T07:44:57Z"
            }
        ]
    });
    let payload = encode_outbox_command(StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "local-worker".to_string(),
        status: worker_status,
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(created.uid.clone()),
            resource_version: None,
        },
        observed_status_stamp: None,
    });

    let result = apply_outbox_transactionally(
        db.as_ref(),
        "node-rv-stale-key",
        OutboxOperation::NodeStatus,
        &payload,
        "local-worker",
    )
    .await
    .expect("stale-RV NodeStatus should apply against the current Node");
    assert!(matches!(result, OutboxApplyResult::Applied { .. }));

    let stored = db
        .get_resource("v1", "Node", None, "local-worker")
        .await
        .expect("get node")
        .expect("node exists");
    assert_eq!(
        stored
            .data
            .pointer("/metadata/annotations/klights.io~1git-commit")
            .and_then(|v| v.as_str()),
        Some("380f96e1"),
        "status-only NodeStatus must not mutate metadata"
    );
    assert_eq!(
        stored.data.pointer("/spec/unschedulable"),
        Some(&serde_json::json!(true)),
        "leader-owned spec fields must not be rolled back by stale worker NodeStatus"
    );
    assert_eq!(
        stored
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find_map(|condition| {
                    (condition.get("type").and_then(|v| v.as_str()) == Some("Ready"))
                        .then(|| condition.get("status").and_then(|v| v.as_str()))
                        .flatten()
                })
            }),
        Some("True"),
        "worker NodeStatus must update status conditions through raft apply"
    );
    assert_eq!(
        stored
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find_map(|condition| {
                    (condition.get("type").and_then(|v| v.as_str()) == Some("NetworkUnavailable"))
                        .then(|| condition.get("status").and_then(|v| v.as_str()))
                        .flatten()
                })
            }),
        Some("False"),
        "worker NodeStatus must update the paired NetworkUnavailable condition"
    );
}

#[tokio::test]
async fn transactional_worker_node_status_preserves_newer_leader_unknown_condition() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "local-worker",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "local-worker",
                    "uid": "node-uid-1"
                },
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "lastTransitionTime": "2026-06-18T10:00:00Z"
                    }]
                }
            }),
        )
        .await
        .expect("create node");

    db.update_status_only_with_preconditions(
        "v1",
        "Node",
        None,
        "local-worker",
        serde_json::json!({
            "conditions": [{
                "type": "Ready",
                "status": "Unknown",
                "reason": "NodeStatusUnknown",
                "lastTransitionTime": "2026-06-18T11:00:00Z"
            }]
        }),
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("leader marks node unknown");

    let payload = encode_outbox_command(StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "local-worker".to_string(),
        status: serde_json::json!({
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "KubeletReady",
                "lastTransitionTime": "2026-06-18T10:00:00Z"
            }]
        }),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(created.uid.clone()),
            resource_version: None,
        },
        observed_status_stamp: None,
    });

    let result = apply_outbox_transactionally(
        db.as_ref(),
        "node-status-stale-condition-key",
        OutboxOperation::NodeStatus,
        &payload,
        "local-worker",
    )
    .await
    .expect("stale-RV NodeStatus should apply without clobbering fresher conditions");
    assert!(matches!(result, OutboxApplyResult::Applied { .. }));

    let stored = db
        .get_resource("v1", "Node", None, "local-worker")
        .await
        .expect("get node")
        .expect("node exists");
    assert_eq!(
        stored
            .data
            .pointer("/status/conditions/0/status")
            .and_then(|v| v.as_str()),
        Some("Unknown"),
        "a stale worker Ready=True snapshot must not overwrite a fresher leader Unknown"
    );
}

#[tokio::test]
async fn outbox_apply_rolls_back_mutation_when_ledger_insert_fails() {
    // Test that when an outbox apply fails at the mutation phase (after placeholder
    // insert), the placeholder is cleaned up so a retry can succeed. We test this
    // by calling the DatastoreBackend method directly, bypassing the outer UID
    // mismatch check, with a payload for a non-existent pod.
    let db = Arc::new(crate::datastore::test_support::in_memory().await);

    // Call db.apply_outbox_transactionally directly (bypasses apply.rs which
    // catches NotFound before placeholder). The pod doesn't exist, so
    // apply_forwarded_command will fail → placeholder should be rolled back.
    let err = db
        .apply_outbox_transactionally(
            "rollback-key",
            "PodStatus",
            &pod_status_payload("uid-rb"),
            "node-a",
        )
        .await
        .expect_err("apply should fail for non-existent pod");

    assert!(
        matches!(err, crate::kubelet::outbox::OutboxApplyError::Retryable(_)),
        "error should be retryable (placeholder rolled back), got: {err:?}"
    );

    // Verify no applied_outbox row remains (placeholder was rolled back).
    let record = db
        .get_applied_outbox("rollback-key")
        .await
        .expect("get ledger");
    assert!(
        record.is_none(),
        "placeholder should have been rolled back after mutation failure"
    );

    // Create the pod now and retry — must succeed.
    // Note: pod_status_payload hardcodes name "web".
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-rb"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod for retry");

    let result = db
        .apply_outbox_transactionally(
            "rollback-key",
            "PodStatus",
            &pod_status_payload("uid-rb"),
            "node-a",
        )
        .await
        .expect("retry after rollback should succeed");

    assert!(
        matches!(result, OutboxApplyResult::Applied { .. }),
        "retry should apply successfully, got: {result:?}"
    );
}

#[tokio::test]
async fn outbox_apply_recovers_from_stale_placeholder() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-stale"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "stale-placeholder-key".to_string(),
        subject_key: String::new(),
        operation: "PodStatus".to_string(),
        first_seen_ms: now_ms - 120_000,
        applied_rv: None,
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert stale placeholder");

    let result = db
        .apply_outbox_transactionally(
            "stale-placeholder-key",
            "PodStatus",
            &pod_status_payload("uid-stale"),
            "node-a",
        )
        .await
        .expect("apply should recover stale placeholder");

    assert!(matches!(result, OutboxApplyResult::Applied { .. }));

    let record = db
        .get_applied_outbox("stale-placeholder-key")
        .await
        .expect("get outbox record")
        .expect("outbox record exists");
    assert!(
        !record.subject_key.is_empty(),
        "placeholder should be replaced"
    );
    assert!(record.applied_rv.is_some(), "applied RV must be captured");
}

#[tokio::test]
async fn outbox_apply_treats_fresh_placeholder_as_retryable_inflight() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-fresh-placeholder"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "fresh-placeholder-key".to_string(),
        subject_key: String::new(),
        operation: "PodStatus".to_string(),
        first_seen_ms: now_ms,
        applied_rv: None,
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert fresh placeholder");

    let err = db
        .apply_outbox_transactionally(
            "fresh-placeholder-key",
            "PodStatus",
            &pod_status_payload("uid-fresh-placeholder"),
            "node-a",
        )
        .await
        .expect_err("fresh placeholder is still in-flight and must retry");

    assert!(
        matches!(err, crate::kubelet::outbox::OutboxApplyError::Retryable(_)),
        "fresh placeholder must be retryable, got: {err:?}"
    );
}

#[tokio::test]
async fn duplicate_outbox_apply_mutates_resource_once() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let r1 = apply_outbox_transactionally(
        db.as_ref(),
        "once-key",
        OutboxOperation::PodStatus,
        &pod_status_payload("uid-1"),
        "node-a",
    )
    .await;

    let r2 = apply_outbox_transactionally(
        db.as_ref(),
        "once-key",
        OutboxOperation::PodStatus,
        &pod_status_payload("uid-1"),
        "node-a",
    )
    .await;

    let results = [r1.expect("r1"), r2.expect("r2")];
    let applied_count = results
        .iter()
        .filter(|r| matches!(r, OutboxApplyResult::Applied { .. }))
        .count();
    let already_count = results
        .iter()
        .filter(|r| matches!(r, OutboxApplyResult::AlreadyApplied { .. }))
        .count();

    assert_eq!(applied_count + already_count, 2);
    assert_eq!(applied_count, 1, "only one should be a fresh apply");
    assert_eq!(already_count, 1, "one should be already-applied");
}

#[tokio::test]
async fn applied_outbox_gc_prunes_ttl_expired() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-1"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let old_ms = now_ms - 13 * 60 * 60 * 1000;

    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "old-key".to_string(),
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: old_ms,
        applied_rv: Some(1),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert old record");

    let recent_ms = now_ms - 3_600_000;
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "recent-key".to_string(),
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: recent_ms,
        applied_rv: Some(2),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert recent record");

    let pruned = gc_applied_outbox(db.as_ref(), now_ms, 12 * 60 * 60 * 1000)
        .await
        .expect("gc");

    assert_eq!(pruned, 1, "should prune exactly one old entry");

    let old = db.get_applied_outbox("old-key").await.expect("get old");
    assert!(old.is_none(), "record older than 12h should be pruned");

    let recent = db
        .get_applied_outbox("recent-key")
        .await
        .expect("get recent");
    assert!(recent.is_some(), "record inside 12h should remain");
}

#[tokio::test]
async fn applied_outbox_gc_does_not_touch_recent() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for i in 0..10 {
        db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
            idempotency_key: format!("recent-{}", i),
            subject_key: format!("v1/Pod/default/web-{}/uid-{}", i, i),
            operation: "PodStatus".to_string(),
            first_seen_ms: now_ms - 11 * 60 * 60 * 1000,
            applied_rv: Some(i),
            result_proto: vec![],
            status_stamp: None,
        })
        .await
        .expect("insert");
    }

    let pruned = gc_applied_outbox(db.as_ref(), now_ms, 12 * 60 * 60 * 1000)
        .await
        .expect("gc");

    assert_eq!(pruned, 0, "no records should be pruned within TTL");

    for i in 0..10 {
        assert!(
            db.get_applied_outbox(&format!("recent-{}", i))
                .await
                .expect("get")
                .is_some(),
            "recent record {} should remain",
            i
        );
    }
}

#[tokio::test]
async fn cleanup_uncommitted_outbox_claim_deletes_only_placeholder_rows() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "placeholder-key".to_string(),
        subject_key: String::new(),
        operation: "PodStatus".to_string(),
        first_seen_ms: 1,
        applied_rv: None,
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .expect("insert placeholder");
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "final-key".to_string(),
        subject_key: "v1/Pod/default/web/uid-1".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: 1,
        applied_rv: Some(42),
        result_proto: vec![1, 2, 3],
        status_stamp: None,
    })
    .await
    .expect("insert final row");

    assert!(
        db.delete_uncommitted_applied_outbox_placeholder("placeholder-key", 0)
            .await
            .expect("delete placeholder"),
        "placeholder row should be removed"
    );
    assert!(
        !db.delete_uncommitted_applied_outbox_placeholder("final-key", 42)
            .await
            .expect("skip final row"),
        "final applied row must not be removed by placeholder cleanup"
    );

    assert!(
        db.get_applied_outbox("placeholder-key")
            .await
            .expect("get placeholder")
            .is_none(),
        "placeholder should be gone"
    );
    assert!(
        db.get_applied_outbox("final-key")
            .await
            .expect("get final")
            .is_some(),
        "final applied row should remain"
    );
}

#[tokio::test]
async fn applied_outbox_gc_prunes_event_create_and_unknown_operations() {
    let db = Arc::new(crate::datastore::test_support::in_memory().await);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let old_ms = now_ms - 13 * 60 * 60 * 1000;

    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "event-key".to_string(),
        subject_key: "events.k8s.io/v1/Event/default/web.1/uid-event".to_string(),
        operation: "EventCreate".to_string(),
        first_seen_ms: old_ms,
        applied_rv: Some(1),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert event record");
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "future-key".to_string(),
        subject_key: "example.io/v1/Future/default/name/uid-future".to_string(),
        operation: "FutureOperation".to_string(),
        first_seen_ms: old_ms,
        applied_rv: Some(2),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert future record");

    let pruned = gc_applied_outbox(db.as_ref(), now_ms, 12 * 60 * 60 * 1000)
        .await
        .expect("gc");

    assert_eq!(
        pruned, 2,
        "GC should prune every expired operation without an allowlist"
    );

    assert!(
        db.get_applied_outbox("event-key")
            .await
            .expect("get")
            .is_none(),
        "expired EventCreate record should be pruned"
    );
    assert!(
        db.get_applied_outbox("future-key")
            .await
            .expect("get")
            .is_none(),
        "expired unknown operation should be pruned"
    );
}

#[tokio::test]
async fn idempotency_survives_gc_replay() {
    // After GC prunes an applied_outbox row, replaying the same outbox
    // must be harmless and produce a consistent result.
    let db = Arc::new(crate::datastore::test_support::in_memory().await);

    // Create the pod first so the outer UID check passes.
    // Note: pod_status_payload hardcodes name "web".
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "web",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "uid-gc"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .expect("create pod");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Insert an applied_outbox record with an OLD timestamp directly.
    let old_ms = now_ms - 100 * 86_400_000i64; // 100 days ago
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "gc-replay-key".to_string(),
        subject_key: "v1/Pod/default/web/uid-gc".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: old_ms,
        applied_rv: Some(1),
        result_proto: vec![],
        status_stamp: None,
    })
    .await
    .expect("insert old ledger record");

    // GC should prune the old entry.
    let pruned = gc_applied_outbox(db.as_ref(), now_ms, 12 * 60 * 60 * 1000)
        .await
        .expect("gc");
    assert_eq!(pruned, 1, "old entry should be pruned");
    assert!(
        db.get_applied_outbox("gc-replay-key")
            .await
            .expect("get")
            .is_none()
    );

    // Replay: since the ledger is gone, a status re-application is harmless.
    // The new apply should succeed as a fresh apply.
    let r2 = apply_outbox_transactionally(
        db.as_ref(),
        "gc-replay-key",
        OutboxOperation::PodStatus,
        &pod_status_payload("uid-gc"),
        "node-a",
    )
    .await
    .expect("replay after gc");
    assert!(
        matches!(r2, OutboxApplyResult::Applied { .. }),
        "replay after GC should succeed as fresh apply, got: {r2:?}"
    );

    // The pod status should be Running (re-applying the same status is idempotent).
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .expect("get pod")
        .expect("pod exists");
    assert_eq!(
        pod.data.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
}
