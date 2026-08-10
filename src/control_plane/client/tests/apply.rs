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

    let pod_delete = StorageCommand::FinalizeBoundPod {
        namespace: "default".to_string(),
        name: "web".to_string(),
        pod_uid: "pod-uid".to_string(),
        node_name: "worker-a".to_string(),
        observed_resource_version: 7,
    };
    authorize_outbox_command(
        OutboxDeliveryOperation::PodMetadata,
        &pod_delete,
        "worker-a",
    )
    .expect("opaque actor-originated bound Pod finalization remains deliverable");

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
async fn pod_metadata_finalization_authorization_is_structural_and_node_bound() {
    let (_datastore, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
    let finalize = |namespace: &str, name: &str, uid: &str, node_name: &str| {
        StorageCommand::FinalizeBoundPod {
            namespace: namespace.to_string(),
            name: name.to_string(),
            pod_uid: uid.to_string(),
            node_name: node_name.to_string(),
            observed_resource_version: 7,
        }
    };

    authorize_live_pod_metadata_command(
        db.as_ref(),
        &finalize("default", "ready", "uid-ready", "worker-a"),
        "worker-a",
    )
    .await
    .expect("structurally valid actor finalization must remain opaque at authorization");

    for (command, reason) in [
        (
            finalize("", "ready", "uid-ready", "worker-a"),
            "empty namespace",
        ),
        (
            finalize("default", "", "uid-ready", "worker-a"),
            "empty name",
        ),
        (finalize("default", "ready", "", "worker-a"), "empty UID"),
        (
            finalize("default", "ready", "uid-ready", "worker-b"),
            "different actor node",
        ),
    ] {
        let error = authorize_live_pod_metadata_command(db.as_ref(), &command, "worker-a")
            .await
            .expect_err(reason);
        assert!(
            error.to_string().contains("invalid actor observation"),
            "{reason} returned unexpected error: {error}",
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

#[test]
fn finalize_bound_pod_subject_is_uid_scoped() {
    let command = StorageCommand::FinalizeBoundPod {
        namespace: "team-a".to_string(),
        name: "web-0".to_string(),
        pod_uid: "uid-web-0".to_string(),
        node_name: "worker-a".to_string(),
        observed_resource_version: 41,
    };

    assert_eq!(
        subject_key_for_command(&command),
        "v1/Pod/team-a/web-0/uid-web-0"
    );
}

mod t10_tests {
    use std::sync::Arc;

    use crate::bootstrap::composition_tests::support::OutboxPayload;
    use crate::control_plane::client::apply::{apply_outbox_transactionally, gc_applied_outbox};
    use crate::datastore::ResourcePreconditions;
    use klights_cluster_core::OutboxApplyOutcome as OutboxApplyResult;
    use klights_cluster_core::command::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;

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

    fn pod_status_payload_with_rv(
        uid: &str,
        expected_rv: i64,
        status: serde_json::Value,
    ) -> Vec<u8> {
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
            observed_status_stamp: Some(1),
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
                klights_cluster_core::OutboxApplyError::UidMismatch { .. }
            ),
            "same-name replacement must remain protected by UID precondition, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn transactional_worker_lease_renew_does_not_touch_cluster_db() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        leader_changed_lease["spec"]["renewTime"] =
            serde_json::json!("2026-05-22T19:26:29.000000Z");
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
                        (condition.get("type").and_then(|v| v.as_str())
                            == Some("NetworkUnavailable"))
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        // The mutation and ledger are one transaction. A mutation failure must leave
        // neither durable state behind, so the same delivery can be retried.
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );

        // Call db.apply_outbox_transactionally directly (bypasses apply.rs which
        // catches NotFound first). The pod does not exist, so the atomic apply fails.
        let err = db
            .apply_outbox_transactionally(
                "rollback-key",
                "PodStatus",
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&pod_status_payload(
                    "uid-rb",
                )),
                "node-a",
            )
            .await
            .expect_err("apply should fail for non-existent pod");

        assert!(
            matches!(err, klights_cluster_core::OutboxApplyError::Retryable(_)),
            "error should be retryable after atomic rollback, got: {err:?}"
        );

        // Verify the failed transaction left no ledger row.
        let record = db
            .get_applied_outbox("rollback-key")
            .await
            .expect("get ledger");
        assert!(
            record.is_none(),
            "failed atomic apply must not leave a ledger row"
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
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&pod_status_payload(
                    "uid-rb",
                )),
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
    async fn outbox_apply_rejects_incomplete_ledger_row_without_age_based_recovery() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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

        let err = db
            .apply_outbox_transactionally(
                "stale-placeholder-key",
                "PodStatus",
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&pod_status_payload(
                    "uid-stale",
                )),
                "node-a",
            )
            .await
            .expect_err("unsupported incomplete ledger rows must not be reclaimed");
        assert!(matches!(
            err,
            klights_cluster_core::OutboxApplyError::Retryable(_)
        ));

        let record = db
            .get_applied_outbox("stale-placeholder-key")
            .await
            .expect("get outbox record")
            .expect("outbox record exists");
        assert!(record.subject_key.is_empty());
        assert!(record.applied_rv.is_none());
        assert!(record.result_proto.is_empty());
    }

    #[tokio::test]
    async fn outbox_apply_rejects_fresh_incomplete_ledger_row_without_consuming_it() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&pod_status_payload(
                    "uid-fresh-placeholder",
                )),
                "node-a",
            )
            .await
            .expect_err("fresh placeholder is still in-flight and must retry");

        assert!(
            matches!(err, klights_cluster_core::OutboxApplyError::Retryable(_)),
            "fresh placeholder must be retryable, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn duplicate_outbox_apply_mutates_resource_once() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
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

        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for i in 0..10 {
            db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
    async fn applied_outbox_gc_prunes_event_create_and_unknown_operations() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let old_ms = now_ms - 13 * 60 * 60 * 1000;

        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );

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
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
}
