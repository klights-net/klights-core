use super::*;
#[tokio::test]
async fn leader_direct_status_apply_preserves_disruption_target_without_outbox_stamp() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    // Post-preemption victim: terminating with the four kubelet-rebuilt
    // conditions plus the scheduler-owned DisruptionTarget condition.
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "victim-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "victim-pod",
                "namespace": "default",
                "uid": "victim-uid",
                "deletionTimestamp": "2026-06-22T12:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {"nodeName": "controlplane1"},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        }),
    )
    .await
    .unwrap();

    // A leader-direct kubelet runtime-reconcile status write (no outbox
    // stamp) carrying a snapshot computed before preemption: the four
    // kubelet-rebuilt conditions and a freshly observed podIP, but NO
    // DisruptionTarget. This is the exact payload shape the leader's
    // `apply_runtime_reconcile_status_inner` forwards when its read of the
    // live row raced the preemption write.
    apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "victim-pod".into(),
            status: json!({
                "phase": "Running",
                "podIP": "10.244.0.5",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "True"}
                ]
            }),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("victim-uid".into()),
                resource_version: None,
            },
            // Leader-direct writes never carry an outbox stamp.
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId("delete-victim-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 1,
            uid: Some("victim-uid".into()),
            timestamp_ms: 0,
            authoring_node: "controlplane1".into(),
        },
    )
    .await
    .unwrap();

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "victim-pod")
        .await
        .unwrap()
        .unwrap();
    assert!(
        stored
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .unwrap_or(&Vec::new())
            .iter()
            .any(|condition| {
                condition.pointer("/type").and_then(|v| v.as_str()) == Some("DisruptionTarget")
                    && condition.pointer("/reason").and_then(|v| v.as_str())
                        == Some("PreemptionByScheduler")
            }),
        "leader-direct UpdateStatus apply (no outbox stamp) must preserve scheduler-owned DisruptionTarget over a stale kubelet snapshot: {:?}",
        stored.data.pointer("/status/conditions")
    );
}

#[tokio::test]
async fn replicated_apply_main_update_allows_status_only_rv_advance() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "status-overlap-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "status-overlap-deploy",
                    "namespace": "default",
                    "uid": "deploy-status-overlap-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "apps/v1",
        "Deployment",
        Some("default"),
        "status-overlap-deploy",
        json!({"availableReplicas": 1}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let mut proposed = (*created.data).clone();
    proposed["spec"]["replicas"] = json!(2);
    proposed["status"] = json!({"availableReplicas": 0});

    apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "status-overlap-deploy".into(),
            data: proposed,
            expected_rv: created.resource_version,
            preconditions: ResourcePreconditions::from_resource(&created),
        },
        CommandMeta {
            command_id: CommandId("update-resource-success".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 3,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect("main update must tolerate a concurrent status-only RV advance");

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "status-overlap-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, 3);
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.data.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "main update must preserve the status that advanced while raft committed"
    );
}

#[tokio::test]
async fn replicated_apply_status_rejects_status_only_rv_conflict() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "status-conflict-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "status-conflict-pod",
                    "namespace": "default",
                    "uid": "status-conflict-pod-uid"
                },
                "spec": {
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "status-conflict-pod",
        json!({"phase": "Running"}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "status-conflict-pod".into(),
            status: json!({"phase": "Succeeded"}),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions::from_resource(&created),
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId("lenient-patch-rv-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 3,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("status updates must remain strict against status conflicts");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected status conflict, got: {err:#}"
    );

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "status-conflict-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
}

#[tokio::test]
async fn replicated_apply_patch_allows_status_only_rv_advance() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-status-overlap-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "patch-status-overlap-deploy",
                    "namespace": "default",
                    "uid": "patch-status-overlap-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "apps/v1",
        "Deployment",
        Some("default"),
        "patch-status-overlap-deploy",
        json!({"availableReplicas": 1}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    apply_command_to_backend(
        &db,
        StorageCommand::PatchResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "patch-status-overlap-deploy".into(),
            patch_kind: PatchKind::Merge,
            patch: json!({"spec": {"replicas": 2}, "status": {"availableReplicas": 0}}),
            preconditions: ResourcePreconditions::from_resource(&created),
            strict_resource_version: false,
        },
        CommandMeta {
            command_id: CommandId("lenient-patch-uid-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 3,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect("main patch must tolerate a concurrent status-only RV advance");

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-status-overlap-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.data.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "main patch must preserve live status"
    );
}

#[tokio::test]
async fn replicated_apply_status_rejects_stale_resource_version() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "apply-status-local-rv",
        json!({
            "metadata": {"name": "apply-status-local-rv", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "apply-status-local-rv".into(),
            status: json!({"phase": "Running"}),
            expected_rv: Some(99),
            preconditions: ResourcePreconditions::resource_version(99),
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId("stale-replicated-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 2,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("stale replicated status update must preserve the command RV precondition");
    assert!(
        err.to_string().contains("409 Conflict"),
        "expected stale RV conflict, got: {err:#}"
    );

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "apply-status-local-rv")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, 1);
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending")
    );
}

#[tokio::test]
async fn raft_mode_main_update_allows_status_only_rv_advance() {
    let (ds, _calls) = make_ds_with_inline_proposer().await;

    let created = ds
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "raft-status-overlap-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "raft-status-overlap-deploy",
                    "namespace": "default",
                    "uid": "raft-status-overlap-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    ds.update_status_only(
        "apps/v1",
        "Deployment",
        Some("default"),
        "raft-status-overlap-deploy",
        json!({"availableReplicas": 1}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let mut proposed = (*created.data).clone();
    proposed["spec"]["replicas"] = json!(2);
    proposed["status"] = json!({"availableReplicas": 0});
    let updated = ds
        .update_main_resource_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "raft-status-overlap-deploy",
            proposed,
            ResourcePreconditions::from_resource(&created),
        )
        .await
        .expect("raft-routed main update must tolerate a status-only RV advance");

    assert_eq!(updated.data.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        updated.data.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "raft-routed main update must preserve live status"
    );
}

#[tokio::test]
async fn raft_mode_patch_allows_status_only_rv_advance() {
    let (ds, _calls) = make_ds_with_inline_proposer().await;

    let created = ds
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "raft-patch-status-overlap-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "raft-patch-status-overlap-deploy",
                    "namespace": "default",
                    "uid": "raft-patch-status-overlap-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    ds.update_status_only(
        "apps/v1",
        "Deployment",
        Some("default"),
        "raft-patch-status-overlap-deploy",
        json!({"availableReplicas": 1}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let updated = ds
        .patch_resource_latest_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "raft-patch-status-overlap-deploy",
            ResourcePatchRequest::new(
                PatchKind::Merge,
                json!({"spec": {"replicas": 2}, "status": {"availableReplicas": 0}}),
                ResourcePreconditions::from_resource(&created),
            ),
        )
        .await
        .expect("raft-routed patch must tolerate a status-only RV advance")
        .expect("deployment exists");

    assert_eq!(updated.data.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        updated.data.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "raft-routed patch must preserve live status"
    );
}

#[tokio::test]
async fn raft_mode_follower_proposer_rejects_network_cluster_writes_no_local_mutation() {
    let (ds, inner) = make_ds_with_follower_proposer().await;

    let subnet_err = ds
        .allocate_node_subnet("worker-1", "10.50.0.0/16", "192.0.2.10")
        .await
        .expect_err("follower subnet allocation must reject");
    assert!(
        subnet_err.to_string().contains("leader"),
        "error must mention leader: {subnet_err}"
    );
    assert!(
        inner.get_node_subnet("worker-1").await.unwrap().is_none(),
        "follower must not locally allocate node_subnets"
    );

    let metadata = klights_cluster_store::DataplanePeerMetadata::try_new(
        "worker-1".to_string(),
        klights_cluster_store::DataplaneMode::Root,
        klights_cluster_store::DataplaneEncryption::Enabled,
        Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        Some("192.0.2.10".to_string()),
        Some(7679),
    )
    .unwrap();
    let dataplane_err = ds
        .update_node_dataplane(metadata)
        .await
        .expect_err("follower dataplane update must reject");
    assert!(
        dataplane_err.to_string().contains("leader"),
        "error must mention leader: {dataplane_err}"
    );
    assert!(
        inner
            .get_node_dataplane("worker-1")
            .await
            .unwrap()
            .is_none(),
        "follower must not locally write node_dataplane"
    );
}

#[tokio::test]
async fn network_cluster_writes_with_proposer_route_through_raft() {
    let (ds, calls) = make_ds_with_inline_proposer().await;

    ds.allocate_node_subnet("worker-1", "10.50.0.0/16", "192.0.2.10")
        .await
        .expect("subnet allocation through proposer must succeed");
    ds.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.10".to_string()),
            Some(7679),
        )
        .unwrap(),
    )
    .await
    .expect("dataplane update through proposer must succeed");

    let calls = calls.lock().unwrap().clone();
    assert!(
        calls.contains(&"AllocateNodeSubnet".to_string()),
        "subnet allocation must be raft-proposed, got {calls:?}"
    );
    assert!(
        calls.contains(&"UpdateNodeDataplane".to_string()),
        "dataplane update must be raft-proposed, got {calls:?}"
    );
}

#[tokio::test]
async fn replicated_stale_status_preserves_live_job_status_scalars() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "replicated-stale-job",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "replicated-stale-job", "namespace": "default", "uid": "replicated-job-uid"},
                "spec": {},
                "status": {
                    "active": 1,
                    "succeeded": 0,
                    "failed": 0,
                    "conditions": [{"type": "Complete", "status": "False"}]
                }
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "batch/v1",
        "Job",
        Some("default"),
        "replicated-stale-job",
        json!({
            "active": 0,
            "succeeded": 1,
            "failed": 0,
            "completionTime": "2026-06-30T12:00:00Z",
            "conditions": [{"type": "Complete", "status": "True", "reason": "CompletionsReached"}]
        }),
        Some(created.resource_version),
    )
    .await
    .expect("live completion status applies first");

    apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: Some("default".into()),
            name: "replicated-stale-job".into(),
            status: json!({
                "active": 1,
                "succeeded": 0,
                "failed": 0,
                "conditions": [{"type": "Complete", "status": "False"}]
            }),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some("replicated-job-uid".into()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId("replicated-job-stale-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: created.resource_version,
            uid: Some("replicated-job-uid".into()),
            timestamp_ms: 0,
            authoring_node: "worker-a".into(),
        },
    )
    .await
    .expect("stale replicated status must apply by rebasing");

    let live = db
        .get_resource("batch/v1", "Job", Some("default"), "replicated-stale-job")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live.data.pointer("/status/active"), Some(&json!(0)));
    assert_eq!(live.data.pointer("/status/succeeded"), Some(&json!(1)));
    assert_eq!(
        live.data.pointer("/status/completionTime"),
        Some(&json!("2026-06-30T12:00:00Z"))
    );
    assert_eq!(
        live.data.pointer("/status/conditions/0/status"),
        Some(&json!("True"))
    );
}

#[tokio::test]
async fn replicated_stale_cronjob_status_applies_newer_last_schedule_time() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "batch/v1",
        kind: "CronJob",
        namespace: Some("default"),
        name: "replicated-stale-cronjob",
        uid: "replicated-cronjob-uid",
        initial: serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": "replicated-stale-cronjob",
                "namespace": "default",
                "uid": "replicated-cronjob-uid"
            },
            "spec": {
                "schedule": "* */1 * * ?",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "main", "image": "nginx"}],
                    "restartPolicy": "OnFailure"
                }}}}
            },
            "status": {"lastScheduleTime": "2026-07-04T06:21:59Z"}
        }),
        stale_status: serde_json::json!({"lastScheduleTime": "2026-07-04T06:22:00Z"}),
        expected_pointer: "/status/lastScheduleTime",
        expected_value: serde_json::json!("2026-07-04T06:22:00Z"),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_pdb_status_applies_disrupted_pods() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "policy/v1",
        kind: "PodDisruptionBudget",
        namespace: Some("default"),
        name: "replicated-stale-pdb",
        uid: "replicated-pdb-uid",
        initial: serde_json::json!({
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {
                "name": "replicated-stale-pdb",
                "namespace": "default",
                "uid": "replicated-pdb-uid"
            },
            "spec": {
                "minAvailable": 1,
                "selector": {"matchLabels": {"app": "pdb-stale"}}
            },
            "status": {
                "expectedPods": 1,
                "currentHealthy": 1,
                "desiredHealthy": 1,
                "disruptionsAllowed": 0
            }
        }),
        stale_status: serde_json::json!({
            "expectedPods": 1,
            "currentHealthy": 1,
            "desiredHealthy": 1,
            "disruptionsAllowed": 0,
            "disruptedPods": {
                "pod-0": "2026-07-04T17:43:00Z"
            }
        }),
        expected_pointer: "/status/disruptedPods/pod-0",
        expected_value: serde_json::json!("2026-07-04T17:43:00Z"),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_persistentvolume_status_preserves_live_phase() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "v1",
        kind: "PersistentVolume",
        namespace: None,
        name: "replicated-stale-pv",
        uid: "replicated-pv-uid",
        initial: serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "replicated-stale-pv", "uid": "replicated-pv-uid"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"]},
            "status": {"phase": "Available"}
        }),
        stale_status: serde_json::json!({"phase": "Bound"}),
        expected_pointer: "/status/phase",
        expected_value: serde_json::json!("Available"),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_persistentvolumeclaim_status_preserves_live_phase() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "v1",
        kind: "PersistentVolumeClaim",
        namespace: Some("default"),
        name: "replicated-stale-pvc",
        uid: "replicated-pvc-uid",
        initial: serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": "replicated-stale-pvc",
                "namespace": "default",
                "uid": "replicated-pvc-uid"
            },
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}},
            "status": {"phase": "Pending"}
        }),
        stale_status: serde_json::json!({"phase": "Bound"}),
        expected_pointer: "/status/phase",
        expected_value: serde_json::json!("Pending"),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_replicaset_status_preserves_conditions() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "apps/v1",
        kind: "ReplicaSet",
        namespace: Some("default"),
        name: "replicated-stale-rs",
        uid: "replicated-rs-uid",
        initial: serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "replicated-stale-rs",
                "namespace": "default",
                "uid": "replicated-rs-uid"
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 0, "conditions": [{"type": "Available", "status": "True"}]}
        }),
        stale_status: serde_json::json!({"conditions": [{"type": "Progressing", "status": "True"}]}),
        expected_pointer: "/status/conditions/0/type",
        expected_value: serde_json::json!("Progressing"),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_statefulset_status_preserves_conditions() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "apps/v1",
        kind: "StatefulSet",
        namespace: Some("default"),
        name: "replicated-stale-sts",
        uid: "replicated-sts-uid",
        initial: serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "replicated-stale-sts",
                "namespace": "default",
                "uid": "replicated-sts-uid"
            },
            "spec": {"replicas": 1},
            "status": {"replicas": 0}
        }),
        stale_status: serde_json::json!({"replicas": 1}),
        expected_pointer: "/status/replicas",
        expected_value: serde_json::json!(1),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_daemonset_status_preserves_fields() {
    apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "apps/v1",
        kind: "DaemonSet",
        namespace: Some("default"),
        name: "replicated-stale-ds",
        uid: "replicated-ds-uid",
        initial: serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {
                "name": "replicated-stale-ds",
                "namespace": "default",
                "uid": "replicated-ds-uid"
            },
            "spec": {},
            "status": {"numberReady": 0}
        }),
        stale_status: serde_json::json!({"numberReady": 1}),
        expected_pointer: "/status/numberReady",
        expected_value: serde_json::json!(1),
    })
    .await;
}

#[tokio::test]
async fn replicated_stale_service_status_preserves_live_load_balancer_and_conditions() {
    use std::collections::HashSet;

    let live = apply_replicated_stale_status_case(ReplicatedStaleStatusCase {
        api_version: "v1",
        kind: "Service",
        namespace: Some("default"),
        name: "replicated-stale-service",
        uid: "replicated-service-uid",
        initial: serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "replicated-stale-service",
                "namespace": "default",
                "uid": "replicated-service-uid"
            },
            "spec": {
                "selector": {
                    "app": "service-status"
                },
                "ports": [
                    {"name": "http", "protocol": "TCP", "port": 80, "targetPort": 8080}
                ]
            },
            "status": {
                "loadBalancer": {
                    "ingress": [{"ip": "198.51.100.1"}]
                },
                "metadataField": "from-live",
                "conditions": [
                    {"type": "Ready", "status": "False"},
                    {"type": "ExternalTrafficPolicy", "status": "True"}
                ]
            }
        }),
        stale_status: serde_json::json!({
            "conditions": [
                {"type": "ExternalTrafficPolicy", "status": "False"}
            ]
        }),
        expected_pointer: "/status/loadBalancer/ingress/0/ip",
        expected_value: serde_json::json!("198.51.100.1"),
    })
    .await;

    let condition_types: HashSet<_> = live
        .data
        .pointer("/status/conditions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|condition| condition.get("type").and_then(serde_json::Value::as_str))
        .collect();
    assert!(
        condition_types.contains("Ready"),
        "stale Service status apply must preserve live Ready condition"
    );
    assert!(
        condition_types.contains("ExternalTrafficPolicy"),
        "stale Service status apply must keep update ExternalTrafficPolicy"
    );
    assert_eq!(
        live.data.pointer("/status/metadataField"),
        Some(&serde_json::json!("from-live")),
        "stale Service status apply must preserve unmentioned status fields"
    );
    let external_status = live
        .data
        .pointer("/status/conditions")
        .and_then(serde_json::Value::as_array)
        .and_then(|conditions| {
            conditions.iter().find_map(|condition| {
                (condition.get("type").and_then(serde_json::Value::as_str)
                    == Some("ExternalTrafficPolicy"))
                .then(|| condition.get("status").and_then(serde_json::Value::as_str))
            })
        })
        .expect("service stale merge should include ExternalTrafficPolicy condition");
    assert_eq!(external_status, Some("False"));
}

#[tokio::test]
async fn replicated_fresh_service_status_replaces_load_balancer_and_preserves_conditions() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "replicated-fresh-service",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": "replicated-fresh-service",
                    "namespace": "default",
                    "uid": "replicated-fresh-service-uid"
                },
                "spec": {
                    "selector": {"app": "service-status"},
                    "ports": [{"name": "http", "protocol": "TCP", "port": 80, "targetPort": 8080}]
                },
                "status": {
                    "loadBalancer": {
                        "ingress": [{"ip": "198.51.100.2"}]
                    },
                    "conditions": [
                        {"type": "Ready", "status": "False"},
                        {"type": "ExternalTrafficPolicy", "status": "False"}
                    ],
                    "metadataField": "from-live"
                }
            }),
        )
        .await
        .unwrap();

    apply_command_to_backend(
        &db,
        StorageCommand::UpdateStatus {
            api_version: "v1".into(),
            kind: "Service".into(),
            namespace: Some("default".into()),
            name: "replicated-fresh-service".into(),
            status: serde_json::json!({
                "loadBalancer": {"ingress": [{"ip": "198.51.100.9"}]},
                "conditions": [
                    {"type": "ExternalTrafficPolicy", "status": "True"}
                ]
            }),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some("replicated-fresh-service-uid".into()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        },
        CommandMeta {
            command_id: CommandId("replicated-fresh-service-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: created.resource_version,
            uid: Some("replicated-fresh-service-uid".into()),
            timestamp_ms: 0,
            authoring_node: "controlplane1".into(),
        },
    )
    .await
    .unwrap();

    let live = db
        .get_resource("v1", "Service", Some("default"), "replicated-fresh-service")
        .await
        .unwrap()
        .expect("fresh Service status apply should persist");
    assert_eq!(
        live.data.pointer("/status/loadBalancer/ingress/0/ip"),
        Some(&serde_json::json!("198.51.100.9")),
        "fresh Service status apply should replace loadBalancer when explicitly provided"
    );
    assert_eq!(
        live.data.pointer("/status/metadataField"),
        Some(&serde_json::json!("from-live")),
        "fresh Service status apply should preserve unmentioned status fields"
    );
    let mut ready_false = false;
    let mut external_true = false;
    for condition in live
        .data
        .pointer("/status/conditions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        match condition.get("type").and_then(serde_json::Value::as_str) {
            Some("Ready") => {
                ready_false =
                    condition.get("status").and_then(serde_json::Value::as_str) == Some("False");
            }
            Some("ExternalTrafficPolicy") => {
                external_true =
                    condition.get("status").and_then(serde_json::Value::as_str) == Some("True");
            }
            _ => {}
        }
    }
    assert!(
        ready_false,
        "fresh Service status apply should preserve Ready"
    );
    assert!(
        external_true,
        "fresh Service status apply should keep provided ExternalTrafficPolicy"
    );
}
