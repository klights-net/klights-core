use super::*;

#[tokio::test]
async fn replicated_update_resource_preserves_disruption_target_over_newer_kubelet_status() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    // Victim is already Running on the node with the four kubelet-rebuilt
    // conditions and no DisruptionTarget.
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
                "uid": "victim-uid"
            },
            "spec": {"nodeName": "worker-a"},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "True"}
                ]
            }
        }),
    )
    .await
    .unwrap();

    // A kubelet status write lands while the preemption command is in
    // flight, bumping the live resourceVersion past the preemption
    // command's meta RV (meta.resource_version = 2 below). The fresh
    // status still lacks DisruptionTarget — it is a pure kubelet snapshot
    // (it carries a podIP that was not present at create time, so the
    // write is a real mutation that advances the resourceVersion).
    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "victim-pod",
        json!({
            "phase": "Running",
            "podIP": "10.244.1.5",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Initialized", "status": "True"},
                {"type": "ContainersReady", "status": "True"},
                {"type": "Ready", "status": "True"}
            ]
        }),
        None,
    )
    .await
    .unwrap();

    let before_preempt = db
        .get_resource("v1", "Pod", Some("default"), "victim-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before_preempt.resource_version, 2,
        "kubelet status write must have advanced the live resourceVersion"
    );

    // The scheduler preemption termination: full UpdateResource carrying
    // metadata.deletionTimestamp and a status that includes the
    // scheduler-owned DisruptionTarget condition (PreemptionByScheduler).
    apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "victim-pod".into(),
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "victim-pod",
                    "namespace": "default",
                    "uid": "victim-uid",
                    "deletionTimestamp": "2026-06-22T12:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-a"},
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
            expected_rv: 0,
            preconditions: ResourcePreconditions {
                uid: Some("victim-uid".into()),
                resource_version: None,
            },
        },
        CommandMeta {
            command_id: CommandId("preserve-live-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            // Deliberately older than the live RV after the kubelet status
            // write so the apply path takes the preserve-live-status branch.
            resource_version: 2,
            uid: Some("victim-uid".into()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
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
        "replicated preemption UpdateResource must preserve scheduler-owned DisruptionTarget when a newer kubelet status landed first: {:?}",
        stored.data.pointer("/status/conditions")
    );
}

#[tokio::test]
async fn replicated_scheduler_bind_overwrites_pod_scheduled_pending_condition() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bind-me",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bind-me",
                "namespace": "default",
                "uid": "bind-me-uid"
            },
            "spec": {"containers": [{"name": "c", "image": "busybox"}]},
            "status": {
                "phase": "Pending",
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "False",
                        "reason": "SchedulingPending"
                    },
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "False"},
                    {"type": "Ready", "status": "False"}
                ]
            }
        }),
    )
    .await
    .unwrap();

    apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "bind-me".into(),
            data: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "bind-me",
                    "namespace": "default",
                    "uid": "bind-me-uid"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "c", "image": "busybox"}]
                },
                "status": {
                    "phase": "Pending",
                    "conditions": [
                        {"type": "PodScheduled", "status": "True"},
                        {"type": "Initialized", "status": "True"},
                        {"type": "ContainersReady", "status": "False"},
                        {"type": "Ready", "status": "False"}
                    ]
                }
            }),
            expected_rv: 0,
            preconditions: ResourcePreconditions {
                uid: Some("bind-me-uid".into()),
                resource_version: None,
            },
        },
        CommandMeta {
            command_id: CommandId("bind-pod-status".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 1,
            uid: Some("bind-me-uid".into()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .unwrap();

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "bind-me")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("worker-a")
    );
    let pod_scheduled = stored
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(|v| v.as_str()) == Some("PodScheduled")
            })
        })
        .expect("PodScheduled condition must be present after scheduler bind");
    assert_eq!(
        pod_scheduled.pointer("/status").and_then(|v| v.as_str()),
        Some("True"),
        "replicated scheduler bind must overwrite the old SchedulingPending condition: {:?}",
        stored.data.pointer("/status/conditions")
    );
    assert!(
        pod_scheduled.pointer("/reason").is_none(),
        "successful scheduling must not retain the SchedulingPending reason: {pod_scheduled:?}"
    );
}
