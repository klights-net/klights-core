use super::*;

#[tokio::test]
async fn single_node_create_resource_works() {
    let (ds, _calls) = make_ds_with_inline_proposer().await;
    let res = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "ha02-test",
            json!({"metadata": {"name": "ha02-test"}}),
        )
        .await
        .unwrap();
    assert!(res.resource_version > 0);
}

#[tokio::test]
async fn raft_mode_mark_for_delete_without_watch_reuses_mark_and_routes_through_raft() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let created = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "mark-safety",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "mark-safety", "namespace": "default", "uid": "mark-uid"}
            }),
        )
        .await
        .unwrap();
    calls.lock().unwrap().clear();

    let first_mark = ds
        .mark_for_delete_without_watch(
            "v1",
            "ConfigMap",
            Some("default"),
            "mark-safety",
            ResourcePreconditions::uid_and_resource_version(
                "mark-uid".to_string(),
                created.resource_version,
            ),
            30,
        )
        .await
        .unwrap()
        .expect("mark_for_delete_without_watch must return the updated resource");
    let delete_timestamp = first_mark
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|value| value.as_str())
        .expect("delete timestamp must be written by mark path");

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &["UpdateResource"],
        "mark path must route through raft in replicated mode"
    );
    assert_eq!(
        first_mark
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some(delete_timestamp)
    );
    assert_eq!(
        first_mark
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|value| value.as_i64()),
        Some(30)
    );

    let second_mark = ds
        .mark_for_delete_without_watch(
            "v1",
            "ConfigMap",
            Some("default"),
            "mark-safety",
            ResourcePreconditions::uid_and_resource_version(
                "mark-uid".to_string(),
                first_mark.resource_version,
            ),
            30,
        )
        .await
        .unwrap()
        .expect("already marked resources should still return a resource");

    assert_eq!(first_mark.resource_version, second_mark.resource_version);
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &["UpdateResource"],
        "idempotent mark_for_delete_without_watch must not re-propose"
    );
    assert_eq!(
        second_mark
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some(delete_timestamp)
    );
}

#[tokio::test]
async fn raft_mode_delete_without_watch_with_tombstone_returns_committed_deleted_object() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let created = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "mark-without-watch",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "mark-without-watch",
                    "namespace": "default",
                    "uid": "terminal-uid"
                }
            }),
        )
        .await
        .unwrap();
    calls.lock().unwrap().clear();

    let deleted = ds
        .delete_resource_without_watch_with_tombstone(
            "v1",
            "ConfigMap",
            Some("default"),
            "mark-without-watch",
            ResourcePreconditions::uid_and_resource_version(
                "terminal-uid".to_string(),
                created.resource_version,
            ),
            30,
        )
        .await
        .unwrap();
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &["DeleteResourceWithTombstone"],
        "terminal delete path must use the tombstone command"
    );
    assert!(
        deleted
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str)
            .is_some(),
        "response should include deletionTimestamp"
    );
    assert_eq!(
        deleted
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(Value::as_i64),
        Some(30),
        "response should include deletion grace"
    );
    let current_rv = ds.passive.get_current_resource_version().await.unwrap();
    assert_eq!(
        deleted.resource_version, current_rv,
        "tombstone delete response must carry the committed RV"
    );
    assert_eq!(
        deleted
            .data
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str),
        Some(current_rv.to_string().as_str()),
        "response object metadata.resourceVersion must match the committed RV"
    );

    let deleted_events = ds
        .list_all_watch_events_since(0)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.event_type == "DELETED" && event.resource.name == "mark-without-watch"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        deleted_events.len(),
        1,
        "tombstone delete must emit exactly one deleted event"
    );
    let event_resource = &deleted_events[0].resource;
    assert_eq!(
        event_resource.resource_version, deleted.resource_version,
        "response and deleted event must share the committed RV"
    );
    assert_eq!(
        event_resource
            .data
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str),
        deleted
            .data
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str),
        "response and deleted event must share one committed object"
    );
    assert_eq!(
        event_resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str),
        deleted
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str),
        "response and deleted event must share deletionTimestamp"
    );
    assert_eq!(
        event_resource
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(Value::as_i64),
        deleted
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(Value::as_i64),
        "response and deleted event must share deletion grace"
    );

    assert!(
        ds.get_resource("v1", "ConfigMap", Some("default"), "mark-without-watch")
            .await
            .unwrap()
            .is_none(),
        "terminal delete must remove the resource row"
    );
}

#[tokio::test]
async fn replicated_apply_preserves_preconditions_through_codec() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "codec-apply",
        json!({
            "metadata": {
                "name": "codec-apply",
                "namespace": "default",
                "uid": "uid-codec"
            }
        }),
    )
    .await
    .unwrap();

    let command = StorageCommand::UpdateStatus {
        api_version: "v1".into(),
        kind: "Pod".into(),
        namespace: Some("default".into()),
        name: "codec-apply".into(),
        status: json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("uid-codec".into()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    let encoded =
        klights_leader_rpc::storage_wire_codec::encode_command_protobuf(&command).unwrap();
    let decoded =
        klights_leader_rpc::storage_wire_codec::decode_command_protobuf(&encoded).unwrap();

    apply_command_to_backend(
        &db,
        decoded,
        CommandMeta {
            command_id: CommandId("protobuf-codec-apply".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 2,
            uid: Some("uid-codec".into()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .unwrap();

    let stored = db
        .get_resource("v1", "Pod", Some("default"), "codec-apply")
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
async fn replicated_apply_create_converges_existing_resource_without_conflict() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "apply-create-existing",
        json!({
            "metadata": {"name": "apply-create-existing", "namespace": "default"},
            "data": {"before": "true"}
        }),
    )
    .await
    .unwrap();

    apply_command_to_backend(
        &db,
        StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "apply-create-existing".into(),
            data: json!({
                "metadata": {"name": "apply-create-existing", "namespace": "default"},
                "data": {"after": "true"}
            }),
        },
        CommandMeta {
            command_id: CommandId("create-existing-resource".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 2,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .unwrap();

    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "apply-create-existing")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, 2);
    assert_eq!(
        stored.data.pointer("/data/after").and_then(|v| v.as_str()),
        Some("true")
    );
}

#[tokio::test]
async fn public_create_rejects_existing_name_with_different_uid() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "same-name",
        json!({
            "metadata": {
                "name": "same-name",
                "namespace": "default",
                "uid": "uid-old"
            }
        }),
    )
    .await
    .unwrap();

    let err = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            json!({
                "metadata": {
                    "name": "same-name",
                    "namespace": "default",
                    "uid": "uid-new"
                }
            }),
        )
        .await
        .expect_err("public create must not replace an existing name");

    assert!(
        err.to_string().contains("Resource already exists"),
        "expected public create conflict, got {err:#}"
    );
}

#[tokio::test]
async fn replicated_apply_create_replaces_stale_same_name_different_uid() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("statefulset-8075"),
        "test-ss-0",
        json!({
            "metadata": {
                "name": "test-ss-0",
                "namespace": "statefulset-8075",
                "uid": "uid-old"
            },
            "spec": {"nodeName": "local-worker"},
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();
    let mut watch = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"));

    apply_command_to_backend(
        &db,
        StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("statefulset-8075".into()),
            name: "test-ss-0".into(),
            data: json!({
                "metadata": {
                    "name": "test-ss-0",
                    "namespace": "statefulset-8075",
                    "uid": "uid-new"
                },
                "spec": {"nodeName": "local-worker"}
            }),
        },
        CommandMeta {
            command_id: CommandId("update-resource-uid-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 5,
            uid: Some("uid-new".into()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect("replicated create must converge a stale local UID slot");

    let stored = db
        .get_resource("v1", "Pod", Some("statefulset-8075"), "test-ss-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.uid, "uid-new");
    assert_eq!(
        stored
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some("uid-new")
    );
    assert_eq!(stored.resource_version, 5);
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        None,
        "replacement pod must not retain stale status from the old UID"
    );

    let event = watch.recv().await.unwrap();
    assert_eq!(event.event_type, klights_watch::EventType::Deleted);
    assert_eq!(
        event
            .object
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some("uid-old")
    );
    assert_eq!(
        event
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("4")
    );

    let event = watch.recv().await.unwrap();
    assert_eq!(event.event_type, klights_watch::EventType::Added);
    assert_eq!(
        event
            .object
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some("uid-new")
    );
    assert_eq!(
        event
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(|v| v.as_str()),
        Some("5")
    );
}

#[tokio::test]
async fn replicated_apply_update_rejects_stale_resource_version() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "apply-update-local-rv",
        json!({
            "metadata": {"name": "apply-update-local-rv", "namespace": "default"},
            "data": {"before": "true"}
        }),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "apply-update-local-rv".into(),
            data: json!({
                "metadata": {"name": "apply-update-local-rv", "namespace": "default"},
                "data": {"after": "true"}
            }),
            expected_rv: 99,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: Some(99),
            },
        },
        CommandMeta {
            command_id: CommandId("delete-resource-rv-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 2,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("stale replicated update must preserve the command RV precondition");
    assert!(
        err.to_string()
            .contains("resourceVersion precondition failed")
            && err.to_string().contains("409 Conflict"),
        "expected stale RV conflict, got: {err:#}"
    );

    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "apply-update-local-rv")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, 1);
    assert_eq!(
        stored.data.pointer("/data/before").and_then(|v| v.as_str()),
        Some("true")
    );
}

#[tokio::test]
async fn replicated_apply_main_update_rejects_true_spec_conflict() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "spec-conflict-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "spec-conflict-deploy",
                    "namespace": "default",
                    "uid": "deploy-spec-conflict-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    let mut live_update = (*created.data).clone();
    live_update["metadata"]["generation"] = json!(2);
    live_update["spec"]["replicas"] = json!(3);
    db.update_main_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "spec-conflict-deploy",
        live_update,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();

    let mut stale_proposed = (*created.data).clone();
    stale_proposed["metadata"]["generation"] = json!(2);
    stale_proposed["spec"]["replicas"] = json!(2);
    let err = apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "spec-conflict-deploy".into(),
            data: stale_proposed,
            expected_rv: created.resource_version,
            preconditions: ResourcePreconditions::from_resource(&created),
        },
        CommandMeta {
            command_id: CommandId("delete-resource-success".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 3,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("stale main update must still reject a real spec conflict");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected spec conflict, got: {err:#}"
    );

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "spec-conflict-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(3)));
}

#[tokio::test]
async fn replicated_apply_main_update_rejects_same_name_replacement() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "replacement-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "replacement-deploy",
                    "namespace": "default",
                    "uid": "old-deploy-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    db.delete_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "replacement-deploy",
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();
    let replacement = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "replacement-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "replacement-deploy",
                    "namespace": "default",
                    "uid": "new-deploy-uid",
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
        "replacement-deploy",
        json!({"availableReplicas": 1}),
        Some(replacement.resource_version),
    )
    .await
    .unwrap();

    let mut stale_proposed = (*created.data).clone();
    stale_proposed["spec"]["replicas"] = json!(2);
    let err = apply_command_to_backend(
        &db,
        StorageCommand::UpdateResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "replacement-deploy".into(),
            data: stale_proposed,
            expected_rv: created.resource_version,
            preconditions: ResourcePreconditions::resource_version(created.resource_version),
        },
        CommandMeta {
            command_id: CommandId("status-update-success".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 5,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("status-only rebase must not cross a same-name replacement");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected replacement conflict, got: {err:#}"
    );

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "replacement-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.uid, "new-deploy-uid");
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(1)));
    assert_eq!(
        stored.data.pointer("/status/availableReplicas"),
        Some(&json!(1))
    );
}

#[tokio::test]
async fn replicated_apply_patch_rejects_stale_resource_version() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "apply-patch-local-rv",
        json!({
            "metadata": {"name": "apply-patch-local-rv", "namespace": "default"},
            "data": {"before": "true"}
        }),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::PatchResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "apply-patch-local-rv".into(),
            patch_kind: PatchKind::Merge,
            patch: json!({"data": {"after": "true"}}),
            preconditions: ResourcePreconditions::resource_version(99),
            strict_resource_version: false,
        },
        CommandMeta {
            command_id: CommandId("lenient-patch-success".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 2,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("stale replicated patch must preserve the command RV precondition");
    assert!(
        err.to_string()
            .contains("resourceVersion precondition failed")
            && err.to_string().contains("409 Conflict"),
        "expected stale RV conflict, got: {err:#}"
    );

    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "apply-patch-local-rv")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, 1);
    assert_eq!(
        stored.data.pointer("/data/before").and_then(|v| v.as_str()),
        Some("true")
    );
    assert!(
        stored.data.pointer("/data/after").is_none(),
        "stale patch must not mutate live data"
    );
}

#[tokio::test]
async fn replicated_apply_patch_rejects_true_spec_conflict() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-spec-conflict-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "patch-spec-conflict-deploy",
                    "namespace": "default",
                    "uid": "patch-spec-conflict-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    let mut live_update = (*created.data).clone();
    live_update["metadata"]["generation"] = json!(2);
    live_update["spec"]["replicas"] = json!(3);
    db.update_main_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "patch-spec-conflict-deploy",
        live_update,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::PatchResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "patch-spec-conflict-deploy".into(),
            patch_kind: PatchKind::Merge,
            patch: json!({"metadata": {"generation": 2}, "spec": {"replicas": 2}}),
            preconditions: ResourcePreconditions::from_resource(&created),
            strict_resource_version: false,
        },
        CommandMeta {
            command_id: CommandId("strict-patch-rv-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 3,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("stale patch must still reject a real spec conflict");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected spec conflict, got: {err:#}"
    );

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-spec-conflict-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(3)));
}

#[tokio::test]
async fn replicated_apply_patch_rejects_same_name_replacement() {
    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-replacement-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "patch-replacement-deploy",
                    "namespace": "default",
                    "uid": "old-patch-deploy-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 0}
            }),
        )
        .await
        .unwrap();

    db.delete_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "patch-replacement-deploy",
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();
    let replacement = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-replacement-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "patch-replacement-deploy",
                    "namespace": "default",
                    "uid": "new-patch-deploy-uid",
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
        "patch-replacement-deploy",
        json!({"availableReplicas": 1}),
        Some(replacement.resource_version),
    )
    .await
    .unwrap();

    let err = apply_command_to_backend(
        &db,
        StorageCommand::PatchResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: "patch-replacement-deploy".into(),
            patch_kind: PatchKind::Merge,
            patch: json!({"spec": {"replicas": 2}}),
            preconditions: ResourcePreconditions::resource_version(created.resource_version),
            strict_resource_version: false,
        },
        CommandMeta {
            command_id: CommandId("status-rv-mismatch".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 5,
            uid: Some(created.uid.clone()),
            timestamp_ms: 0,
            authoring_node: "leader".into(),
        },
    )
    .await
    .expect_err("patch rebase must not cross a same-name replacement");
    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "expected replacement conflict, got: {err:#}"
    );

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "patch-replacement-deploy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.uid, "new-patch-deploy-uid");
    assert_eq!(stored.data.pointer("/spec/replicas"), Some(&json!(1)));
    assert_eq!(
        stored.data.pointer("/status/availableReplicas"),
        Some(&json!(1))
    );
}

#[tokio::test]
async fn leader_allows_writes() {
    let (ds, _calls) = make_ds_with_inline_proposer().await;
    let res = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "leader-cm",
            json!({"metadata": {"name": "leader-cm"}}),
        )
        .await
        .unwrap();
    assert!(res.resource_version > 0);
}

#[tokio::test]
async fn leader_write_routes_through_proposer() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let resource = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "replication-observed",
            json!({"metadata": {"name": "replication-observed"}}),
        )
        .await
        .unwrap();
    assert!(resource.resource_version > 0);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], "CreateResource");
}

#[tokio::test]
async fn delete_resource_exposes_committed_rv_for_leader_log_apply() {
    let leader = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let deleted = leader
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "delete-rv-source",
            json!({
                "metadata": {
                    "name": "delete-rv-source",
                    "namespace": "default",
                    "uid": "delete-rv-source-uid"
                }
            }),
        )
        .await
        .unwrap();

    let delete_rv = leader
        .delete_resource_with_preconditions_observed_rv(
            "v1",
            "ConfigMap",
            Some("default"),
            "delete-rv-source",
            ResourcePreconditions::from_resource(&deleted),
        )
        .await
        .unwrap();
    let later = leader
        .create_resource(
            "v1",
            "Event",
            Some("default"),
            "after-delete",
            json!({
                "metadata": {
                    "name": "after-delete",
                    "namespace": "default",
                    "uid": "after-delete-uid"
                }
            }),
        )
        .await
        .unwrap();

    assert!(delete_rv > deleted.resource_version);
    assert!(later.resource_version > delete_rv);

    let follower = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(klights_cluster_core::LogApplyCommit::put_resource(&deleted))
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(klights_cluster_core::LogApplyCommit::delete_resource(
            "v1",
            "ConfigMap",
            Some("default".to_string()),
            "delete-rv-source",
            deleted.uid.clone(),
        ))
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(klights_cluster_core::LogApplyCommit::put_resource(&later))
        .await
        .expect("later write must not collide with the delete watch event RV");
}

#[tokio::test]
async fn raft_mode_create_resource_routes_via_proposer() {
    use crate::datastore::backend::DatastoreHandle;

    struct InlineProposer {
        inner: DatastoreHandle,
        calls: std::sync::Mutex<Vec<StorageCommand>>,
    }

    #[async_trait]
    impl super::super::RaftProposal for InlineProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            self.calls.lock().unwrap().push(command.clone());
            crate::bootstrap::outbox_apply_adapter::propose_command_on_backend(
                self.inner.as_ref(),
                command,
            )
            .await
            .map_err(|e| anyhow::anyhow!("inline propose apply: {e}"))
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            self.calls.lock().unwrap().push(command.clone());
            let result = crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                self.inner.as_ref(),
                idempotency_key,
                klights_kubelet::node_outbox::payload::OutboxOperation::try_from(operation)
                    .map_err(|e| {
                        klights_cluster_core::OutboxApplyError::Retryable(e.to_string())
                    })?,
                command,
                authoring_node,
                None,
            )
            .await?;
            Ok(result.into_parts().0)
        }
    }

    let proposer = Arc::new(InlineProposer {
        inner: Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        ),
        calls: Default::default(),
    });
    let inner = proposer.inner.clone();
    let proposer_dyn: Arc<dyn super::super::RaftProposal> = proposer.clone();
    let ds = SequencedDatastore::new(inner.clone(), proposer_dyn);

    let res = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "raft-cm",
            json!({"metadata": {"name": "raft-cm", "namespace": "default"}}),
        )
        .await
        .expect("create_resource via raft proposer");
    assert_eq!(res.name, "raft-cm");
    let calls = proposer.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "proposer must be called exactly once");
    match &calls[0] {
        StorageCommand::CreateResource {
            api_version,
            kind,
            name,
            ..
        } => {
            assert_eq!(api_version, "v1");
            assert_eq!(kind, "ConfigMap");
            assert_eq!(name, "raft-cm");
        }
        other => panic!("expected CreateResource, got {:?}", other.variant_name()),
    }
}

#[tokio::test]
async fn replicated_apply_resource_batch_proposes_one_raft_command() {
    use crate::datastore::backend::DatastoreHandle;

    struct RecordingProposer {
        calls: std::sync::Mutex<Vec<StorageCommand>>,
    }

    #[async_trait]
    impl super::super::RaftProposal for RecordingProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            self.calls.lock().unwrap().push(command);
            Ok(klights_cluster_store::StorageCommandResult::default())
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            unreachable!("resource batch routing should use propose_command")
        }
    }

    let proposer = Arc::new(RecordingProposer {
        calls: Default::default(),
    });
    let inner: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let db = SequencedDatastore::new(inner, proposer.clone());
    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "batched".to_string(),
            data: json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "batched", "namespace": "default"},
                "subsets": []
            }),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "batched-klights".to_string(),
            data: json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {"name": "batched-klights", "namespace": "default"},
                "addressType": "IPv4",
                "endpoints": [],
                "ports": []
            }),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ])
    .await
    .unwrap();

    let calls = proposer.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(matches!(
        &calls[0],
        StorageCommand::ApplyResourceBatch { operations } if operations.len() == 2
    ));
}

#[tokio::test]
async fn raft_mode_delete_resource_routes_via_proposer() {
    use crate::datastore::backend::DatastoreHandle;

    struct InlineProposer {
        inner: DatastoreHandle,
        calls: std::sync::Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl super::super::RaftProposal for InlineProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            let sequence = {
                let mut calls = self.calls.lock().unwrap();
                calls.push(command.variant_name());
                calls.len()
            };
            apply_command_to_backend(
                self.inner.as_ref(),
                command,
                CommandMeta {
                    command_id: CommandId(format!("raft-inline-{sequence}")),
                    codec_version: COMMAND_CODEC_VERSION,
                    resource_version: 0,
                    uid: None,
                    timestamp_ms: 0,
                    authoring_node: "raft-inline".into(),
                },
            )
            .await?;
            Ok(klights_cluster_store::StorageCommandResult::default())
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            self.calls.lock().unwrap().push(command.variant_name());
            let outcome =
                crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    klights_kubelet::node_outbox::payload::OutboxOperation::try_from(operation)
                        .map_err(|e| {
                            klights_cluster_core::OutboxApplyError::Retryable(e.to_string())
                        })?,
                    command,
                    authoring_node,
                    None,
                )
                .await
                .map_err(|e| klights_cluster_core::OutboxApplyError::Retryable(e.to_string()))?;
            Ok(outcome.into_parts().0)
        }
    }

    let inner: DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    inner
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "to-delete",
            json!({"metadata": {"name": "to-delete", "namespace": "default"}}),
        )
        .await
        .unwrap();

    let proposer = Arc::new(InlineProposer {
        inner: inner.clone(),
        calls: Default::default(),
    });
    let proposer_dyn: Arc<dyn super::super::RaftProposal> = proposer.clone();
    let ds = SequencedDatastore::new(inner.clone(), proposer_dyn);

    let rv = ds
        .delete_resource_with_preconditions_observed_rv(
            "v1",
            "ConfigMap",
            Some("default"),
            "to-delete",
            ResourcePreconditions::default(),
        )
        .await
        .expect("delete via raft proposer");
    assert!(rv > 0);
    let calls = proposer.calls.lock().unwrap();
    assert_eq!(calls.as_slice(), &["DeleteResource"]);
    let still_there = inner
        .get_resource("v1", "ConfigMap", Some("default"), "to-delete")
        .await
        .unwrap();
    assert!(
        still_there.is_none(),
        "raft-routed delete must remove the row from inner"
    );
}

#[tokio::test]
async fn raft_mode_delete_resource_stale_precondition_surfaces_conflict() {
    let (ds, _calls) = make_ds_with_inline_proposer().await;

    let created = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-delete",
            json!({
                "metadata": {
                    "name": "stale-delete",
                    "namespace": "default",
                    "uid": "stale-delete-uid"
                },
                "data": {"before": "true"}
            }),
        )
        .await
        .unwrap();

    let mut bumped_data = (*created.data).clone();
    bumped_data["data"]["after"] = json!("true");
    let bumped = ds
        .update_resource_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-delete",
            bumped_data,
            ResourcePreconditions::from_resource(&created),
        )
        .await
        .unwrap();
    assert!(bumped.resource_version > created.resource_version);

    let err = ds
        .delete_resource_with_preconditions_observed_rv(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-delete",
            ResourcePreconditions::uid_and_resource_version(
                created.uid.clone(),
                created.resource_version,
            ),
        )
        .await
        .expect_err("stale delete precondition must be rejected");

    assert!(
        klights_cluster_datastore::errors::is_conflict_error(&err),
        "stale raft delete precondition must surface as conflict, got: {err:#}"
    );
    assert!(
        !err.to_string().contains("Query returned no rows"),
        "stale raft delete precondition must not leak sqlite no-rows as API/internal error: {err:#}"
    );
}

#[tokio::test]
async fn raft_mode_identical_normal_patch_does_not_advance_rv_or_watch() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let created = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "raft-identical-patch",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "raft-identical-patch",
                    "namespace": "default",
                    "uid": "raft-identical-patch-uid",
                    "annotations": {"example.test/value": "unchanged"}
                },
                "data": {"value": "before"}
            }),
        )
        .await
        .unwrap();
    let before_rv = ds.passive.get_current_resource_version().await.unwrap();
    let before_events = ds.list_all_watch_events_since(0).await.unwrap();
    calls.lock().unwrap().clear();

    let unchanged = ds
        .patch_resource_latest_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "raft-identical-patch",
            ResourcePatchRequest::new(
                PatchKind::Merge,
                json!({
                    "metadata": {
                        "annotations": {"example.test/value": "unchanged"}
                    }
                }),
                ResourcePreconditions::from_resource(&created),
            ),
        )
        .await
        .unwrap()
        .expect("ConfigMap must remain present");

    assert_eq!(calls.lock().unwrap().as_slice(), &["PatchResource"]);
    assert_eq!(
        unchanged.resource_version, created.resource_version,
        "an identical normalized patch must preserve the live resourceVersion"
    );
    assert_eq!(unchanged.data, created.data);
    assert_eq!(
        ds.passive.get_current_resource_version().await.unwrap(),
        before_rv,
        "an identical normalized patch must not consume a public resourceVersion"
    );
    assert_eq!(
        ds.list_all_watch_events_since(0).await.unwrap().len(),
        before_events.len(),
        "an identical normalized patch must not append a MODIFIED watch event"
    );

    let changed = ds
        .patch_resource_latest_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "raft-identical-patch",
            ResourcePatchRequest::new(
                PatchKind::Merge,
                json!({"data": {"value": "after"}}),
                ResourcePreconditions::from_resource(&unchanged),
            ),
        )
        .await
        .unwrap()
        .expect("ConfigMap must remain present");

    assert_eq!(changed.resource_version, before_rv + 1);
    assert_eq!(changed.data.pointer("/data/value"), Some(&json!("after")));
    assert_eq!(
        ds.list_all_watch_events_since(0).await.unwrap().len(),
        before_events.len() + 1,
        "a real patch must append exactly one MODIFIED watch event"
    );
}
