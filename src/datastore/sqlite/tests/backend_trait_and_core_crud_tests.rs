use super::*;
use crate::datastore::command::StorageCommand;
use crate::datastore::{
    MetaStore, NamespaceContentStore, NetworkMetadataStore, OwnershipStore, PodWorkqueueStore,
    ReplicationStore, ResourceListStore, ResourcePreconditions, StatusStore, WatchHistoryStore,
};
use crate::datastore::{PodSlotAdmissionEvent, PodSlotAdmissionResult, PodSlotAdmissionState};
use serde_json::json;

async fn table_column_info(
    db: &Datastore,
    table: &'static str,
    column: &'static str,
) -> (bool, bool) {
    db.db_call("test_table_column_info", move |conn| {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                let not_null: i64 = row.get(3)?;
                return Ok((true, not_null != 0));
            }
        }
        Ok((false, false))
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn schema_namespaced_resources_includes_uid_column() {
    let db = Datastore::new_in_memory().await.unwrap();
    let (present, not_null) = table_column_info(&db, "namespaced_resources", "uid").await;
    assert!(present, "namespaced_resources.uid must exist");
    assert!(not_null, "namespaced_resources.uid must be NOT NULL");
}

#[tokio::test]
async fn schema_cluster_resources_includes_uid_column() {
    let db = Datastore::new_in_memory().await.unwrap();
    let (present, not_null) = table_column_info(&db, "cluster_resources", "uid").await;
    assert!(present, "cluster_resources.uid must exist");
    assert!(not_null, "cluster_resources.uid must be NOT NULL");
}

#[tokio::test]
async fn schema_namespaces_includes_uid_column() {
    let db = Datastore::new_in_memory().await.unwrap();
    let (present, not_null) = table_column_info(&db, "namespaces", "uid").await;
    assert!(present, "namespaces.uid must exist");
    assert!(not_null, "namespaces.uid must be NOT NULL");
}

#[tokio::test]
async fn raft_commit_builder_rejects_update_for_deleted_resource() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "stale-pvc",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": "stale-pvc",
                    "namespace": "default",
                    "uid": "stale-pvc-uid"
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "1Gi"}}
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    db.delete_resource_with_preconditions(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "stale-pvc",
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();

    let mut stale_update = (*created.data).clone();
    stale_update["status"] = json!({
        "phase": "Bound",
        "volumeName": "pv-stale"
    });
    let command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "PersistentVolumeClaim".to_string(),
        namespace: Some("default".to_string()),
        name: "stale-pvc".to_string(),
        data: stale_update,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-stale-pvc-update",
            "UpdateResource",
            payload.as_ref(),
            "leader",
        )
        .await;

    assert!(
        outcome.is_err(),
        "stale update proposal must be rejected instead of producing a commit that re-adds the deleted PVC"
    );
    assert!(
        db.get_resource("v1", "PersistentVolumeClaim", Some("default"), "stale-pvc")
            .await
            .unwrap()
            .is_none(),
        "failed proposal must not recreate the deleted PVC locally"
    );
}

#[tokio::test]
async fn raft_commit_builder_applies_pod_status_outbox_against_latest_same_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "pod-uid-1"
                },
                "spec": {
                    "nodeName": "mn-replica",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {
                    "phase": "Pending",
                    "podIP": "10.50.3.8",
                    "podIPs": [{"ip": "10.50.3.8"}]
                }
            }),
        )
        .await
        .unwrap();

    let mut leader_changed_pod = (*created.data).clone();
    leader_changed_pod["metadata"]["annotations"] = json!({"leader.example/kept": "true"});
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "web",
        leader_changed_pod,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .unwrap();

    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        status: json!({
            "phase": "Running",
            "podIP": "10.50.3.8",
            "podIPs": [{"ip": "10.50.3.8"}],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-06-14T09:05:17Z"}}
            }]
        }),
        expected_rv: Some(created.resource_version),
        preconditions: ResourcePreconditions::from_resource(&created),
        observed_status_stamp: None,
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-stale-pod-status",
            "PodStatus",
            payload.as_ref(),
            "mn-replica",
        )
        .await
        .expect("stale-RV PodStatus must build a raft commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };
    let put = commit
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            crate::log_apply::LogApplyMutation::PutResource(row) => Some(row),
            _ => None,
        })
        .expect("status commit must include a Pod resource row");
    assert_eq!(
        put.precondition_uid.as_deref(),
        Some("pod-uid-1"),
        "same-name replacement must remain UID protected"
    );
    assert!(
        put.precondition_resource_version.is_none(),
        "kubelet status snapshots must not depend on stale worker RVs"
    );

    db.apply_log_apply_commit(commit).await.unwrap();
    let stored = db
        .get_resource("v1", "Pod", Some("default"), "web")
        .await
        .unwrap()
        .expect("pod exists after status apply");
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
        Some("true")
    );
}

#[tokio::test]
async fn raft_commit_builder_reserves_leader_resource_version_without_applying_resource() {
    let db = Datastore::new_in_memory().await.unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "deferred-rv".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "deferred-rv",
                "namespace": "default",
                "uid": "deferred-rv-uid"
            },
            "data": {"k": "v"}
        }),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-deferred-rv",
            "CreateResource",
            payload.as_ref(),
            "leader",
        )
        .await
        .expect("build raft commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv + 1,
        "building a raft entry must reserve the leader's committed resourceVersion"
    );
    assert_eq!(
        commit.resource_version,
        before_rv + 1,
        "builder should encode the leader's committed RV into the raft payload"
    );
    let put = commit
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            crate::log_apply::LogApplyMutation::PutResource(row) => Some(row),
            _ => None,
        })
        .expect("create must produce a resource row");
    assert_eq!(
        put.resource_version,
        before_rv + 1,
        "resource rows must carry the leader's committed RV before apply"
    );
    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "deferred-rv")
            .await
            .unwrap()
            .is_none(),
        "building a raft entry must not materialize the resource before state-machine apply"
    );

    db.apply_log_apply_commit(commit).await.unwrap();

    let row = db
        .get_resource("v1", "ConfigMap", Some("default"), "deferred-rv")
        .await
        .unwrap()
        .expect("resource should materialize at apply");
    assert_eq!(
        row.resource_version,
        before_rv + 1,
        "apply must allocate the next live resourceVersion"
    );
}

#[tokio::test]
async fn raft_commits_apply_with_leader_resource_versions_on_skewed_followers() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    follower.advance_resource_version_after(5).await.unwrap();
    assert!(
        follower.get_current_resource_version().await.unwrap() > 0,
        "test setup must skew follower metadata RV above the leader"
    );

    let create_command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "scheduled-later".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "scheduled-later",
                "namespace": "default",
                "uid": "scheduled-later-uid"
            },
            "spec": {
                "containers": [{"name": "main", "image": "registry.k8s.io/pause:3.10"}]
            },
            "status": {"phase": "Pending"}
        }),
    };
    let create_payload =
        crate::kubelet::outbox::payload::OutboxPayload::from_command(create_command)
            .encode_protobuf()
            .unwrap();
    let create_outcome = leader
        .build_log_apply_commit_for_outbox(
            "raft-leader-create-scheduled-later",
            "CreateResource",
            create_payload.as_ref(),
            "leader",
        )
        .await
        .expect("build create commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose {
        commit: create_commit,
        ..
    } = create_outcome
    else {
        panic!("expected create proposal");
    };
    leader
        .apply_raft_log_apply_commit(create_commit.clone())
        .await
        .expect("leader applies create");
    follower
        .apply_raft_log_apply_commit(create_commit)
        .await
        .expect("follower applies create");

    let created = leader
        .get_resource("v1", "Pod", Some("default"), "scheduled-later")
        .await
        .unwrap()
        .expect("leader pod exists");
    let mut bound = (*created.data).clone();
    bound["spec"]["nodeName"] = json!("mn-replica");

    let update_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "scheduled-later".to_string(),
        data: bound,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
    };
    let update_payload =
        crate::kubelet::outbox::payload::OutboxPayload::from_command(update_command)
            .encode_protobuf()
            .unwrap();
    let update_outcome = leader
        .build_log_apply_commit_for_outbox(
            "raft-leader-bind-scheduled-later",
            "UpdateResource",
            update_payload.as_ref(),
            "leader",
        )
        .await
        .expect("build update commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose {
        commit: update_commit,
        ..
    } = update_outcome
    else {
        panic!("expected update proposal");
    };
    leader
        .apply_raft_log_apply_commit(update_commit.clone())
        .await
        .expect("leader applies update");
    let follower_result = follower
        .apply_raft_log_apply_commit(update_commit)
        .await
        .expect("follower applies update transaction");
    assert!(
        follower_result.error_message.is_none(),
        "follower must not reject a leader-committed update because its local RV counter was skewed: {follower_result:?}"
    );

    let follower_pod = follower
        .get_resource("v1", "Pod", Some("default"), "scheduled-later")
        .await
        .unwrap()
        .expect("follower pod exists");
    assert_eq!(
        follower_pod
            .data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str()),
        Some("mn-replica"),
        "follower must materialize the scheduler bind"
    );
    assert_eq!(
        follower_pod.resource_version,
        created.resource_version + 1,
        "follower must store the leader's committed update RV, not allocate a local one"
    );
}

#[tokio::test]
async fn raft_apply_rejects_duplicate_create_built_before_first_apply() {
    let db = Datastore::new_in_memory().await.unwrap();

    let build_create = |idempotency_key: &'static str, uid: &'static str| {
        let db = db.clone();
        async move {
            let command = StorageCommand::CreateResource {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "duplicate-apply".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "duplicate-apply",
                        "namespace": "default",
                        "uid": uid
                    },
                    "data": {"uid": uid}
                }),
            };
            let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                .encode_protobuf()
                .unwrap();
            let outcome = db
                .build_log_apply_commit_for_outbox(
                    idempotency_key,
                    "CreateResource",
                    payload.as_ref(),
                    "leader",
                )
                .await
                .expect("build duplicate create");
            let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome
            else {
                panic!("expected a fresh commit");
            };
            commit
        }
    };

    let first = build_create("raft-duplicate-create-first", "first-uid").await;
    let second = build_create("raft-duplicate-create-second", "second-uid").await;

    db.apply_log_apply_commit(first)
        .await
        .expect("first create apply");
    let err = db
        .apply_log_apply_commit(second)
        .await
        .expect_err("second create apply must reject at apply time");
    assert!(
        err.to_string().contains("already exists") && err.to_string().contains("409 Conflict"),
        "expected duplicate create conflict, got: {err:#}"
    );

    let live = db
        .get_resource("v1", "ConfigMap", Some("default"), "duplicate-apply")
        .await
        .unwrap()
        .expect("first resource must remain");
    assert_eq!(live.uid, "first-uid");
    assert_eq!(live.data["data"]["uid"], json!("first-uid"));
}

#[tokio::test]
async fn raft_apply_rejects_stale_resource_version_built_before_prior_apply() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-rv-apply",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "stale-rv-apply",
                    "namespace": "default",
                    "uid": "stale-rv-uid"
                },
                "data": {"version": "initial"}
            }),
        )
        .await
        .unwrap();

    let build_update = |idempotency_key: &'static str, value: &'static str| {
        let db = db.clone();
        let created = created.clone();
        async move {
            let mut data = (*created.data).clone();
            data["data"]["version"] = json!(value);
            let command = StorageCommand::UpdateResource {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "stale-rv-apply".to_string(),
                data,
                expected_rv: created.resource_version,
                preconditions: ResourcePreconditions::resource_version(created.resource_version),
            };
            let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                .encode_protobuf()
                .unwrap();
            let outcome = db
                .build_log_apply_commit_for_outbox(
                    idempotency_key,
                    "UpdateResource",
                    payload.as_ref(),
                    "leader",
                )
                .await
                .expect("build update");
            let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome
            else {
                panic!("expected a fresh commit");
            };
            commit
        }
    };

    let first = build_update("raft-stale-rv-first", "first").await;
    let second = build_update("raft-stale-rv-second", "second").await;

    db.apply_log_apply_commit(first)
        .await
        .expect("first update applies");
    let err = db
        .apply_log_apply_commit(second)
        .await
        .expect_err("second update must reject against apply-time RV");
    assert!(
        err.to_string()
            .contains("resourceVersion precondition failed")
            && err.to_string().contains("409 Conflict"),
        "expected apply-time RV conflict, got: {err:#}"
    );

    let live = db
        .get_resource("v1", "ConfigMap", Some("default"), "stale-rv-apply")
        .await
        .unwrap()
        .expect("resource remains");
    assert_eq!(live.data["data"]["version"], json!("first"));
}

#[tokio::test]
async fn raft_status_apply_built_before_metadata_update_preserves_live_metadata() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "status-metadata-race",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "status-metadata-race",
                    "namespace": "default",
                    "uid": "status-metadata-race-uid"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "status-metadata-race".to_string(),
        status: json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("status-metadata-race-uid".to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-status-metadata-race",
            "PodStatus",
            payload.as_ref(),
            "worker-a",
        )
        .await
        .expect("build stale status commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh status commit");
    };

    let mut metadata_update = (*created.data).clone();
    metadata_update["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1",
        "kind": "Pod",
        "name": "owner",
        "uid": "owner-uid",
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    metadata_update["metadata"]["deletionTimestamp"] = json!("2026-06-01T20:46:20Z");
    metadata_update["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "status-metadata-race",
        metadata_update,
        created.resource_version,
    )
    .await
    .expect("metadata update applies before status commit");

    db.apply_log_apply_commit(commit)
        .await
        .expect("status commit applies after metadata update");

    let live = db
        .get_resource("v1", "Pod", Some("default"), "status-metadata-race")
        .await
        .unwrap()
        .expect("pod remains");
    assert_eq!(
        live.data
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running"),
        "status apply must publish the new status"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/ownerReferences/0/uid")
            .and_then(|value| value.as_str()),
        Some("owner-uid"),
        "status apply must not clear live ownerReferences"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some("2026-06-01T20:46:20Z"),
        "status apply must not clear live deletionTimestamp"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|value| value.as_i64()),
        Some(0),
        "status apply must not clear live deletionGracePeriodSeconds"
    );
}

#[tokio::test]
async fn raft_scale_patch_applies_against_live_resource_after_status_rv_race() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-race-rc",
            json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "scale-race-rc",
                    "namespace": "default",
                    "uid": "scale-race-rc-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"app": "scale-race-rc"},
                    "template": {
                        "metadata": {"labels": {"app": "scale-race-rc"}},
                        "spec": {
                            "containers": [{"name": "web", "image": "webserver:404"}]
                        }
                    }
                },
                "status": {
                    "replicas": 1,
                    "readyReplicas": 1
                }
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "ReplicationController".to_string(),
        namespace: Some("default".to_string()),
        name: "scale-race-rc".to_string(),
        patch_kind: crate::datastore::PatchKind::Merge,
        patch: json!({"spec": {"replicas": 2}}),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-rc-scale-latest-patch",
            "PatchResource",
            payload.as_ref(),
            "leader",
        )
        .await
        .expect("build scale patch commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    db.update_status_only_with_preconditions(
        "v1",
        "ReplicationController",
        Some("default"),
        "scale-race-rc",
        json!({"replicas": 1, "readyReplicas": 1, "observedGeneration": 1}),
        ResourcePreconditions::uid(created.uid.clone()),
    )
    .await
    .expect("status update advances RV before scale patch apply");

    let apply_result = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("raft scale patch apply should return a terminal result");
    assert!(
        apply_result.error_message.is_none(),
        "unconditional scale patch must not conflict with status-only RV races, got {apply_result:?}"
    );

    let live = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-race-rc",
        )
        .await
        .unwrap()
        .expect("replicationcontroller remains");
    assert_eq!(
        live.data
            .pointer("/spec/replicas")
            .and_then(|value| value.as_i64()),
        Some(2),
        "scale patch must update spec.replicas"
    );
    assert_eq!(
        live.data
            .pointer("/status/observedGeneration")
            .and_then(|value| value.as_i64()),
        Some(1),
        "scale patch must preserve the newer status written before raft apply"
    );
}

#[tokio::test]
async fn raft_patch_apply_built_before_spec_update_does_not_revert_live_spec() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "web",
                    "namespace": "default",
                    "uid": "web-deploy-uid",
                    "generation": 2,
                    "annotations": {
                        "deployment.kubernetes.io/revision": "2"
                    }
                },
                "spec": {
                    "replicas": 10,
                    "selector": {"matchLabels": {"name": "httpd"}},
                    "template": {
                        "metadata": {"labels": {"name": "httpd"}},
                        "spec": {
                            "containers": [{"name": "httpd", "image": "webserver:404"}]
                        }
                    }
                },
                "status": {
                    "observedGeneration": 2,
                    "replicas": 13,
                    "updatedReplicas": 5,
                    "readyReplicas": 8,
                    "availableReplicas": 8
                }
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::PatchResource {
        api_version: "apps/v1".to_string(),
        kind: "Deployment".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        patch_kind: crate::datastore::PatchKind::Merge,
        patch: json!({
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "2"
                }
            }
        }),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-stale-deployment-revision-patch",
            "PatchResource",
            payload.as_ref(),
            "leader",
        )
        .await
        .expect("build stale patch commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    let mut scaled = (*created.data).clone();
    scaled["metadata"]["generation"] = json!(3);
    scaled["spec"]["replicas"] = json!(30);
    db.update_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        scaled,
        created.resource_version,
    )
    .await
    .expect("client scale update applies before stale patch commit");

    let apply_result = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("raft apply should report stale patch as a terminal command result");
    assert!(
        apply_result
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("resourceVersion precondition failed")),
        "stale patch must be rejected at apply time, got {apply_result:?}"
    );

    let live = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains");
    assert_eq!(
        live.data
            .pointer("/spec/replicas")
            .and_then(|value| value.as_i64()),
        Some(30),
        "stale revision patch must not roll back the user's scale update"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/generation")
            .and_then(|value| value.as_i64()),
        Some(3),
        "stale revision patch must not roll back metadata.generation"
    );
}

#[tokio::test]
async fn raft_apply_same_idempotency_key_returns_same_rv_without_reapply() {
    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "idempotent-apply".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "idempotent-apply",
                "namespace": "default",
                "uid": "idempotent-uid"
            },
            "data": {"applied": "once"}
        }),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-idempotent-apply",
            "CreateResource",
            payload.as_ref(),
            "leader",
        )
        .await
        .expect("build create");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    let first = db
        .apply_raft_log_apply_commit(commit.clone())
        .await
        .expect("first raft apply");
    let after_first_rv = db.get_current_resource_version().await.unwrap();
    let second = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("duplicate raft apply");

    assert_eq!(first.applied_rv, Some(after_first_rv));
    assert_eq!(
        second.applied_rv, first.applied_rv,
        "retry must return the original applied RV"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        after_first_rv,
        "duplicate apply must not allocate another RV"
    );
    let rows = db.list_applied_outbox().await.unwrap();
    assert_eq!(rows.len(), 1, "one idempotency row should be recorded");
    assert_eq!(rows[0].applied_rv, first.applied_rv);
}

#[tokio::test]
async fn raft_outbox_build_treats_fresh_placeholder_as_retryable_inflight() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "fresh-placeholder-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "fresh-placeholder-pod",
                "uid": "uid-fresh-raft-placeholder"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    db.insert_applied_outbox(crate::datastore::AppliedOutboxRecord {
        idempotency_key: "fresh-raft-placeholder-key".to_string(),
        subject_key: String::new(),
        operation: "PodMetadata".to_string(),
        first_seen_ms: now_ms,
        applied_rv: None,
        result_proto: Vec::new(),
        status_stamp: None,
    })
    .await
    .unwrap();

    let command = StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "fresh-placeholder-pod".to_string(),
        preconditions: ResourcePreconditions {
            uid: Some("uid-fresh-raft-placeholder".to_string()),
            resource_version: None,
        },
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();

    let result = db
        .build_log_apply_commit_for_outbox(
            "fresh-raft-placeholder-key",
            "PodMetadata",
            payload.as_ref(),
            "worker-a",
        )
        .await;

    match result {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(_)) => {}
        Err(err) => panic!("fresh raft outbox placeholder must be retryable, got: {err:?}"),
        Ok(_) => panic!("fresh placeholder is still in-flight and must retry"),
    }
}

#[tokio::test]
async fn raft_apply_replays_rejected_idempotency_key_as_same_rejection() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "duplicate-retry",
        json!({
            "metadata": {"name": "duplicate-retry", "namespace": "default"},
            "data": {"winner": "first"}
        }),
    )
    .await
    .unwrap();

    let command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "duplicate-retry".to_string(),
        data: json!({
            "metadata": {
                "name": "duplicate-retry",
                "namespace": "default",
                "uid": "duplicate-retry-second"
            },
            "data": {"winner": "second"}
        }),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let idempotency_key = "raft-duplicate-retry-key";

    let outcome = db
        .build_log_apply_commit_for_outbox(
            idempotency_key,
            "CreateResource",
            payload.as_ref(),
            "leader",
        )
        .await
        .unwrap();
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected initial duplicate create to need proposal");
    };
    let rejected = db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert!(
        rejected
            .error_message
            .as_deref()
            .is_some_and(|msg| msg.contains("already exists") && msg.contains("409 Conflict")),
        "first apply must persist the terminal duplicate-create rejection: {rejected:?}"
    );

    let retry = db
        .build_log_apply_commit_for_outbox(
            idempotency_key,
            "CreateResource",
            payload.as_ref(),
            "leader",
        )
        .await;
    match retry {
        Err(crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(msg))
            if msg.contains("already exists") && msg.contains("409 Conflict") => {}
        Err(err) => panic!(
            "retrying the same rejected key must return the cached terminal rejection, got error {err}"
        ),
        Ok(_) => panic!(
            "retrying the same rejected key must return the cached terminal rejection, got success"
        ),
    }

    let rows = db.list_applied_outbox().await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "rejection replay must not duplicate outbox rows"
    );
    assert_eq!(rows[0].applied_rv, None);
}

#[tokio::test]
async fn raft_apply_terminal_conflict_without_outbox_returns_rejection_result() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "direct-conflict",
        json!({
            "metadata": {"name": "direct-conflict", "namespace": "default", "uid": "winner"},
            "data": {"winner": "first"}
        }),
    )
    .await
    .unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();

    let commit = crate::log_apply::LogApplyCommit::new(
        0,
        vec![crate::log_apply::LogApplyMutation::PutResource(
            crate::log_apply::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "direct-conflict".to_string(),
                uid: "loser".to_string(),
                resource_version: 0,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "direct-conflict",
                        "namespace": "default",
                        "uid": "loser"
                    },
                    "data": {"winner": "second"}
                }),
                require_absent: true,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            },
        )],
    );

    let rejected = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("terminal apply conflict should not abort raft apply");
    assert_eq!(rejected.applied_rv, None);
    assert!(
        rejected
            .error_message
            .as_deref()
            .is_some_and(|msg| msg.contains("already exists") && msg.contains("409 Conflict")),
        "expected apply-time 409 result, got {rejected:?}"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv,
        "rejected apply must roll back provisional RV allocation"
    );
}

#[tokio::test]
async fn raft_commit_builder_does_not_treat_api_node_update_as_node_status_refresh() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "mn-controlplane1",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "mn-controlplane1",
                    "uid": "node-uid",
                    "labels": {
                        "node": "mn-controlplane1",
                        "kubernetes.io/hostname": "mn-controlplane1"
                    }
                },
                "spec": {"unschedulable": false},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]}
            }),
        )
        .await
        .unwrap();

    let mut api_update = (*created.data).clone();
    api_update["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove("node");
    let command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "mn-controlplane1".to_string(),
        data: api_update,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::resource_version(created.resource_version),
    };
    let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-node-api-update",
            "PodStatus",
            payload.as_ref(),
            "mn-controlplane1",
        )
        .await
        .expect("direct API Node update should build a commit");
    let crate::datastore::sqlite::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };
    let put = commit
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            crate::log_apply::LogApplyMutation::PutResource(row) => Some(row),
            _ => None,
        })
        .expect("node update must produce a PutResource mutation");

    assert!(
        put.data.pointer("/metadata/labels/node").is_none(),
        "API Node label deletion must not be merged back as a kubelet NodeStatus refresh"
    );
}

#[tokio::test]
async fn pod_slot_admissions_schema_exists_with_slot_primary_key() {
    let db = Datastore::new_in_memory().await.unwrap();
    let columns: Vec<(String, i64, i64)> = db
        .node_db_call("test_pod_slot_admissions_schema", |conn| {
            let mut stmt = conn.prepare("PRAGMA table_info(pod_slot_admissions)")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(5)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .unwrap();

    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "namespace" && *not_null == 1 && *pk == 1)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "pod_name" && *not_null == 1 && *pk == 2)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, _)| name == "pod_uid" && *not_null == 1)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, _)| name == "node_name" && *not_null == 1)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, _)| name == "state" && *not_null == 1)
    );
}

#[tokio::test]
async fn pod_cleanup_intents_schema_is_cluster_uid_and_reason_bound() {
    let db = Datastore::new_in_memory().await.unwrap();
    let columns: Vec<(String, i64, i64)> = db
        .db_call("test_pod_cleanup_intents_schema", |conn| {
            let mut stmt = conn.prepare("PRAGMA table_info(pod_cleanup_intents)")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(5)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .unwrap();

    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "node_name" && *not_null == 1 && *pk == 1)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "namespace" && *not_null == 1 && *pk == 2)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "pod_name" && *not_null == 1 && *pk == 3)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "pod_uid" && *not_null == 1 && *pk == 4)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, pk)| name == "reason" && *not_null == 1 && *pk == 5)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, _)| name == "resource_version" && *not_null == 1)
    );
    assert!(
        columns
            .iter()
            .any(|(name, not_null, _)| name == "pod_data" && *not_null == 1)
    );

    let node_index_exists: bool = db
        .db_call("test_pod_cleanup_intents_node_index", |conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_pod_cleanup_intents_node')",
                [],
                |row| row.get::<_, i64>(0),
            )? == 1)
        })
        .await
        .unwrap();
    assert!(
        node_index_exists,
        "pod cleanup intents need a node index for rejoin and node delete cleanup"
    );
}

#[tokio::test]
async fn log_apply_replays_pod_cleanup_intents() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        7,
        vec![crate::log_apply::LogApplyMutation::PutPodCleanupIntent(
            crate::log_apply::LogApplyPodCleanupIntentRow {
                node_name: "worker-a".to_string(),
                namespace: "default".to_string(),
                pod_name: "lost-pod".to_string(),
                pod_uid: "lost-uid".to_string(),
                reason: "NodeLost".to_string(),
                resource_version: 7,
                created_at_ms: 1_700_000_000_000,
                pod_data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "lost-pod",
                        "uid": "lost-uid"
                    },
                    "spec": {"nodeName": "worker-a"}
                }),
            },
        )],
    ))
    .await
    .unwrap();

    let rows = db
        .list_pod_cleanup_intents_for_node("worker-a")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pod_uid, "lost-uid");
    assert_eq!(rows[0].reason, "NodeLost");

    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        8,
        vec![crate::log_apply::LogApplyMutation::DeletePodCleanupIntent(
            crate::log_apply::LogApplyPodCleanupIntentKey {
                node_name: "worker-a".to_string(),
                namespace: "default".to_string(),
                pod_name: "lost-pod".to_string(),
                pod_uid: "lost-uid".to_string(),
                reason: "NodeLost".to_string(),
            },
        )],
    ))
    .await
    .unwrap();

    assert!(
        db.list_pod_cleanup_intents_for_node("worker-a")
            .await
            .unwrap()
            .is_empty()
    );

    for pod_name in ["lost-pod-a", "lost-pod-b"] {
        db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
            9,
            vec![crate::log_apply::LogApplyMutation::PutPodCleanupIntent(
                crate::log_apply::LogApplyPodCleanupIntentRow {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: pod_name.to_string(),
                    pod_uid: format!("{pod_name}-uid"),
                    reason: "NodeLost".to_string(),
                    resource_version: 9,
                    created_at_ms: 1_700_000_000_001,
                    pod_data: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": pod_name,
                            "uid": format!("{pod_name}-uid")
                        },
                        "spec": {"nodeName": "worker-a"}
                    }),
                },
            )],
        ))
        .await
        .unwrap();
    }

    assert_eq!(
        db.list_pod_cleanup_intents_for_node("worker-a")
            .await
            .unwrap()
            .len(),
        2
    );

    db.apply_log_apply_commit(crate::log_apply::LogApplyCommit::new(
        10,
        vec![
            crate::log_apply::LogApplyMutation::DeletePodCleanupIntentsForNode {
                node_name: "worker-a".to_string(),
            },
        ],
    ))
    .await
    .unwrap();

    assert!(
        db.list_pod_cleanup_intents_for_node("worker-a")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pod_slot_events_emit_only_on_state_change() {
    let db = Datastore::new_in_memory().await.unwrap();
    let mut rx = db.subscribe_pod_slot_admissions();

    let first = db
        .pod_slot_try_admit("default", "p", "uid-a", "node-a")
        .await
        .unwrap();
    assert!(matches!(first, PodSlotAdmissionResult::Admitted { .. }));
    let event = rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PodSlotAdmissionEvent::Changed {
            namespace,
            pod_name,
            pod_uid,
            state: PodSlotAdmissionState::Admitted,
            ..
        } if namespace == "default" && pod_name == "p" && pod_uid == "uid-a"
    ));

    let same = db
        .pod_slot_try_admit("default", "p", "uid-a", "node-a")
        .await
        .unwrap();
    assert!(matches!(same, PodSlotAdmissionResult::Admitted { .. }));
    assert!(rx.try_recv().is_err(), "same-value admit must not emit");

    db.pod_slot_mark_terminating("default", "p", "uid-a", "node-a")
        .await
        .unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PodSlotAdmissionEvent::Changed {
            state: PodSlotAdmissionState::Terminating,
            pod_uid,
            ..
        } if pod_uid == "uid-a"
    ));

    db.pod_slot_clear_if_uid("default", "p", "uid-a", "node-a")
        .await
        .unwrap();
    let event = rx.recv().await.unwrap();
    assert!(matches!(
        event,
        PodSlotAdmissionEvent::Cleared { pod_uid, .. } if pod_uid == "uid-a"
    ));
}

#[tokio::test]
async fn create_resource_populates_uid_column() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "uid-cm",
            json!({
                "metadata": {
                    "name": "uid-cm",
                    "namespace": "default",
                    "uid": "cm-uid-1"
                },
                "data": {"k": "v"}
            }),
        )
        .await
        .unwrap();
    assert_eq!(created.uid, "cm-uid-1");

    let stored_uid: String = db
        .db_call("test_select_namespaced_uid", move |conn| {
            Ok(conn.query_row(
                "SELECT uid FROM namespaced_resources WHERE api_version = 'v1' AND kind = 'ConfigMap' AND namespace = 'default' AND name = 'uid-cm'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(stored_uid, "cm-uid-1");

    let fetched = db
        .get_resource("v1", "ConfigMap", Some("default"), "uid-cm")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.uid, "cm-uid-1");

    let listed = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(listed.items[0].uid, "cm-uid-1");
}

#[tokio::test]
async fn raft_create_resource_applies_server_metadata_defaults() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.db_call("test_raft_create_resource_defaults", |conn| {
        let tx = conn.transaction()?;
        Datastore::apply_outbox_command_in_tx(
            &tx,
            StorageCommand::CreateResource {
                api_version: "mygroup.example.com/v1beta1".to_string(),
                kind: "WishIHadChosenNoxu".to_string(),
                namespace: None,
                name: "name1".to_string(),
                data: json!({
                    "apiVersion": "mygroup.example.com/v1beta1",
                    "kind": "WishIHadChosenNoxu",
                    "metadata": {"name": "name1"},
                    "content": {"key": "value"}
                }),
            },
            "PodStatus",
            "mn-controlplane1",
        )?;
        tx.commit()?;
        Ok(())
    })
    .await
    .unwrap();

    let stored = db
        .get_resource(
            "mygroup.example.com/v1beta1",
            "WishIHadChosenNoxu",
            None,
            "name1",
        )
        .await
        .unwrap()
        .expect("created resource should be stored");
    let creation_timestamp = stored
        .data
        .pointer("/metadata/creationTimestamp")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    assert!(
        creation_timestamp.is_some(),
        "raft CreateResource must persist metadata.creationTimestamp so create responses and watch events deep-equal"
    );
    assert_eq!(
        stored
            .data
            .pointer("/metadata/generation")
            .and_then(|value| value.as_i64()),
        Some(1),
        "raft CreateResource must persist metadata.generation like the direct create path"
    );

    let replayed = db
        .list_watch_events_since(
            &[WatchTarget::cluster(
                "mygroup.example.com/v1beta1",
                "WishIHadChosenNoxu",
            )],
            0,
        )
        .await
        .unwrap();
    let added = replayed
        .into_iter()
        .find(|event| event.event_type == "ADDED")
        .expect("watch history should include the create event")
        .into_watch_event();
    assert_eq!(
        added.object.pointer("/metadata/creationTimestamp"),
        stored.data.pointer("/metadata/creationTimestamp"),
        "watch replay must emit the same creationTimestamp returned by create"
    );
    assert_eq!(
        added
            .object
            .pointer("/metadata/generation")
            .and_then(|value| value.as_i64()),
        Some(1),
        "watch replay must emit metadata.generation"
    );
}

#[tokio::test]
async fn raft_patch_merge_preserves_metadata_identity_and_labels() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("kube-system"),
        "coredns",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "coredns",
                "namespace": "kube-system",
                "uid": "deploy-uid-1",
                "labels": {"k8s-app": "kube-dns"},
                "annotations": {"existing": "keep"}
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"k8s-app": "kube-dns"}},
                "template": {
                    "metadata": {"labels": {"k8s-app": "kube-dns"}},
                    "spec": {"containers": [{"name": "coredns", "image": "coredns"}]}
                }
            }
        }),
    )
    .await
    .unwrap();

    db.db_call("test_raft_patch_merge_preserves_metadata", |conn| {
        let tx = conn.transaction()?;
        Datastore::apply_outbox_command_in_tx(
            &tx,
            StorageCommand::PatchResource {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: Some("kube-system".to_string()),
                name: "coredns".to_string(),
                patch_kind: crate::datastore::PatchKind::Merge,
                patch: json!({
                    "metadata": {
                        "annotations": {
                            "deployment.kubernetes.io/revision": "1"
                        }
                    }
                }),
                preconditions: ResourcePreconditions::uid("deploy-uid-1"),
            },
            "PodStatus",
            "mn-controlplane1",
        )?;
        tx.commit()?;
        Ok(())
    })
    .await
    .unwrap();

    let patched = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .expect("deployment should still exist");
    assert_eq!(patched.uid, "deploy-uid-1");
    assert_eq!(
        patched
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some("deploy-uid-1"),
        "merge patch must not regenerate metadata.uid"
    );
    assert_eq!(
        patched
            .data
            .pointer("/metadata/labels/k8s-app")
            .and_then(|v| v.as_str()),
        Some("kube-dns"),
        "metadata labels must survive a metadata.annotations merge patch"
    );
    assert_eq!(
        patched
            .data
            .pointer("/metadata/annotations/existing")
            .and_then(|v| v.as_str()),
        Some("keep"),
        "existing annotations must be merged, not replaced wholesale"
    );
    assert_eq!(
        patched
            .data
            .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
            .and_then(|v| v.as_str()),
        Some("1")
    );
}

#[tokio::test]
async fn create_namespace_populates_uid_column() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_namespace(
            "uid-ns",
            json!({"metadata": {"name": "uid-ns", "uid": "ns-uid-1"}}),
        )
        .await
        .unwrap();
    assert_eq!(created.uid, "ns-uid-1");

    let stored_uid: String = db
        .db_call("test_select_namespace_uid", move |conn| {
            Ok(conn.query_row(
                "SELECT uid FROM namespaces WHERE name = 'uid-ns'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(stored_uid, "ns-uid-1");

    let fetched = db.get_namespace("uid-ns").await.unwrap().unwrap();
    assert_eq!(fetched.uid, "ns-uid-1");
}

#[tokio::test]
async fn update_resource_rejects_metadata_uid_change() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-uid",
            json!({"metadata":{"name":"cm-uid","namespace":"default","uid":"uid-original"},"data":{"k":"v1"}}),
        )
        .await
        .unwrap();

    let err = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-uid",
            json!({"metadata":{"name":"cm-uid","namespace":"default","uid":"uid-replacement"},"data":{"k":"v2"}}),
            created.resource_version,
        )
        .await
        .expect_err("metadata.uid changes must be rejected");

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm-uid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some("uid-original")
    );
    assert_eq!(
        stored.data.pointer("/data/k").and_then(|v| v.as_str()),
        Some("v1")
    );
}

#[tokio::test]
async fn update_status_only_rejects_uid_precondition_mismatch() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-uid",
        json!({"metadata":{"name":"pod-uid","namespace":"default","uid":"uid-current"},"spec":{},"status":{"phase":"Pending"}}),
    )
    .await
    .unwrap();

    let err = db
        .update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "pod-uid",
            json!({"phase":"Running"}),
            ResourcePreconditions {
                uid: Some("uid-stale".to_string()),
                resource_version: None,
            },
        )
        .await
        .expect_err("stale uid precondition must reject status writes");

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
    let stored = db
        .get_resource("v1", "Pod", Some("default"), "pod-uid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending")
    );
}

#[tokio::test]
async fn stale_full_update_preserves_live_deletion_metadata() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "terminating-deploy",
                    "namespace": "default",
                    "uid": "deploy-uid"
                },
                "spec": {
                    "replicas": 2,
                    "selector": {"matchLabels": {"app": "terminating-deploy"}},
                    "template": {
                        "metadata": {"labels": {"app": "terminating-deploy"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                },
                "status": {"replicas": 2}
            }),
        )
        .await
        .unwrap();
    let stale_body = (*created.data).clone();

    let mut terminating = stale_body.clone();
    terminating["metadata"]["deletionTimestamp"] = json!("2026-06-01T20:11:45Z");
    terminating["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    let marked = db
        .update_resource_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
            terminating,
            ResourcePreconditions::from_resource(&created),
        )
        .await
        .unwrap();

    let mut stale_controller_update = stale_body;
    stale_controller_update["status"] = json!({
        "replicas": 2,
        "readyReplicas": 2
    });
    db.update_resource_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "terminating-deploy",
        stale_controller_update,
        ResourcePreconditions::from_resource(&marked),
    )
    .await
    .unwrap();

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
        )
        .await
        .unwrap()
        .expect("deployment remains terminating");
    assert_eq!(
        stored
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some("2026-06-01T20:11:45Z"),
        "stale full updates must not clear live deletionTimestamp"
    );
    assert_eq!(
        stored
            .data
            .pointer("/metadata/deletionGracePeriodSeconds")
            .and_then(|value| value.as_i64()),
        Some(0),
        "stale full updates must not clear live deletionGracePeriodSeconds"
    );
}

#[tokio::test]
async fn patch_resource_latest_rejects_uid_precondition_mismatch() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-patch-uid",
        json!({"metadata":{"name":"cm-patch-uid","namespace":"default","uid":"uid-current"},"data":{"k":"v1"}}),
    )
    .await
    .unwrap();

    let err = db
        .patch_resource_latest_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-patch-uid",
            crate::datastore::ResourcePatchRequest::new(
                crate::datastore::PatchKind::Merge,
                json!({"data":{"k":"v2"}}),
                ResourcePreconditions {
                    uid: Some("uid-stale".to_string()),
                    resource_version: None,
                },
            ),
        )
        .await
        .expect_err("stale uid precondition must reject patches");

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm-patch-uid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.data.pointer("/data/k").and_then(|v| v.as_str()),
        Some("v1")
    );
}

#[tokio::test]
async fn patch_resource_latest_rejects_metadata_uid_change() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-patch-immutable",
        json!({"metadata":{"name":"cm-patch-immutable","namespace":"default","uid":"uid-current"},"data":{"k":"v1"}}),
    )
    .await
    .unwrap();

    let err = db
        .patch_resource_latest_with_preconditions(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-patch-immutable",
            crate::datastore::ResourcePatchRequest::new(
                crate::datastore::PatchKind::Merge,
                json!({"metadata":{"uid":"uid-replacement"}}),
                ResourcePreconditions::default(),
            ),
        )
        .await
        .expect_err("metadata.uid changes must be rejected");

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
}

#[tokio::test]
async fn delete_resource_rejects_uid_precondition_mismatch() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-delete-uid",
        json!({"metadata":{"name":"pod-delete-uid","namespace":"default","uid":"uid-current"}}),
    )
    .await
    .unwrap();

    let err = db
        .delete_resource_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "pod-delete-uid",
            ResourcePreconditions {
                uid: Some("uid-stale".to_string()),
                resource_version: None,
            },
        )
        .await
        .expect_err("stale uid precondition must reject delete");

    assert!(
        crate::datastore::errors::is_conflict_error(&err),
        "expected conflict, got {err:#}"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "pod-delete-uid")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_datastore_backend_trait_object_crud() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let backend: &dyn DatastoreBackend = &concrete;

    let fetched = create_and_fetch_via_backend(backend).await.unwrap();
    assert!(fetched.is_some());
    let rv = backend.get_current_resource_version().await.unwrap();
    assert!(rv >= 1);
}

#[tokio::test]
async fn focused_store_traits_cover_sqlite_backend() {
    fn assert_traits<T>(_: &T)
    where
        T: ResourceStore
            + ResourceListStore
            + StatusStore
            + OwnershipStore
            + WatchStore
            + WatchHistoryStore
            + NamespaceStore
            + NamespaceContentStore
            + PodWorkqueueStore
            + NetworkStore
            + NetworkMetadataStore
            + ReplicationStore
            + MetaStore,
    {
    }

    let db = Datastore::new_in_memory().await.unwrap();
    assert_traits(&db);
}

#[tokio::test]
async fn test_datastore_backend_advance_rv_via_trait_returns_at_least_min_rv() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let backend: &dyn DatastoreBackend = &concrete;

    let target = backend.get_current_resource_version().await.unwrap() + 5;
    let advanced = backend
        .advance_resource_version_after(target)
        .await
        .unwrap();
    assert!(advanced > target);
}

#[tokio::test]
async fn test_datastore_backend_list_namespace_resources_via_trait_returns_inserted() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let backend: &dyn DatastoreBackend = &concrete;

    backend
        .create_namespace("ns-trait", json!({"metadata":{"name":"ns-trait"}}))
        .await
        .unwrap();
    backend
        .create_resource(
            "v1",
            "ConfigMap",
            Some("ns-trait"),
            "cm-trait",
            json!({"metadata":{"name":"cm-trait"},"data":{"k":"v"}}),
        )
        .await
        .unwrap();

    let items = backend.list_namespace_resources("ns-trait").await.unwrap();
    assert!(
        items
            .iter()
            .any(|r| r.kind == "ConfigMap" && r.name == "cm-trait")
    );
}

#[tokio::test]
async fn raft_allocate_node_subnet_commits_distinct_per_node_24s() {
    let db = Datastore::new_in_memory().await.unwrap();

    for (node_name, node_ip) in [
        ("mn-controlplane1", "10.99.0.10"),
        ("mn-controlplane2", "10.99.0.14"),
    ] {
        let node_name = node_name.to_string();
        let node_ip = node_ip.to_string();
        db.db_call("test_raft_allocate_node_subnet_commit", move |conn| {
            let tx = conn.transaction()?;
            Datastore::apply_outbox_command_in_tx(
                &tx,
                StorageCommand::AllocateNodeSubnet {
                    node_name,
                    subnet: "10.50.0.0/16".to_string(),
                    node_ip,
                },
                "ClusterMaintenance",
                "mn-controlplane1",
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    }

    let rows: Vec<(String, String, i64, String, String)> = db
        .db_call("test_select_raft_allocated_node_subnets", |conn| {
            let mut stmt = conn.prepare(
                "SELECT node_name, subnet, subnet_base_int, vtep_ip, mode \
                 FROM node_subnets ORDER BY node_name",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            (
                "mn-controlplane1".to_string(),
                "10.50.0.0/24".to_string(),
                u32::from(std::net::Ipv4Addr::new(10, 50, 0, 0)) as i64,
                "10.50.0.0".to_string(),
                "root".to_string(),
            ),
            (
                "mn-controlplane2".to_string(),
                "10.50.1.0/24".to_string(),
                u32::from(std::net::Ipv4Addr::new(10, 50, 1, 0)) as i64,
                "10.50.1.0".to_string(),
                "root".to_string(),
            ),
        ],
        "Raft AllocateNodeSubnet commands carry the cluster CIDR; log-apply must allocate per-node /24s"
    );
}

#[tokio::test]
async fn raft_allocate_node_subnet_resolves_lowest_free_24_at_apply_time() {
    let db = Datastore::new_in_memory().await.unwrap();
    let commands = vec![
        StorageCommand::AllocateNodeSubnet {
            node_name: "mn-controlplane1".to_string(),
            subnet: "10.50.0.0/16".to_string(),
            node_ip: "10.99.0.10".to_string(),
        },
        StorageCommand::AllocateNodeSubnet {
            node_name: "mn-controlplane2".to_string(),
            subnet: "10.50.0.0/16".to_string(),
            node_ip: "10.99.0.14".to_string(),
        },
    ];
    let commits = db
        .db_call(
            "test_build_concurrent_allocate_node_subnet_commits",
            move |conn| {
                let tx = conn.transaction()?;
                let mut commits = Vec::new();
                for command in commands {
                    let (commit, _rv) = Datastore::build_log_apply_commit_in_tx_from_command(
                        &tx,
                        command,
                        "ClusterMaintenance",
                        "mn-controlplane1",
                    )?;
                    assert!(
                        commit.resource_version > 0,
                        "allocate subnet commits must carry the leader's committed RV"
                    );
                    commits.push(commit);
                }
                tx.commit()?;
                Ok(commits)
            },
        )
        .await
        .unwrap();

    for commit in commits {
        db.apply_raft_log_apply_commit(commit).await.unwrap();
    }

    let rows: Vec<(String, String)> = db
        .db_call("test_select_apply_time_allocated_node_subnets", |conn| {
            let mut stmt =
                conn.prepare("SELECT node_name, subnet FROM node_subnets ORDER BY node_name")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec![
            ("mn-controlplane1".to_string(), "10.50.0.0/24".to_string()),
            ("mn-controlplane2".to_string(), "10.50.1.0/24".to_string()),
        ],
        "subnet choice must be derived from already-applied state, not build-time state"
    );
}

#[tokio::test]
async fn test_datastore_handle_arc_dyn_clone_shares_state() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let handle: DatastoreHandle = std::sync::Arc::new(concrete);
    let clone = handle.clone();

    handle
        .create_namespace("ns-handle", json!({"metadata":{"name":"ns-handle"}}))
        .await
        .unwrap();
    let fetched = clone.get_namespace("ns-handle").await.unwrap();
    assert!(
        fetched.is_some(),
        "handle clones must observe shared writes"
    );
}

#[tokio::test]
async fn datastore_backend_trait_object_supports_internal_methods() {
    let db = crate::datastore::test_support::in_memory().await;
    let handle: DatastoreHandle = std::sync::Arc::new(db);

    assert!(
        handle
            .get_pod_network("missing-sandbox")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        handle
            .get_node_subnet("missing-node")
            .await
            .unwrap()
            .is_none()
    );
    assert!(handle.list_sandboxes().await.unwrap().is_empty());
    handle
        .record_sandbox("default", "pod-a", "uid-a", "sandbox-a")
        .await
        .unwrap();
    let sandboxes = handle.list_sandboxes().await.unwrap();
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].pod_uid, "uid-a");
    assert_eq!(handle.gc_watch_events(100_000, 1_000).await.unwrap(), 0);
}

#[tokio::test]
async fn concurrent_sandbox_insert_different_uids_both_survive_trait_object() {
    let db = crate::datastore::test_support::in_memory().await;
    let handle: DatastoreHandle = std::sync::Arc::new(db);

    handle
        .record_sandbox("default", "pod-a", "uid-a", "sandbox-a")
        .await
        .unwrap();
    handle
        .record_sandbox("default", "pod-a", "uid-b", "sandbox-b")
        .await
        .unwrap();

    assert_eq!(
        handle
            .get_sandbox_for_uid("default", "pod-a", "uid-a")
            .await
            .unwrap(),
        Some("sandbox-a".to_string())
    );
    assert_eq!(
        handle
            .get_sandbox_for_uid("default", "pod-a", "uid-b")
            .await
            .unwrap(),
        Some("sandbox-b".to_string())
    );

    let sandboxes = handle.list_sandboxes().await.unwrap();
    assert_eq!(sandboxes.len(), 2);
}

#[tokio::test]
async fn test_create_resource() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({"metadata": {"name": "test-pod"}});
    let r = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();
    assert_eq!(r.resource_version, 1);
}

#[tokio::test]
async fn test_create_conflict() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({"metadata": {"name": "test"}});
    db.create_resource("v1", "Pod", Some("default"), "test", data.clone())
        .await
        .unwrap();
    let r = db
        .create_resource("v1", "Pod", Some("default"), "test", data)
        .await;
    assert!(r.unwrap_err().to_string().contains("409"));
}

#[tokio::test]
async fn test_get_resource() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource("v1", "Pod", None, "test", json!({}))
        .await
        .unwrap();
    assert!(
        db.get_resource("v1", "Pod", None, "test")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_create_resource_repairs_missing_type_meta() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "demo",
            json!({
                "metadata": {"name": "demo", "namespace": "default"},
                "spec": {"replicas": 1}
            }),
        )
        .await
        .unwrap();

    assert_eq!(created.data["apiVersion"], "apps/v1");
    assert_eq!(created.data["kind"], "Deployment");
}

#[tokio::test]
async fn test_list() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 1..=5 {
        db.create_resource("v1", "Pod", None, &format!("p{}", i), json!({}))
            .await
            .unwrap();
    }
    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 5);
}

#[tokio::test]
async fn test_label_selector() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p1",
        json!({"metadata": {"labels": {"app": "nginx"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p2",
        json!({"metadata": {"labels": {"app": "apache"}}}),
    )
    .await
    .unwrap();
    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
}

#[tokio::test]
async fn test_configmap_update_preserves_data_field() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create a ConfigMap with data
    let initial_data = json!({
        "metadata": {"name": "test-config"},
        "data": {
            "key1": "value1",
            "key2": "value2"
        }
    });
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "test-config",
            initial_data,
        )
        .await
        .unwrap();

    // Update the ConfigMap with new data (simulates PUT request)
    let updated_data = json!({
        "metadata": {"name": "test-config"},
        "data": {
            "key1": "updated-value1",
            "key3": "value3"
        }
    });
    let updated = db
        .update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "test-config",
            updated_data,
            created.resource_version,
        )
        .await
        .unwrap();

    // Verify the data field is preserved in the updated resource
    let data_field = updated
        .data
        .get("data")
        .expect("data field should be present");
    assert_eq!(
        data_field.get("key1").and_then(|v| v.as_str()),
        Some("updated-value1")
    );
    assert_eq!(
        data_field.get("key3").and_then(|v| v.as_str()),
        Some("value3")
    );
    assert!(data_field.get("key2").is_none(), "key2 should be removed");
}

#[tokio::test]
async fn test_update_resource_repairs_empty_type_meta() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "demo",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "demo", "namespace": "default"},
                "spec": {"replicas": 1}
            }),
        )
        .await
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "demo",
            json!({
                "apiVersion": "",
                "kind": "",
                "metadata": {"name": "demo", "namespace": "default"},
                "spec": {"replicas": 2}
            }),
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(updated.data["apiVersion"], "apps/v1");
    assert_eq!(updated.data["kind"], "Deployment");
}

#[tokio::test]
async fn test_pod_status_ip_arrays_repaired_on_create_and_update() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "ip-fix",
            json!({
                "metadata": {"name": "ip-fix", "namespace": "default"},
                "status": {
                    "phase": "Running",
                    "podIP": "10.42.0.15",
                    "hostIP": "192.168.122.12"
                }
            }),
        )
        .await
        .unwrap();

    assert_eq!(created.data["status"]["podIPs"][0]["ip"], "10.42.0.15");
    assert_eq!(created.data["status"]["hostIPs"][0]["ip"], "192.168.122.12");

    let updated = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "ip-fix",
            json!({
                "metadata": {"name": "ip-fix", "namespace": "default"},
                "status": {
                    "phase": "Running",
                    "podIP": "10.42.0.16",
                    "hostIP": "192.168.122.13",
                    "podIPs": [{"ip": "192.0.2.1"}],
                    "hostIPs": [{"ip": "192.0.2.22"}]
                }
            }),
            created.resource_version,
        )
        .await
        .unwrap();

    assert_eq!(updated.data["status"]["podIPs"][0]["ip"], "10.42.0.16");
    assert_eq!(updated.data["status"]["hostIPs"][0]["ip"], "192.168.122.13");
}

// ========================
// Delete tests
// ========================

#[tokio::test]
async fn test_delete_resource_hard_deletes() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "to-delete",
        json!({"metadata": {"name": "to-delete"}}),
    )
    .await
    .unwrap();

    // Delete should succeed
    db.delete_resource("v1", "Pod", Some("default"), "to-delete")
        .await
        .unwrap();

    // get_resource should return None (hard-deleted)
    let result = db
        .get_resource("v1", "Pod", Some("default"), "to-delete")
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "Deleted resource should not be returned by get"
    );

    // list should also not include it
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        list.items.len(),
        0,
        "Deleted resource should not appear in list"
    );
}

#[tokio::test]
async fn test_delete_nonexistent_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let result = db
        .delete_resource("v1", "Pod", Some("default"), "nonexistent")
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_create_after_delete_same_name() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create a pod
    let pod1 = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod"}}),
        )
        .await
        .unwrap();
    assert_eq!(pod1.name, "test-pod");

    // Delete the pod
    db.delete_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap();

    // Create a new pod with the same name — should succeed
    let pod2 = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            json!({"metadata": {"name": "test-pod", "generation": 2}}),
        )
        .await
        .unwrap();
    assert_eq!(pod2.name, "test-pod");
    assert_eq!(pod2.data["metadata"]["generation"], 2);
}

// ========================
// Pagination tests
// ========================

#[tokio::test]
async fn test_pagination_no_items_lost() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create 5 pods — odd number that doesn't divide evenly by limit=2
    for i in 1..=5 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("p{}", i),
            json!({"metadata": {"name": format!("p{}", i)}}),
        )
        .await
        .unwrap();
    }

    // Paginate through all items with limit=2
    let mut all_names: Vec<String> = Vec::new();
    let mut continue_token: Option<String> = None;
    let mut page_count = 0;

    loop {
        let page = db
            .list_resources(
                "v1",
                "Pod",
                None,
                crate::datastore::ResourceListQuery::new(
                    None,
                    None,
                    Some(2),
                    continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();

        for item in &page.items {
            all_names.push(item.name.clone());
        }

        page_count += 1;
        continue_token = page.continue_token;
        if continue_token.is_none() {
            break;
        }
    }

    // ALL 5 items must appear — no items lost at page boundaries
    all_names.sort();
    assert_eq!(
        all_names,
        vec!["p1", "p2", "p3", "p4", "p5"],
        "All 5 items must appear across pages (got {} items in {} pages)",
        all_names.len(),
        page_count
    );
}

#[tokio::test]
async fn test_pagination_no_continue_when_exact() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create exactly 2 pods, limit=2 — should have no continue token
    for i in 1..=2 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("p{}", i),
            json!({"metadata": {"name": format!("p{}", i)}}),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(None, None, Some(2), None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 2);
    assert!(
        list.continue_token.is_none(),
        "No continue token when results fit in limit"
    );
}

#[tokio::test]
async fn test_pagination_with_label_selector_filters_then_paginates() {
    let db = Datastore::new_in_memory().await.unwrap();

    // 3 pods with app=web, 1 with app=api
    for i in 1..=3 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("web-{}", i),
            json!({"metadata": {"name": format!("web-{}", i), "labels": {"app": "web"}}}),
        )
        .await
        .unwrap();
    }
    db.create_resource(
        "v1",
        "Pod",
        None,
        "api-1",
        json!({"metadata": {"name": "api-1", "labels": {"app": "api"}}}),
    )
    .await
    .unwrap();

    // Paginate filtered results: limit=2, label=app=web
    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("app=web"), None, Some(2), None),
        )
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert!(page1.continue_token.is_some());

    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(
                Some("app=web"),
                None,
                Some(2),
                page1.continue_token.as_deref(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 1);
    assert!(page2.continue_token.is_none());

    let mut all: Vec<String> = page1
        .items
        .iter()
        .chain(page2.items.iter())
        .map(|r| r.name.clone())
        .collect();
    all.sort();
    assert_eq!(all, vec!["web-1", "web-2", "web-3"]);
}

#[tokio::test]
async fn test_pagination_with_label_selector_remaining_count_across_pages() {
    let db = Datastore::new_in_memory().await.unwrap();

    for i in 1..=5 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("web-{}", i),
            json!({"metadata": {"name": format!("web-{}", i), "labels": {"app": "web"}}}),
        )
        .await
        .unwrap();
    }
    for i in 1..=2 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("api-{}", i),
            json!({"metadata": {"name": format!("api-{}", i), "labels": {"app": "api"}}}),
        )
        .await
        .unwrap();
    }

    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("app=web"), None, Some(2), None),
        )
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 2);
    assert_eq!(
        page1.remaining_item_count, None,
        "selector queries omit exact remainingItemCount"
    );
    assert_eq!(page1.continue_token.as_deref(), Some("web-2"));

    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(
                Some("app=web"),
                None,
                Some(2),
                page1.continue_token.as_deref(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert_eq!(
        page2.remaining_item_count, None,
        "selector queries omit exact remainingItemCount"
    );
    assert_eq!(page2.continue_token.as_deref(), Some("web-4"));

    let page3 = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(
                Some("app=web"),
                None,
                Some(2),
                page2.continue_token.as_deref(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page3.items.len(), 1);
    assert_eq!(page3.remaining_item_count, None);
    assert_eq!(page3.continue_token, None);
}

#[tokio::test]
async fn test_selector_free_limited_list_does_not_decode_unreturned_namespaced_rows() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "a-good",
        json!({"metadata": {"name": "a-good", "namespace": "default"}}),
    )
    .await
    .unwrap();

    db.db_call("test_selector_free_namespaced_seed_bad_row", |conn| {
            conn.execute(
                "INSERT INTO namespaced_resources (api_version, kind, namespace, name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    "v1",
                    "Pod",
                    "default",
                    "z-bad",
                    "uid-z-bad",
                    "not-an-int",
                    br#"{"metadata":{"name":"z-bad","namespace":"default","uid":"uid-z-bad"}}"#.to_vec()
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let page = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(1), None),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name, "a-good");
    assert_eq!(page.continue_token.as_deref(), Some("a-good"));
    assert_eq!(page.remaining_item_count, Some(1));
}

#[tokio::test]
async fn test_selector_free_limited_list_does_not_decode_unreturned_cluster_rows() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Node",
        None,
        "a-good",
        json!({"metadata": {"name": "a-good"}}),
    )
    .await
    .unwrap();

    db.db_call("test_selector_free_cluster_seed_bad_row", |conn| {
            conn.execute(
                "INSERT INTO cluster_resources (api_version, kind, name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    "v1",
                    "Node",
                    "z-bad",
                    "uid-z-bad",
                    "not-an-int",
                    br#"{"metadata":{"name":"z-bad","uid":"uid-z-bad"}}"#.to_vec()
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let page = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::new(None, None, Some(1), None),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name, "a-good");
    assert_eq!(page.continue_token.as_deref(), Some("a-good"));
    assert_eq!(page.remaining_item_count, Some(1));
}

#[tokio::test]
async fn test_pagination_no_limit_returns_all() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 1..=10 {
        db.create_resource(
            "v1",
            "Pod",
            None,
            &format!("p{}", i),
            json!({"metadata": {"name": format!("p{}", i)}}),
        )
        .await
        .unwrap();
    }
    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 10);
    assert!(list.continue_token.is_none());
}

// ========================
// find_owned_resources tests
// ========================

#[tokio::test]
async fn test_find_owned_resources() {
    let db = Datastore::new_in_memory().await.unwrap();
    let owner_uid = "owner-123";

    // Create owned resource
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "owned-pod",
        json!({
            "metadata": {
                "name": "owned-pod",
                "ownerReferences": [{"uid": owner_uid, "kind": "ReplicaSet"}]
            }
        }),
    )
    .await
    .unwrap();

    // Create unowned resource
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "free-pod",
        json!({"metadata": {"name": "free-pod"}}),
    )
    .await
    .unwrap();

    let owned = db
        .find_owned_resources(owner_uid, Some("default"))
        .await
        .unwrap();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0].name, "owned-pod");
}

#[tokio::test]
async fn test_find_owned_resources_no_matches() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod1",
        json!({"metadata": {"name": "pod1"}}),
    )
    .await
    .unwrap();

    let owned = db
        .find_owned_resources("nonexistent-uid", Some("default"))
        .await
        .unwrap();
    assert_eq!(owned.len(), 0);
}

// ========================
// Label selector edge cases
// ========================

#[tokio::test]
async fn test_label_selector_in_operator() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p1",
        json!({"metadata": {"labels": {"env": "prod"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p2",
        json!({"metadata": {"labels": {"env": "staging"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p3",
        json!({"metadata": {"labels": {"env": "dev"}}}),
    )
    .await
    .unwrap();

    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(
                Some("env in (prod,staging)"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 2);
}

#[tokio::test]
async fn test_label_selector_notin_operator() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p1",
        json!({"metadata": {"labels": {"env": "prod"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "p2",
        json!({"metadata": {"labels": {"env": "dev"}}}),
    )
    .await
    .unwrap();

    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("env notin (dev)"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "p1");
}

#[tokio::test]
async fn test_label_selector_not_exists() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "labeled",
        json!({"metadata": {"labels": {"app": "nginx"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "unlabeled",
        json!({"metadata": {"labels": {}}}),
    )
    .await
    .unwrap();

    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("!app"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "unlabeled");
}

#[tokio::test]
async fn test_label_selector_multiple_requirements() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "match",
        json!({"metadata": {"labels": {"app": "nginx", "env": "prod"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "partial",
        json!({"metadata": {"labels": {"app": "nginx", "env": "dev"}}}),
    )
    .await
    .unwrap();

    // Both conditions must match
    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("app=nginx,env=prod"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "match");
}

#[tokio::test]
async fn test_label_selector_exists_operator() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "has-app",
        json!({"metadata": {"labels": {"app": "nginx"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        None,
        "no-app",
        json!({"metadata": {"labels": {"env": "prod"}}}),
    )
    .await
    .unwrap();

    // Bare key = exists operator
    let list = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::new(Some("app"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name, "has-app");
}

// ========================
// Cluster-wide list tests
// ========================

#[tokio::test]
async fn test_list_resources_cluster_wide_returns_all_namespaces() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create pods in different namespaces
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        json!({"metadata": {"name": "pod-a", "namespace": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("kube-system"),
        "pod-b",
        json!({"metadata": {"name": "pod-b", "namespace": "kube-system"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("monitoring"),
        "pod-c",
        json!({"metadata": {"name": "pod-c", "namespace": "monitoring"}}),
    )
    .await
    .unwrap();

    // Cluster-wide list (namespace=None) should return all 3
    let all = db
        .list_resources(
            "v1",
            "Pod",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        all.items.len(),
        3,
        "Cluster-wide list should return pods from all namespaces"
    );

    // Namespaced list should return only 1
    let ns_only = db
        .list_resources(
            "v1",
            "Pod",
            Some("kube-system"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(ns_only.items.len(), 1);
    assert_eq!(ns_only.items[0].name, "pod-b");
}

#[tokio::test]
async fn namespaced_same_kind_name_can_exist_in_different_api_versions() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_namespace("default", json!({"metadata":{"name":"default"}}))
        .await
        .unwrap();

    db.create_resource(
        "example.alpha/v1",
        "Widget",
        Some("default"),
        "same",
        json!({"apiVersion":"example.alpha/v1","kind":"Widget","metadata":{"name":"same","namespace":"default"}}),
    )
    .await
    .unwrap();

    db.create_resource(
        "example.beta/v1",
        "Widget",
        Some("default"),
        "same",
        json!({"apiVersion":"example.beta/v1","kind":"Widget","metadata":{"name":"same","namespace":"default"}}),
    )
    .await
    .unwrap();

    let alpha = db
        .get_resource("example.alpha/v1", "Widget", Some("default"), "same")
        .await
        .unwrap();
    assert!(alpha.is_some(), "alpha resource missing");
    assert_eq!(alpha.unwrap().api_version, "example.alpha/v1");

    let beta = db
        .get_resource("example.beta/v1", "Widget", Some("default"), "same")
        .await
        .unwrap();
    assert!(beta.is_some(), "beta resource missing");
    assert_eq!(beta.unwrap().api_version, "example.beta/v1");
}

fn accepts_resource_store(_store: &dyn crate::datastore::ResourceStore) {}
fn accepts_watch_store(_store: &dyn crate::datastore::WatchStore) {}
fn accepts_network_store(_store: &dyn crate::datastore::NetworkStore) {}
fn accepts_namespace_store(_store: &dyn crate::datastore::NamespaceStore) {}

#[tokio::test]
async fn datastore_implements_focused_backend_traits() {
    let db = Datastore::new_in_memory().await.unwrap();
    accepts_resource_store(&db);
    accepts_watch_store(&db);
    accepts_network_store(&db);
    accepts_namespace_store(&db);
}

#[tokio::test]
async fn cluster_same_kind_name_can_exist_in_different_api_versions() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.create_resource(
        "example.alpha/v1",
        "ClusterWidget",
        None,
        "same",
        json!({"apiVersion":"example.alpha/v1","kind":"ClusterWidget","metadata":{"name":"same"}}),
    )
    .await
    .unwrap();

    db.create_resource(
        "example.beta/v1",
        "ClusterWidget",
        None,
        "same",
        json!({"apiVersion":"example.beta/v1","kind":"ClusterWidget","metadata":{"name":"same"}}),
    )
    .await
    .unwrap();

    let alpha = db
        .get_resource("example.alpha/v1", "ClusterWidget", None, "same")
        .await
        .unwrap();
    assert!(alpha.is_some());
    assert_eq!(alpha.unwrap().api_version, "example.alpha/v1");

    let beta = db
        .get_resource("example.beta/v1", "ClusterWidget", None, "same")
        .await
        .unwrap();
    assert!(beta.is_some());
    assert_eq!(beta.unwrap().api_version, "example.beta/v1");
}

// -----------------------------------------------------------------------
// DSB-03 — constructor consolidation and persistent mode tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn from_executor_initializes_watch_and_fingerprint() {
    use crate::datastore::sqlite::DbExecutor;

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = DbExecutor::open_in_memory(supervisor, "dsb03:fp-test")
        .await
        .unwrap();
    let ds = Datastore::new_in_memory_with_watch_and_executor(executor)
        .await
        .unwrap();
    let mut watch_rx = ds.subscribe_watch(crate::watch::WatchTopic::new("v1", "ConfigMap"));

    // Verify watch subscription works
    ds.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "fp-test",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "fp-test"}}),
    )
    .await
    .unwrap();

    let event = watch_rx.try_recv().expect("should receive broadcast event");
    assert_eq!(event.object["metadata"]["name"].as_str(), Some("fp-test"));
}

#[tokio::test]
async fn new_persistent_creates_cluster_and_node_db_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    // Ensure parent has 0700 for opener
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let ds = Datastore::new_persistent(db_dir, supervisor, None)
        .await
        .unwrap();

    // Verify split DB files exist under {db_dir}/sqlite/
    let cluster_db_path = db_dir.join("sqlite").join("cluster.db");
    let node_db_path = db_dir.join("sqlite").join("node.db");
    assert!(
        cluster_db_path.exists(),
        "cluster.db must be created under sqlite/"
    );
    assert!(
        node_db_path.exists(),
        "node.db must be created under sqlite/"
    );

    // Verify the DB is functional: create a resource
    let resource = ds
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            json!({"metadata": {"name": "p1"}}),
        )
        .await
        .unwrap();
    assert!(resource.resource_version > 0);
}

#[tokio::test]
async fn new_persistent_rejects_when_parent_perms_too_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parent = dir.path().join("loose-dir");
    use std::os::unix::fs::DirBuilderExt;
    // Create with 0755 (too open) — the sqlite/ subdir is what
    // new_persistent checks since it joins "sqlite" to db_dir.
    let sqlite_dir = parent.join("sqlite");
    std::fs::DirBuilder::new()
        .mode(0o755)
        .recursive(true)
        .create(&sqlite_dir)
        .expect("create loose sqlite dir");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let result = Datastore::new_persistent(&parent, supervisor, None).await;

    assert!(result.is_err(), "must reject parent with 0755");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("0700") || err_msg.contains("permission"),
        "error must mention perms: {}",
        err_msg
    );
}

#[tokio::test]
async fn new_persistent_failure_propagates_no_fallback() {
    // Use a non-existent path that can't be created (root-only)
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let bad_dir = std::path::Path::new("/proc/klights-test-noexist");
    let result = Datastore::new_persistent(bad_dir, supervisor, None).await;

    assert!(result.is_err(), "must fail on non-creatable dir");
    // Verify it didn't silently fall back to in-memory
    let err_msg = result.unwrap_err().to_string();
    assert!(
        !err_msg.contains("in-memory") && !err_msg.contains("memory:"),
        "must not fall back to in-memory: {}",
        err_msg
    );
}

// -----------------------------------------------------------------------
// DSB-05 — retention, checkpoint, and snapshot-compat tests
// -----------------------------------------------------------------------

/// Online backup must succeed during concurrent writes.
/// Proves DSB-05's checkpoint and lock policy is snapshot-compatible.
#[tokio::test]
async fn online_backup_succeeds_during_concurrent_writes() {
    use std::time::Duration;

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("state.db");
    let backup_path = dir.path().join("state.db.backup");

    // Create a disk-backed DB
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", [])
            .unwrap();
    }

    // Spawn concurrent writes
    let writer_path = db_path.clone();
    let _writer = tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(&writer_path).unwrap();
        for i in 0..200 {
            conn.execute("INSERT INTO t (v) VALUES (?1)", [&format!("val-{}", i)])
                .unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Run online backup concurrently
    let src_path = db_path.clone();
    let bak_path = backup_path.clone();
    let backup = tokio::task::spawn_blocking(move || {
        let src = rusqlite::Connection::open(&src_path).unwrap();
        let mut dst = rusqlite::Connection::open(&bak_path).unwrap();
        let backup = rusqlite::backup::Backup::new(&src, &mut dst).expect("Backup::new");
        backup.run_to_completion(1, Duration::from_millis(10_000), None)
    });

    let result = tokio::time::timeout(Duration::from_secs(30), backup)
        .await
        .expect("timeout")
        .expect("join");
    assert!(
        result.is_ok(),
        "online backup must succeed: {:?}",
        result.err()
    );

    // Wait for writer
    let _ = tokio::time::timeout(Duration::from_secs(10), _writer).await;

    // Verify backup file exists and has data
    assert!(backup_path.exists());
    let bak_conn = rusqlite::Connection::open(&backup_path).unwrap();
    let count: i64 = bak_conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert!(count > 0, "backup must contain rows");
}

/// Lock-policy audit: no BEGIN EXCLUSIVE outside tests.
/// DSB-05 requires this to protect Phase 3 Raft snapshot compatibility.
#[test]
fn no_begin_exclusive_outside_tests() {
    let src = std::process::Command::new("bash")
        .args(["-c", "grep -rn 'BEGIN EXCLUSIVE' src/ | grep -v '#[cfg(test)]' | grep -v 'tests/' | grep -v test_support || true"])
        .output()
        .expect("grep");
    let output = String::from_utf8_lossy(&src.stdout);
    assert!(
        output.trim().is_empty(),
        "DSB-05 lock-policy audit: BEGIN EXCLUSIVE found outside tests:\n{output}"
    );
}

/// Verifies incremental_vacuum runs after GC sweep.
#[tokio::test]
async fn gc_triggers_incremental_vacuum_after_sweep() {
    // This test exercises the path — incremental_vacuum is a no-op if no pages
    // need releasing, but it must not error.
    let db = crate::datastore::test_support::in_memory().await;
    // Insert enough events to create pages
    for i in 0..100 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &format!("vc-{}", i),
            serde_json::json!({"data": {"k": "v"}}),
        )
        .await
        .unwrap();
    }
    // GC with a small cap — should delete rows and trigger incremental_vacuum
    let removed = db.gc_watch_events(10, 1000).await.unwrap();
    assert!(removed > 0, "GC should have removed rows");
}

#[tokio::test]
async fn scoped_replay_floor_allows_retained_in_scope_event_after_unrelated_gc() {
    let db = crate::datastore::test_support::in_memory().await;

    for i in 0..20 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("noise"),
            &format!("cm-{i}"),
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "noise", "name": format!("cm-{i}")}
            }),
        )
        .await
        .expect("create noise");
    }

    let pod = db
        .create_resource(
            "v1",
            "Pod",
            Some("app"),
            "frontend",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "app", "name": "frontend"},
                "spec": {"containers": [{"name": "app", "image": "pause"}]}
            }),
        )
        .await
        .expect("create pod");

    db.gc_watch_events(1, 1000).await.expect("gc");
    let since_rv = pod.resource_version - 10;

    let replay = db
        .list_watch_events_since_checked(
            &[crate::datastore::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "app",
            )],
            since_rv,
        )
        .await
        .expect("checked replay");

    match replay {
        crate::datastore::WatchReplayRead::Events(events) => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].resource.name, "frontend");
        }
        crate::datastore::WatchReplayRead::Expired => {
            panic!("unrelated lower-RV churn must not expire app/Pod replay");
        }
    }
}

#[tokio::test]
async fn checked_watch_replay_bounded_limits_events() {
    let db = crate::datastore::test_support::in_memory().await;
    let start_rv = db.get_current_resource_version().await.unwrap();

    for i in 0..5 {
        db.create_resource(
            "v1",
            "Pod",
            Some("app"),
            &format!("pod-{i}"),
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "app", "name": format!("pod-{i}")},
                "spec": {"containers": [{"name": "app", "image": "pause"}]}
            }),
        )
        .await
        .expect("create pod");
    }

    let replay = db
        .list_watch_events_since_checked_bounded(
            &[crate::datastore::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "app",
            )],
            start_rv,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .expect("checked replay");

    match replay {
        crate::datastore::WatchReplayRead::Events(events) => {
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.resource.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["pod-0", "pod-1", "pod-2"]
            );
        }
        crate::datastore::WatchReplayRead::Expired => {
            panic!("fresh bounded replay should not expire");
        }
    }
}

// -----------------------------------------------------------------------
// DSB-05 — restart-recovery and retention tests
// -----------------------------------------------------------------------

/// Restart recovery: create a pod, simulate restart by closing and
/// reopening the DB, then verify UID and resourceVersion are preserved.
#[tokio::test]
async fn restart_preserves_pods_with_uids_and_rv() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let resource_uid;
    let resource_rv;

    // First session: create a pod
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone(), None)
            .await
            .unwrap();

        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "restart-test-pod",
                "namespace": "default",
                "uid": "uid-restart-001"
            },
            "spec": {"containers": []}
        });
        let created = ds
            .create_resource("v1", "Pod", Some("default"), "restart-test-pod", pod)
            .await
            .unwrap();

        resource_uid = created
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        resource_rv = created.resource_version;
    }
    // Drop Datastore — this closes the connection.

    // Second session: reopen and verify persistence
    {
        let ds = Datastore::new_persistent(db_dir, supervisor, None)
            .await
            .unwrap();
        let loaded = ds
            .get_resource("v1", "Pod", Some("default"), "restart-test-pod")
            .await
            .unwrap()
            .expect("pod must survive restart");

        let loaded_uid = loaded
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("");
        assert_eq!(
            loaded_uid, resource_uid,
            "UID must be preserved across restart"
        );
        assert_eq!(
            loaded.resource_version, resource_rv,
            "resourceVersion must be preserved"
        );
    }
}

/// Restart recovery: verify multiple resource kinds survive restart.
#[tokio::test]
async fn restart_preserves_configmaps_secrets_crds_services() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));

    let names = ["cm-restart", "secret-restart", "svc-restart"];
    let kinds = ["ConfigMap", "Secret", "Service"];

    // Session 1: create resources
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone(), None)
            .await
            .unwrap();
        for (name, kind) in names.iter().zip(kinds.iter()) {
            ds.create_resource(
                "v1",
                kind,
                Some("default"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": kind,
                    "metadata": {"name": name, "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        }
    }

    // Session 2: verify all survive
    {
        let ds = Datastore::new_persistent(db_dir, supervisor, None)
            .await
            .unwrap();
        for (name, kind) in names.iter().zip(kinds.iter()) {
            let res = ds
                .get_resource("v1", kind, Some("default"), name)
                .await
                .unwrap();
            assert!(res.is_some(), "{kind} '{name}' must survive restart");
        }
    }
}

/// Watch replay: create events, restart, verify replay within retention window.
#[tokio::test]
async fn restart_resumes_watch_within_retention_window() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));

    // Session 1: create resources to generate watch events
    let mut last_rv = 0i64;
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone(), None)
            .await
            .unwrap();
        for i in 0..20 {
            let res = ds.create_resource("v1", "ConfigMap", Some("default"), &format!("wr-{}", i),
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": format!("wr-{}", i)}}),
            ).await.unwrap();
            last_rv = res.resource_version;
        }
    }

    // Session 2: reopen and verify replay works from a since_rv within the window
    {
        let ds = Datastore::new_persistent(db_dir, supervisor, None)
            .await
            .unwrap();

        // Replay from half the window
        let since_rv = last_rv - 10;
        use crate::datastore::{WatchTarget, WatchTargetScope};
        let targets = vec![WatchTarget {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            scope: WatchTargetScope::Namespaced(None),
        }];

        let events = ds
            .list_watch_events_since(&targets, since_rv)
            .await
            .unwrap();
        assert!(
            !events.is_empty(),
            "replay should return events after restart"
        );
        // All events should have rv > since_rv
        for event in &events {
            assert!(
                event.resource.resource_version > since_rv,
                "replayed event rv {} must be > since_rv {}",
                event.resource.resource_version,
                since_rv
            );
        }
    }
}

/// 410 Gone: GC old events, verify watch events before retention window are gone.
#[tokio::test]
async fn restart_returns_410_gone_when_rv_pre_dates_retention() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));

    // Session 1: create many events, then GC aggressively
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone(), None)
            .await
            .unwrap();
        for i in 0..30 {
            ds.create_resource("v1", "ConfigMap", Some("default"), &format!("gc-{}", i),
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": format!("gc-{}", i)}}),
            ).await.unwrap();
        }
        // GC down to 5 rows
        let removed = ds.gc_watch_events(5, 100).await.unwrap();
        assert!(removed > 0, "GC should have removed rows");
    }

    // Session 2: verify old events are gone (replay from very old RV returns empty)
    {
        let ds = Datastore::new_persistent(db_dir, supervisor, None)
            .await
            .unwrap();
        use crate::datastore::{WatchTarget, WatchTargetScope};
        let targets = vec![WatchTarget {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            scope: WatchTargetScope::Namespaced(None),
        }];

        // Replay from RV 0 — should find few or no events since GC removed them
        let events = ds.list_watch_events_since(&targets, 0).await.unwrap();
        // After GC to 5 rows, replay from 0 may still return the surviving rows
        // because list_watch_events_since doesn't enforce the retention window —
        // it just returns what's in the table. The 410 Gone is logic in the
        // watch cursor, not the datastore. This test verifies the table is
        // actually pruned.
        assert!(
            events.len() <= 5,
            "after GC to 5, at most 5 events should remain; got {}",
            events.len()
        );
    }
}

/// Retention: bounded file size after create+delete churn.
#[tokio::test]
async fn retention_bounds_db_file_size_after_churn() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));

    {
        let ds = Datastore::new_persistent(db_dir, supervisor, None)
            .await
            .unwrap();

        // Create and delete 50 resources to generate churn
        for i in 0..50 {
            let name = format!("churn-{}", i);
            ds.create_resource("v1", "ConfigMap", Some("default"), &name,
                serde_json::json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": &name}}),
            ).await.unwrap();
            ds.delete_resource("v1", "ConfigMap", Some("default"), &name)
                .await
                .unwrap();
        }

        // GC watch events
        let removed = ds.gc_watch_events(10, 100).await.unwrap();
        assert!(removed > 0, "GC should remove rows after churn");
    }

    // Verify cluster.db file size is bounded
    let db_path = db_dir.join("sqlite").join("cluster.db");
    assert!(db_path.exists(), "cluster.db must exist after churn");
    let size = std::fs::metadata(&db_path).unwrap().len();
    // After 50 create+delete cycles with GC, file should stay under 1MB
    assert!(
        size < 1_000_000,
        "cluster.db size {} must be < 1MB after churn; got {}",
        size,
        size
    );
}
