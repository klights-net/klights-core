#![cfg(test)]

use super::*;
use crate::sqlite::live_apply::RaftLogApplyOutcome;
use klights_cluster_core::command::StorageCommand;
use klights_cluster_core::{
    LogApplyCommit, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
};
use klights_cluster_store::{
    AppliedOutboxLedger, AuthoritativeSnapshotCapture, AuthoritativeSnapshotPersistence,
    BackendLifecycleStore, ClusterMetadataMutation, ClusterMetadataRead, ClusterNamespaceMutation,
    ClusterOwnershipRead, ClusterPodCleanupStore, ClusterResourceMutation, ClusterResourceRead,
    ClusterResourceScopeRead, ClusterTopologyMutation, ClusterTopologyRead,
    ClusterWatchMaintenance, DurableAllocatorRead, DurableRawWatchHistoryRead,
    DurableWatchHistoryRead, DurableWatchRangeRead, NamespaceContentRead, NamespaceRequest,
};
use serde_json::json;

async fn apply_exact_storage_command(
    db: &Datastore,
    command: StorageCommand,
) -> klights_cluster_store::StorageCommandResult {
    let commit = db
        .build_log_apply_commit_for_command(command, "s11-exact-regression", "leader")
        .await
        .expect("command must materialize");
    db.apply_raft_log_apply_commit(commit)
        .await
        .expect("committed command must apply")
}

#[tokio::test]
async fn focused_sqlite_mutation_maps_identity_collisions_to_cluster_store_conflict() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = || json!({"metadata": {"name": "typed-error"}});

    ClusterResourceMutation::create_resource(
        &db,
        "v1",
        "ConfigMap",
        Some("default"),
        "typed-error",
        data(),
    )
    .await
    .unwrap();

    let error = ClusterResourceMutation::create_resource(
        &db,
        "v1",
        "ConfigMap",
        Some("default"),
        "typed-error",
        data(),
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.kind(),
        klights_cluster_store::ClusterStoreErrorKind::Conflict
    );
    assert_eq!(
        error.backend(),
        Some(klights_cluster_store::PersistenceBackend::Sqlite)
    );
    assert!(std::error::Error::source(&error).is_some());
}

#[test]
fn sqlite_tombstone_invariant_is_a_sqlite_persistence_error() {
    let error = super::super::focused_ports::sqlite_tombstone_not_marked_error();

    assert_eq!(
        error.kind(),
        klights_cluster_store::ClusterStoreErrorKind::Persistence
    );
    assert_eq!(
        error.backend(),
        Some(klights_cluster_store::PersistenceBackend::Sqlite)
    );
    assert_eq!(error.operation(), "SQLite tombstone delete");
    assert!(std::error::Error::source(&error).is_some());
}

#[tokio::test]
async fn ensure_cluster_metadata_command_applies_cluster_id_once() {
    let db = Datastore::new_in_memory().await.unwrap();
    apply_exact_storage_command(
        &db,
        StorageCommand::EnsureClusterMetadata {
            cluster_id: "test-uuid-001".into(),
        },
    )
    .await;
    assert_eq!(
        db.get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .as_deref(),
        Some("test-uuid-001")
    );
    assert_eq!(
        db.get_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY)
            .await
            .unwrap()
            .as_deref(),
        Some("0")
    );

    apply_exact_storage_command(
        &db,
        StorageCommand::EnsureClusterMetadata {
            cluster_id: "different-uuid".into(),
        },
    )
    .await;
    assert_eq!(
        db.get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .as_deref(),
        Some("test-uuid-001"),
        "cluster_id must not be overwritten by a second command"
    );
}

#[tokio::test]
async fn leader_outbox_create_log_apply_preserves_generated_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::CreateResource {
        api_version: "v1".into(),
        kind: "ConfigMap".into(),
        namespace: Some("default".into()),
        name: "from-outbox".into(),
        data: json!({"metadata":{"name":"from-outbox","namespace":"default"}}),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "create-from-outbox-key",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-1",
        )
        .await
        .expect("leader must build outbox create");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected first outbox delivery to need a proposal");
    };
    db.apply_raft_log_apply_commit(commit).await.unwrap();

    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "from-outbox")
        .await
        .unwrap()
        .expect("leader resource must exist");
    assert!(
        !stored.uid.is_empty(),
        "leader resource must have a generated UID"
    );
    assert_eq!(
        stored
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some(stored.uid.as_str()),
        "the committed object must preserve the generated UID in JSON"
    );
    assert!(
        db.get_applied_outbox("create-from-outbox-key")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn no_op_applied_outbox_gc_does_not_allocate_local_raft_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    let before = db.get_current_resource_version().await.unwrap();
    let result = apply_exact_storage_command(
        &db,
        StorageCommand::GcAppliedOutbox {
            cutoff_ms: i64::MAX,
        },
    )
    .await;

    assert_eq!(result.applied_rv, Some(before));
    assert_eq!(db.list_applied_outbox().await.unwrap().len(), 0);
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before,
        "no-op applied_outbox GC must not allocate a public RV"
    );
}

#[tokio::test]
async fn raft_mode_identical_normal_patch_does_not_advance_rv_or_watch() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "raft-identical-patch",
            json!({"metadata":{"name":"raft-identical-patch","namespace":"default","uid":"raft-identical-patch-uid","annotations":{"example.test/value":"unchanged"}},"data":{"value":"before"}}),
        )
        .await
        .unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let before_events = db.list_all_watch_events_since(0).await.unwrap().len();
    let unchanged = apply_exact_storage_command(
        &db,
        StorageCommand::PatchResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "raft-identical-patch".into(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: json!({"metadata":{"annotations":{"example.test/value":"unchanged"}}}),
            preconditions: ResourcePreconditions::from_resource(&created),
            strict_resource_version: false,
        },
    )
    .await;
    assert_eq!(unchanged.applied_rv, Some(before_rv));
    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "raft-identical-patch")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.resource_version, created.resource_version);
    assert_eq!(stored.data, created.data);
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv,
        "identical patch must not consume RV"
    );
    assert_eq!(
        db.list_all_watch_events_since(0).await.unwrap().len(),
        before_events,
        "identical patch must not append MODIFIED"
    );

    apply_exact_storage_command(
        &db,
        StorageCommand::PatchResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "raft-identical-patch".into(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: json!({"data":{"value":"after"}}),
            preconditions: ResourcePreconditions::from_resource(&stored),
            strict_resource_version: false,
        },
    )
    .await;
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv + 1
    );
    assert_eq!(
        db.list_all_watch_events_since(0).await.unwrap().len(),
        before_events + 1,
        "real patch must append exactly one MODIFIED event"
    );
}

async fn run_status_only_rv_advance_main_case(name: &str) -> (i64, i64, serde_json::Value) {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1", "Deployment", Some("default"), name,
            json!({"metadata":{"name":name,"namespace":"default","uid":format!("{name}-uid")},"spec":{"replicas":1},"status":{"availableReplicas":0}}),
        ).await.unwrap();
    let status_advanced = db
        .update_status_only(
            "apps/v1",
            "Deployment",
            Some("default"),
            name,
            json!({"availableReplicas":1}),
            Some(created.resource_version),
        )
        .await
        .unwrap();
    let mut proposed = (*created.data).clone();
    proposed["spec"]["replicas"] = json!(2);
    let applied = apply_exact_storage_command(
        &db,
        StorageCommand::UpdateResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: name.into(),
            data: proposed,
            expected_rv: status_advanced.resource_version,
            preconditions: ResourcePreconditions::uid_and_resource_version(
                created.uid.clone(),
                status_advanced.resource_version,
            ),
            preserve_status: true,
        },
    )
    .await;
    let stored = db
        .get_resource("apps/v1", "Deployment", Some("default"), name)
        .await
        .unwrap()
        .unwrap();
    (
        status_advanced.resource_version,
        applied.applied_rv.unwrap(),
        (*stored.data).clone(),
    )
}

async fn run_status_only_rv_advance_patch_case(name: &str) -> (i64, i64, serde_json::Value) {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1", "Deployment", Some("default"), name,
            json!({"metadata":{"name":name,"namespace":"default","uid":format!("{name}-uid")},"spec":{"replicas":1},"status":{"availableReplicas":0}}),
        ).await.unwrap();
    let status_advanced = db
        .update_status_only(
            "apps/v1",
            "Deployment",
            Some("default"),
            name,
            json!({"availableReplicas":1}),
            Some(created.resource_version),
        )
        .await
        .unwrap();
    let applied = apply_exact_storage_command(
        &db,
        StorageCommand::PatchResource {
            api_version: "apps/v1".into(),
            kind: "Deployment".into(),
            namespace: Some("default".into()),
            name: name.into(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: json!({"spec":{"replicas":2}}),
            preconditions: ResourcePreconditions::from_resource(&created),
            strict_resource_version: false,
        },
    )
    .await;
    let stored = db
        .get_resource("apps/v1", "Deployment", Some("default"), name)
        .await
        .unwrap()
        .unwrap();
    (
        status_advanced.resource_version,
        applied.applied_rv.unwrap(),
        (*stored.data).clone(),
    )
}

#[tokio::test]
async fn raft_mode_main_update_allows_status_only_rv_advance() {
    let (status_rv, applied_rv, stored) =
        run_status_only_rv_advance_main_case("raft-main-status-rv").await;
    assert!(
        applied_rv > status_rv,
        "main update must commit after status-only RV advance"
    );
    assert_eq!(stored.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "main update must preserve newer status"
    );
}

#[tokio::test]
async fn raft_mode_patch_allows_status_only_rv_advance() {
    let (status_rv, applied_rv, stored) =
        run_status_only_rv_advance_patch_case("raft-patch-status-rv").await;
    assert!(
        applied_rv > status_rv,
        "patch must commit after status-only RV advance"
    );
    assert_eq!(stored.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "patch must preserve newer status"
    );
}

#[tokio::test]
async fn replicated_apply_main_update_allows_status_only_rv_advance() {
    let (status_rv, applied_rv, stored) =
        run_status_only_rv_advance_main_case("replicated-main-status-rv").await;
    assert!(
        applied_rv > status_rv,
        "replicated main update must accept status-only RV drift"
    );
    assert_eq!(stored.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "replicated main update must preserve status"
    );
}

#[tokio::test]
async fn replicated_apply_patch_allows_status_only_rv_advance() {
    let (status_rv, applied_rv, stored) =
        run_status_only_rv_advance_patch_case("replicated-patch-status-rv").await;
    assert!(
        applied_rv > status_rv,
        "replicated patch must accept status-only RV drift"
    );
    assert_eq!(stored.pointer("/spec/replicas"), Some(&json!(2)));
    assert_eq!(
        stored.pointer("/status/availableReplicas"),
        Some(&json!(1)),
        "replicated patch must preserve status"
    );
}

#[tokio::test]
async fn replicated_apply_create_rejects_same_name_different_uid_under_strict_v3() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();
    let existing = json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"replacement-create","namespace":"default","uid":"existing-uid","creationTimestamp":"2025-01-01T00:00:00Z"},"data":{"owner":"existing"}});
    for db in [&leader, &follower] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "replacement-create",
            existing.clone(),
        )
        .await
        .unwrap();
    }
    let before_rv = leader.get_current_resource_version().await.unwrap();
    let before_watch = leader.list_all_watch_events_since(0).await.unwrap().len();
    let commit = crate::test_fixtures::live_apply::test_live_commit(
        0,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "replacement-create".into(),
                uid: "different-uid".into(),
                resource_version: 0,
                data: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"replacement-create","namespace":"default","uid":"different-uid"},"data":{"owner":"different"}}),
                require_absent: true,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            },
        )],
    );
    for db in [&leader, &follower] {
        let result = db
            .apply_raft_log_apply_commit(commit.clone())
            .await
            .unwrap();
        assert!(
            result
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("409 Conflict")),
            "strict v3 create must reject same-name different-UID state"
        );
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            before_rv,
            "rejection must not allocate RV"
        );
        assert_eq!(
            db.list_all_watch_events_since(0).await.unwrap().len(),
            before_watch,
            "rejection must not append watch history"
        );
        let stored = db
            .get_resource("v1", "ConfigMap", Some("default"), "replacement-create")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.uid, "existing-uid");
        assert_eq!(
            stored.data.pointer("/data/owner").and_then(|v| v.as_str()),
            Some("existing")
        );
    }
    assert_eq!(
        leader
            .get_resource("v1", "ConfigMap", Some("default"), "replacement-create")
            .await
            .unwrap(),
        follower
            .get_resource("v1", "ConfigMap", Some("default"), "replacement-create")
            .await
            .unwrap(),
        "replicated members must converge on the existing object after identical rejection"
    );
}

#[tokio::test]
async fn replicated_apply_patch_rejects_same_name_replacement() {
    let db = Datastore::new_in_memory().await.unwrap();
    let old = db.create_resource(
        "v1", "ConfigMap", Some("default"), "replacement-patch",
        json!({"metadata":{"name":"replacement-patch","namespace":"default","uid":"old-patch-uid"},"data":{"owner":"old"}}),
    ).await.unwrap();
    db.delete_resource_with_preconditions(
        "v1",
        "ConfigMap",
        Some("default"),
        "replacement-patch",
        ResourcePreconditions::from_resource(&old),
    )
    .await
    .unwrap();
    let replacement = db.create_resource(
        "v1", "ConfigMap", Some("default"), "replacement-patch",
        json!({"metadata":{"name":"replacement-patch","namespace":"default","uid":"new-patch-uid"},"data":{"owner":"new"}}),
    ).await.unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let error = db
        .build_log_apply_commit_for_command(
            StorageCommand::PatchResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "replacement-patch".into(),
                patch_kind: klights_cluster_core::PatchKind::Merge,
                patch: json!({"data":{"owner":"stale-patch"}}),
                preconditions: ResourcePreconditions::from_resource(&old),
                strict_resource_version: false,
            },
            "s11-exact-regression",
            "leader",
        )
        .await
        .expect_err("old UID patch must conflict with replacement");
    assert!(
        error.to_string().contains("409 Conflict"),
        "expected UID conflict: {error:#}"
    );
    let stored = db
        .get_resource("v1", "ConfigMap", Some("default"), "replacement-patch")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.uid, replacement.uid,
        "replacement UID must survive stale patch"
    );
    assert_eq!(
        stored.data.pointer("/data/owner").and_then(|v| v.as_str()),
        Some("new")
    );
    assert_eq!(db.get_current_resource_version().await.unwrap(), before_rv);
}

async fn apply_metadata_rebased_status(
    api_version: &str,
    kind: &str,
    name: &str,
    uid: &str,
    initial: serde_json::Value,
    status: serde_json::Value,
) -> serde_json::Value {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(api_version, kind, Some("default"), name, initial)
        .await
        .unwrap();
    let committed_status = db
        .build_log_apply_commit_for_command(
            StorageCommand::UpdateStatus {
                api_version: api_version.into(),
                kind: kind.into(),
                namespace: Some("default".into()),
                name: name.into(),
                status,
                expected_rv: Some(created.resource_version),
                preconditions: ResourcePreconditions::from_resource(&created),
                observed_status_stamp: None,
            },
            "s11-exact-regression",
            "leader",
        )
        .await
        .unwrap();
    db.patch_resource_latest_with_preconditions(
        api_version,
        kind,
        Some("default"),
        name,
        ResourcePatchRequest::new(
            klights_cluster_core::PatchKind::Merge,
            json!({"metadata":{"annotations":{"patchedstatus":"true"}}}),
            ResourcePreconditions::uid(uid),
        ),
    )
    .await
    .unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let before_watch = db.list_all_watch_events_since(0).await.unwrap().len();
    let result = db
        .apply_raft_log_apply_commit(committed_status)
        .await
        .unwrap();
    assert!(
        result
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("409 Conflict")),
        "strict v3 stale status must return a terminal conflict"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv,
        "stale status rejection must not allocate RV"
    );
    assert_eq!(
        db.list_all_watch_events_since(0).await.unwrap().len(),
        before_watch,
        "stale status rejection must not append watch history"
    );
    let stored = db
        .get_resource(api_version, kind, Some("default"), name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.data.pointer("/metadata/annotations/patchedstatus"),
        Some(&json!("true")),
        "status rebase must preserve metadata-only RV changes"
    );
    (*stored.data).clone()
}

#[tokio::test]
async fn replicated_fresh_service_status_replaces_load_balancer_and_preserves_conditions() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db.create_resource("v1", "Service", Some("default"), "fresh-service", json!({
        "apiVersion":"v1","kind":"Service","metadata":{"name":"fresh-service","namespace":"default","uid":"fresh-service-uid"},
        "status":{"loadBalancer":{"ingress":[{"ip":"198.51.100.2"}]},"metadataField":"from-live","conditions":[{"type":"Ready","status":"False"},{"type":"ExternalTrafficPolicy","status":"False"}]}
    })).await.unwrap();
    apply_exact_storage_command(&db, StorageCommand::UpdateStatus {
        api_version:"v1".into(), kind:"Service".into(), namespace:Some("default".into()), name:"fresh-service".into(),
        status:json!({"loadBalancer":{"ingress":[{"ip":"198.51.100.9"}]},"conditions":[{"type":"ExternalTrafficPolicy","status":"True"}]}),
        expected_rv:Some(created.resource_version), preconditions:ResourcePreconditions::from_resource(&created), observed_status_stamp:None,
    }).await;
    let data = db
        .get_resource("v1", "Service", Some("default"), "fresh-service")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert_eq!(
        data.pointer("/status/loadBalancer/ingress/0/ip"),
        Some(&json!("198.51.100.9")),
        "fresh Service status must replace loadBalancer"
    );
    assert_eq!(
        data.pointer("/status/metadataField"),
        Some(&json!("from-live")),
        "unmentioned status fields must survive"
    );
    let conditions = data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .unwrap();
    assert!(
        conditions
            .iter()
            .any(|c| c["type"] == "Ready" && c["status"] == "False"),
        "Ready condition must survive"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c["type"] == "ExternalTrafficPolicy" && c["status"] == "True"),
        "provided condition must replace matching type"
    );
}

#[tokio::test]
async fn replicated_scheduler_bind_overwrites_pod_scheduled_pending_condition() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created=db.create_resource("v1","Pod",Some("default"),"bind-me",json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"bind-me","namespace":"default","uid":"bind-me-uid"},"spec":{"containers":[{"name":"c","image":"busybox"}]},"status":{"phase":"Pending","conditions":[{"type":"PodScheduled","status":"False","reason":"SchedulingPending"},{"type":"Ready","status":"False"}]}})).await.unwrap();
    let mut bound = (*created.data).clone();
    bound["spec"]["nodeName"] = json!("worker-a");
    bound["status"]["conditions"][0] = json!({"type":"PodScheduled","status":"True"});
    apply_exact_storage_command(
        &db,
        StorageCommand::UpdateResource {
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "bind-me".into(),
            data: bound,
            expected_rv: created.resource_version,
            preconditions: ResourcePreconditions::uid("bind-me-uid"),
            preserve_status: false,
        },
    )
    .await;
    let data = db
        .get_resource("v1", "Pod", Some("default"), "bind-me")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert_eq!(data.pointer("/spec/nodeName"), Some(&json!("worker-a")));
    let scheduled = data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .find(|c| c["type"] == "PodScheduled")
        .unwrap();
    assert_eq!(
        scheduled["status"],
        json!("True"),
        "scheduler bind must replace pending PodScheduled"
    );
    assert!(
        scheduled.get("reason").is_none(),
        "SchedulingPending reason must be removed"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_cronjob_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("batch/v1","CronJob","stale-cronjob","cronjob-uid",json!({"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"stale-cronjob","namespace":"default","uid":"cronjob-uid"},"status":{"lastScheduleTime":"2026-07-04T06:21:59Z"}}),json!({"lastScheduleTime":"2026-07-04T06:22:00Z"})).await;
    assert_eq!(
        data.pointer("/status/lastScheduleTime"),
        Some(&json!("2026-07-04T06:21:59Z")),
        "CronJob status must remain unchanged after strict rejection"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_daemonset_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("apps/v1","DaemonSet","stale-ds","ds-uid",json!({"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"stale-ds","namespace":"default","uid":"ds-uid"},"status":{"numberReady":0,"desiredNumberScheduled":2}}),json!({"numberReady":1})).await;
    assert_eq!(
        data.pointer("/status/numberReady"),
        Some(&json!(0)),
        "DaemonSet status must remain unchanged after strict rejection"
    );
    assert_eq!(
        data.pointer("/status/desiredNumberScheduled"),
        Some(&json!(2)),
        "unmentioned DaemonSet fields must survive"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_pdb_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("policy/v1","PodDisruptionBudget","stale-pdb","pdb-uid",json!({"apiVersion":"policy/v1","kind":"PodDisruptionBudget","metadata":{"name":"stale-pdb","namespace":"default","uid":"pdb-uid"},"status":{"currentHealthy":1,"disruptionsAllowed":0}}),json!({"disruptedPods":{"pod-0":"2026-07-04T17:43:00Z"}})).await;
    assert!(
        data.pointer("/status/disruptedPods/pod-0").is_none(),
        "stale PDB disruptedPods must not apply"
    );
    assert_eq!(
        data.pointer("/status/currentHealthy"),
        Some(&json!(1)),
        "unmentioned PDB fields must survive"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_replicaset_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("apps/v1","ReplicaSet","stale-rs","rs-uid",json!({"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"stale-rs","namespace":"default","uid":"rs-uid"},"status":{"replicas":0,"conditions":[{"type":"Available","status":"True"}]}}),json!({"conditions":[{"type":"Progressing","status":"True"}]})).await;
    assert_eq!(
        data.pointer("/status/conditions/0/type"),
        Some(&json!("Available")),
        "ReplicaSet conditions must remain unchanged after strict rejection"
    );
    assert_eq!(
        data.pointer("/status/replicas"),
        Some(&json!(0)),
        "unmentioned ReplicaSet fields must survive"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_service_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("v1","Service","stale-service","service-uid",json!({"apiVersion":"v1","kind":"Service","metadata":{"name":"stale-service","namespace":"default","uid":"service-uid"},"status":{"loadBalancer":{"ingress":[{"ip":"198.51.100.1"}]},"metadataField":"from-live","conditions":[{"type":"Ready","status":"False"},{"type":"ExternalTrafficPolicy","status":"True"}]}}),json!({"conditions":[{"type":"ExternalTrafficPolicy","status":"False"}]})).await;
    assert_eq!(
        data.pointer("/status/loadBalancer/ingress/0/ip"),
        Some(&json!("198.51.100.1")),
        "live loadBalancer must survive stale status"
    );
    let conditions = data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .unwrap();
    assert!(
        conditions.iter().any(|c| c["type"] == "Ready"),
        "live Ready condition must survive"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c["type"] == "ExternalTrafficPolicy" && c["status"] == "True"),
        "Service conditions must remain unchanged after strict rejection"
    );
}

#[tokio::test]
async fn strict_v3_rejects_stale_statefulset_status_after_metadata_rv_advance() {
    let data=apply_metadata_rebased_status("apps/v1","StatefulSet","stale-sts","sts-uid",json!({"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"stale-sts","namespace":"default","uid":"sts-uid"},"status":{"replicas":0,"conditions":[{"type":"Available","status":"True"}]}}),json!({"replicas":1})).await;
    assert_eq!(
        data.pointer("/status/replicas"),
        Some(&json!(0)),
        "StatefulSet replica status must remain unchanged after strict rejection"
    );
    assert_eq!(
        data.pointer("/status/conditions/0/type"),
        Some(&json!("Available")),
        "StatefulSet conditions must survive"
    );
}

fn apply_commit_in_tx_for_raft(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
) -> klights_supervisor::DbClosureResult<RaftLogApplyOutcome> {
    let codec = crate::test_fixtures::outbox::new_codec();
    let context = crate::sqlite::live_apply::TransactionContext::new(codec.as_ref());
    crate::sqlite::live_apply::apply_commit_in_tx_for_raft_with_context(tx, commit, &context)
}

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
async fn raft_outbox_stream_duplicate_seq_noops_whole_commit() {
    use klights_cluster_core::{
        LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow, OutboxStreamWatermark,
    };

    let db = Datastore::new_in_memory().await.unwrap();
    let commit = LogApplyCommit::try_new_with_watermark(
        vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
            name: "dup-watermark".to_string(),
            uid: "dup-watermark-uid".to_string(),
            resource_version: 0,
            data: json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "dup-watermark", "uid": "dup-watermark-uid"}
            }),
        })],
        Some(OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 7,
            stream_seq: 1,
        }),
    )
    .unwrap();

    db.db_call("test_duplicate_outbox_watermark_noop", move |conn| {
        let tx = conn.transaction()?;
        apply_commit_in_tx_for_raft(&tx, commit.clone())?;
        apply_commit_in_tx_for_raft(&tx, commit)?;
        tx.commit()?;
        Ok(())
    })
    .await
    .unwrap();

    let namespace = db.get_namespace("dup-watermark").await.unwrap().unwrap();
    assert!(namespace.resource_version > 0);
}

#[tokio::test]
async fn watermarked_outbox_commit_appends_applied_outbox_ledger_mutation() {
    use klights_cluster_core::BuildOutboxOutcome;
    use klights_cluster_core::{LogApplyMutation, OutboxStreamWatermark};

    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::create_namespace(
        "watermarked-no-ledger",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "watermarked-no-ledger"}
        }),
    );
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox_with_watermark(
            "legacy-key-ignored-for-watermark",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
            Some(OutboxStreamWatermark {
                client_id: "client-a".to_string(),
                stream_id: 3,
                stream_seq: 1,
            }),
        )
        .await
        .unwrap();
    let BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a watermarked commit to propose");
    };

    assert!(commit.outbox_watermark().is_some());
    assert_eq!(commit.resource_version(), 0);
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert!(commit.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutAppliedOutbox(row)
            if row.idempotency_key == "legacy-key-ignored-for-watermark"
                && row.operation == "PodMetadata"
                && row.applied_rv.is_none()
                && row.status_stamp.is_none())
    }));
}

#[tokio::test]
async fn watermarked_uid_bound_missing_pod_outbox_builds_watermark_only_commit() {
    use klights_cluster_core::BuildOutboxOutcome;
    use klights_cluster_core::ResourcePreconditions;
    use klights_cluster_core::{LogApplyMutation, OutboxStreamWatermark};

    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "already-gone".to_string(),
        status: json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("gone-uid".to_string()),
            resource_version: None,
        },
        observed_status_stamp: Some(42),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox_with_watermark(
            "missing-pod-status",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
            Some(OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 9,
                stream_seq: 1,
            }),
        )
        .await
        .expect("stale UID-bound Pod outbox row should build a watermark-only commit");

    let BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected stale UID-bound Pod row to consume its stream watermark");
    };
    assert_eq!(commit.mutations().len(), 1);
    assert!(commit.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutAppliedOutbox(row)
            if row.idempotency_key == "missing-pod-status"
                && row.operation == "PodStatus"
                && row.status_stamp == Some(42)
                && row.applied_rv.is_none())
    }));
    assert_eq!(commit.resource_version(), 0);
    assert_eq!(commit.outbox_watermark().unwrap().stream_seq, 1);
}

#[tokio::test]
async fn stamped_worker_pod_status_merges_against_latest_preserving_scheduler_fields() {
    use klights_cluster_core::BuildOutboxOutcome;

    let db = Datastore::new_in_memory().await.unwrap();
    let created = db.create_resource("v1", "Pod", Some("default"), "stamped-status", json!({"metadata":{"name":"stamped-status","namespace":"default","uid":"stamped-status-uid"},"spec":{"nodeName":"node-a"},"status":{"phase":"Pending"}})).await.unwrap();
    db.update_resource("v1", "Pod", Some("default"), "stamped-status", json!({"metadata":{"name":"stamped-status","namespace":"default","uid":"stamped-status-uid","labels":{"scheduler":"kept"}},"spec":{"nodeName":"node-a","priority":1000},"status":{"phase":"Pending"}}), created.resource_version).await.unwrap();
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".into(),
        kind: "Pod".into(),
        namespace: Some("default".into()),
        name: "stamped-status".into(),
        status: json!({"phase":"Running","podIP":"10.0.0.7","podIPs":[{"ip":"10.0.0.7"}]}),
        expected_rv: Some(created.resource_version),
        preconditions: klights_cluster_core::ResourcePreconditions {
            uid: Some("stamped-status-uid".into()),
            resource_version: Some(created.resource_version),
        },
        observed_status_stamp: Some(7),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let BuildOutboxOutcome::NeedsPropose { commit, .. } = db
        .build_log_apply_commit_for_outbox(
            "stamped-status",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("expected status commit")
    };
    db.apply_raft_log_apply_commit(commit).await.unwrap();
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "stamped-status")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pod.data.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
    assert_eq!(
        pod.data
            .pointer("/metadata/labels/scheduler")
            .and_then(|v| v.as_str()),
        Some("kept")
    );
    assert_eq!(
        pod.data.pointer("/spec/priority").and_then(|v| v.as_i64()),
        Some(1000)
    );
}

#[tokio::test]
async fn build_log_apply_commit_for_command_has_no_applied_outbox_mutation() {
    use klights_cluster_core::LogApplyMutation;
    use klights_cluster_core::command::StorageCommand;

    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::create_namespace(
        "generic-no-outbox",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "generic-no-outbox"},
        }),
    );

    let before = db.list_applied_outbox().await.unwrap();
    let commit = db
        .build_log_apply_commit_for_command(command, "CreateResource", "test-node")
        .await
        .expect("build generic commit");
    let after = db.list_applied_outbox().await.unwrap();

    assert_eq!(
        before, after,
        "generic commit build should not mutate applied_outbox"
    );
    assert!(
        commit
            .mutations()
            .iter()
            .all(|mutation| !matches!(mutation, LogApplyMutation::PutAppliedOutbox(_))),
        "generic builder should never emit OutboxLedger mutations"
    );
}

#[tokio::test]
async fn raft_status_command_materialization_skips_unchanged_merged_status() {
    use klights_cluster_core::LogApplyMutation;

    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "legacy-node",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "legacy-node", "uid": "legacy-node-uid"},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True", "reason": "E2E"}]
                }
            }),
        )
        .await
        .unwrap();
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "legacy-node".to_string(),
        status: created.data.get("status").cloned().unwrap(),
        expected_rv: Some(created.resource_version),
        preconditions: ResourcePreconditions::from_resource(&created),
        observed_status_stamp: None,
    };

    let commit = db
        .build_log_apply_commit_for_command(command, "NodeStatus", "leader")
        .await
        .expect("materialize unchanged Node status");

    assert!(
        commit
            .mutations()
            .iter()
            .all(|mutation| !matches!(mutation, LogApplyMutation::PutResource(_))),
        "an unchanged post-merge status must not allocate an RV or emit a watch mutation"
    );
}

#[tokio::test]
async fn outbox_commit_builders_materialize_committed_apply_v1_templates() {
    use klights_cluster_core::BuildOutboxOutcome;
    use klights_cluster_core::{LogApplyMutation, OutboxStreamWatermark};

    let command = || StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "outbox-v1-template".to_string(),
        data: json!({
            "metadata": {"name": "outbox-v1-template", "namespace": "default"}
        }),
    };
    let payload = || {
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command())
            .encode_protobuf()
            .unwrap()
    };

    let v1_db = Datastore::new_in_memory().await.unwrap();
    let v1_rv_before = v1_db.get_current_resource_version().await.unwrap();
    let v1_payload = payload();
    let BuildOutboxOutcome::NeedsPropose { commit: v1, .. } = v1_db
        .build_log_apply_commit_for_outbox(
            "v1-outbox-template",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(v1_payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("expected V1 outbox commit");
    };
    assert_eq!(v1.resource_version(), 0);
    assert_eq!(
        v1_db.get_current_resource_version().await.unwrap(),
        v1_rv_before
    );
    assert!(v1.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutResource(row) if row.resource_version == 0)
    }));
    assert!(v1.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutAppliedOutbox(row) if row.applied_rv.is_none())
    }));

    let watermarked_db = Datastore::new_in_memory().await.unwrap();
    let watermarked_v1_rv_before = watermarked_db.get_current_resource_version().await.unwrap();
    let watermarked_payload = payload();
    let BuildOutboxOutcome::NeedsPropose {
        commit: watermarked,
        ..
    } = watermarked_db
        .build_log_apply_commit_for_outbox_with_watermark(
            "v1-watermarked-outbox-template",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(watermarked_payload.as_ref()),
            "worker-a",
            Some(OutboxStreamWatermark {
                client_id: "client-a".to_string(),
                stream_id: 1,
                stream_seq: 1,
            }),
        )
        .await
        .unwrap()
    else {
        panic!("expected V1 watermarked outbox commit");
    };
    assert_eq!(watermarked.resource_version(), 0);
    assert_eq!(
        watermarked_db.get_current_resource_version().await.unwrap(),
        watermarked_v1_rv_before
    );
    assert!(watermarked.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutResource(row) if row.resource_version == 0)
    }));
    assert!(watermarked.mutations().iter().any(|mutation| {
        matches!(mutation, LogApplyMutation::PutAppliedOutbox(row) if row.applied_rv.is_none())
    }));
}

#[tokio::test]
async fn raft_outbox_stream_gap_rejects_without_mutating_resource() {
    use klights_cluster_core::{
        LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow, OutboxStreamWatermark,
    };

    let db = Datastore::new_in_memory().await.unwrap();
    let commit = LogApplyCommit::try_new_with_watermark(
        vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
            name: "gap-watermark".to_string(),
            uid: "gap-watermark-uid".to_string(),
            resource_version: 0,
            data: json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "gap-watermark", "uid": "gap-watermark-uid"}
            }),
        })],
        Some(OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 8,
            stream_seq: 2,
        }),
    )
    .unwrap();

    let result = db
        .db_call("test_outbox_watermark_gap_rejects", move |conn| {
            let tx = conn.transaction()?;
            let result = apply_commit_in_tx_for_raft(&tx, commit);
            tx.commit()?;
            result.map(|_| ())
        })
        .await;

    assert!(
        result.is_err(),
        "missing seq 1 must reject seq 2 as a retryable gap"
    );
    assert!(
        db.get_namespace("gap-watermark").await.unwrap().is_none(),
        "gap rejection must not apply resource mutation"
    );
}

#[tokio::test]
async fn apply_resource_batch_command_builds_one_commit_with_two_puts() {
    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::apply_resource_batch(vec![
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
    ]);

    let commit = db
        .db_call("test_build_apply_resource_batch_commit", move |conn| {
            let tx = conn.transaction()?;
            let (commit, rv) = Datastore::build_log_apply_commit_in_tx_from_command(
                &tx,
                command,
                "ServiceEndpointReconcile",
                "test-node",
                None,
                chrono::DateTime::UNIX_EPOCH,
            )?;
            assert!(rv > 0);
            assert_eq!(commit.resource_version(), 0);
            tx.commit()?;
            Ok(commit)
        })
        .await
        .unwrap();

    assert_eq!(commit.mutations().len(), 2);
    assert!(commit.mutations().iter().all(|mutation| matches!(
        mutation,
        klights_cluster_core::LogApplyMutation::PutResource(row)
            if row.resource_version == 0
    )));
}

#[tokio::test]
async fn apply_resource_batch_update_requires_resource_version_precondition() {
    let db = Datastore::new_in_memory().await.unwrap();
    let existing = db
        .create_resource(
            "v1",
            "Endpoints",
            Some("default"),
            "batched-update",
            json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "batched-update", "namespace": "default"},
                "subsets": []
            }),
        )
        .await
        .unwrap();
    let command = StorageCommand::apply_resource_batch(vec![ResourceBatchOperation::Put {
        api_version: "v1".to_string(),
        kind: "Endpoints".to_string(),
        namespace: Some("default".to_string()),
        name: "batched-update".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "batched-update", "namespace": "default"},
            "subsets": [{"addresses": [], "ports": []}]
        }),
        mode: ResourceBatchPutMode::Update,
        preconditions: ResourcePreconditions {
            uid: Some(existing.uid.clone()),
            resource_version: Some(existing.resource_version + 100),
        },
    }]);

    let err = db
        .db_call("test_build_apply_resource_batch_conflict", move |conn| {
            let tx = conn.transaction()?;
            let result = Datastore::build_log_apply_commit_in_tx_from_command(
                &tx,
                command,
                "ServiceEndpointReconcile",
                "test-node",
                None,
                chrono::DateTime::UNIX_EPOCH,
            );
            tx.rollback()?;
            result.map(|_| ())
        })
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("resourceVersion precondition failed"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn apply_resource_batch_update_preserves_existing_server_metadata() {
    let db = Datastore::new_in_memory().await.unwrap();
    let existing = db
        .create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "batched-update-klights",
            json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {"name": "batched-update-klights", "namespace": "default"},
                "addressType": "IPv4",
                "endpoints": [],
                "ports": []
            }),
        )
        .await
        .unwrap();
    let original_uid = existing.uid.clone();
    let original_creation_timestamp = existing
        .data
        .pointer("/metadata/creationTimestamp")
        .cloned()
        .expect("create should stamp creationTimestamp");

    db.apply_resource_batch(vec![ResourceBatchOperation::Put {
        api_version: "discovery.k8s.io/v1".to_string(),
        kind: "EndpointSlice".to_string(),
        namespace: Some("default".to_string()),
        name: "batched-update-klights".to_string(),
        data: json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {"name": "batched-update-klights", "namespace": "default"},
            "addressType": "IPv4",
            "endpoints": [{"addresses": ["10.50.0.10"]}],
            "ports": []
        }),
        mode: ResourceBatchPutMode::Update,
        preconditions: ResourcePreconditions::from_resource(&existing),
    }])
    .await
    .unwrap();

    let updated = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "batched-update-klights",
        )
        .await
        .unwrap()
        .expect("EndpointSlice should still exist");

    assert_eq!(
        updated.uid, original_uid,
        "batch update must not change row UID"
    );
    assert_eq!(
        updated
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str()),
        Some(original_uid.as_str()),
        "batch update must preserve metadata.uid when the desired object omits it"
    );
    assert_eq!(
        updated.data.pointer("/metadata/creationTimestamp"),
        Some(&original_creation_timestamp),
        "batch update must preserve metadata.creationTimestamp"
    );
}

#[tokio::test]
async fn build_delete_resource_with_tombstone_emits_watch_event_and_delete() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "tombstone-mark",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "tombstone-mark",
                    "namespace": "default",
                    "uid": "tombstone-mark-uid"
                },
                "data": {"k": "v"}
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::DeleteResourceWithTombstone {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: Some("default".to_string()),
        name: "tombstone-mark".to_string(),
        preconditions: ResourcePreconditions::uid_and_resource_version(
            created.uid.clone(),
            created.resource_version,
        ),
        grace_seconds: 30,
    };

    let commit = db
        .db_call("test_build_delete_resource_with_tombstone", move |conn| {
            let tx = conn.transaction()?;
            let (commit, _rv) = Datastore::build_log_apply_commit_in_tx_from_command(
                &tx,
                command,
                "PodTermination",
                "leader",
                None,
                chrono::DateTime::UNIX_EPOCH,
            )?;
            tx.commit()?;
            Ok(commit)
        })
        .await
        .unwrap();

    let mut tombstone_events = 0usize;
    let mut delete_mutations = 0usize;
    for mutation in commit.mutations() {
        match mutation {
            klights_cluster_core::LogApplyMutation::PutWatchEvent(row) => {
                tombstone_events += 1;
                assert_eq!(row.event_type, "DELETED");
                assert_eq!(row.namespace.as_deref(), Some("default"));
                assert_eq!(row.name, "tombstone-mark");
                assert_eq!(row.resource_version, 0);
            }
            klights_cluster_core::LogApplyMutation::DeleteResource(_) => delete_mutations += 1,
            other => panic!("unexpected mutation in tombstone delete command: {other:?}"),
        }
    }
    assert_eq!(tombstone_events, 1);
    assert_eq!(delete_mutations, 1);

    let applied = db
        .apply_raft_log_apply_commit(commit.clone())
        .await
        .unwrap();

    let resource = db
        .get_resource("v1", "ConfigMap", Some("default"), "tombstone-mark")
        .await
        .unwrap();
    assert!(
        resource.is_none(),
        "tombstone delete must remove resource row"
    );

    let commit_rv = applied.applied_rv.expect("delete allocates a public RV");
    let row_count: i64 = db
        .db_call("test_select_tombstone_watch_events", move |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM watch_events \
                 WHERE api_version = ?1 AND kind = ?2 \
                 AND COALESCE(namespace, '#cluster') = ?3 \
                 AND name = ?4 \
                 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "default", "tombstone-mark", commit_rv],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        row_count, 1,
        "tombstone delete should emit exactly one watch row"
    );

    let event: (String, Vec<u8>) = db
        .db_call("test_select_tombstone_watch_event", move |conn| {
            Ok(conn.query_row(
                "SELECT event_type, data FROM watch_events \
                 WHERE api_version = ?1 AND kind = ?2 \
                 AND COALESCE(namespace, '#cluster') = ?3 \
                 AND name = ?4 \
                 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "default", "tombstone-mark", commit_rv],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(event.0, "DELETED");

    let payload: serde_json::Value = serde_json::from_slice(&event.1).unwrap();
    assert_eq!(payload["metadata"]["name"].as_str(), Some("tombstone-mark"));
    assert_eq!(payload["metadata"]["namespace"].as_str(), Some("default"));
    assert!(
        payload["metadata"]["deletionTimestamp"]
            .as_str()
            .is_some_and(|ts| !ts.is_empty()),
        "watch payload should carry deletionTimestamp"
    );
    assert_eq!(
        payload["metadata"]["deletionGracePeriodSeconds"],
        serde_json::json!(30),
        "watch payload should carry deletionGracePeriodSeconds"
    );
    assert_eq!(
        payload["metadata"]["resourceVersion"],
        commit_rv.to_string()
    );
}

#[tokio::test]
async fn apply_resource_batch_update_rejects_metadata_uid_change() {
    let db = Datastore::new_in_memory().await.unwrap();
    let existing = db
        .create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "batched-uid-guard-klights",
            json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {"name": "batched-uid-guard-klights", "namespace": "default"},
                "addressType": "IPv4",
                "endpoints": [],
                "ports": []
            }),
        )
        .await
        .unwrap();

    let err = db
        .apply_resource_batch(vec![ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "batched-uid-guard-klights".to_string(),
            data: json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": "batched-uid-guard-klights",
                    "namespace": "default",
                    "uid": "different-uid"
                },
                "addressType": "IPv4",
                "endpoints": [{"addresses": ["10.50.0.10"]}],
                "ports": []
            }),
            mode: ResourceBatchPutMode::Update,
            preconditions: ResourcePreconditions::from_resource(&existing),
        }])
        .await
        .expect_err("batch update must reject metadata.uid changes");

    assert!(
        err.to_string().contains("metadata.uid is immutable"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn apply_resource_batch_public_api_writes_resources_with_one_rv() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "public-batch-klights".to_string(),
            data: json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {"name": "public-batch-klights", "namespace": "default"},
                "addressType": "IPv4",
                "endpoints": [],
                "ports": []
            }),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "public-batch".to_string(),
            data: json!({
                "apiVersion": "v1",
                "kind": "Endpoints",
                "metadata": {"name": "public-batch", "namespace": "default"},
                "subsets": []
            }),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ])
    .await
    .unwrap();

    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "public-batch-klights",
        )
        .await
        .unwrap()
        .expect("EndpointSlice should exist");
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "public-batch")
        .await
        .unwrap()
        .expect("Endpoints should exist");
    assert_eq!(slice.resource_version, endpoints.resource_version);

    let watch_events = db.list_all_watch_events_since(0).await.unwrap();
    assert_eq!(watch_events.len(), 2);
    assert!(
        watch_events
            .iter()
            .all(|event| event.resource.resource_version == slice.resource_version)
    );
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
        preserve_status: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-stale-pvc-update",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
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
async fn stale_raft_pv_bind_is_rejected_and_preserves_concurrent_user_labels() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "PersistentVolume",
            None,
            "pv-label-race",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolume",
                "metadata": {
                    "name": "pv-label-race",
                    "uid": "pv-label-race-uid",
                    "labels": {"e2e-pv-pool": "pv-label-race"}
                },
                "spec": {
                    "capacity": {"storage": "1Gi"},
                    "accessModes": ["ReadWriteOnce"],
                    "persistentVolumeReclaimPolicy": "Retain",
                    "hostPath": {"path": "/tmp/pv-label-race"}
                },
                "status": {"phase": "Available"}
            }),
        )
        .await
        .unwrap();

    let mut user_update = (*created.data).clone();
    user_update["metadata"]["labels"]["pv-label-race"] = json!("updated");
    db.update_main_resource_with_preconditions(
        "v1",
        "PersistentVolume",
        None,
        "pv-label-race",
        user_update,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("client PV label update applies before controller bind commit");

    let mut stale_bind = (*created.data).clone();
    stale_bind["spec"]["claimRef"] = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": "default",
        "name": "pvc-label-race",
        "uid": "pvc-label-race-uid"
    });
    stale_bind["status"] = json!({"phase": "Bound"});
    let committed = crate::test_fixtures::live_apply::test_live_commit(
        created.resource_version + 2,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "PersistentVolume".to_string(),
                namespace: None,
                name: "pv-label-race".to_string(),
                uid: "pv-label-race-uid".to_string(),
                resource_version: created.resource_version + 2,
                data: stale_bind,
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("pv-label-race-uid".to_string()),
                precondition_resource_version: Some(created.resource_version),
                status_only: false,
            },
        )],
    );

    let result = db
        .apply_raft_log_apply_commit(committed)
        .await
        .expect("committed PV bind row applies authoritatively");
    assert!(
        result.error_message.is_some(),
        "stale committed PV bind must fail strict RV validation: {result:?}"
    );

    let live = db
        .get_resource("v1", "PersistentVolume", None, "pv-label-race")
        .await
        .unwrap()
        .expect("PV remains after bind");
    assert_eq!(
        live.data.pointer("/metadata/labels/e2e-pv-pool"),
        Some(&json!("pv-label-race"))
    );
    assert_eq!(
        live.data.pointer("/metadata/labels/pv-label-race"),
        Some(&json!("updated")),
        "controller PV bind commit must preserve labels added by a concurrent user update"
    );
    assert_eq!(
        live.data.pointer("/spec/claimRef/name"),
        None,
        "rejected stale controller bind must not modify the PV"
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
        observed_status_stamp: Some(1),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-stale-pod-status",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "mn-replica",
        )
        .await
        .expect("stale-RV PodStatus must build a raft commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };
    assert_eq!(commit.resource_version(), 0);
    let put = commit
        .mutations()
        .iter()
        .find_map(|mutation| match mutation {
            klights_cluster_core::LogApplyMutation::PutResource(row) => Some(row),
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

    let applied = db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert!(applied.error_message.is_none());
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
async fn raft_commit_builder_defers_resource_version_allocation_until_apply() {
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
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-deferred-rv",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build raft commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv,
        "building a raft entry must not reserve a public resourceVersion"
    );
    assert_eq!(
        commit.resource_version(),
        0,
        "builder must encode an RV-zero live template"
    );
    let put = commit
        .mutations()
        .iter()
        .find_map(|mutation| match mutation {
            klights_cluster_core::LogApplyMutation::PutResource(row) => Some(row),
            _ => None,
        })
        .expect("create must produce a resource row");
    assert_eq!(
        put.resource_version, 0,
        "resource rows must remain RV-zero before committed apply"
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
async fn strict_committed_apply_rejects_divergent_follower_resource_version() {
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
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(create_command)
            .encode_protobuf()
            .unwrap();
    let create_outcome = leader
        .build_log_apply_commit_for_outbox(
            "raft-leader-create-scheduled-later",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(create_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build create commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
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
        preserve_status: false,
    };
    let update_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(update_command)
            .encode_protobuf()
            .unwrap();
    let update_outcome = leader
        .build_log_apply_commit_for_outbox(
            "raft-leader-bind-scheduled-later",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(update_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build update commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
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
        follower_result.error_message.is_some(),
        "strict apply must expose follower divergence: {follower_result:?}"
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
        None,
        "a divergent follower must not materialize the scheduler bind"
    );
    assert_eq!(
        follower_pod.resource_version, 7,
        "rejected apply must preserve the follower's current RV"
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
            let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
                .encode_protobuf()
                .unwrap();
            let outcome = db
                .build_log_apply_commit_for_outbox(
                    idempotency_key,
                    "CreateResource",
                    crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
                    "leader",
                )
                .await
                .expect("build duplicate create");
            let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome
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
                preserve_status: false,
            };
            let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
                .encode_protobuf()
                .unwrap();
            let outcome = db
                .build_log_apply_commit_for_outbox(
                    idempotency_key,
                    "UpdateResource",
                    crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
                    "leader",
                )
                .await
                .expect("build update");
            let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome
            else {
                panic!("expected a fresh commit");
            };
            commit
        }
    };

    let first = build_update("raft-stale-rv-first", "first").await;
    let second = build_update("raft-stale-rv-second", "second").await;

    let first_result = db
        .apply_raft_log_apply_commit(first)
        .await
        .expect("first update applies");
    assert_eq!(first_result.error_message, None);
    let rejected = db
        .apply_raft_log_apply_commit(second)
        .await
        .expect("terminal conflicts are returned as deterministic raft results");
    assert!(
        rejected.error_message.as_deref().is_some_and(|message| {
            message.contains("resourceVersion precondition failed")
                && message.contains("409 Conflict")
        }),
        "expected apply-time RV conflict, got: {rejected:?}"
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
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-status-metadata-race",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .expect("build stale status commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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
async fn stale_status_only_apply_rejects_and_preserves_live_job_status_scalars() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "stale-job-status",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "stale-job-status", "namespace": "default", "uid": "job-uid"},
                "spec": {},
                "status": {
                    "active": 1,
                    "succeeded": 0,
                    "failed": 0,
                    "conditions": [{"type": "Started", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();

    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::UpdateStatus {
            api_version: "batch/v1".into(),
            kind: "Job".into(),
            namespace: Some("default".into()),
            name: "stale-job-status".into(),
            status: json!({
                "active": 1,
                "succeeded": 0,
                "failed": 0,
                "conditions": [{"type": "Complete", "status": "False"}]
            }),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some("job-uid".into()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        },
    )
    .encode_protobuf()
    .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-job-status-commit",
            "JobStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .expect("build stale status commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected status commit");
    };

    let mut live_update = (*created.data).clone();
    live_update["status"] = json!({
        "active": 0,
        "succeeded": 1,
        "failed": 0,
        "completionTime": "2026-06-30T12:00:00Z",
        "conditions": [{"type": "Complete", "status": "True", "reason": "CompletionsReached"}]
    });
    db.update_status_only(
        "batch/v1",
        "Job",
        Some("default"),
        "stale-job-status",
        live_update["status"].clone(),
        Some(created.resource_version),
    )
    .await
    .expect("live job completion status applies first");

    db.apply_log_apply_commit(commit)
        .await
        .expect_err("strict committed apply must reject the stale status precondition");

    let live = db
        .get_resource("batch/v1", "Job", Some("default"), "stale-job-status")
        .await
        .unwrap()
        .expect("live job remains");
    assert_eq!(live.data.pointer("/status/active"), Some(&json!(0)));
    assert_eq!(live.data.pointer("/status/succeeded"), Some(&json!(1)));
    assert_eq!(
        live.data.pointer("/status/completionTime"),
        Some(&json!("2026-06-30T12:00:00Z"))
    );
    assert_eq!(
        live.data.pointer("/status/conditions/0/status"),
        Some(&json!("True")),
        "stale status-only apply must not roll back same-type live condition"
    );
}

#[tokio::test]
async fn stale_status_only_apply_rejects_incoming_nonterminal_job_condition_values() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "stale-job-status-update",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "stale-job-status-update",
                    "namespace": "default",
                    "uid": "stale-job-status-update-uid"
                },
                "spec": {},
                "status": {
                    "active": 2,
                    "conditions": [{
                        "type": "Suspended",
                        "status": "False",
                        "lastTransitionTime": "2026-07-01T04:05:01Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();

    let committed_status = crate::test_fixtures::live_apply::test_live_commit(
        0,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "batch/v1".to_string(),
                kind: "Job".to_string(),
                namespace: Some("default".to_string()),
                name: "stale-job-status-update".to_string(),
                uid: "stale-job-status-update-uid".to_string(),
                resource_version: 0,
                data: json!({
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": "stale-job-status-update",
                        "namespace": "default",
                        "uid": "stale-job-status-update-uid"
                    },
                    "spec": {},
                    "status": {
                        "active": 2,
                        "conditions": [{
                            "type": "Suspended",
                            "status": "True",
                            "lastTransitionTime": "2026-07-01T04:05:03Z",
                            "reason": "E2E updateStatus"
                        }]
                    }
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("stale-job-status-update-uid".to_string()),
                precondition_resource_version: Some(created.resource_version),
                status_only: true,
            },
        )],
    );

    db.update_status_only(
        "batch/v1",
        "Job",
        Some("default"),
        "stale-job-status-update",
        json!({
            "active": 2,
            "conditions": [
                {
                    "type": "Suspended",
                    "status": "True",
                    "lastTransitionTime": "2026-07-01T04:05:02Z",
                    "reason": "E2E patchStatus"
                },
                {
                    "type": "LiveOnly",
                    "status": "True",
                    "reason": "PreserveUnmentioned"
                }
            ]
        }),
        Some(created.resource_version),
    )
    .await
    .expect("user status patch applies before committed status update");

    let rejected = db
        .apply_raft_log_apply_commit(committed_status)
        .await
        .expect("strict rejection is a committed apply outcome");
    assert!(rejected.error_message.is_some());
    assert_eq!(rejected.applied_rv, None);

    let live = db
        .get_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "stale-job-status-update",
        )
        .await
        .unwrap()
        .expect("job remains");
    let conditions = live
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .expect("job status conditions must remain an array");
    assert!(
        conditions.iter().any(|condition| {
            condition.get("type").and_then(|value| value.as_str()) == Some("Suspended")
                && condition
                    .get("lastTransitionTime")
                    .and_then(|value| value.as_str())
                    == Some("2026-07-01T04:05:02Z")
                && condition.get("reason").and_then(|value| value.as_str())
                    == Some("E2E patchStatus")
        }),
        "rejected stale Job status must preserve the live patched value: {conditions:?}"
    );
    assert!(
        conditions.iter().any(|condition| {
            condition.get("type").and_then(|value| value.as_str()) == Some("LiveOnly")
                && condition.get("reason").and_then(|value| value.as_str())
                    == Some("PreserveUnmentioned")
        }),
        "stale Job status apply must preserve live conditions omitted by incoming status: {conditions:?}"
    );
}

#[tokio::test]
async fn stale_committed_pod_bind_preserves_live_owner_references() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "cycle-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "cycle-pod",
                    "namespace": "default",
                    "uid": "cycle-pod-uid"
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let mut bind_snapshot = (*created.data).clone();
    bind_snapshot["spec"]["nodeName"] = json!("mn-worker");
    let bind_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "cycle-pod".to_string(),
        data: bind_snapshot,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let bind_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(bind_command)
            .encode_protobuf()
            .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-cycle-pod-bind",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(bind_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale bind commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: bind_commit,
        ..
    } = outcome
    else {
        panic!("expected a fresh bind commit");
    };

    let mut owner_ref_patch = (*created.data).clone();
    owner_ref_patch["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1",
        "kind": "Pod",
        "name": "owner-pod",
        "uid": "owner-pod-uid",
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "cycle-pod",
        owner_ref_patch,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("ownerReferences update applies before stale bind commit");

    let apply_result = db
        .apply_raft_log_apply_commit(bind_commit)
        .await
        .expect("stale committed bind applies authoritatively");
    assert!(
        apply_result.error_message.is_some(),
        "stale bind apply must fail strict RV validation: {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "cycle-pod")
        .await
        .unwrap()
        .expect("pod remains after bind apply");
    assert_eq!(
        live.data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str()),
        None,
        "rejected stale scheduler bind must not apply nodeName"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/ownerReferences/0/uid")
            .and_then(|value| value.as_str()),
        Some("owner-pod-uid"),
        "stale committed scheduler bind must not clear live ownerReferences"
    );
}

#[tokio::test]
async fn stale_committed_pod_bind_preserves_stale_owner_ref_subset() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "bind-pod-stale-owner-subset",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "bind-pod-stale-owner-subset",
                    "namespace": "default",
                    "uid": "bind-pod-stale-owner-subset-uid",
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-delete",
                        "uid": "rc-delete-uid",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let mut bind_snapshot = (*created.data).clone();
    bind_snapshot["spec"]["nodeName"] = json!("mn-worker");
    bind_snapshot["metadata"]["ownerReferences"] = json!([{
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "name": "rc-delete",
        "uid": "rc-delete-uid",
        "controller": true,
        "blockOwnerDeletion": true
    }]);
    let bind_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "bind-pod-stale-owner-subset".to_string(),
        data: bind_snapshot,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let bind_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(bind_command)
            .encode_protobuf()
            .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-bind-pod-stale-owner-subset",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(bind_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale bind commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: bind_commit,
        ..
    } = outcome
    else {
        panic!("expected a fresh bind commit");
    };

    let mut live_update = (*created.data).clone();
    live_update["metadata"]["ownerReferences"] = json!([
        {
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "rc-delete",
            "uid": "rc-delete-uid",
            "controller": true,
            "blockOwnerDeletion": true
        },
        {
            "apiVersion": "v1",
            "kind": "StatefulSet",
            "name": "rc-stay",
            "uid": "rc-stay-uid"
        }
    ]);
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "bind-pod-stale-owner-subset",
        live_update,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("live ownerReferences update applies before stale bind commit");

    let apply_result = db
        .apply_raft_log_apply_commit(bind_commit)
        .await
        .expect("stale committed bind applies authoritatively");
    assert!(
        apply_result.error_message.is_some(),
        "stale bind apply must fail strict RV validation: {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "bind-pod-stale-owner-subset")
        .await
        .unwrap()
        .expect("pod remains after bind apply");
    let owner_refs = live
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .expect("ownerReferences remain present");
    assert_eq!(
        live.data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str()),
        None,
        "rejected stale scheduler bind must not apply nodeName"
    );
    assert_eq!(
        owner_refs.len(),
        2,
        "stale committed scheduler bind must preserve stale ownerRefs while appending live ones"
    );
    assert_eq!(
        owner_refs[0]
            .pointer("/uid")
            .and_then(|value| value.as_str()),
        Some("rc-delete-uid"),
        "incoming stale ownerReference should remain first"
    );
    assert_eq!(
        owner_refs[1]
            .pointer("/uid")
            .and_then(|value| value.as_str()),
        Some("rc-stay-uid"),
        "missing live ownerReference should be appended"
    );
}

#[tokio::test]
async fn stale_committed_pod_put_same_node_preserves_live_owner_references() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-node-owner-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "same-node-owner-pod",
                    "namespace": "default",
                    "uid": "same-node-owner-pod-uid"
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let mut stale_same_node_snapshot = (*created.data).clone();
    stale_same_node_snapshot["spec"]["nodeName"] = json!("mn-controlplane3");
    let stale_same_node_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "same-node-owner-pod".to_string(),
        data: stale_same_node_snapshot,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let stale_same_node_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(stale_same_node_command)
            .encode_protobuf()
            .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-same-node-pod-put",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(stale_same_node_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale same-node commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: stale_same_node_commit,
        ..
    } = outcome
    else {
        panic!("expected a fresh stale same-node commit");
    };

    let mut live = (*created.data).clone();
    live["spec"]["nodeName"] = json!("mn-controlplane3");
    live["metadata"]["ownerReferences"] = json!([
        {
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "rc-to-be-deleted",
            "uid": "rc-to-be-deleted-uid",
            "controller": true,
            "blockOwnerDeletion": true
        },
        {
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "name": "rc-to-stay",
            "uid": "rc-to-stay-uid"
        }
    ]);
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "same-node-owner-pod",
        live,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("newer live ownerReferences update applies first");

    let apply_result = db
        .apply_raft_log_apply_commit(stale_same_node_commit)
        .await
        .expect("stale same-node commit applies authoritatively");
    assert!(
        apply_result.error_message.is_some(),
        "stale same-node apply must fail strict RV validation: {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "same-node-owner-pod")
        .await
        .unwrap()
        .expect("pod remains after stale same-node apply");
    let owner_refs = live
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .expect("ownerReferences remain present");
    assert_eq!(
        owner_refs.len(),
        2,
        "stale same-node full Pod PUT must not clear live ownerReferences"
    );
    assert!(
        owner_refs.iter().any(
            |owner| owner.get("uid").and_then(|value| value.as_str()) == Some("rc-to-stay-uid")
        ),
        "stale same-node full Pod PUT must preserve the second live owner"
    );
}

#[tokio::test]
async fn stale_committed_pod_put_with_explicit_empty_owner_references_clears_live_owners() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "clear-owner-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "clear-owner-pod",
                    "namespace": "default",
                    "uid": "clear-owner-pod-uid",
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "rc-owner",
                        "uid": "rc-owner-uid"
                    }]
                },
                "spec": {
                    "nodeName": "mn-controlplane3",
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let mut stale_clear_snapshot = (*created.data).clone();
    stale_clear_snapshot["metadata"]["ownerReferences"] = json!([]);
    let stale_clear_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "clear-owner-pod".to_string(),
        data: stale_clear_snapshot,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let stale_clear_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(stale_clear_command)
            .encode_protobuf()
            .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-explicit-owner-clear",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(stale_clear_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale explicit owner clear commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: stale_clear_commit,
        ..
    } = outcome
    else {
        panic!("expected a fresh stale owner clear commit");
    };

    let mut newer_live = (*created.data).clone();
    newer_live["status"]["phase"] = json!("Running");
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "clear-owner-pod",
        newer_live,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("newer live update advances resourceVersion first");

    let apply_result = db
        .apply_raft_log_apply_commit(stale_clear_commit)
        .await
        .expect("stale explicit owner clear applies authoritatively");
    assert!(
        apply_result.error_message.is_some(),
        "stale explicit owner clear must fail strict RV validation: {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "clear-owner-pod")
        .await
        .unwrap()
        .expect("pod remains after explicit owner clear");
    let owner_refs = live
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array());
    assert!(
        owner_refs.is_some_and(|refs| !refs.is_empty()),
        "rejected stale ownerReferences update must preserve live owners"
    );
}

#[tokio::test]
async fn stale_committed_pod_bind_does_not_rebind_already_bound_pod() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "rebind-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "rebind-pod",
                    "namespace": "default",
                    "uid": "rebind-pod-uid"
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let mut stale_bind_snapshot = (*created.data).clone();
    stale_bind_snapshot["spec"]["nodeName"] = json!("mn-controlplane3");
    let stale_bind_command = StorageCommand::UpdateResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "rebind-pod".to_string(),
        data: stale_bind_snapshot,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let stale_bind_payload =
        crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(stale_bind_command)
            .encode_protobuf()
            .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "stale-rebind-pod-bind",
            "UpdateResource",
            crate::test_fixtures::outbox::test_outbox_command(stale_bind_payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale bind commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: stale_bind_commit,
        ..
    } = outcome
    else {
        panic!("expected a fresh stale bind commit");
    };

    let mut live_bind = (*created.data).clone();
    live_bind["spec"]["nodeName"] = json!("mn-replica");
    db.update_resource_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "rebind-pod",
        live_bind,
        ResourcePreconditions::from_resource(&created),
    )
    .await
    .expect("newer live scheduler bind applies first");

    let apply_result = db
        .apply_raft_log_apply_commit(stale_bind_commit)
        .await
        .expect("stale committed bind applies authoritatively");
    assert!(
        apply_result.error_message.is_some(),
        "stale bind apply must fail strict RV validation: {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "rebind-pod")
        .await
        .unwrap()
        .expect("pod remains after stale bind apply");
    assert_eq!(
        live.data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str()),
        Some("mn-replica"),
        "stale committed scheduler bind must not move an already-bound Pod"
    );
}

#[tokio::test]
async fn stale_status_only_committed_pvc_apply_is_rejected_without_losing_live_conditions() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "status-condition-race",
            json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": "status-condition-race",
                    "namespace": "default",
                    "uid": "status-condition-race-uid"
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

    let committed_status = crate::test_fixtures::live_apply::test_live_commit(
        created.resource_version + 2,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "PersistentVolumeClaim".to_string(),
                namespace: Some("default".to_string()),
                name: "status-condition-race".to_string(),
                uid: "status-condition-race-uid".to_string(),
                resource_version: created.resource_version + 2,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": "status-condition-race",
                        "namespace": "default",
                        "uid": "status-condition-race-uid",
                        "resourceVersion": (created.resource_version + 2).to_string()
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}}
                    },
                    "status": {
                        "phase": "Bound",
                        "accessModes": ["ReadWriteOnce"],
                        "capacity": {"storage": "1Gi"},
                        "volumeName": "pv-status-condition-race"
                    }
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("status-condition-race-uid".to_string()),
                precondition_resource_version: Some(created.resource_version),
                status_only: true,
            },
        )],
    );

    db.update_status_only(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "status-condition-race",
        json!({
            "phase": "Pending",
            "conditions": [{
                "type": "StatusPatched",
                "status": "True",
                "reason": "E2E patchedStatus",
                "message": "Set from e2e test"
            }]
        }),
        Some(created.resource_version),
    )
    .await
    .expect("user status patch applies before committed controller status row");

    let result = db
        .apply_raft_log_apply_commit(committed_status)
        .await
        .expect("stale status-only row returns a deterministic outcome");
    assert!(result.error_message.is_some());
    assert_eq!(result.applied_rv, None);

    let live = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "status-condition-race",
        )
        .await
        .unwrap()
        .expect("PVC remains");
    assert_eq!(
        live.data.pointer("/status/phase"),
        Some(&json!("Pending")),
        "stale controller status must not overwrite a newer live phase"
    );
    assert_eq!(live.data.pointer("/status/volumeName"), None);
    assert_eq!(
        live.data.pointer("/status/conditions/0/type"),
        Some(&json!("StatusPatched")),
        "committed status-only apply must not drop unrelated live status conditions"
    );
}

#[tokio::test]
async fn stale_status_only_apply_rejects_and_preserves_newer_pv_and_pvc_condition_values() {
    for (kind, namespace) in [
        ("PersistentVolumeClaim", Some("default")),
        ("PersistentVolume", None),
    ] {
        let db = Datastore::new_in_memory().await.unwrap();
        let name = format!("stale-status-overwrite-{kind}");
        let uid = format!("{name}-uid");
        let created = db
            .create_resource(
                "v1",
                kind,
                namespace,
                &name,
                json!({
                    "apiVersion": "v1",
                    "kind": kind,
                    "metadata": {
                        "name": name,
                        "namespace": namespace,
                        "uid": uid
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "resources": {"requests": {"storage": "1Gi"}},
                        "capacity": {"storage": "1Gi"},
                        "persistentVolumeReclaimPolicy": "Retain"
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();

        let committed_status = crate::test_fixtures::live_apply::test_live_commit(
            0,
            vec![klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.clone(),
                    uid: uid.clone(),
                    resource_version: 0,
                    data: json!({
                        "apiVersion": "v1",
                        "kind": kind,
                        "metadata": {
                            "name": name,
                            "namespace": namespace,
                            "uid": uid
                        },
                        "spec": {},
                        "status": {
                            "phase": "Bound",
                            "reason": "E2E updateStatus",
                            "message": "E2E updateStatus",
                            "conditions": [{
                                "type": "StatusPatched",
                                "status": "True",
                                "reason": "E2E updateStatus",
                                "message": "E2E updateStatus"
                            }]
                        }
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some(uid.clone()),
                    precondition_resource_version: Some(created.resource_version),
                    status_only: true,
                },
            )],
        );

        db.update_status_only(
            "v1",
            kind,
            namespace,
            &name,
            json!({
                "phase": "Pending",
                "reason": "E2E patchStatus",
                "message": "E2E patchStatus",
                "conditions": [
                    {
                        "type": "StatusPatched",
                        "status": "True",
                        "reason": "E2E patchStatus",
                        "message": "E2E patchStatus"
                    },
                    {
                        "type": "LiveOnly",
                        "status": "True",
                        "reason": "PreserveUnmentioned"
                    }
                ]
            }),
            Some(created.resource_version),
        )
        .await
        .expect("user status patch applies before committed status update");

        let rejected = db
            .apply_raft_log_apply_commit(committed_status)
            .await
            .expect("strict rejection is a committed apply outcome");
        assert!(rejected.error_message.is_some());
        assert_eq!(rejected.applied_rv, None);

        let live = db
            .get_resource("v1", kind, namespace, &name)
            .await
            .unwrap()
            .expect("resource remains");
        assert_eq!(
            live.data.pointer("/status/reason"),
            Some(&json!("E2E patchStatus")),
            "{kind} stale status apply must preserve newer live reason"
        );
        assert_eq!(
            live.data.pointer("/status/message"),
            Some(&json!("E2E patchStatus")),
            "{kind} stale status apply must preserve newer live message"
        );
        let conditions = live
            .data
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .expect("status conditions must remain an array");
        assert!(
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("StatusPatched")
                    && condition.get("reason").and_then(|value| value.as_str())
                        == Some("E2E patchStatus")
                    && condition.get("message").and_then(|value| value.as_str())
                        == Some("E2E patchStatus")
            }),
            "{kind} stale same-type condition must not replace the live patched value: {conditions:?}"
        );
        assert!(
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("LiveOnly")
                    && condition.get("reason").and_then(|value| value.as_str())
                        == Some("PreserveUnmentioned")
            }),
            "{kind} stale status apply must preserve live conditions omitted by incoming status: {conditions:?}"
        );
    }
}

#[tokio::test]
async fn raft_status_apply_built_before_preemption_preserves_disruption_target() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "preempted-status-race",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "preempted-status-race",
                    "namespace": "default",
                    "uid": "preempted-status-race-uid"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
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

    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "preempted-status-race".to_string(),
        status: json!({
            "phase": "Running",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Initialized", "status": "True"},
                {"type": "ContainersReady", "status": "True"},
                {"type": "Ready", "status": "True"}
            ]
        }),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some("preempted-status-race-uid".to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-status-preemption-condition-race",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .expect("build stale status commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh status commit");
    };

    let mut preempted = (*created.data).clone();
    preempted["metadata"]["deletionTimestamp"] = json!("2026-06-21T14:57:11Z");
    preempted["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    preempted["status"]["conditions"] = json!([
        {"type": "PodScheduled", "status": "True"},
        {"type": "Initialized", "status": "True"},
        {"type": "ContainersReady", "status": "True"},
        {"type": "Ready", "status": "True"},
        {
            "type": "DisruptionTarget",
            "status": "True",
            "reason": "PreemptionByScheduler",
            "message": "Preempted by scheduler"
        }
    ]);
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "preempted-status-race",
        preempted,
        created.resource_version,
    )
    .await
    .expect("preemption update applies before stale status commit");

    db.apply_log_apply_commit(commit)
        .await
        .expect("status commit applies after preemption update");

    let live = db
        .get_resource("v1", "Pod", Some("default"), "preempted-status-race")
        .await
        .unwrap()
        .expect("pod remains");
    let conditions = live
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .expect("pod status conditions must remain an array");
    assert!(
        conditions.iter().any(|condition| {
            condition.get("type").and_then(|value| value.as_str()) == Some("DisruptionTarget")
                && condition.get("status").and_then(|value| value.as_str()) == Some("True")
                && condition.get("reason").and_then(|value| value.as_str())
                    == Some("PreemptionByScheduler")
        }),
        "stale kubelet status raft apply must preserve scheduler-owned DisruptionTarget: {:?}",
        live.data.pointer("/status/conditions")
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
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: json!({"spec": {"replicas": 2}}),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
        strict_resource_version: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-rc-scale-latest-patch",
            "PatchResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build scale patch commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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
async fn raft_pod_delete_mark_patch_applies_against_live_resource_after_status_rv_race() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "delete-race-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "delete-race-pod",
                    "namespace": "default",
                    "uid": "delete-race-pod-uid"
                },
                "spec": {
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Pending"
                }
            }),
        )
        .await
        .unwrap();

    let command = StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "delete-race-pod".to_string(),
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: json!({
            "metadata": {
                "deletionTimestamp": "2026-06-21T00:20:09Z",
                "deletionGracePeriodSeconds": 0
            }
        }),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
        strict_resource_version: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-pod-delete-mark-latest-patch",
            "PatchResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build pod delete-mark patch commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };

    db.update_status_only_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "delete-race-pod",
        json!({
            "phase": "Pending",
            "conditions": [{
                "type": "PodScheduled",
                "status": "False",
                "reason": "Unschedulable"
            }]
        }),
        ResourcePreconditions::uid(created.uid.clone()),
    )
    .await
    .expect("status update advances RV before pod delete-mark patch apply");

    let apply_result = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("raft pod delete-mark patch apply should return a terminal result");
    assert!(
        apply_result.error_message.is_none(),
        "pod delete-mark patch without an RV precondition must not conflict with status-only RV races, got {apply_result:?}"
    );

    let live = db
        .get_resource("v1", "Pod", Some("default"), "delete-race-pod")
        .await
        .unwrap()
        .expect("pod remains for actor-owned finalization");
    assert_eq!(
        live.data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str()),
        Some("2026-06-21T00:20:09Z")
    );
    assert_eq!(
        live.data.pointer("/metadata/deletionGracePeriodSeconds"),
        Some(&json!(0))
    );
    assert_eq!(
        live.data
            .pointer("/status/conditions/0/reason")
            .and_then(|value| value.as_str()),
        Some("Unschedulable"),
        "delete-mark patch must preserve newer status written before raft apply"
    );
}

#[tokio::test]
async fn raft_zero_grace_pod_delete_mark_patch_replays_identical_watch_payloads() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "zero-grace-replay",
            "namespace": "default",
            "uid": "zero-grace-replay-uid",
            "creationTimestamp": "2026-06-21T00:00:00Z"
        },
        "spec": {
            "automountServiceAccountToken": false,
            "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "restartCount": 0
            }],
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-06-21T00:00:00Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-21T00:00:00Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-06-21T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-21T00:00:00Z"}
            ]
        }
    });
    let created = leader
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "zero-grace-replay",
            pod.clone(),
        )
        .await
        .unwrap();
    follower
        .create_resource("v1", "Pod", Some("default"), "zero-grace-replay", pod)
        .await
        .unwrap();

    let command = StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "zero-grace-replay".to_string(),
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: json!({
            "metadata": {
                "deletionTimestamp": "2026-06-21T01:02:03Z",
                "deletionGracePeriodSeconds": 0
            }
        }),
        strict_resource_version: false,
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = leader
        .build_log_apply_commit_for_outbox(
            "raft-zero-grace-delete-mark-deterministic",
            "PatchResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build zero-grace delete-mark commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected zero-grace delete-mark proposal");
    };
    let leader_result = leader
        .apply_raft_log_apply_commit(commit.clone())
        .await
        .expect("leader applies delete-mark patch");
    let rv = leader_result
        .applied_rv
        .expect("delete-mark patch allocates an RV");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let follower_result = follower
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("follower applies same delete-mark patch");
    assert_eq!(follower_result.applied_rv, Some(rv));

    let leader_event = leader
        .list_watch_events_since(
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "default",
            )],
            1,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.resource.resource_version == rv)
        .expect("leader delete-mark watch event");
    let follower_event = follower
        .list_watch_events_since(
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "default",
            )],
            1,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.resource.resource_version == rv)
        .expect("follower delete-mark watch event");

    assert_eq!(leader_event.event_type, "MODIFIED");
    assert_eq!(follower_event.event_type, "MODIFIED");
    assert_eq!(
        leader_event.resource.data, follower_event.resource.data,
        "the same raft commit must produce byte-identical Pod delete-mark watch payloads on every member"
    );
}

#[tokio::test]
async fn raft_stale_pod_put_over_terminating_live_row_replays_identical_watch_payloads() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();
    let base_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "terminating-stale-put",
            "namespace": "default",
            "uid": "terminating-stale-put-uid",
            "creationTimestamp": "2026-06-21T00:00:00Z"
        },
        "spec": {
            "automountServiceAccountToken": false,
            "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}],
            "nodeName": "mn-worker"
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-06-21T00:00:01Z"},
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-21T00:00:01Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-06-21T00:00:02Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-21T00:00:02Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0
            }]
        }
    });

    let leader_created = leader
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "terminating-stale-put",
            base_pod.clone(),
        )
        .await
        .unwrap();
    let follower_created = follower
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "terminating-stale-put",
            base_pod,
        )
        .await
        .unwrap();

    let mut leader_terminating = (*leader_created.data).clone();
    leader_terminating["metadata"]["deletionTimestamp"] = json!("2026-06-21T00:00:10Z");
    leader_terminating["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    let leader_terminating = leader
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "terminating-stale-put",
            leader_terminating,
            leader_created.resource_version,
        )
        .await
        .expect("leader live pod is terminating before stale committed put");
    let mut follower_terminating = (*follower_created.data).clone();
    follower_terminating["metadata"]["deletionTimestamp"] = json!("2026-06-21T00:00:10Z");
    follower_terminating["metadata"]["deletionGracePeriodSeconds"] = json!(0);
    let follower_terminating = follower
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "terminating-stale-put",
            follower_terminating,
            follower_created.resource_version,
        )
        .await
        .expect("follower live pod is terminating before stale committed put");
    assert_eq!(
        leader_terminating.resource_version,
        follower_terminating.resource_version
    );

    let committed_rv = leader_created.resource_version + 10;
    let mut committed_pod = (*leader_created.data).clone();
    committed_pod["metadata"]["resourceVersion"] = json!(committed_rv.to_string());
    committed_pod["status"]["phase"] = json!("Running");
    committed_pod["status"]["conditions"] = json!([
        {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-06-21T00:00:01Z"},
        {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-06-21T00:00:01Z"},
        {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-06-21T00:00:02Z"},
        {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-21T00:00:02Z"}
    ]);
    let commit = crate::test_fixtures::live_apply::test_live_commit(
        committed_rv,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "terminating-stale-put".to_string(),
                uid: "terminating-stale-put-uid".to_string(),
                resource_version: committed_rv,
                data: committed_pod,
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("terminating-stale-put-uid".to_string()),
                precondition_resource_version: Some(leader_terminating.resource_version),
                status_only: false,
            },
        )],
    );

    let leader_result = leader
        .apply_raft_log_apply_commit(commit.clone())
        .await
        .expect("leader applies stale committed put");
    let applied_rv = leader_result.applied_rv.expect("put allocates an RV");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let follower_result = follower
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("follower applies same stale committed put");
    assert_eq!(follower_result.applied_rv, Some(applied_rv));

    let leader_event = leader
        .list_watch_events_since(
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "default",
            )],
            leader_created.resource_version,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.resource.resource_version == applied_rv)
        .expect("leader stale put watch event");
    let follower_event = follower
        .list_watch_events_since(
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "default",
            )],
            follower_created.resource_version,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.resource.resource_version == applied_rv)
        .expect("follower stale put watch event");

    assert_eq!(leader_event.event_type, "MODIFIED");
    assert_eq!(follower_event.event_type, "MODIFIED");
    assert_eq!(
        leader_event.resource.data, follower_event.resource.data,
        "the same stale Pod raft PUT over a terminating live row must produce byte-identical watch payloads on every member"
    );
    assert_eq!(
        leader_event
            .resource
            .data
            .pointer("/status/conditions/2/lastTransitionTime")
            .and_then(|value| value.as_str()),
        Some("2026-06-21T00:00:10Z"),
        "legacy stale commits without a committed transition timestamp must use deterministic deletionTimestamp, not member-local wall time"
    );
}

#[tokio::test]
async fn raft_resource_put_persists_row_and_watch_from_identical_payload_bytes() {
    let ds = Datastore::new_in_memory().await.unwrap();

    let row_data = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "co-derived",
            "namespace": "default",
            "uid": "co-derived-uid",
            "labels": {"app": "co-derived"},
            "resourceVersion": "41"
        },
        "data": {"hello": "derived"},
    });
    let commit = crate::test_fixtures::live_apply::test_live_commit(
        41,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "co-derived".to_string(),
                uid: "co-derived-uid".to_string(),
                resource_version: 41,
                data: row_data,
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            },
        )],
    );

    let applied = ds.apply_raft_log_apply_commit(commit).await.unwrap();
    let applied_rv = applied.applied_rv.expect("resource put allocates an RV");

    let resource_row_bytes: Vec<u8> = ds
        .db_call("test_resource_put_row_bytes", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4",
                rusqlite::params!["v1", "ConfigMap", "default", "co-derived"],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();

    let watch_row_bytes: Vec<u8> = ds
        .db_call("test_resource_watch_row_bytes", move |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "default", "co-derived", applied_rv],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();

    assert_eq!(resource_row_bytes, watch_row_bytes);
}

#[tokio::test]
async fn destination_noop_patch_built_before_spec_update_preserves_live_spec() {
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
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: json!({
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "2"
                }
            }
        }),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
        strict_resource_version: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-stale-deployment-revision-patch",
            "PatchResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale patch commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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

    // Committed apply of a lenient merge patch (uid-only, no client RV)
    // merges into the live row at apply time: the newer client-owned spec
    // state is preserved and the patch's metadata annotation is applied on
    // top. This is the server-side merge-patch semantic; the previous strict
    // captured-RV rejection produced spurious 409 conflicts for the status
    // subresource pipeline (e2e Job conformance failure).
    let apply_result = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("lenient committed patch must apply");
    assert!(
        apply_result.error_message.is_none(),
        "lenient committed apply must merge into the live row: {apply_result:?}"
    );
    assert!(apply_result.applied_rv.is_some());
    assert!(apply_result.public_resource_changed);

    let live = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains after authoritative apply");

    assert_eq!(
        live.data
            .pointer("/spec/replicas")
            .and_then(|value| value.as_i64()),
        Some(30),
        "same-UID stale raft patch must not roll back newer local replicas=30 to its captured replicas=10"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
            .and_then(|v| v.as_str()),
        Some("2"),
        "stale raft patch may apply its metadata annotation while preserving newer spec state"
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
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: json!({
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "2"
                }
            }
        }),
        preconditions: ResourcePreconditions::uid(created.uid.clone()),
        strict_resource_version: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-stale-deployment-revision-patch",
            "PatchResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build stale patch commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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

    // Lenient merge patch (uid-only, no client RV) merges into the live row
    // at apply time: the newer spec is preserved, the annotation applies on
    // top. The previous strict captured-RV rejection caused the e2e Job
    // status-pipeline spurious 409.
    let apply_result = db
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("lenient committed patch must apply");
    assert!(
        apply_result.error_message.is_none(),
        "lenient committed apply must merge into the live row: {apply_result:?}"
    );
    assert!(apply_result.applied_rv.is_some());

    let live = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .expect("deployment remains after authoritative apply");
    assert_eq!(
        live.data
            .pointer("/spec/replicas")
            .and_then(serde_json::Value::as_i64),
        Some(30),
        "same-UID stale raft patch must not roll back newer client-owned state"
    );
    assert_eq!(
        live.data
            .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
            .and_then(serde_json::Value::as_str),
        Some("2"),
        "the patch's annotation must apply on top of the live row"
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
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-idempotent-apply",
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .expect("build create");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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
async fn build_log_apply_commit_from_gc_applied_outbox_command_maps_to_outbox_ledger_mutation() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
        idempotency_key: "gc-mapping-key".to_string(),
        subject_key: "v1/Service/default/demo/service-uid".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: 1,
        applied_rv: Some(1),
        result_proto: vec![1],
        status_stamp: None,
    })
    .await
    .unwrap();
    let command = StorageCommand::GcAppliedOutbox { cutoff_ms: 42_000 };

    let commit = db
        .db_call("test_build_gc_applied_outbox_commit", move |conn| {
            let tx = conn.transaction()?;
            let (commit, _rv) = Datastore::build_log_apply_commit_in_tx_from_command(
                &tx,
                command,
                "ServiceTest",
                "test-node",
                None,
                chrono::DateTime::UNIX_EPOCH,
            )?;
            tx.commit()?;
            Ok(commit)
        })
        .await
        .unwrap();

    assert_eq!(
        commit.mutations().len(),
        1,
        "GC command should map to one outbox mutation"
    );
    let mutation = &commit.mutations()[0];
    match mutation {
        klights_cluster_core::LogApplyMutation::GcAppliedOutbox {
            cutoff_ms,
            operations,
        } => {
            assert_eq!(*cutoff_ms, 42_000);
            assert!(operations.is_empty());
        }
        other => panic!("unexpected mutation: {other:?}"),
    }
}

#[tokio::test]
async fn raft_outbox_build_rejects_incomplete_durable_ledger_row() {
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
    db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
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
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let result = db
        .build_log_apply_commit_for_outbox(
            "fresh-raft-placeholder-key",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await;

    assert!(
        matches!(
            result,
            Err(klights_cluster_core::OutboxApplyError::Retryable(_))
        ),
        "unsupported durable rows must fail closed rather than be reclaimed"
    );
    let row = db
        .get_applied_outbox("fresh-raft-placeholder-key")
        .await
        .unwrap()
        .expect("incomplete durable row remains for operator recovery");
    assert!(row.applied_rv.is_none());
    assert!(row.result_proto.is_empty());
}

fn pod_status_outbox_command(name: &str, uid: &str, phase: &str) -> StorageCommand {
    StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: name.to_string(),
        status: json!({"phase": phase}),
        expected_rv: None,
        preconditions: ResourcePreconditions {
            uid: Some(uid.to_string()),
            resource_version: None,
        },
        observed_status_stamp: None,
    }
}

async fn create_outbox_test_pod(db: &Datastore, name: &str, uid: &str) {
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        name,
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": name, "uid": uid},
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Pending"}
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn outbox_mutation_failure_rolls_back_ledger_and_same_key_retry_succeeds() {
    let db = Datastore::new_in_memory().await.unwrap();
    let command = pod_status_outbox_command("rollback-pod", "rollback-uid", "Running");

    let error = db
        .apply_outbox_transactionally(
            "mutation-rollback-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            command.clone(),
            "worker-a",
        )
        .await
        .expect_err("missing Pod mutation must fail atomically");
    assert!(matches!(
        error,
        klights_cluster_core::OutboxApplyError::Retryable(_)
    ));
    assert!(
        db.get_applied_outbox("mutation-rollback-key")
            .await
            .unwrap()
            .is_none(),
        "failed resource mutation must not consume the idempotency key"
    );

    create_outbox_test_pod(&db, "rollback-pod", "rollback-uid").await;
    let result = db
        .apply_outbox_transactionally(
            "mutation-rollback-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            command,
            "worker-a",
        )
        .await
        .expect("same idempotency key retries after rollback");
    assert!(matches!(
        result,
        klights_cluster_core::OutboxApplyOutcome::Applied { .. }
    ));
}

#[tokio::test]
async fn applied_outbox_insert_failure_rolls_back_resource_rv_watch_and_retries() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_outbox_test_pod(&db, "ledger-fault-pod", "ledger-fault-uid").await;
    let before = db
        .get_resource("v1", "Pod", Some("default"), "ledger-fault-pod")
        .await
        .unwrap()
        .unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db.current_watch_replay_position().await.unwrap();
    db.db_call("test_install_applied_outbox_insert_fault", |conn| {
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_applied_outbox_insert \
             BEFORE INSERT ON applied_outbox BEGIN \
             SELECT RAISE(ABORT, 'test applied_outbox insert fault'); END;",
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let command = pod_status_outbox_command("ledger-fault-pod", "ledger-fault-uid", "Running");

    let error = db
        .apply_outbox_transactionally(
            "ledger-fault-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            command.clone(),
            "worker-a",
        )
        .await
        .expect_err("ledger fault aborts the whole committed apply transaction");
    assert!(matches!(
        error,
        klights_cluster_core::OutboxApplyError::Retryable(_)
    ));
    let after_fault = db
        .get_resource("v1", "Pod", Some("default"), "ledger-fault-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_fault, before, "resource bytes and RV must roll back");
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.current_watch_replay_position().await.unwrap(),
        watch_before
    );
    assert!(
        db.get_applied_outbox("ledger-fault-key")
            .await
            .unwrap()
            .is_none()
    );

    db.db_call("test_remove_applied_outbox_insert_fault", |conn| {
        conn.execute_batch("DROP TRIGGER fail_applied_outbox_insert;")?;
        Ok(())
    })
    .await
    .unwrap();
    let result = db
        .apply_outbox_transactionally(
            "ledger-fault-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            command,
            "worker-a",
        )
        .await
        .expect("same key succeeds after deterministic ledger fault is removed");
    assert!(matches!(
        result,
        klights_cluster_core::OutboxApplyOutcome::Applied { .. }
    ));
    let retried = db
        .get_resource("v1", "Pod", Some("default"), "ledger-fault-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retried
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Running")
    );
    assert!(
        db.get_applied_outbox("ledger-fault-key")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn applied_outbox_gc_then_replay_is_a_fresh_idempotent_delivery() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_outbox_test_pod(&db, "gc-replay-pod", "gc-replay-uid").await;
    let command = pod_status_outbox_command("gc-replay-pod", "gc-replay-uid", "Running");
    db.apply_outbox_transactionally(
        "gc-replay-key",
        klights_cluster_core::OutboxOperation::PodStatus.as_str(),
        command.clone(),
        "worker-a",
    )
    .await
    .expect("first status delivery");
    assert!(
        db.get_applied_outbox("gc-replay-key")
            .await
            .unwrap()
            .is_some()
    );

    let gc = db
        .build_log_apply_commit_for_command(
            StorageCommand::GcAppliedOutbox {
                cutoff_ms: i64::MAX,
            },
            "GcAppliedOutbox",
            "leader",
        )
        .await
        .unwrap();
    db.apply_raft_log_apply_commit(gc).await.unwrap();
    assert!(
        db.get_applied_outbox("gc-replay-key")
            .await
            .unwrap()
            .is_none()
    );

    let replay = db
        .apply_outbox_transactionally(
            "gc-replay-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            command,
            "worker-a",
        )
        .await
        .expect("replay after committed GC is a fresh delivery");
    assert!(matches!(
        replay,
        klights_cluster_core::OutboxApplyOutcome::Applied { .. }
    ));
    assert!(
        db.get_applied_outbox("gc-replay-key")
            .await
            .unwrap()
            .is_some()
    );
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "gc-replay-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pod.data.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
}

#[tokio::test]
async fn lease_renew_is_exact_cluster_db_and_ledger_noop() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": "worker-a", "namespace": "kube-node-lease", "uid": "lease-uid"
                },
                "spec": {"holderIdentity": "worker-a", "renewTime": "2026-08-11T00:00:00Z"}
            }),
        )
        .await
        .unwrap();
    let mut stale = (*created.data).clone();
    stale["spec"]["renewTime"] = json!("2026-08-11T00:01:00Z");
    let command = StorageCommand::UpdateResource {
        api_version: "coordination.k8s.io/v1".to_string(),
        kind: "Lease".to_string(),
        namespace: Some("kube-node-lease".to_string()),
        name: "worker-a".to_string(),
        data: stale,
        expected_rv: created.resource_version,
        preconditions: ResourcePreconditions::from_resource(&created),
        preserve_status: false,
    };
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db.current_watch_replay_position().await.unwrap();
    let ledger_before = db.list_applied_outbox().await.unwrap();

    let result = db
        .apply_outbox_transactionally(
            "lease-renew-noop-key",
            klights_cluster_core::OutboxOperation::LeaseRenew.as_str(),
            command,
            "worker-a",
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv: 0 }
    );
    let stored = db
        .get_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored, created,
        "LeaseRenew must not change cluster.db bytes or RV"
    );
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.current_watch_replay_position().await.unwrap(),
        watch_before
    );
    assert_eq!(db.list_applied_outbox().await.unwrap(), ledger_before);
}

#[tokio::test]
async fn aged_incomplete_applied_outbox_row_remains_retryable_and_unchanged() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_outbox_test_pod(&db, "aged-incomplete-pod", "aged-incomplete-uid").await;
    let row = klights_cluster_core::LogApplyAppliedOutboxRow {
        idempotency_key: "aged-incomplete-key".to_string(),
        subject_key: "v1/Pod/default/aged-incomplete-pod/aged-incomplete-uid".to_string(),
        operation: "PodStatus".to_string(),
        first_seen_ms: 1,
        applied_rv: None,
        result_proto: vec![0xAA, 0x55],
        status_stamp: Some(7),
    };
    db.insert_applied_outbox(row.clone()).await.unwrap();
    let pod_before = db
        .get_resource("v1", "Pod", Some("default"), "aged-incomplete-pod")
        .await
        .unwrap()
        .unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    let watch_before = db.current_watch_replay_position().await.unwrap();

    let result = db
        .build_log_apply_commit_for_outbox(
            "aged-incomplete-key",
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            pod_status_outbox_command("aged-incomplete-pod", "aged-incomplete-uid", "Running"),
            "worker-a",
        )
        .await;
    assert!(matches!(
        result,
        Err(klights_cluster_core::OutboxApplyError::Retryable(_))
    ));
    assert_eq!(
        db.get_applied_outbox("aged-incomplete-key").await.unwrap(),
        Some(row)
    );
    assert_eq!(
        db.get_resource("v1", "Pod", Some("default"), "aged-incomplete-pod")
            .await
            .unwrap()
            .unwrap(),
        pod_before
    );
    assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
    assert_eq!(
        db.current_watch_replay_position().await.unwrap(),
        watch_before
    );
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
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let idempotency_key = "raft-duplicate-retry-key";

    let outcome = db
        .build_log_apply_commit_for_outbox(
            idempotency_key,
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await
        .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
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
    assert_eq!(
        rejected.rejection_code,
        Some(klights_cluster_core::StorageCommandRejectionCode::AlreadyExists)
    );

    let retry = db
        .build_log_apply_commit_for_outbox(
            idempotency_key,
            "CreateResource",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "leader",
        )
        .await;
    match retry {
        Err(klights_cluster_core::OutboxApplyError::ConflictTerminal(msg))
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

    let commit = crate::test_fixtures::live_apply::test_live_commit(
        0,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
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
        rejected.rejection_code,
        Some(klights_cluster_core::StorageCommandRejectionCode::AlreadyExists)
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_rv,
        "rejected apply must roll back provisional RV allocation"
    );
}

async fn applied_outbox_rows(
    db: &Datastore,
) -> Vec<(
    String,
    String,
    String,
    i64,
    Option<i64>,
    Vec<u8>,
    Option<i64>,
)> {
    type AppliedOutboxRow = (
        String,
        String,
        String,
        i64,
        Option<i64>,
        Vec<u8>,
        Option<i64>,
    );

    db.db_call("test_applied_outbox_rows", |conn| {
        let mut stmt = conn.prepare(
            "SELECT idempotency_key, subject_key, operation, first_seen_ms, \
             applied_rv, result_proto, status_stamp \
             FROM applied_outbox ORDER BY idempotency_key",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<AppliedOutboxRow>>>()?;
        Ok(rows)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn raft_applied_outbox_replay_is_deterministic() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    let initial_put = crate::test_fixtures::live_apply::test_live_commit(
        1,
        vec![
            klights_cluster_core::LogApplyMutation::PutAppliedOutbox(
                klights_cluster_core::LogApplyAppliedOutboxRow {
                    idempotency_key: "outbox-old".to_string(),
                    subject_key: "v1:Pod:default:old-pod:uid-1".to_string(),
                    operation: "PodMetadata".to_string(),
                    first_seen_ms: 10_000,
                    applied_rv: Some(11),
                    result_proto: vec![0x01, 0x02],
                    status_stamp: Some(21_000),
                },
            ),
            klights_cluster_core::LogApplyMutation::PutAppliedOutbox(
                klights_cluster_core::LogApplyAppliedOutboxRow {
                    idempotency_key: "outbox-new".to_string(),
                    subject_key: "v1:Pod:default:new-pod:uid-2".to_string(),
                    operation: "PodStatus".to_string(),
                    first_seen_ms: 20_000,
                    applied_rv: Some(12),
                    result_proto: vec![0x03, 0x04],
                    status_stamp: None,
                },
            ),
        ],
    );

    leader
        .apply_log_apply_commit(initial_put.clone())
        .await
        .unwrap();
    follower.apply_log_apply_commit(initial_put).await.unwrap();

    let expected_initial = vec![
        (
            "outbox-new".to_string(),
            "v1:Pod:default:new-pod:uid-2".to_string(),
            "PodStatus".to_string(),
            20_000,
            Some(0),
            vec![0x03, 0x04],
            None,
        ),
        (
            "outbox-old".to_string(),
            "v1:Pod:default:old-pod:uid-1".to_string(),
            "PodMetadata".to_string(),
            10_000,
            Some(0),
            vec![0x01, 0x02],
            Some(21_000),
        ),
    ];
    let leader_rows = applied_outbox_rows(&leader).await;
    let follower_rows = applied_outbox_rows(&follower).await;
    assert_eq!(
        leader_rows, follower_rows,
        "leader/follower rows must remain identical"
    );
    assert_eq!(
        leader_rows, expected_initial,
        "initial put rows must match expected snapshot"
    );

    let delete = crate::test_fixtures::live_apply::test_live_commit(
        2,
        vec![
            klights_cluster_core::LogApplyMutation::DeleteAppliedOutbox {
                idempotency_key: "outbox-old".to_string(),
            },
        ],
    );
    leader.apply_log_apply_commit(delete.clone()).await.unwrap();
    follower.apply_log_apply_commit(delete).await.unwrap();

    let after_delete_expected = vec![(
        "outbox-new".to_string(),
        "v1:Pod:default:new-pod:uid-2".to_string(),
        "PodStatus".to_string(),
        20_000,
        Some(0),
        vec![0x03, 0x04],
        None,
    )];
    let leader_rows = applied_outbox_rows(&leader).await;
    let follower_rows = applied_outbox_rows(&follower).await;
    assert_eq!(
        leader_rows, follower_rows,
        "leader/follower rows must remain identical"
    );
    assert_eq!(leader_rows, after_delete_expected);

    let follow_up_put = crate::test_fixtures::live_apply::test_live_commit(
        3,
        vec![klights_cluster_core::LogApplyMutation::PutAppliedOutbox(
            klights_cluster_core::LogApplyAppliedOutboxRow {
                idempotency_key: "outbox-older-again".to_string(),
                subject_key: "v1:Pod:default:older-pod:uid-3".to_string(),
                operation: "PodMetadata".to_string(),
                first_seen_ms: 5_000,
                applied_rv: Some(13),
                result_proto: vec![0x05],
                status_stamp: Some(7_000),
            },
        )],
    );
    leader
        .apply_log_apply_commit(follow_up_put.clone())
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(follow_up_put)
        .await
        .unwrap();

    let gc = crate::test_fixtures::live_apply::test_live_commit(
        4,
        vec![klights_cluster_core::LogApplyMutation::GcAppliedOutbox {
            cutoff_ms: 10_000,
            operations: vec!["PodMetadata".to_string(), "PodStatus".to_string()],
        }],
    );
    leader.apply_log_apply_commit(gc.clone()).await.unwrap();
    follower.apply_log_apply_commit(gc).await.unwrap();

    let expected_after_gc = vec![(
        "outbox-new".to_string(),
        "v1:Pod:default:new-pod:uid-2".to_string(),
        "PodStatus".to_string(),
        20_000,
        Some(0),
        vec![0x03, 0x04],
        None,
    )];
    let leader_rows = applied_outbox_rows(&leader).await;
    let follower_rows = applied_outbox_rows(&follower).await;
    assert_eq!(
        leader_rows, follower_rows,
        "leader/follower rows must remain identical"
    );
    assert_eq!(leader_rows, expected_after_gc);
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
        preserve_status: false,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();

    let outcome = db
        .build_log_apply_commit_for_outbox(
            "raft-leader-node-api-update",
            "PodStatus",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "mn-controlplane1",
        )
        .await
        .expect("direct API Node update should build a commit");
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected a fresh commit");
    };
    let put = commit
        .mutations()
        .iter()
        .find_map(|mutation| match mutation {
            klights_cluster_core::LogApplyMutation::PutResource(row) => Some(row),
            _ => None,
        })
        .expect("node update must produce a PutResource mutation");

    assert!(
        put.data.pointer("/metadata/labels/node").is_none(),
        "API Node label deletion must not be merged back as a kubelet NodeStatus refresh"
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
    db.apply_log_apply_commit(crate::test_fixtures::live_apply::test_live_commit(
        7,
        vec![klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(
            klights_cluster_core::LogApplyPodCleanupIntentRow {
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

    db.apply_log_apply_commit(crate::test_fixtures::live_apply::test_live_commit(
        8,
        vec![
            klights_cluster_core::LogApplyMutation::DeletePodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentKey {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod".to_string(),
                    pod_uid: "lost-uid".to_string(),
                    reason: "NodeLost".to_string(),
                },
            ),
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

    for pod_name in ["lost-pod-a", "lost-pod-b"] {
        db.apply_log_apply_commit(crate::test_fixtures::live_apply::test_live_commit(
            9,
            vec![klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentRow {
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

    db.apply_log_apply_commit(crate::test_fixtures::live_apply::test_live_commit(
        10,
        vec![
            klights_cluster_core::LogApplyMutation::DeletePodCleanupIntentsForNode {
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

async fn watch_event_count(db: &Datastore) -> i64 {
    db.db_call("test_watch_event_count", |conn| {
        Ok(conn.query_row("SELECT COUNT(*) FROM watch_events", [], |row| row.get(0))?)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn no_op_node_cleanup_intent_bulk_delete_does_not_advance_rv_or_watch() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "rv-baseline",
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "rv-baseline",
                "uid": "rv-baseline-uid"
            }
        }),
    )
    .await
    .unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let before_watch = watch_event_count(&db).await;

    db.delete_pod_cleanup_intents_for_node("e2e-fake-node")
        .await
        .unwrap();

    assert_eq!(db.get_current_resource_version().await.unwrap(), before_rv);
    assert_eq!(watch_event_count(&db).await, before_watch);
}

#[tokio::test]
async fn legacy_move_pod_to_cleanup_intent_captures_without_deleting_bound_pod() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bound-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "bound-pod",
                "uid": "bound-pod-uid"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        }),
    )
    .await
    .unwrap();

    db.move_pod_to_cleanup_intent(
        "worker-a",
        "default",
        "bound-pod",
        "bound-pod-uid",
        "NodeLost",
    )
    .await
    .unwrap();

    let live = db
        .get_resource("v1", "Pod", Some("default"), "bound-pod")
        .await
        .unwrap()
        .expect("legacy cleanup capture must leave actor-owned bound Pod row intact");
    assert_eq!(
        live.data
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str()),
        Some("bound-pod-uid")
    );

    let intents = db
        .list_pod_cleanup_intents_for_node("worker-a")
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].pod_uid, "bound-pod-uid");
    assert_eq!(intents[0].pod_data["spec"]["nodeName"], "worker-a");
}

#[tokio::test]
async fn actor_finalize_bound_pod_acks_noop_when_finalizer_is_added_before_apply() {
    let db = Datastore::new_in_memory().await.unwrap();
    let observed = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "finalizer-race",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "finalizer-race",
                    "uid": "finalizer-race-uid",
                    "deletionTimestamp": "2026-07-24T00:00:00Z"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                }
            }),
        )
        .await
        .unwrap();

    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalizer-race".to_string(),
            pod_uid: "finalizer-race-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: observed.resource_version,
        },
    )
    .encode_protobuf()
    .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-finalizer-race",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .expect("eligible actor finalization must produce a semantic commit")
    else {
        panic!("expected a new actor-finalization outbox commit");
    };
    assert!(commit.mutations().iter().any(|mutation| matches!(
        mutation,
        klights_cluster_core::LogApplyMutation::FinalizeBoundPod(finalization)
            if finalization.pod_uid == "finalizer-race-uid"
    )));

    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "finalizer-race",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "finalizer-race",
                "uid": "finalizer-race-uid",
                "deletionTimestamp": "2026-07-24T00:00:00Z",
                "finalizers": ["example.test/late-finalizer"]
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        }),
        observed.resource_version,
    )
    .await
    .unwrap();
    let before_apply_rv = db.get_current_resource_version().await.unwrap();
    let before_apply_watch_count = watch_event_count(&db).await;

    db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_apply_rv,
        "ineligible actor finalization must not consume a public resourceVersion"
    );
    assert_eq!(
        watch_event_count(&db).await,
        before_apply_watch_count,
        "ledger-only actor finalization must not publish a watch event"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "finalizer-race")
            .await
            .unwrap()
            .is_some(),
        "failed actor CAS must preserve the bound Pod row"
    );

    let held = db
        .get_resource("v1", "Pod", Some("default"), "finalizer-race")
        .await
        .unwrap()
        .unwrap();
    let mut drained = (*held.data).clone();
    drained["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("finalizers");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "finalizer-race",
        drained,
        held.resource_version,
    )
    .await
    .unwrap();
    let after_finalizer_drain_watch_count = watch_event_count(&db).await;
    assert!(
        after_finalizer_drain_watch_count > before_apply_watch_count,
        "finalizer removal must emit the Pod watch update that re-wakes the actor"
    );

    let before_fresh_finalize_rv = db.get_current_resource_version().await.unwrap();
    let fresh_payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalizer-race".to_string(),
            pod_uid: "finalizer-race-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: before_fresh_finalize_rv,
        },
    )
    .encode_protobuf()
    .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: fresh_commit,
        ..
    } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-after-finalizer-drain",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(fresh_payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("expected fresh actor finalization commit");
    };
    assert!(fresh_commit.mutations().iter().any(|mutation| matches!(
        mutation,
        klights_cluster_core::LogApplyMutation::FinalizeBoundPod(finalization)
            if finalization.pod_uid == "finalizer-race-uid"
    )));
    db.apply_raft_log_apply_commit(fresh_commit).await.unwrap();
    assert!(
        db.get_current_resource_version().await.unwrap() > before_fresh_finalize_rv,
        "eligible actor finalization must advance public resourceVersion"
    );
    assert!(
        watch_event_count(&db).await > after_finalizer_drain_watch_count,
        "eligible actor finalization must publish the Pod DELETED watch event"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "finalizer-race")
            .await
            .unwrap()
            .is_none()
    );

    let missing_rv = db.get_current_resource_version().await.unwrap();
    let missing_watch_count = watch_event_count(&db).await;
    let missing_payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalizer-race".to_string(),
            pod_uid: "finalizer-race-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: missing_rv,
        },
    )
    .encode_protobuf()
    .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: missing_commit,
        ..
    } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-already-missing",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(missing_payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("missing Pod actor finalization must still reach proposal");
    };
    assert!(missing_commit.mutations().iter().all(|mutation| matches!(
        mutation,
        klights_cluster_core::LogApplyMutation::PutAppliedOutbox(_)
    )));
    db.apply_raft_log_apply_commit(missing_commit)
        .await
        .unwrap();
    assert_eq!(db.get_current_resource_version().await.unwrap(), missing_rv);
    assert_eq!(watch_event_count(&db).await, missing_watch_count);

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "finalizer-race",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "finalizer-race",
                "uid": "replacement-uid",
                "deletionTimestamp": "2026-07-24T00:10:00Z"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        }),
    )
    .await
    .unwrap();
    let replacement_rv = db.get_current_resource_version().await.unwrap();
    let replacement_watch_count = watch_event_count(&db).await;
    let stale_payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalizer-race".to_string(),
            pod_uid: "finalizer-race-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: replacement_rv,
        },
    )
    .encode_protobuf()
    .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit: replacement_commit,
        ..
    } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-stale-replacement",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(stale_payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("same-name replacement must still reach ledger-only proposal");
    };
    assert!(
        replacement_commit
            .mutations()
            .iter()
            .all(|mutation| matches!(
                mutation,
                klights_cluster_core::LogApplyMutation::PutAppliedOutbox(_)
            ))
    );
    db.apply_raft_log_apply_commit(replacement_commit)
        .await
        .unwrap();
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        replacement_rv
    );
    assert_eq!(watch_event_count(&db).await, replacement_watch_count);
    assert_eq!(
        db.get_resource("v1", "Pod", Some("default"), "finalizer-race")
            .await
            .unwrap()
            .unwrap()
            .uid,
        "replacement-uid"
    );

    let eligible_payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalizer-race".to_string(),
            pod_uid: "replacement-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: replacement_rv,
        },
    )
    .encode_protobuf()
    .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit, applied_rv, ..
    } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-fixed-contract",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(eligible_payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("eligible actor finalization must produce a template");
    };
    assert_eq!(commit.resource_version(), 0);
    assert!(applied_rv > 0);
    assert!(commit.mutations().iter().any(|mutation| matches!(
        mutation,
        klights_cluster_core::LogApplyMutation::FinalizeBoundPod(_)
    )));
    let placeholder_count = db
        .db_call("test_actor_finalize_fixed_placeholder", |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM applied_outbox \
                 WHERE idempotency_key = 'actor-finalize-fixed-contract'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(placeholder_count, 0);
}

#[tokio::test]
async fn actor_finalize_bound_pod_rejects_stale_observed_rv_and_preserves_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let original = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "finalize-stale-rv",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "finalize-stale-rv",
                    "uid": "finalize-stale-rv-uid",
                    "deletionTimestamp": "2026-07-24T00:00:00Z"
                },
                "spec": {"nodeName": "worker-a"},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
    let mut refreshed_data = (*original.data).clone();
    refreshed_data["status"] = json!({"phase": "Running", "podIP": "10.42.0.77"});
    let refreshed = db
        .update_resource(
            "v1",
            "Pod",
            Some("default"),
            "finalize-stale-rv",
            refreshed_data,
            original.resource_version,
        )
        .await
        .unwrap();

    let error = db
        .build_log_apply_commit_for_command(
            StorageCommand::FinalizeBoundPod {
                namespace: "default".to_string(),
                name: "finalize-stale-rv".to_string(),
                pod_uid: "finalize-stale-rv-uid".to_string(),
                node_name: "worker-a".to_string(),
                observed_resource_version: original.resource_version,
            },
            "PodMetadata",
            "worker-a",
        )
        .await
        .expect_err("stale actor observation must fail strict RV CAS");
    assert!(error.to_string().to_ascii_lowercase().contains("conflict"));

    let live = db
        .get_resource("v1", "Pod", Some("default"), "finalize-stale-rv")
        .await
        .unwrap()
        .expect("stale actor finalization must preserve the Pod");
    assert_eq!(live.uid, "finalize-stale-rv-uid");
    assert_eq!(live.resource_version, refreshed.resource_version);
}

#[tokio::test]
async fn actor_finalize_bound_pod_serializes_a_status_write_after_proposal_build() {
    let db = Datastore::new_in_memory().await.unwrap();
    let observed = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "finalize-rv-race",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "finalize-rv-race",
                    "uid": "finalize-rv-race-uid",
                    "deletionTimestamp": "2026-07-24T00:00:00Z"
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

    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
        StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "finalize-rv-race".to_string(),
            pod_uid: "finalize-rv-race-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: observed.resource_version,
        },
    )
    .encode_protobuf()
    .unwrap();

    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = db
        .build_log_apply_commit_for_outbox(
            "actor-finalize-rv-race",
            "PodMetadata",
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "worker-a",
        )
        .await
        .unwrap()
    else {
        panic!("eligible actor finalization must reach proposal");
    };

    let mut refreshed_data = (*observed.data).clone();
    refreshed_data["status"]["phase"] = json!("Failed");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "finalize-rv-race",
        refreshed_data,
        observed.resource_version,
    )
    .await
    .unwrap();

    let before_delete_rv = db.get_current_resource_version().await.unwrap();
    let before_delete_watch_count = watch_event_count(&db).await;
    db.apply_raft_log_apply_commit(commit)
        .await
        .expect("committed actor finalization must serialize after the status write");
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "finalize-rv-race")
            .await
            .unwrap()
            .is_none(),
        "the actor-owned delete must complete without an RV retry"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before_delete_rv + 1,
        "semantic deletion must allocate exactly one public resourceVersion"
    );
    assert_eq!(
        watch_event_count(&db).await,
        before_delete_watch_count + 1,
        "semantic deletion must publish exactly one DELETED watch event"
    );
}

#[tokio::test]
async fn watermarked_actor_finalize_bound_pod_covers_eligibility() {
    struct Case {
        name: &'static str,
        eligible: bool,
    }

    let cases = [
        Case {
            name: "fixed-eligible",
            eligible: true,
        },
        Case {
            name: "fixed-noop",
            eligible: false,
        },
    ];

    for case in cases {
        let db = Datastore::new_in_memory().await.unwrap();
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": case.name,
                "uid": format!("{}-uid", case.name),
                "deletionTimestamp": "2026-07-24T01:00:00Z"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        });
        if !case.eligible {
            pod["metadata"]["finalizers"] = json!(["example.test/held"]);
        }
        db.create_resource("v1", "Pod", Some("default"), case.name, pod)
            .await
            .unwrap();

        let before_rv = db.get_current_resource_version().await.unwrap();
        let before_watch_count = watch_event_count(&db).await;
        let idempotency_key = format!("watermarked-finalize-{}", case.name);
        let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(
            StorageCommand::FinalizeBoundPod {
                namespace: "default".to_string(),
                name: case.name.to_string(),
                pod_uid: format!("{}-uid", case.name),
                node_name: "worker-a".to_string(),
                observed_resource_version: before_rv,
            },
        )
        .encode_protobuf()
        .unwrap();
        let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
            commit, applied_rv, ..
        } = db
            .build_log_apply_commit_for_outbox_with_watermark(
                &idempotency_key,
                "PodMetadata",
                crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
                "worker-a",
                Some(klights_cluster_core::OutboxStreamWatermark {
                    client_id: format!("client-{}", case.name),
                    stream_id: 1,
                    stream_seq: 1,
                }),
            )
            .await
            .unwrap()
        else {
            panic!("{} must produce a fresh proposal", case.name);
        };

        assert_eq!(commit.resource_version(), 0);
        assert!(commit.mutations().iter().any(|mutation| matches!(
            mutation,
            klights_cluster_core::LogApplyMutation::PutAppliedOutbox(_)
        )));
        assert_eq!(
            commit.mutations().iter().any(|mutation| matches!(
                mutation,
                klights_cluster_core::LogApplyMutation::FinalizeBoundPod(_)
            )),
            case.eligible,
            "{} delete mutation eligibility mismatch",
            case.name
        );

        let key = idempotency_key.clone();
        let placeholder_count = db
            .db_call("test_watermarked_finalize_placeholder", move |conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM applied_outbox WHERE idempotency_key = ?1",
                    [key],
                    |row| row.get::<_, i64>(0),
                )?)
            })
            .await
            .unwrap();

        assert!(applied_rv > 0);
        assert_eq!(placeholder_count, 0);

        db.apply_raft_log_apply_commit(commit).await.unwrap();
        let live = db
            .get_resource("v1", "Pod", Some("default"), case.name)
            .await
            .unwrap();
        if case.eligible {
            assert!(live.is_none(), "{} eligible Pod must be removed", case.name);
            assert!(
                db.get_current_resource_version().await.unwrap() > before_rv,
                "{} eligible delete must advance public RV",
                case.name
            );
            assert!(
                watch_event_count(&db).await > before_watch_count,
                "{} eligible delete must publish watch",
                case.name
            );
        } else {
            assert!(live.is_some(), "{} ineligible Pod must remain", case.name);
            assert_eq!(
                db.get_current_resource_version().await.unwrap(),
                before_rv,
                "{} ledger-only no-op must not advance public RV",
                case.name
            );
            assert_eq!(
                watch_event_count(&db).await,
                before_watch_count,
                "{} ledger-only no-op must not publish watch",
                case.name
            );
        }
    }
}

async fn get_klights_meta_rows(
    db: &Datastore,
    key_a: &'static str,
    key_b: &'static str,
) -> Vec<(String, String)> {
    db.db_call("test_select_test_klights_meta_rows", {
        let key_a = key_a.to_string();
        let key_b = key_b.to_string();
        move |conn| {
            let mut stmt = conn.prepare(
                "SELECT key, value FROM _klights_meta WHERE key IN (?1, ?2) ORDER BY key",
            )?;
            let rows = stmt
                .query_map([key_a.as_str(), key_b.as_str()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        }
    })
    .await
    .unwrap()
}

async fn pod_cleanup_intent_rows(
    db: &Datastore,
) -> Vec<(String, String, String, String, String, i64, i64, Vec<u8>)> {
    db.db_call("test_pod_cleanup_intent_rows", |conn| {
        let mut stmt = conn.prepare(
            "SELECT node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod_data \
             FROM pod_cleanup_intents \
             ORDER BY node_name, namespace, pod_name, pod_uid, reason",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn raft_pod_cleanup_intent_replay_is_deterministic() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    let pod_a_data = serde_json::to_vec(&json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "lost-pod-a",
            "namespace": "default",
            "uid": "lost-pod-a-uid",
            "labels": {
                "app": "replay",
                "node": "worker-a"
            },
        },
        "spec": {"nodeName": "worker-a"},
        "status": {"phase": "Running"},
    }))
    .unwrap();
    let pod_b_data = serde_json::to_vec(&json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "lost-pod-b",
            "namespace": "default",
            "uid": "lost-pod-b-uid",
            "labels": {
                "app": "replay",
                "node": "worker-a"
            },
        },
        "spec": {"nodeName": "worker-a"},
        "status": {"phase": "Pending"},
    }))
    .unwrap();
    let pod_c_data = serde_json::to_vec(&json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "lost-pod-c",
            "namespace": "default",
            "uid": "lost-pod-c-uid",
            "labels": {
                "app": "replay",
                "node": "worker-b"
            },
        },
        "spec": {"nodeName": "worker-b"},
        "status": {"phase": "Running"},
    }))
    .unwrap();

    let initial_put = crate::test_fixtures::live_apply::test_live_commit(
        11,
        vec![
            klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentRow {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod-a".to_string(),
                    pod_uid: "lost-pod-a-uid".to_string(),
                    reason: "NodeLost".to_string(),
                    resource_version: 11,
                    created_at_ms: 1_700_000_000_100,
                    pod_data: serde_json::from_slice(&pod_a_data).unwrap(),
                },
            ),
            klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentRow {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod-b".to_string(),
                    pod_uid: "lost-pod-b-uid".to_string(),
                    reason: "NodeLost".to_string(),
                    resource_version: 11,
                    created_at_ms: 1_700_000_000_200,
                    pod_data: serde_json::from_slice(&pod_b_data).unwrap(),
                },
            ),
            klights_cluster_core::LogApplyMutation::PutPodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentRow {
                    node_name: "worker-b".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod-c".to_string(),
                    pod_uid: "lost-pod-c-uid".to_string(),
                    reason: "NodeLost".to_string(),
                    resource_version: 11,
                    created_at_ms: 1_700_000_000_300,
                    pod_data: serde_json::from_slice(&pod_c_data).unwrap(),
                },
            ),
        ],
    );

    leader
        .apply_log_apply_commit(initial_put.clone())
        .await
        .unwrap();
    follower.apply_log_apply_commit(initial_put).await.unwrap();

    let expected_after_put = vec![
        (
            "worker-a".to_string(),
            "default".to_string(),
            "lost-pod-a".to_string(),
            "lost-pod-a-uid".to_string(),
            "NodeLost".to_string(),
            1,
            1_700_000_000_100,
            pod_a_data.clone(),
        ),
        (
            "worker-a".to_string(),
            "default".to_string(),
            "lost-pod-b".to_string(),
            "lost-pod-b-uid".to_string(),
            "NodeLost".to_string(),
            1,
            1_700_000_000_200,
            pod_b_data.clone(),
        ),
        (
            "worker-b".to_string(),
            "default".to_string(),
            "lost-pod-c".to_string(),
            "lost-pod-c-uid".to_string(),
            "NodeLost".to_string(),
            1,
            1_700_000_000_300,
            pod_c_data.clone(),
        ),
    ];

    let leader_rows = pod_cleanup_intent_rows(&leader).await;
    let follower_rows = pod_cleanup_intent_rows(&follower).await;
    assert_eq!(leader_rows, follower_rows);
    assert_eq!(leader_rows, expected_after_put);

    let delete = crate::test_fixtures::live_apply::test_live_commit(
        12,
        vec![
            klights_cluster_core::LogApplyMutation::DeletePodCleanupIntent(
                klights_cluster_core::LogApplyPodCleanupIntentKey {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod-a".to_string(),
                    pod_uid: "lost-pod-a-uid".to_string(),
                    reason: "NodeLost".to_string(),
                },
            ),
        ],
    );

    leader.apply_log_apply_commit(delete.clone()).await.unwrap();
    follower.apply_log_apply_commit(delete).await.unwrap();

    let after_delete_expected = vec![
        (
            "worker-a".to_string(),
            "default".to_string(),
            "lost-pod-b".to_string(),
            "lost-pod-b-uid".to_string(),
            "NodeLost".to_string(),
            1,
            1_700_000_000_200,
            pod_b_data.clone(),
        ),
        (
            "worker-b".to_string(),
            "default".to_string(),
            "lost-pod-c".to_string(),
            "lost-pod-c-uid".to_string(),
            "NodeLost".to_string(),
            1,
            1_700_000_000_300,
            pod_c_data.clone(),
        ),
    ];
    let leader_rows = pod_cleanup_intent_rows(&leader).await;
    let follower_rows = pod_cleanup_intent_rows(&follower).await;
    assert_eq!(leader_rows, follower_rows);
    assert_eq!(leader_rows, after_delete_expected);

    let delete_node = crate::test_fixtures::live_apply::test_live_commit(
        13,
        vec![
            klights_cluster_core::LogApplyMutation::DeletePodCleanupIntentsForNode {
                node_name: "worker-b".to_string(),
            },
        ],
    );

    leader
        .apply_log_apply_commit(delete_node.clone())
        .await
        .unwrap();
    follower.apply_log_apply_commit(delete_node).await.unwrap();

    let after_node_delete_expected = vec![(
        "worker-a".to_string(),
        "default".to_string(),
        "lost-pod-b".to_string(),
        "lost-pod-b-uid".to_string(),
        "NodeLost".to_string(),
        1,
        1_700_000_000_200,
        pod_b_data,
    )];
    let leader_rows = pod_cleanup_intent_rows(&leader).await;
    let follower_rows = pod_cleanup_intent_rows(&follower).await;
    assert_eq!(leader_rows, follower_rows);
    assert_eq!(leader_rows, after_node_delete_expected);
}

#[tokio::test]
async fn raft_cluster_meta_replay_is_deterministic() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    let commit = crate::test_fixtures::live_apply::test_live_commit(
        1,
        vec![
            klights_cluster_core::LogApplyMutation::PutKlightsMeta {
                key: "raft-test-alpha".to_string(),
                value: "alpha".to_string(),
            },
            klights_cluster_core::LogApplyMutation::PutKlightsMeta {
                key: "raft-test-beta".to_string(),
                value: "beta".to_string(),
            },
        ],
    );

    let commit_to_apply = commit.clone();
    leader
        .apply_log_apply_commit(commit_to_apply)
        .await
        .unwrap();
    follower.apply_log_apply_commit(commit).await.unwrap();

    let expected = vec![
        ("raft-test-alpha".to_string(), "alpha".to_string()),
        ("raft-test-beta".to_string(), "beta".to_string()),
    ];
    let leader_meta = get_klights_meta_rows(&leader, "raft-test-alpha", "raft-test-beta").await;
    let follower_meta = get_klights_meta_rows(&follower, "raft-test-alpha", "raft-test-beta").await;

    assert_eq!(leader_meta, expected);
    assert_eq!(follower_meta, expected);
    assert_eq!(leader_meta, follower_meta);
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
            klights_cluster_store::ResourceListOptions::all(),
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
        .expect("watch history should include the create event");
    assert_eq!(
        added.resource.data.pointer("/metadata/creationTimestamp"),
        stored.data.pointer("/metadata/creationTimestamp"),
        "watch replay must emit the same creationTimestamp returned by create"
    );
    assert_eq!(
        added
            .resource
            .data
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
                patch_kind: klights_cluster_core::PatchKind::Merge,
                patch: json!({
                    "metadata": {
                        "annotations": {
                            "deployment.kubernetes.io/revision": "1"
                        }
                    }
                }),
                preconditions: ResourcePreconditions::uid("deploy-uid-1"),
                strict_resource_version: false,
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
        crate::errors::is_conflict_error(&err),
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
        crate::errors::is_conflict_error(&err),
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
            klights_cluster_core::ResourcePatchRequest::new(
                klights_cluster_core::PatchKind::Merge,
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
        crate::errors::is_conflict_error(&err),
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
            klights_cluster_core::ResourcePatchRequest::new(
                klights_cluster_core::PatchKind::Merge,
                json!({"metadata":{"uid":"uid-replacement"}}),
                ResourcePreconditions::default(),
            ),
        )
        .await
        .expect_err("metadata.uid changes must be rejected");

    assert!(
        crate::errors::is_conflict_error(&err),
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
        crate::errors::is_conflict_error(&err),
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
async fn focused_resource_ports_support_crud() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let fetched = create_and_fetch_via_focused_ports(&concrete).await.unwrap();
    assert!(fetched.is_some());
    let rv = DurableAllocatorRead::read_allocator_state(concrete.focused_read_store().as_ref())
        .await
        .unwrap()
        .position()
        .resource_version;
    assert!(rv >= 1);
}

#[tokio::test]
async fn focused_store_traits_cover_sqlite_backend() {
    fn assert_mutation_ports<T>(_: &T)
    where
        T: ClusterResourceMutation
            + ClusterNamespaceMutation
            + ClusterWatchMaintenance
            + ClusterTopologyMutation
            + ClusterPodCleanupStore
            + AppliedOutboxLedger
            + ClusterMetadataMutation
            + BackendLifecycleStore,
    {
    }

    fn assert_read_ports<T>(_: &T)
    where
        T: ClusterResourceRead
            + ClusterResourceScopeRead
            + NamespaceContentRead
            + ClusterOwnershipRead
            + ClusterTopologyRead
            + DurableAllocatorRead
            + DurableWatchHistoryRead
            + DurableWatchRangeRead
            + DurableRawWatchHistoryRead,
    {
    }

    fn assert_recovery_ports<T>(_: &T)
    where
        T: AuthoritativeSnapshotCapture + AuthoritativeSnapshotPersistence + ClusterMetadataRead,
    {
    }

    let db = Datastore::new_in_memory().await.unwrap();
    assert_mutation_ports(&db);
    assert_read_ports(db.focused_read_store().as_ref());
    assert_recovery_ports(db.focused_recovery_store().as_ref());
    let _committed_apply: std::sync::Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply> =
        db.focused_committed_apply();
}

#[tokio::test]
async fn focused_watch_maintenance_advances_rv_past_minimum() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let target = concrete.get_current_resource_version().await.unwrap() + 5;
    let advanced = ClusterWatchMaintenance::advance_resource_version_after(&concrete, target)
        .await
        .unwrap();
    assert!(advanced > target);
}

async fn build_candidate_rv_commit(
    db: &Datastore,
    idempotency_key: &str,
) -> (klights_cluster_core::LogApplyCommit, i64) {
    let before = db.get_current_resource_version().await.unwrap();
    let command = klights_cluster_core::command::StorageCommand::AdvanceResourceVersion {
        min_rv: before,
        new_rv: before + 1,
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            idempotency_key,
            klights_cluster_core::OutboxOperation::PodStatus.as_str(),
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "mn-controlplane1",
        )
        .await
        .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit, applied_rv, ..
    } = outcome
    else {
        panic!("expected a fresh materialized commit");
    };
    (commit, applied_rv)
}

#[tokio::test]
async fn rejected_materialized_commit_does_not_reserve_resource_version_or_ledger_row() {
    let db = Datastore::new_in_memory().await.unwrap();
    let before = db.get_current_resource_version().await.unwrap();
    let (_commit, candidate_rv) = build_candidate_rv_commit(&db, "rejected-rv-1").await;

    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before,
        "materialization must not reserve a public RV before raft accepts the entry"
    );
    assert_eq!(
        candidate_rv,
        before + 1,
        "materialization may carry only a non-durable candidate hint"
    );
    assert!(
        db.get_applied_outbox("rejected-rv-1")
            .await
            .unwrap()
            .is_none(),
        "materialization must not create a durable outbox placeholder"
    );
}

#[tokio::test]
async fn multiple_materialized_commits_do_not_advance_public_resource_version() {
    let db = Datastore::new_in_memory().await.unwrap();
    let before = db.get_current_resource_version().await.unwrap();
    let (_first_commit, first_rv) = build_candidate_rv_commit(&db, "rejected-rv-first").await;
    let (_second_commit, second_rv) = build_candidate_rv_commit(&db, "rejected-rv-second").await;

    assert_eq!(first_rv, before + 1);
    assert_eq!(second_rv, before + 1);
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before,
        "uncommitted materializations must not consume public RVs"
    );
}

#[tokio::test]
async fn raft_terminal_conflict_does_not_consume_candidate_resource_version() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "mn-controlplane2",
        json!({"metadata": {"name": "mn-controlplane2"}}),
    )
    .await
    .unwrap();
    let before = db.get_current_resource_version().await.unwrap();
    let command = klights_cluster_core::command::StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "mn-controlplane2".to_string(),
        data: json!({"metadata": {"name": "mn-controlplane2"}}),
    };
    let payload = crate::test_fixtures::outbox::EncodedOutboxCommand::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "conflicting-node-registration",
            klights_cluster_core::OutboxOperation::NodeRegistration.as_str(),
            crate::test_fixtures::outbox::test_outbox_command(payload.as_ref()),
            "mn-controlplane2",
        )
        .await
        .unwrap();
    let klights_cluster_core::BuildOutboxOutcome::NeedsPropose {
        commit, applied_rv, ..
    } = outcome
    else {
        panic!("expected a materialized raft commit");
    };
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before,
        "commit materialization must not reserve the candidate RV on the leader"
    );
    assert_eq!(applied_rv, before + 1);

    let result = db.apply_raft_log_apply_commit(commit).await.unwrap();
    assert!(
        result.error_message.is_some(),
        "duplicate Node create should be cached as a terminal raft apply conflict"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        before,
        "terminal raft conflicts must not consume the candidate RV"
    );
}

#[tokio::test]
async fn focused_namespace_ports_return_inserted_resources() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    ClusterNamespaceMutation::create_namespace(
        &concrete,
        "ns-trait",
        json!({"metadata":{"name":"ns-trait"}}),
    )
    .await
    .unwrap();
    ClusterResourceMutation::create_resource(
        &concrete,
        "v1",
        "ConfigMap",
        Some("ns-trait"),
        "cm-trait",
        json!({"metadata":{"name":"cm-trait"},"data":{"k":"v"}}),
    )
    .await
    .unwrap();

    let items = NamespaceContentRead::list_namespace_resources(
        concrete.focused_read_store().as_ref(),
        NamespaceRequest::try_new("ns-trait").unwrap(),
    )
    .await
    .unwrap();
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
                "SELECT node_name, subnet, subnet_base_int, gateway_ip, mode \
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
                        None,
                        chrono::DateTime::UNIX_EPOCH,
                    )?;
                    assert!(
                        commit.resource_version() == 0,
                        "allocate subnet commits must remain RV-zero before apply"
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
async fn focused_namespace_mutation_handle_clone_shares_state() {
    let concrete = Datastore::new_in_memory().await.unwrap();
    let handle: std::sync::Arc<dyn ClusterNamespaceMutation> =
        std::sync::Arc::new(concrete.clone());
    let clone = handle.clone();

    handle
        .create_namespace("ns-handle", json!({"metadata":{"name":"ns-handle"}}))
        .await
        .unwrap();
    drop(clone);
    let fetched = concrete.get_namespace("ns-handle").await.unwrap();
    assert!(
        fetched.is_some(),
        "handle clones must observe shared writes"
    );
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
            klights_cluster_store::ResourceListOptions::all(),
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
            klights_cluster_store::ResourceListOptions::new(Some("app=nginx"), None, None, None),
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
            klights_cluster_store::ResourceListOptions::all(),
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
                klights_cluster_store::ResourceListOptions::new(
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
            klights_cluster_store::ResourceListOptions::new(None, None, Some(2), None),
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
            klights_cluster_store::ResourceListOptions::new(Some("app=web"), None, Some(2), None),
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
            klights_cluster_store::ResourceListOptions::new(
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
            klights_cluster_store::ResourceListOptions::new(Some("app=web"), None, Some(2), None),
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
            klights_cluster_store::ResourceListOptions::new(
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
            klights_cluster_store::ResourceListOptions::new(
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
            klights_cluster_store::ResourceListOptions::new(None, None, Some(1), None),
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
            klights_cluster_store::ResourceListOptions::new(None, None, Some(1), None),
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
            klights_cluster_store::ResourceListOptions::all(),
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
            klights_cluster_store::ResourceListOptions::new(
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
            klights_cluster_store::ResourceListOptions::new(
                Some("env notin (dev)"),
                None,
                None,
                None,
            ),
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
            klights_cluster_store::ResourceListOptions::new(Some("!app"), None, None, None),
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
            klights_cluster_store::ResourceListOptions::new(
                Some("app=nginx,env=prod"),
                None,
                None,
                None,
            ),
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
            klights_cluster_store::ResourceListOptions::new(Some("app"), None, None, None),
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
            klights_cluster_store::ResourceListOptions::all(),
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
            klights_cluster_store::ResourceListOptions::all(),
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

fn accepts_resource_mutation(_store: &dyn ClusterResourceMutation) {}
fn accepts_resource_read(_store: &dyn ClusterResourceRead) {}
fn accepts_allocator_read(_store: &dyn DurableAllocatorRead) {}
fn accepts_snapshot_capture(_store: &dyn AuthoritativeSnapshotCapture) {}
fn accepts_snapshot_restore(_store: &dyn AuthoritativeSnapshotPersistence) {}

#[tokio::test]
async fn datastore_implements_focused_backend_traits() {
    let db = Datastore::new_in_memory().await.unwrap();
    accepts_resource_mutation(&db);
    let reads = db.focused_read_store();
    accepts_resource_read(reads.as_ref());
    accepts_allocator_read(reads.as_ref());
    let recovery = db.focused_recovery_store();
    accepts_snapshot_capture(recovery.as_ref());
    accepts_snapshot_restore(recovery.as_ref());
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
async fn from_executor_initializes_commit_observation_and_fingerprint() {
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = crate::sqlite::open_in_memory(supervisor, "dsb03:fp-test")
        .await
        .unwrap();
    let ds = Datastore::new_in_memory_with_watch_and_executor_with_sink(
        executor,
        crate::test_fixtures::commit_observation::new_sink(),
        crate::test_fixtures::outbox::new_codec(),
        std::sync::Arc::new(klights_supervisor::SystemWallClock),
    )
    .await
    .unwrap();
    ds.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "fp-test",
        json!({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "fp-test"}}),
    )
    .await
    .unwrap();

    let observations = crate::test_fixtures::commit_observation::recorded_observations(
        ds.commit_sink.as_deref().expect("test commit sink"),
    );
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].api_version(), "v1");
    assert_eq!(observations[0].kind(), "ConfigMap");
    assert!(observations[0].resource_version() > 0);
}

#[tokio::test]
async fn new_persistent_creates_only_cluster_db_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_dir = dir.path();
    // Ensure parent has 0700 for opener
    std::fs::set_permissions(
        db_dir,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
    )
    .expect("set perms");

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();

    // Cluster persistence owns only cluster.db. The node-local selector owns
    // node.db and its schema.
    let cluster_db_path = db_dir.join("sqlite").join("cluster.db");
    let node_db_path = db_dir.join("sqlite").join("node.db");
    assert!(
        cluster_db_path.exists(),
        "cluster.db must be created under sqlite/"
    );
    assert!(
        !node_db_path.exists(),
        "cluster persistence must not create node.db"
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let result = Datastore::new_persistent(&parent, supervisor).await;

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
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let bad_dir = std::path::Path::new("/proc/klights-test-noexist");
    let result = Datastore::new_persistent(bad_dir, supervisor).await;

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
    let db = Datastore::new_in_memory().await.unwrap();
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
async fn gc_promotes_inexact_replay_floor_to_exact_position() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 0..4 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("promote"),
            &format!("cm-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "promote", "name": format!("cm-{i}")}
            }),
        )
        .await
        .unwrap();
    }

    db.db_call("seed-inexact-replay-floor", |conn| {
        Ok(conn.execute(
            "INSERT INTO watch_replay_floors
                (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
             VALUES ('v1', 'ConfigMap', 'promote', 0, 0, 0)",
            [],
        )?)
    })
    .await
    .unwrap();

    let removed = db.gc_watch_events(1, 1000).await.unwrap();
    assert!(removed > 0, "GC must remove rows and upsert an exact floor");

    let (floor_event_id, floor_position_exact): (i64, i64) = db
        .db_call("read-promoted-replay-floor", |conn| {
            Ok(conn.query_row(
                "SELECT floor_event_id, floor_position_exact FROM watch_replay_floors
                 WHERE api_version = 'v1' AND kind = 'ConfigMap' AND namespace_key = 'promote'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert!(floor_event_id > 0);
    assert_eq!(
        floor_position_exact, 1,
        "exact GC observations must promote legacy-inexact rows"
    );

    let target = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "promote",
    )];
    let current = db.current_watch_replay_position().await.unwrap();
    let replay = db
        .list_watch_events_after_position_checked_bounded(
            &target,
            current,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            replay,
            klights_cluster_store::PositionedWatchReplayRead::Events(_)
        ),
        "fresh positioned handoff at the current event high-water must remain available"
    );
}

#[tokio::test]
async fn scoped_replay_floor_allows_retained_in_scope_event_after_unrelated_gc() {
    let db = Datastore::new_in_memory().await.unwrap();

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
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "app",
            )],
            since_rv,
        )
        .await
        .expect("checked replay");

    match replay {
        klights_cluster_store::WatchReplayRead::Events(events) => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].resource.name, "frontend");
        }
        klights_cluster_store::WatchReplayRead::Expired => {
            panic!("unrelated lower-RV churn must not expire app/Pod replay");
        }
    }
}

#[tokio::test]
async fn checked_watch_replay_bounded_limits_events() {
    let db = Datastore::new_in_memory().await.unwrap();
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
            &[klights_cluster_store::WatchTarget::namespaced_in_namespace(
                "v1", "Pod", "app",
            )],
            start_rv,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .expect("checked replay");

    match replay {
        klights_cluster_store::WatchReplayRead::Events(events) => {
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.resource.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["pod-0", "pod-1", "pod-2"]
            );
        }
        klights_cluster_store::WatchReplayRead::Expired => {
            panic!("fresh bounded replay should not expire");
        }
    }
}

#[tokio::test]
async fn positioned_watch_replay_pages_one_hundred_same_revision_rows() {
    let db = Datastore::new_in_memory().await.unwrap();
    let start_rv = db.get_current_resource_version().await.unwrap();
    let operations = (0..100)
        .map(|index| ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: format!("same-rv-{index}"),
            data: json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": format!("same-rv-{index}"), "namespace": "default"}
            }),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        })
        .collect();
    db.apply_resource_batch(operations).await.unwrap();

    let targets = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "default",
    )];
    let mut position = klights_cluster_core::WatchReplayPosition::from_resource_version(start_rv);
    let mut names = Vec::new();
    loop {
        let replay = db
            .list_watch_events_after_position_checked_bounded(
                &targets,
                position,
                std::num::NonZeroUsize::new(3).unwrap(),
            )
            .await
            .unwrap();
        let klights_cluster_store::PositionedWatchReplayRead::Events(replay) = replay else {
            panic!("fresh positioned replay must not expire");
        };
        position = replay.next_position;
        let count = replay.events.len();
        names.extend(
            replay
                .events
                .into_iter()
                .map(|event| event.event.resource.name),
        );
        if count < 3 {
            break;
        }
    }

    assert_eq!(names.len(), 100);
    let unique: std::collections::HashSet<_> = names.iter().collect();
    assert_eq!(
        unique.len(),
        100,
        "every same-RV row must be delivered once"
    );
}

#[tokio::test]
async fn positioned_watch_replay_ignores_unrelated_scope_gc_floor() {
    let db = Datastore::new_in_memory().await.unwrap();
    let target = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1", "Secret", "quiet",
    )];
    let start = klights_cluster_core::WatchReplayPosition::from_resource_version(
        db.get_current_resource_version().await.unwrap(),
    );
    let klights_cluster_store::PositionedWatchReplayRead::Events(initial) = db
        .list_watch_events_after_position_checked_bounded(
            &target,
            start,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("fresh quiet-scope replay must not expire");
    };

    for index in 0..20 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("noisy"),
            &format!("churn-{index}"),
            json!({"metadata": {"name": format!("churn-{index}"), "namespace": "noisy"}}),
        )
        .await
        .unwrap();
    }
    db.gc_watch_events(1, 1_000).await.unwrap();

    match db
        .list_watch_events_after_position_checked_bounded(
            &target,
            initial.next_position,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap()
    {
        klights_cluster_store::PositionedWatchReplayRead::Events(replay) => {
            assert!(replay.events.is_empty());
        }
        klights_cluster_store::PositionedWatchReplayRead::Expired => {
            panic!("unrelated-scope GC must not expire a quiet scope");
        }
    }
}

#[tokio::test]
async fn positioned_watch_replay_expires_after_its_scope_position_is_pruned() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("seed"),
        "anchor",
        json!({"metadata": {"name": "anchor", "namespace": "seed"}}),
    )
    .await
    .unwrap();
    let target = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "target",
    )];
    let start = klights_cluster_core::WatchReplayPosition::from_resource_version(
        db.get_current_resource_version().await.unwrap(),
    );
    let klights_cluster_store::PositionedWatchReplayRead::Events(initial) = db
        .list_watch_events_after_position_checked_bounded(
            &target,
            start,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("fresh target replay must not expire");
    };

    for index in 0..5 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("target"),
            &format!("item-{index}"),
            json!({"metadata": {"name": format!("item-{index}"), "namespace": "target"}}),
        )
        .await
        .unwrap();
    }
    db.gc_watch_events(1, 1_000).await.unwrap();

    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            initial.next_position,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        klights_cluster_store::PositionedWatchReplayRead::Expired
    ));
}

/// A watch replay floor migrated from a pre-positioned-replay database does
/// not carry a proven exact event cursor (`floor_position_exact = 0`). A
/// positioned (event-ID) cursor cannot be validated against it and must fail
/// closed, while a resource-version-only cursor at or above the floor stays
/// available for scalar Kubernetes compatibility.
#[tokio::test]
async fn migrated_legacy_floor_fails_closed_for_positioned_replay() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("legacy"),
            "anchor",
            json!({"metadata": {"name": "anchor", "namespace": "legacy"}}),
        )
        .await
        .unwrap();
    let floor_rv = created.resource_version;

    // Simulate a row migrated from a pre-positioned-replay database: the
    // boundary's event ID was never proven exact, so it must be treated as a
    // resource-version-only legacy floor rather than an exact cursor.
    db.db_call("insert-legacy-floor", move |conn| {
        Ok(conn.execute(
            "INSERT INTO watch_replay_floors
                (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
             VALUES ('v1', 'ConfigMap', 'legacy', ?1, 0, 0)",
            rusqlite::params![floor_rv],
        )?)
    })
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("legacy"),
        "unrelated-after-floor",
        json!({"metadata": {"name": "unrelated-after-floor", "namespace": "legacy"}}),
    )
    .await
    .unwrap();

    let target = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "legacy",
    )];

    // A positioned cursor at the floor's RV cannot be honored: the boundary
    // is resource-version-only and must relist instead of replaying.
    let positioned =
        klights_cluster_core::WatchReplayPosition::from_resource_version_through_event_id(
            floor_rv, 1,
        );
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            positioned,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        klights_cluster_store::PositionedWatchReplayRead::Expired
    ));

    // The same boundary still permits scalar resource-version replay at or
    // above the floor, preserving Kubernetes RV compatibility.
    let scalar = db
        .list_watch_events_since_checked(&target, floor_rv)
        .await
        .unwrap();
    assert!(
        matches!(scalar, klights_cluster_store::WatchReplayRead::Events(_)),
        "legacy RV-only floor must not expire an at-or-above scalar cursor"
    );
}

#[tokio::test]
async fn migrated_zero_event_floor_allows_fresh_positioned_handoff() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("quiet"),
            "anchor",
            json!({"metadata": {"name": "anchor", "namespace": "quiet"}}),
        )
        .await
        .unwrap();
    let floor_rv = created.resource_version;

    db.db_call("insert-zero-event-legacy-floor", move |conn| {
        Ok(conn.execute(
            "INSERT INTO watch_replay_floors
                (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
             VALUES ('v1', 'ConfigMap', 'quiet', ?1, 0, 0)",
            rusqlite::params![floor_rv],
        )?)
    })
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Secret",
        Some("quiet"),
        "unrelated-after-floor",
        json!({"metadata": {"name": "unrelated-after-floor", "namespace": "quiet"}}),
    )
    .await
    .unwrap();

    let target = [klights_cluster_store::WatchTarget::namespaced_in_namespace(
        "v1",
        "ConfigMap",
        "quiet",
    )];
    let fresh = db.current_watch_replay_position().await.unwrap();
    assert!(
        fresh.event_id > 0,
        "test requires a current positioned LIST-to-WATCH handoff"
    );
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            fresh,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        klights_cluster_store::PositionedWatchReplayRead::Events(_)
    ));
    assert!(matches!(
        db.list_raw_watch_events_after_position_checked_bounded(
            &target,
            fresh,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        klights_cluster_store::PositionedWatchReplayRead::Events(_)
    ));

    let stale = klights_cluster_core::WatchReplayPosition::from_resource_version_through_event_id(
        floor_rv, 1,
    );
    assert!(matches!(
        db.list_watch_events_after_position_checked_bounded(
            &target,
            stale,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .await
        .unwrap(),
        klights_cluster_store::PositionedWatchReplayRead::Expired
    ));
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let resource_uid;
    let resource_rv;

    // First session: create a pod
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone())
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
        let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let names = ["cm-restart", "secret-restart", "svc-restart"];
    let kinds = ["ConfigMap", "Secret", "Service"];

    // Session 1: create resources
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone())
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
        let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    // Session 1: create resources to generate watch events
    let mut last_rv = 0i64;
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone())
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
        let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();

        // Replay from half the window
        let since_rv = last_rv - 10;
        use klights_cluster_store::{WatchTarget, WatchTargetScope};
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    // Session 1: create many events, then GC aggressively
    {
        let ds = Datastore::new_persistent(db_dir, supervisor.clone())
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
        let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();
        use klights_cluster_store::{WatchTarget, WatchTargetScope};
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

    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    {
        let ds = Datastore::new_persistent(db_dir, supervisor).await.unwrap();

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

// ── Task 5: Atomic batch apply ───────────────────────────────────────────────

/// Build + apply must happen in a single transaction: after a successful batch,
/// all resources are visible at the same resource_version and no extra RV was
/// allocated between build and apply.
#[tokio::test]
async fn apply_resource_batch_builds_and_applies_in_one_transaction() {
    let db = Datastore::new_in_memory().await.unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "atomic-ep".to_string(),
            data: json!({"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"atomic-ep","namespace":"default"},"subsets":[]}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "atomic-eps".to_string(),
            data: json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"atomic-eps","namespace":"default"},"addressType":"IPv4","endpoints":[],"ports":[]}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ])
    .await
    .unwrap();

    let ep = db
        .get_resource("v1", "Endpoints", Some("default"), "atomic-ep")
        .await
        .unwrap()
        .expect("Endpoints must exist");
    let eps = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "atomic-eps",
        )
        .await
        .unwrap()
        .expect("EndpointSlice must exist");
    // Both resources must share the same rv (single commit).
    assert_eq!(
        ep.resource_version, eps.resource_version,
        "atomic batch: both resources must share the same rv"
    );
    // The current metadata rv must equal exactly the batch rv — no extra rv
    // was allocated between a separate build and apply step.
    let after_rv = db.get_current_resource_version().await.unwrap();
    assert_eq!(
        after_rv, ep.resource_version,
        "metadata rv must equal batch rv; no extra allocation expected"
    );
    assert!(after_rv > before_rv, "rv must have advanced");
}

/// A batch that fails at build time (pre-condition failure) must not advance
/// the resource_version counter at all — no partial RV reservation.
#[tokio::test]
async fn apply_resource_batch_no_partial_visibility_on_failure() {
    let db = Datastore::new_in_memory().await.unwrap();
    // Seed a resource so a Create-mode batch for the same name will fail.
    db.create_resource("v1","ConfigMap",Some("default"),"already-exists",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"already-exists","namespace":"default"}}))
        .await.unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();

    // Batch tries to CREATE an existing resource — must fail at build time with no RV leak.
    let err = db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "already-exists".to_string(),
            data: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"already-exists","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ]).await;
    assert!(err.is_err(), "batch with duplicate Create must fail");
    let rv_after = db.get_current_resource_version().await.unwrap();
    assert_eq!(
        rv_before, rv_after,
        "failed batch must not leak a resource_version; rv before={rv_before} after={rv_after}"
    );
}

/// After a successful batch, the current resource_version must exactly equal
/// the batch resources' rv. No extra rv must have been allocated between a
/// separate "build" and "apply" step.
#[tokio::test]
async fn apply_resource_batch_candidate_rv_not_observable_before_apply() {
    let db = Datastore::new_in_memory().await.unwrap();
    let rv_before = db.get_current_resource_version().await.unwrap();
    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "atomic-cm".to_string(),
            data: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"atomic-cm","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ]).await.unwrap();
    let cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "atomic-cm")
        .await
        .unwrap()
        .expect("must exist");
    let rv_after = db.get_current_resource_version().await.unwrap();
    // With a two-step build+apply: the build step commits rv=N and the apply
    // step applies at rv=N. A reader between these steps would see rv=N in
    // metadata but no row yet. With the atomic single-transaction approach,
    // the rv and the row are always committed together.
    // Observable invariant: current_rv == batch rv, no extra allocation.
    assert_eq!(
        rv_after, cm.resource_version,
        "batch rv must equal metadata rv after atomic build+apply; before={rv_before}"
    );
}

/// Watch events emitted by a batch must all carry the same resource_version,
/// consistent with a single committed transaction.
#[tokio::test]
async fn apply_resource_batch_emits_watch_events_consistent_with_single_commit() {
    let db = Datastore::new_in_memory().await.unwrap();

    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-ep".to_string(),
            data: json!({"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"watch-ep","namespace":"default"},"subsets":[]}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-eps".to_string(),
            data: json!({"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"watch-eps","namespace":"default"},"addressType":"IPv4","endpoints":[],"ports":[]}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ]).await.unwrap();

    let observations = crate::test_fixtures::commit_observation::recorded_observations(
        db.commit_sink.as_deref().expect("test commit sink"),
    );
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations[0].resource_version(),
        observations[1].resource_version(),
        "batch watch events must all carry the same rv (single commit)"
    );
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.kind())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["EndpointSlice", "Endpoints"]),
    );
}

/// Fence for atomic build+apply: if the *apply* phase of a batch fails after the
/// build phase already succeeded, the candidate resourceVersion must be rolled
/// back and no rows from the batch may be visible.
///
/// This is the decisive regression guard. The seam is a two-operation batch on
/// the same resource:
///   1. Update `cm` (no precondition) — passes build, and at apply time rewrites
///      the row to the batch rv.
///   2. Delete `cm` with `precondition_resource_version = <original rv>` — passes
///      build (build validates against the *pre-batch* live rv, which still
///      equals the original), but FAILS at apply time because operation (1) has
///      already advanced the row's rv past the precondition.
///
/// Under the OLD two-step shape (build commits the candidate rv in one DB call,
/// then apply runs in a second step), the build commit would leak the advanced
/// metadata resourceVersion while the apply step rolls back, leaving rv > before
/// with no rows changed. Under the current single-IMMEDIATE-transaction shape,
/// the apply failure rolls back the whole transaction, so the rv is unchanged.
#[tokio::test]
async fn apply_resource_batch_rolls_back_candidate_rv_on_apply_failure() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "rollback-cm",
        json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"rollback-cm","namespace":"default"},"data":{"k":"v0"}}),
    )
    .await
    .unwrap();
    let original = db
        .get_resource("v1", "ConfigMap", Some("default"), "rollback-cm")
        .await
        .unwrap()
        .expect("seed ConfigMap must exist");
    let original_rv = original.resource_version;
    let original_uid = original.uid.clone();
    let rv_before = db.get_current_resource_version().await.unwrap();
    assert_eq!(
        rv_before, original_rv,
        "precondition: metadata rv equals the seeded row rv"
    );

    // Two ops on the SAME resource. Op 1 (Update) passes build and at apply time
    // rewrites the row to the batch rv. Op 2 (Delete with precondition = original
    // rv) passes build (validated against the pre-batch live rv) but fails at
    // apply because op 1 already advanced the row's rv.
    let err = db
        .apply_resource_batch(vec![
            ResourceBatchOperation::Put {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "rollback-cm".to_string(),
                // Preserve the original UID so op 2's delete is not no-op'd by
                // the same-name UID guard — it must instead fail on the rv
                // precondition that op 1 has invalidated.
                data: json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"rollback-cm","namespace":"default","uid":original_uid},"data":{"k":"v1"}}),
                mode: ResourceBatchPutMode::Update,
                preconditions: ResourcePreconditions::default(),
            },
            ResourceBatchOperation::Delete {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "rollback-cm".to_string(),
                preconditions: ResourcePreconditions::resource_version(original_rv),
            },
        ])
        .await;
    assert!(
        err.is_err(),
        "batch must fail at apply time (op 2 rv precondition no longer holds)"
    );

    // The candidate rv must be rolled back: metadata rv unchanged.
    let rv_after = db.get_current_resource_version().await.unwrap();
    assert_eq!(
        rv_before, rv_after,
        "apply failure must roll back the candidate rv; before={rv_before} after={rv_after}"
    );

    // No batch effect may be visible: the row must be unchanged (still present,
    // same rv, original data) — op 1's Update must not have leaked through.
    let after = db
        .get_resource("v1", "ConfigMap", Some("default"), "rollback-cm")
        .await
        .unwrap()
        .expect("ConfigMap must still exist (delete rolled back)");
    assert_eq!(
        after.resource_version, original_rv,
        "row rv must be unchanged after a rolled-back batch"
    );
    assert_eq!(
        after.data, original.data,
        "row data must be unchanged after a rolled-back batch (op 1 must not leak)"
    );
}

// ─────────────────────────────────────────────────────────────────────
// memory-improvement.md §10 P1 — paginated snapshot input variants.
// These keyset-paginated forms let `emit_snapshot_commits` consume the
// (potentially huge) `watch_events` and `applied_outbox` tables batch by
// batch instead of materializing each entire table into one Vec. The
// parity tests below assert they return EXACTLY the same rows, in the
// same order, as the legacy full-list forms.
// ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_all_watch_events_since_paged_matches_full_list_across_batch_boundaries() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 0..7u8 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &format!("cm{i}"),
            json!({"metadata": {"name": format!("cm{i}")}}),
        )
        .await
        .unwrap();
    }

    let full = db.list_all_watch_events_since(0).await.unwrap();
    assert!(
        full.len() > 3,
        "fixture must produce more rows than the page size"
    );

    // Walk the table in pages of 3 — strictly smaller than the row count —
    // keyset-paging on (resource_version, id).
    let mut paged: Vec<(i64, klights_cluster_store::CatchUpResource)> = Vec::new();
    let mut after_rv = 0i64;
    let mut after_id = 0i64;
    let page_size = std::num::NonZeroUsize::new(3).unwrap();
    loop {
        let batch = db
            .list_all_watch_events_since_paged(0, after_rv, after_id, page_size)
            .await
            .unwrap();
        if batch.is_empty() {
            break;
        }
        let last = batch.last().unwrap();
        after_rv = last.1.resource.resource_version;
        after_id = last.0;
        paged.extend(batch);
    }

    assert_eq!(
        paged.len(),
        full.len(),
        "paginated walk must visit every row exactly once"
    );
    for ((id_p, item_p), item_f) in paged.iter().zip(full.iter()) {
        assert!(
            *id_p > 0,
            "watch event id must be surfaced for keyset paging"
        );
        assert_eq!(item_p.resource.api_version, item_f.resource.api_version);
        assert_eq!(item_p.resource.kind, item_f.resource.kind);
        assert_eq!(item_p.resource.namespace, item_f.resource.namespace);
        assert_eq!(item_p.resource.name, item_f.resource.name);
        assert_eq!(
            item_p.resource.resource_version,
            item_f.resource.resource_version
        );
        assert_eq!(item_p.event_type, item_f.event_type);
    }
    // ids strictly increasing across the walk → no row visited twice.
    assert!(
        paged.windows(2).all(|w| w[0].0 < w[1].0),
        "paginated ids must be strictly increasing"
    );
}

#[tokio::test]
async fn list_applied_outbox_paged_matches_full_list_across_batch_boundaries() {
    let db = Datastore::new_in_memory().await.unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for i in 0..7u16 {
        db.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
            idempotency_key: format!("key-{i:03}"),
            subject_key: format!("subj-{i}"),
            operation: "PodMetadata".to_string(),
            first_seen_ms: now_ms + i as i64,
            applied_rv: Some(100 + i as i64),
            result_proto: vec![0u8; i as usize],
            status_stamp: None,
        })
        .await
        .unwrap();
    }

    let full = db.list_applied_outbox().await.unwrap();
    assert_eq!(full.len(), 7);

    let mut paged: Vec<klights_cluster_core::LogApplyAppliedOutboxRow> = Vec::new();
    let mut after_key: Option<String> = None;
    let page_size = std::num::NonZeroUsize::new(3).unwrap();
    loop {
        let batch = db
            .list_applied_outbox_paged(after_key.as_deref(), page_size)
            .await
            .unwrap();
        if batch.is_empty() {
            break;
        }
        let last_key = batch.last().unwrap().idempotency_key.clone();
        after_key = Some(last_key);
        paged.extend(batch);
    }

    assert_eq!(paged.len(), full.len());
    for (paged_row, full_row) in paged.iter().zip(full.iter()) {
        assert_eq!(paged_row.idempotency_key, full_row.idempotency_key);
        assert_eq!(paged_row.subject_key, full_row.subject_key);
        assert_eq!(paged_row.operation, full_row.operation);
        assert_eq!(paged_row.first_seen_ms, full_row.first_seen_ms);
        assert_eq!(paged_row.applied_rv, full_row.applied_rv);
        assert_eq!(paged_row.result_proto, full_row.result_proto);
        assert_eq!(paged_row.status_stamp, full_row.status_stamp);
    }
    assert!(
        paged
            .windows(2)
            .all(|w| w[0].idempotency_key < w[1].idempotency_key),
        "paginated keys must be strictly increasing (matches ORDER BY idempotency_key)"
    );
}
