use super::*;

#[tokio::test]
async fn sequenced_facade_rejects_committed_apply_through_both_trait_views() {
    let passive: crate::datastore::backend::DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let ds = SequencedDatastore::new(passive.clone(), Arc::new(PanicProposal));

    assert_application_apply_rejected(
        DatastoreBackend::replace_replicated_resource_state(&ds, Vec::new(), 0, None, None, None)
            .await
            .expect_err("application facade must reject snapshot replacement"),
        "replace_replicated_resource_state",
    );
    assert_application_apply_rejected(
        DatastoreBackend::apply_log_apply_commit(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(1, Vec::new()),
        )
        .await
        .expect_err("application facade must reject legacy committed apply"),
        "apply_log_apply_commit",
    );
    assert_application_apply_rejected(
        DatastoreBackend::apply_raft_log_apply_commit(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(2, Vec::new()),
        )
        .await
        .expect_err("application facade must reject Raft committed apply"),
        "apply_raft_log_apply_commit",
    );
    assert_application_apply_rejected(
        DatastoreBackend::apply_raft_log_apply_commit_receipt(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(3, Vec::new()),
        )
        .await
        .expect_err("application facade must reject Raft committed apply outcomes"),
        "apply_raft_log_apply_commit_receipt",
    );

    assert_application_apply_rejected(
        crate::datastore::ReplicationStore::replace_replicated_resource_state(
            &ds,
            Vec::new(),
            0,
            None,
            None,
            None,
        )
        .await
        .expect_err("replication compatibility facade must reject snapshot replacement"),
        "replace_replicated_resource_state",
    );
    assert_application_apply_rejected(
        crate::datastore::ReplicationStore::apply_log_apply_commit(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(4, Vec::new()),
        )
        .await
        .expect_err("replication compatibility facade must reject legacy committed apply"),
        "apply_log_apply_commit",
    );
    assert_application_apply_rejected(
        crate::datastore::ReplicationStore::apply_raft_log_apply_commit(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(5, Vec::new()),
        )
        .await
        .expect_err("replication compatibility facade must reject Raft committed apply"),
        "apply_raft_log_apply_commit",
    );
    assert_application_apply_rejected(
        crate::datastore::ReplicationStore::apply_raft_log_apply_commit_receipt(
            &ds,
            klights_cluster_datastore::test_support::test_live_commit(6, Vec::new()),
        )
        .await
        .expect_err("replication compatibility facade must reject Raft committed apply outcomes"),
        "apply_raft_log_apply_commit_receipt",
    );

    assert_eq!(
        passive.get_current_resource_version().await.unwrap(),
        0,
        "denied application-side apply must not mutate passive storage"
    );
}

#[tokio::test]
async fn replicated_backend_raft_apply_returns_terminal_conflict_result() {
    let inner: crate::datastore::backend::DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    inner
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "dupe",
            json!({
                "metadata": {
                    "namespace": "default",
                    "name": "dupe",
                    "uid": "existing-uid"
                }
            }),
        )
        .await
        .expect("seed existing resource");
    let ds = inner;
    let commit = klights_cluster_datastore::test_support::test_live_commit(
        0,
        vec![klights_cluster_core::LogApplyMutation::PutResource(
            klights_cluster_core::LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "dupe".to_string(),
                uid: "new-uid".to_string(),
                resource_version: 0,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": "dupe",
                        "uid": "new-uid"
                    }
                }),
                require_absent: true,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            },
        )],
    );

    let result = ds
        .apply_raft_log_apply_commit(commit)
        .await
        .expect("passive backend must use raft terminal-conflict apply path");
    assert_eq!(result.applied_rv, None);
    assert!(
        result
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("409 Conflict")),
        "terminal conflict should be returned in raft result: {result:?}"
    );
}

#[tokio::test]
async fn datastore_applier_maps_all_variants() {
    use klights_cluster_core::command::StorageCommand;

    let db = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let meta = klights_cluster_core::command::CommandMeta {
        command_id: klights_cluster_core::command::CommandId("test".into()),
        codec_version: klights_cluster_core::command::COMMAND_CODEC_VERSION,
        resource_version: 1,
        uid: None,
        timestamp_ms: 0,
        authoring_node: "test".into(),
    };

    // CreateResource
    db.apply_command(
        StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "ac".into(),
            data: json!({"metadata": {"name": "ac"}}),
        },
        meta.clone(),
    )
    .await
    .unwrap();

    // Verify it was created
    let r = db
        .get_resource("v1", "ConfigMap", Some("default"), "ac")
        .await
        .unwrap();
    assert!(r.is_some());

    // DeleteResource
    db.apply_command(
        StorageCommand::DeleteResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "ac".into(),
            preconditions: ResourcePreconditions::default(),
        },
        meta,
    )
    .await
    .unwrap();

    let r = db
        .get_resource("v1", "ConfigMap", Some("default"), "ac")
        .await
        .unwrap();
    assert!(r.is_none());
}

#[tokio::test]
async fn raft_mode_follower_proposer_rejects_create_no_local_mutation() {
    let (ds, inner) = make_ds_with_follower_proposer().await;
    let err = ds
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "follower-cm",
            json!({"metadata": {"name": "follower-cm"}}),
        )
        .await
        .expect_err("follower must reject");
    assert!(
        err.to_string().contains("leader"),
        "error must mention leader: {err}"
    );
    // Verify no local SQLite mutation
    assert!(
        inner
            .get_resource("v1", "ConfigMap", Some("default"), "follower-cm")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn raft_mode_follower_proposer_rejects_delete_no_local_mutation() {
    let (ds, inner) = make_ds_with_follower_proposer().await;
    // Pre-seed a resource directly in the inner backend
    inner
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "to-delete",
            json!({"metadata": {"name": "to-delete"}}),
        )
        .await
        .unwrap();
    let err = ds
        .delete_resource_with_preconditions_observed_rv(
            "v1",
            "ConfigMap",
            Some("default"),
            "to-delete",
            ResourcePreconditions::default(),
        )
        .await
        .expect_err("follower delete must reject");
    assert!(err.to_string().contains("leader"));
    // Resource must still exist — no local mutation
    assert!(
        inner
            .get_resource("v1", "ConfigMap", Some("default"), "to-delete")
            .await
            .unwrap()
            .is_some()
    );
}
