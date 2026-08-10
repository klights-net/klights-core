use super::*;

#[tokio::test]
async fn no_op_watch_events_gc_does_not_allocate_local_raft_rv() {
    let inner: crate::datastore::backend::DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let ds = SequencedDatastore::new(inner.clone(), Arc::new(PanicProposal));
    let before = inner.get_current_resource_version().await.unwrap();

    let removed = ds.gc_watch_events(100_000, 5_000).await.unwrap();

    assert_eq!(removed, 0, "empty watch history should make GC a no-op");
    assert_eq!(
        inner.get_current_resource_version().await.unwrap(),
        before,
        "no-op watch-events GC must not create leader-local raft metadata RV drift"
    );
}

#[tokio::test]
async fn raft_mode_advance_resource_version_routes_through_proposer() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let before = ds.get_current_resource_version().await.unwrap();

    let advanced = ds
        .advance_resource_version_after(before)
        .await
        .expect("raft-mode RV advance must commit through proposer");

    assert!(
        advanced > before,
        "advance_resource_version_after must return an RV above the requested floor"
    );
    assert_eq!(
        ds.get_current_resource_version().await.unwrap(),
        advanced,
        "public RV must reflect the raft-applied commit"
    );
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &["AdvanceResourceVersion"],
        "RV-only metadata writes must route through the raft proposer"
    );
}

#[tokio::test]
async fn raft_mode_watch_events_gc_routes_through_proposer_and_prunes_via_apply() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    for i in 0..12 {
        ds.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &format!("gc-via-raft-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": format!("gc-via-raft-{i}")
                }
            }),
        )
        .await
        .expect("seed watch event");
    }
    calls.lock().unwrap().clear();

    let removed = ds
        .gc_watch_events(5, 100)
        .await
        .expect("watch-events GC must commit through raft");

    assert!(removed > 0, "GC should report pruned watch events");
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        &["GcWatchEvents"],
        "watch-events GC must route through the raft proposer instead of writing locally"
    );
    let retained = ds
        .list_resources_modified_since("v1", "ConfigMap", Some("default"), 0)
        .await
        .expect("list retained watch events");
    assert!(
        retained.len() <= 5,
        "raft-applied GC must prune the watch table to the retained window; got {} events",
        retained.len()
    );
}

#[tokio::test]
async fn ensure_cluster_metadata_command_applies_cluster_id_once() {
    use crate::bootstrap::sequenced_datastore::apply_command_to_backend;
    use klights_cluster_core::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };

    let db = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let meta = CommandMeta {
        command_id: CommandId("ensure-cluster-metadata".to_string()),
        codec_version: COMMAND_CODEC_VERSION,
        resource_version: 1,
        uid: None,
        timestamp_ms: 0,
        authoring_node: "seed".into(),
    };
    // First apply: writes cluster_id
    apply_command_to_backend(
        &db,
        StorageCommand::EnsureClusterMetadata {
            cluster_id: "test-uuid-001".into(),
        },
        meta.clone(),
    )
    .await
    .unwrap();
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

    // Second apply with different cluster_id must NOT overwrite
    apply_command_to_backend(
        &db,
        StorageCommand::EnsureClusterMetadata {
            cluster_id: "different-uuid".into(),
        },
        CommandMeta {
            resource_version: 2,
            ..meta.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .as_deref(),
        Some("test-uuid-001"),
        "cluster_id must not be overwritten by a second proposal"
    );
}

#[test]
fn ensure_cluster_metadata_protobuf_round_trip() {
    use klights_cluster_core::command::StorageCommand;
    use klights_leader_rpc::storage_wire_codec as codec;

    let cmd = StorageCommand::EnsureClusterMetadata {
        cluster_id: "round-trip-uuid".into(),
    };
    let bytes = codec::encode_command_protobuf(&cmd).unwrap();
    let decoded = codec::decode_command_protobuf(&bytes).unwrap();
    assert_eq!(decoded, cmd);
}

#[tokio::test]
async fn set_klights_meta_with_proposer_routes_through_raft() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    ds.set_klights_meta("leader_hint", "mn-controlplane1")
        .await
        .expect("set_klights_meta with proposer must succeed");
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "proposer must be called exactly once");
    assert_eq!(calls[0], "SetKlightsMeta");
    drop(calls);
    assert_eq!(
        ds.get_klights_meta("leader_hint").await.unwrap().as_deref(),
        Some("mn-controlplane1"),
        "value must be readable after raft apply"
    );
}

#[tokio::test]
async fn set_klights_meta_follower_proposer_rejects_no_local_mutation() {
    let (ds, inner) = make_ds_with_follower_proposer().await;
    let err = ds
        .set_klights_meta("voters", r#"["other"]"#)
        .await
        .expect_err("follower set_klights_meta must reject");
    assert!(
        err.to_string().contains("leader"),
        "error must mention leader: {err}"
    );
    assert!(
        inner.get_klights_meta("voters").await.unwrap().is_none(),
        "inner backend must not be mutated on follower"
    );
}
