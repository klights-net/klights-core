//! Root-owned wire-codec to passive-persistence integration coverage.

use std::{any::Any, sync::Arc};

use klights_cluster_core::{LogApplyMutation, LogApplyResourceRow};

struct PassiveCommitSink;

impl klights_cluster_store::CommitObservationSink for PassiveCommitSink {
    fn observe(&self, _observations: &[klights_cluster_store::StagedPostCommit]) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn passive_sqlite_store(
    connection_key: &'static str,
) -> klights_cluster_datastore::sqlite::embedded::Datastore {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = klights_cluster_datastore::sqlite::open_in_memory(supervisor, connection_key)
        .await
        .unwrap();
    klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor_with_sink(
        executor,
        Arc::new(PassiveCommitSink),
        crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
        Arc::new(klights_supervisor::SystemWallClock),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn committed_apply_json_and_protobuf_paths_produce_identical_rows() {
    let commit = crate::datastore::test_support::test_live_commit(
        110,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "encoding-cm".to_string(),
            uid: "cm-enc".to_string(),
            resource_version: 110,
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "encoding-cm",
                    "namespace": "default",
                    "uid": "cm-enc",
                    "resourceVersion": "110"
                },
                "data": {"encoded": "true"}
            }),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        })],
    );

    let json_bytes = klights_replication::log_apply_wire::encode_commit_json(&commit).unwrap();
    let json_commit = klights_replication::log_apply_wire::decode_commit_json(&json_bytes).unwrap();
    let json_store = passive_sqlite_store("root-codec-json").await;
    json_store
        .apply_raft_log_apply_commit_receipt(json_commit)
        .await
        .unwrap();

    let protobuf_bytes =
        klights_replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
    let protobuf_commit =
        klights_replication::log_apply_wire::decode_commit_protobuf(&protobuf_bytes).unwrap();
    let protobuf_store = passive_sqlite_store("root-codec-protobuf").await;
    protobuf_store
        .apply_raft_log_apply_commit_receipt(protobuf_commit)
        .await
        .unwrap();

    let json_row = json_store
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("JSON apply must materialize the row");
    let protobuf_row = protobuf_store
        .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
        .await
        .unwrap()
        .expect("protobuf apply must materialize the row");

    assert_eq!(json_row.uid, protobuf_row.uid);
    assert_eq!(json_row.resource_version, protobuf_row.resource_version);
    assert_eq!(json_row.data, protobuf_row.data);
}
