//! Immutable replication-owned datastore sequencing tests.

mod cases {
    // Test assertions briefly lock a mock proposer's recorded-call log to
    // inspect it after an awaited operation; the std guard is dropped at end of
    // statement and the test runtime is single-threaded.
    #![allow(clippy::await_holding_lock)]
    use super::super::*;
    use crate::datastore::backend::DatastoreBackend;
    use async_trait::async_trait;
    use klights_cluster_core::command::{COMMAND_CODEC_VERSION, CommandId};
    use klights_cluster_core::{
        PatchKind, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
    };
    use serde_json::{Value, json};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Create a completely constructed sequencer with an inline proposal
    /// capability that applies commands to the passive backend.
    async fn make_ds_with_inline_proposer() -> (
        SequencedDatastore,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use crate::datastore::backend::DatastoreHandle;

        struct InlineProposer {
            inner: DatastoreHandle,
            calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl super::super::RaftProposal for InlineProposer {
            async fn propose_command(
                &self,
                command: StorageCommand,
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
                self.calls
                    .lock()
                    .unwrap()
                    .push(command.variant_name().to_string());
                if matches!(command, StorageCommand::DeleteResourceWithTombstone { .. }) {
                    let commit = self
                        .inner
                        .build_log_apply_commit_for_command(
                            command,
                            crate::node_outbox::payload::OutboxOperation::PodStatus.as_str(),
                            "inline-proposer",
                        )
                        .await?;
                    return self.inner.apply_raft_log_apply_commit(commit).await;
                }
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("inline-{}", uuid::Uuid::new_v4());
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::node_outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "inline-proposer",
                )
                .await
                .map_err(|e| anyhow::anyhow!("inline propose: {e}"))?;
                Ok(klights_replication::types::StorageCommandResult::new(
                    outcome.applied_resource_version(),
                    None,
                    None,
                    false,
                    None,
                    Default::default(),
                ))
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                self.calls
                    .lock()
                    .unwrap()
                    .push(command.variant_name().to_string());
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()))?;
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::node_outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(outcome.result)
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let proposer = Arc::new(InlineProposer {
            inner: inner.clone(),
            calls: calls.clone(),
        });
        let ds = SequencedDatastore::new(inner, proposer);
        (ds, calls)
    }

    struct PanicProposal;

    #[async_trait]
    impl super::super::RaftProposal for PanicProposal {
        async fn propose_command(
            &self,
            _command: StorageCommand,
        ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
            panic!("this operation must not submit a raft proposal")
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            crate::node_outbox::OutboxApplyResult,
            crate::node_outbox::OutboxApplyError,
        > {
            panic!("this operation must not submit an outbox proposal")
        }
    }

    fn assert_application_apply_rejected(error: anyhow::Error, operation: &str) {
        let message = error.to_string();
        assert!(
            message.contains("sequenced datastore rejects application-side committed apply"),
            "unexpected {operation} rejection: {message}"
        );
        assert!(
            message.contains(operation),
            "{operation} rejection must name the denied operation: {message}"
        );
        assert!(
            message.contains("private passive Raft state-machine backend"),
            "{operation} rejection must identify the privileged owner: {message}"
        );
    }

    #[tokio::test]
    async fn sequenced_facade_rejects_committed_apply_through_both_trait_views() {
        let passive: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = SequencedDatastore::new(passive.clone(), Arc::new(PanicProposal));

        assert_application_apply_rejected(
            DatastoreBackend::replace_replicated_resource_state(
                &ds,
                Vec::new(),
                0,
                None,
                None,
                None,
            )
            .await
            .expect_err("application facade must reject snapshot replacement"),
            "replace_replicated_resource_state",
        );
        assert_application_apply_rejected(
            DatastoreBackend::apply_log_apply_commit(
                &ds,
                crate::datastore::test_support::test_live_commit(1, Vec::new()),
            )
            .await
            .expect_err("application facade must reject legacy committed apply"),
            "apply_log_apply_commit",
        );
        assert_application_apply_rejected(
            DatastoreBackend::apply_raft_log_apply_commit(
                &ds,
                crate::datastore::test_support::test_live_commit(2, Vec::new()),
            )
            .await
            .expect_err("application facade must reject Raft committed apply"),
            "apply_raft_log_apply_commit",
        );
        assert_application_apply_rejected(
            DatastoreBackend::apply_raft_log_apply_commit_receipt(
                &ds,
                crate::datastore::test_support::test_live_commit(3, Vec::new()),
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
                crate::datastore::test_support::test_live_commit(4, Vec::new()),
            )
            .await
            .expect_err("replication compatibility facade must reject legacy committed apply"),
            "apply_log_apply_commit",
        );
        assert_application_apply_rejected(
            crate::datastore::ReplicationStore::apply_raft_log_apply_commit(
                &ds,
                crate::datastore::test_support::test_live_commit(5, Vec::new()),
            )
            .await
            .expect_err("replication compatibility facade must reject Raft committed apply"),
            "apply_raft_log_apply_commit",
        );
        assert_application_apply_rejected(
            crate::datastore::ReplicationStore::apply_raft_log_apply_commit_receipt(
                &ds,
                crate::datastore::test_support::test_live_commit(6, Vec::new()),
            )
            .await
            .expect_err(
                "replication compatibility facade must reject Raft committed apply outcomes",
            ),
            "apply_raft_log_apply_commit_receipt",
        );

        assert_eq!(
            passive.get_current_resource_version().await.unwrap(),
            0,
            "denied application-side apply must not mutate passive storage"
        );
    }

    /// DSB-HA-02: SingleNode (Raft N=1) exercises the replicated path
    /// through the raft proposer.
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
    async fn replicated_backend_raft_apply_returns_terminal_conflict_result() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
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
        let commit = crate::datastore::test_support::test_live_commit(
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
    async fn raft_mode_create_pod_injects_serviceaccount_projected_volume_before_commit() {
        let (ds, _calls) = make_ds_with_inline_proposer().await;

        let res = ds
            .create_resource(
                "v1",
                "Pod",
                Some("sonobuoy"),
                "sonobuoy",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "sonobuoy", "namespace": "sonobuoy"},
                    "spec": {
                        "serviceAccountName": "sonobuoy-serviceaccount",
                        "containers": [{
                            "name": "kube-sonobuoy",
                            "image": "sonobuoy/sonobuoy:v0.57.3"
                        }]
                    }
                }),
            )
            .await
            .expect("raft pod create must commit");

        let sa_volume_name = res
            .data
            .pointer("/spec/volumes")
            .and_then(|value| value.as_array())
            .and_then(|volumes| {
                volumes.iter().find_map(|volume| {
                    let name = volume.get("name").and_then(|value| value.as_str())?;
                    name.starts_with("kube-api-access-").then_some(name)
                })
            })
            .expect("raft-created pod must include kube-api-access projected volume");
        let sources = res
            .data
            .pointer("/spec/volumes")
            .and_then(|value| value.as_array())
            .and_then(|volumes| {
                volumes.iter().find(|volume| {
                    volume.get("name").and_then(|value| value.as_str()) == Some(sa_volume_name)
                })
            })
            .and_then(|volume| volume.pointer("/projected/sources"))
            .and_then(|value| value.as_array())
            .expect("service account volume must have projected sources");
        assert!(
            sources
                .iter()
                .any(|source| source.get("serviceAccountToken").is_some()),
            "projected service account volume must include serviceAccountToken source"
        );
        let mounts = res
            .data
            .pointer("/spec/containers/0/volumeMounts")
            .and_then(|value| value.as_array())
            .expect("service account volume mount must be injected");
        assert!(
            mounts.iter().any(|mount| {
                mount.get("name").and_then(|value| value.as_str()) == Some(sa_volume_name)
                    && mount.get("mountPath").and_then(|value| value.as_str())
                        == Some("/var/run/secrets/kubernetes.io/serviceaccount")
                    && mount.get("readOnly").and_then(|value| value.as_bool()) == Some(true)
            }),
            "service account projected volume must be mounted read-only at the Kubernetes serviceaccount path"
        );
    }

    #[tokio::test]
    async fn no_op_watch_events_gc_does_not_allocate_local_raft_rv() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
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
    async fn no_op_applied_outbox_gc_does_not_allocate_local_raft_rv() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = SequencedDatastore::new(inner.clone(), Arc::new(PanicProposal));
        let before = inner.get_current_resource_version().await.unwrap();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let removed = ds
            .gc_applied_outbox(now_ms, 60_000)
            .await
            .expect("empty outbox should not require proposer");

        assert_eq!(removed, 0, "empty applied_outbox should make GC a no-op");
        assert_eq!(
            inner.get_current_resource_version().await.unwrap(),
            before,
            "no-op applied_outbox GC must not create leader-local raft metadata RV drift"
        );
    }

    #[tokio::test]
    async fn raft_mode_applied_outbox_gc_routes_through_proposer_and_prunes_via_apply() {
        let (ds, calls) = make_ds_with_inline_proposer().await;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        ds.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
            idempotency_key: "old-legacy-row".into(),
            subject_key: "v1:Pod:default:old".into(),
            operation: "PodStatus".into(),
            first_seen_ms: now_ms - 86_400_000,
            applied_rv: Some(12),
            result_proto: vec![0x01, 0x02, 0x03],
            status_stamp: None,
        })
        .await
        .expect("seed legacy outbox row");
        ds.insert_applied_outbox(klights_cluster_core::LogApplyAppliedOutboxRow {
            idempotency_key: "recent-legacy-row".into(),
            subject_key: "v1:Pod:default:recent".into(),
            operation: "PodStatus".into(),
            first_seen_ms: now_ms - 1_000,
            applied_rv: Some(13),
            result_proto: vec![0x04],
            status_stamp: None,
        })
        .await
        .expect("seed recent outbox row");
        calls.lock().unwrap().clear();

        let removed = ds
            .gc_applied_outbox(now_ms, 60_000)
            .await
            .expect("applied_outbox GC must commit through raft");

        assert_eq!(
            removed, 1,
            "only the stale row should be reported as prunable"
        );
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &["GcAppliedOutbox"],
            "applied_outbox GC must route through the raft proposer instead of writing locally"
        );

        let mut remaining = ds
            .list_applied_outbox()
            .await
            .expect("query remaining applied_outbox rows");
        remaining.sort_by(|a, b| a.idempotency_key.cmp(&b.idempotency_key));
        let remaining_keys: Vec<_> = remaining
            .iter()
            .map(|row| row.idempotency_key.as_str())
            .collect();
        assert!(
            remaining_keys.iter().any(|key| *key == "recent-legacy-row"),
            "recent legacy row should survive GC"
        );
        assert!(
            !remaining_keys.iter().any(|key| *key == "old-legacy-row"),
            "stale legacy row should be removed by raft-applied GC"
        );
        assert!(
            remaining
                .iter()
                .any(|row| row.subject_key == "GcAppliedOutbox"),
            "GC command should leave an outbox ledger row for this operation"
        );
    }

    #[tokio::test]
    async fn replicated_apply_preserves_preconditions_through_codec() {
        let db = crate::datastore::test_support::in_memory().await;
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
        let db = crate::datastore::test_support::in_memory().await;
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
        let db = crate::datastore::test_support::in_memory().await;
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
        let db = crate::datastore::test_support::in_memory().await;
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
        assert_eq!(event.event_type, crate::watch::EventType::Deleted);
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
        assert_eq!(event.event_type, crate::watch::EventType::Added);
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
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_main_update_allows_status_only_rv_advance() {
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_main_update_rejects_true_spec_conflict() {
        let db = crate::datastore::test_support::in_memory().await;
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
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_status_rejects_status_only_rv_conflict() {
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_patch_rejects_stale_resource_version() {
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_patch_allows_status_only_rv_advance() {
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_patch_rejects_true_spec_conflict() {
        let db = crate::datastore::test_support::in_memory().await;
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
        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_apply_status_rejects_stale_resource_version() {
        let db = crate::datastore::test_support::in_memory().await;
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

    /// DSB-HA-02: leader allows writes through raft proposer.
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

    /// T7.2: leader writes route through the raft proposer.
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

    // T3: `leader_write_appends_durable_log_apply_entry` deleted —
    // `log_apply_entries` table and its backend methods are removed.
    // Raft AppendEntries through apply_log_apply_commit is the only
    // replication path (T1.3).

    // T3: `log_apply_commit_uses_watch_row_*` and `log_apply_auto_index_*`
    // tests deleted — `log_apply_entries` table and
    // `log_apply_commit_for_applied_command` method are removed.

    #[tokio::test]
    async fn delete_resource_exposes_committed_rv_for_leader_log_apply() {
        let leader = crate::datastore::test_support::in_memory().await;
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

        let follower = crate::datastore::test_support::in_memory().await;
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

    /// LeaseRenew outbox operations are short-circuited and return
    /// early without routing through the raft proposer.
    #[tokio::test]
    async fn lease_renew_outbox_does_not_route_through_proposer() {
        let (ds, calls) = make_ds_with_inline_proposer().await;
        let inner = crate::datastore::test_support::in_memory().await;
        inner
            .create_resource(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                "worker-1",
                json!({
                    "metadata": {
                        "name": "worker-1",
                        "namespace": "kube-node-lease",
                        "uid": "lease-uid-1"
                    },
                    "spec": {
                        "holderIdentity": "worker-1",
                        "renewTime": "2026-05-24T21:00:00Z"
                    }
                }),
            )
            .await
            .unwrap();

        let command = StorageCommand::UpdateResource {
            api_version: "coordination.k8s.io/v1".to_string(),
            kind: "Lease".to_string(),
            namespace: Some("kube-node-lease".to_string()),
            name: "worker-1".to_string(),
            data: json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "namespace": "kube-node-lease",
                    "name": "worker-1",
                    "uid": "lease-uid-1"
                },
                "spec": {
                    "holderIdentity": "worker-1",
                    "leaseDurationSeconds": 50,
                    "renewTime": "2026-05-25T13:15:21.000000Z"
                }
            }),
            expected_rv: 1,
            preconditions: ResourcePreconditions {
                uid: Some("lease-uid-1".to_string()),
                resource_version: Some(1),
            },
        };
        let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

        let result = ds
            .apply_outbox_transactionally(
                "lease-renew-key",
                crate::node_outbox::payload::OutboxOperation::LeaseRenew.as_str(),
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
                "worker-1",
            )
            .await
            .unwrap();
        let crate::node_outbox::OutboxApplyResult::Applied { applied_rv } = result else {
            panic!("expected LeaseRenew to be accepted");
        };
        assert_eq!(applied_rv, 0);

        // LeaseRenew is short-circuited and must NOT go through the proposer
        let calls = calls.lock().unwrap();
        assert!(
            calls.is_empty(),
            "LeaseRenew must not route through proposer, got: {calls:?}"
        );
    }

    #[tokio::test]
    async fn leader_outbox_create_log_apply_preserves_generated_uid() {
        let (ds, _calls) = make_ds_with_inline_proposer().await;
        let command = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "from-outbox".into(),
            data: json!({"metadata": {"name": "from-outbox", "namespace": "default"}}),
        };
        let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

        let result = ds
            .apply_outbox_transactionally(
                "create-from-outbox-key",
                crate::node_outbox::payload::OutboxOperation::NodeRegistration.as_str(),
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
                "worker-1",
            )
            .await
            .unwrap();
        let crate::node_outbox::OutboxApplyResult::Applied { .. } = result else {
            panic!("expected first outbox apply to mutate the leader");
        };

        // The inline proposer applies through the raft state machine
        // which creates the resource. Verify via the ds read path.
        let leader_resource = ds
            .get_resource("v1", "ConfigMap", Some("default"), "from-outbox")
            .await
            .unwrap()
            .expect("leader resource must exist");
        assert!(
            !leader_resource.uid.is_empty(),
            "leader resource must have a uid"
        );
    }

    /// DSB-HA-02 coverage gate: the DatastoreApplier impl maps every
    /// StorageCommand variant to a corresponding Datastore method.
    #[tokio::test]
    async fn datastore_applier_maps_all_variants() {
        use klights_cluster_core::command::StorageCommand;

        let db = Arc::new(crate::datastore::test_support::in_memory().await);
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

    /// P3-11c4: in Raft mode with a RaftProposal attached, `create_resource`
    /// must route the StorageCommand through the proposer instead of
    /// hitting the inner backend directly. The inline proposer in this
    /// test records each call and then applies the command synchronously
    /// against the inner so the wrapper's read-back succeeds.
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
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("inline-{}", uuid::Uuid::new_v4());
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::node_outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "raft-inline",
                )
                .await
                .map_err(|e| anyhow::anyhow!("inline propose apply: {e}"))?;
                Ok(klights_replication::types::StorageCommandResult::new(
                    outcome.applied_resource_version(),
                    None,
                    None,
                    false,
                    None,
                    Default::default(),
                ))
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()))?;
                let result = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::node_outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(result.result)
            }
        }

        let proposer = Arc::new(InlineProposer {
            inner: Arc::new(crate::datastore::test_support::in_memory().await),
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
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
                self.calls.lock().unwrap().push(command);
                Ok(klights_replication::types::StorageCommandResult::default())
            }

            async fn propose_outbox_command(
                &self,
                _idempotency_key: &str,
                _operation: &str,
                _command: StorageCommand,
                _authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                unreachable!("resource batch routing should use propose_command")
            }
        }

        let proposer = Arc::new(RecordingProposer {
            calls: Default::default(),
        });
        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
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
    async fn raft_mode_apply_outbox_transactionally_routes_via_proposer() {
        use crate::datastore::backend::DatastoreHandle;

        struct InlineProposer {
            inner: DatastoreHandle,
            calls: std::sync::Mutex<Vec<StorageCommand>>,
        }

        #[async_trait]
        impl super::super::RaftProposal for InlineProposer {
            async fn propose_command(
                &self,
                _command: StorageCommand,
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
                unreachable!("outbox routing test should use propose_outbox_command")
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()))?;
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::node_outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(outcome.result)
            }
        }

        let proposer = Arc::new(InlineProposer {
            inner: Arc::new(crate::datastore::test_support::in_memory().await),
            calls: Default::default(),
        });
        let inner = proposer.inner.clone();
        let ds = SequencedDatastore::new(inner.clone(), proposer.clone());

        let payload = crate::node_outbox::payload::OutboxPayload::from_command(
            StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "from-outbox".into(),
                data: json!({"metadata": {"name": "from-outbox", "namespace": "default"}}),
            },
        )
        .encode_protobuf()
        .unwrap();

        let result = ds
            .apply_outbox_transactionally(
                "outbox-key",
                crate::node_outbox::payload::OutboxOperation::PodStatus.as_str(),
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
                "worker-1",
            )
            .await
            .expect("apply_outbox via proposer");
        let crate::node_outbox::OutboxApplyResult::Applied { .. } = result else {
            panic!("expected Applied for first outbox apply");
        };

        let calls = proposer.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "proposer should receive one outbox command");
        assert!(
            matches!(&calls[0], StorageCommand::CreateResource { name, .. } if name == "from-outbox")
        );

        let row = inner
            .get_resource("v1", "ConfigMap", Some("default"), "from-outbox")
            .await
            .unwrap();
        assert!(
            row.is_some(),
            "outbox propose path should still materialize resource"
        );
    }

    /// P3-11c4: delete_resource_with_preconditions_observed_rv must route
    /// the DeleteResource command through raft, then surface the cluster's
    /// current resource version (read back after the apply path advances).
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
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
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
                Ok(klights_replication::types::StorageCommandResult::default())
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.variant_name());
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()))?;
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::node_outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await
                .map_err(|e| crate::node_outbox::OutboxApplyError::Retryable(e.to_string()))?;
                Ok(outcome.result)
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
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

    // ── T7.1: EnsureClusterMetadata command ──

    #[tokio::test]
    async fn ensure_cluster_metadata_command_applies_cluster_id_once() {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;
        use klights_cluster_core::command::{
            COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
        };

        let db = crate::datastore::test_support::in_memory().await;
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

    // ── T7.3: follower proposer rejects before local mutation ──

    /// Helper: creates a SequencedDatastore in Raft mode with a
    /// proposer that always rejects (simulating a non-leader node).
    async fn make_ds_with_follower_proposer() -> (
        SequencedDatastore,
        std::sync::Arc<dyn crate::datastore::DatastoreBackend>,
    ) {
        let inner: std::sync::Arc<dyn crate::datastore::DatastoreBackend> =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        struct FollowerProposer;
        #[async_trait]
        impl super::super::RaftProposal for FollowerProposer {
            async fn propose_command(
                &self,
                _command: klights_cluster_core::command::StorageCommand,
            ) -> anyhow::Result<klights_replication::types::StorageCommandResult> {
                Err(anyhow::anyhow!(
                    "not the leader; forward to current raft leader"
                ))
            }
            async fn propose_outbox_command(
                &self,
                _k: &str,
                _o: &str,
                _c: klights_cluster_core::command::StorageCommand,
                _a: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                Err(crate::node_outbox::OutboxApplyError::Retryable(
                    "not the leader".into(),
                ))
            }
        }
        let ds = SequencedDatastore::new(inner.clone(), std::sync::Arc::new(FollowerProposer));
        (ds, inner)
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
    async fn raft_mode_follower_proposer_rejects_outbox_apply_no_local_mutation() {
        let (ds, inner) = make_ds_with_follower_proposer().await;
        let payload = crate::node_outbox::payload::OutboxPayload::from_command(
            StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "follower-outbox".into(),
                data: json!({"metadata": {"name": "follower-outbox"}}),
            },
        )
        .encode_protobuf()
        .unwrap();
        let err = ds
            .apply_outbox_transactionally(
                "key",
                "PodStatus",
                klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
                "worker-1",
            )
            .await
            .expect_err("follower outbox must reject");
        assert!(
            matches!(err, crate::node_outbox::OutboxApplyError::Retryable(_)),
            "expected Retryable error, got: {err:?}"
        );
        assert!(
            inner
                .get_resource("v1", "ConfigMap", Some("default"), "follower-outbox")
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

    // ── T7.1 gap: set_klights_meta must route through raft proposer ──

    /// With an inline proposer, set_klights_meta must route through raft
    /// and the value must be visible after apply.
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

    /// Follower proposer must reject set_klights_meta without local mutation.
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

    /// Live multinode regression: a leader-side scheduler preemption writes the
    /// victim's termination as a full `UpdateResource` (metadata.deletionTimestamp
    /// plus a status carrying the scheduler-owned `DisruptionTarget` condition).
    /// That write is replicated through raft, so it lands in
    /// `apply_command_to_backend`. A concurrent kubelet status write can bump the
    /// live row's resourceVersion ahead of the preemption command's meta RV
    /// before the preemption command applies. In that case the apply path
    /// preserves the live `.status` over the proposed one via
    /// `preserve_status_subresource_on_main_update` — and that preserve step
    /// MUST route through the central Pod status merge so the scheduler-owned
    /// `DisruptionTarget` condition is not dropped on the floor.
    #[tokio::test]
    async fn replicated_update_resource_preserves_disruption_target_over_newer_kubelet_status() {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
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

    /// Multinode scheduler bind regression: the scheduler writes a full Pod
    /// `UpdateResource` carrying both `spec.nodeName` and `PodScheduled=True`.
    /// Raft apply preserves Pod status for ordinary main-resource updates, but
    /// it must not preserve the old `SchedulingPending` condition over a
    /// scheduler-owned bind transition. Otherwise the object becomes internally
    /// inconsistent (`spec.nodeName` set while `PodScheduled=False`) and e2e
    /// waits for Running time out on a pod that kubelet never admits.
    #[tokio::test]
    async fn replicated_scheduler_bind_overwrites_pod_scheduled_pending_condition() {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
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

    /// Reproduces the live SchedulerPreemption conformance failure: after the
    /// leader-side scheduler preemption writes `DisruptionTarget` to the victim,
    /// the leader's own kubelet runtime-reconcile status write races the
    /// preemption and lands a snapshot computed BEFORE preemption (no
    /// DisruptionTarget). That status write is proposed through raft as
    /// `StorageCommand::UpdateStatus` with `observed_status_stamp: None` — the
    /// leader-direct path never carries an outbox stamp. The raft apply must
    /// still preserve scheduler-owned Pod conditions, otherwise the stale
    /// kubelet snapshot permanently clobbers `DisruptionTarget` (subsequent
    /// reconciles read the clobbered row and never restore the condition),
    /// which is exactly what the live run observed: victim terminating with no
    /// DisruptionTarget.
    #[tokio::test]
    async fn leader_direct_status_apply_preserves_disruption_target_without_outbox_stamp() {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
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
    async fn replicated_stale_status_preserves_live_job_status_scalars() {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
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

    struct ReplicatedStaleStatusCase {
        api_version: &'static str,
        kind: &'static str,
        namespace: Option<&'static str>,
        name: &'static str,
        uid: &'static str,
        initial: serde_json::Value,
        stale_status: serde_json::Value,
        expected_pointer: &'static str,
        expected_value: serde_json::Value,
    }

    async fn apply_replicated_stale_status_case(
        case: ReplicatedStaleStatusCase,
    ) -> crate::datastore::Resource {
        use crate::bootstrap::sequenced_datastore::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                case.api_version,
                case.kind,
                case.namespace,
                case.name,
                case.initial,
            )
            .await
            .expect("create stale status fixture");

        db.patch_resource_latest_with_preconditions(
            case.api_version,
            case.kind,
            case.namespace,
            case.name,
            crate::datastore::ResourcePatchRequest::new(
                crate::datastore::PatchKind::Merge,
                serde_json::json!({"metadata": {"annotations": {"patchedstatus": "true"}}}),
                ResourcePreconditions {
                    uid: Some(case.uid.to_string()),
                    resource_version: None,
                },
            ),
        )
        .await
        .expect("metadata patch advances live resourceVersion");

        apply_command_to_backend(
            &db,
            StorageCommand::UpdateStatus {
                api_version: case.api_version.into(),
                kind: case.kind.into(),
                namespace: case.namespace.map(str::to_string),
                name: case.name.into(),
                status: case.stale_status.clone(),
                expected_rv: Some(created.resource_version),
                preconditions: ResourcePreconditions {
                    uid: Some(case.uid.into()),
                    resource_version: Some(created.resource_version),
                },
                observed_status_stamp: None,
            },
            CommandMeta {
                command_id: CommandId(format!("stale-status-{}-{}", case.kind, case.name)),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: created.resource_version,
                uid: Some(case.uid.into()),
                timestamp_ms: 0,
                authoring_node: "controlplane1".into(),
            },
        )
        .await
        .expect("same-UID stale status apply should rebase onto metadata-only rv churn");

        let live = db
            .get_resource(case.api_version, case.kind, case.namespace, case.name)
            .await
            .expect("read final stale status fixture")
            .expect("final stale status fixture exists");
        assert_eq!(
            live.data.pointer(case.expected_pointer),
            Some(&case.expected_value)
        );
        assert_eq!(
            live.data.pointer("/metadata/annotations/patchedstatus"),
            Some(&serde_json::json!("true")),
            "status rebase must preserve metadata-only changes that advanced the resourceVersion"
        );
        live
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

        let db = crate::datastore::test_support::in_memory().await;
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
                    ready_false = condition.get("status").and_then(serde_json::Value::as_str)
                        == Some("False");
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
}
