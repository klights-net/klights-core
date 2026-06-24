//! ReplicatedDatastore tests — extracted from replicated.rs.

mod cases {
    // Test assertions briefly lock a mock proposer's recorded-call log to
    // inspect it after an awaited operation; the std guard is dropped at end of
    // statement and the test runtime is single-threaded.
    #![allow(clippy::await_holding_lock)]
    use super::super::*;
    use crate::datastore::backend::DatastoreBackend;
    use crate::datastore::errors::OpenError;
    use crate::datastore::types::*;
    use async_trait::async_trait;
    use serde_json::json;

    /// T7.2: Create a ReplicatedDatastore in Raft mode with an inline
    /// proposer that applies commands directly to the inner backend.
    /// Every cluster.db write now requires a proposer — even SingleNode
    /// (N=1 raft) because the node can promote when CPs join.
    async fn make_ds_with_inline_proposer() -> (
        ReplicatedDatastore,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use crate::datastore::backend::DatastoreHandle;

        struct InlineProposer {
            inner: DatastoreHandle,
            calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl super::super::RaftProposer for InlineProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                self.calls
                    .lock()
                    .unwrap()
                    .push(command.variant_name().to_string());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("inline-{}", uuid::Uuid::new_v4());
                crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "inline-proposer",
                )
                .await
                .map_err(|e| anyhow::anyhow!("inline propose: {e}"))?;
                Ok(())
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                self.calls
                    .lock()
                    .unwrap()
                    .push(command.variant_name().to_string());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string())
                    })?;
                let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::kubelet::outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()),
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
        let ds = ReplicatedDatastore::new(
            inner,
            ReplicationMode::Raft {
                node_name: "test-node".into(),
            },
        );
        ds.set_raft_proposer(proposer);
        (ds, calls)
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
        let ds = ReplicatedDatastore::new(
            inner,
            ReplicationMode::Raft {
                node_name: "test-node".into(),
            },
        );
        let commit = crate::log_apply::LogApplyCommit::new(
            0,
            vec![crate::log_apply::LogApplyMutation::PutResource(
                crate::log_apply::LogApplyResourceRow {
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
            .expect("replicated wrapper must use raft terminal-conflict apply path");
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
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "leader".into(),
            },
        );
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
    async fn raft_mode_advance_resource_version_requires_proposer_without_local_rv_change() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "leader".into(),
            },
        );
        let before = inner.get_current_resource_version().await.unwrap();

        let err = ds
            .advance_resource_version_after(before)
            .await
            .expect_err("raft-mode RV advances must go through the raft proposer");

        assert!(
            err.to_string().contains("raft proposer not attached"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            inner.get_current_resource_version().await.unwrap(),
            before,
            "rejected raft-mode RV advance must not mutate leader-local metadata"
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
    async fn raft_mode_pod_slot_admissions_remain_node_local() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let observer = ReplicationObserver::new();
        let observed_for_callback = observed.clone();
        observer
            .set(Arc::new(move |command, _meta| {
                observed_for_callback
                    .lock()
                    .unwrap()
                    .push(command.variant_name().to_string());
            }))
            .await;
        let ds = ReplicatedDatastore::with_observer(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "leader".into(),
            },
            Some(observer),
        );
        let before = inner.get_current_resource_version().await.unwrap();

        let admitted = ds
            .pod_slot_try_admit("default", "slot-pod", "slot-uid", "node-a")
            .await
            .expect("pod slot admit is node-local and must not require raft proposer");
        assert!(matches!(
            admitted,
            PodSlotAdmissionResult::Admitted { resource_version } if resource_version > 0
        ));
        ds.pod_slot_mark_terminating("default", "slot-pod", "slot-uid", "node-a")
            .await
            .expect("pod slot termination is node-local and must not require raft proposer");
        ds.pod_slot_clear_if_uid("default", "slot-pod", "slot-uid", "node-a")
            .await
            .expect("pod slot clear is node-local and must not require raft proposer");

        assert_eq!(
            inner.get_current_resource_version().await.unwrap(),
            before,
            "node-local pod slot mutations must not allocate cluster resourceVersion"
        );
        assert!(
            observed.lock().unwrap().is_empty(),
            "node-local pod slot mutations must not emit cluster replication commands"
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
    async fn leader_rejects_forwarded_write_with_stale_uid() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "recreated",
            json!({
                "metadata": {
                    "name": "recreated",
                    "namespace": "default",
                    "uid": "uid-current"
                }
            }),
        )
        .await
        .unwrap();

        let err = crate::replication::apply::apply_forwarded_command(
            &db,
            StorageCommand::UpdateStatus {
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: Some("default".into()),
                name: "recreated".into(),
                status: json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("uid-stale".into()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            },
            "worker-1".into(),
        )
        .await
        .expect_err("stale UID must be rejected by the leader");

        assert!(
            crate::datastore::errors::is_conflict_error(&err),
            "expected conflict from stale UID write, got {err:#}"
        );
        let stored = db
            .get_resource("v1", "Pod", Some("default"), "recreated")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            None,
            "stale forwarded status must not mutate the replacement pod"
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
        let encoded = crate::datastore::command::encode_command_protobuf(&command).unwrap();
        let decoded = crate::datastore::command::decode_command_protobuf(&encoded).unwrap();

        apply_command_to_backend(
            &db,
            decoded,
            CommandMeta {
                command_id: CommandId::new(),
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
                command_id: CommandId::new(),
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
        let mut watch = db.subscribe_watch(crate::watch::WatchTopic::new("v1", "Pod"));

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
                command_id: CommandId::new(),
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
                command_id: CommandId::new(),
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
            },
            CommandMeta {
                command_id: CommandId::new(),
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
                command_id: CommandId::new(),
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
            .apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&deleted))
            .await
            .unwrap();
        follower
            .apply_log_apply_commit(crate::log_apply::LogApplyCommit::delete_resource(
                delete_rv,
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                "delete-rv-source",
                deleted.uid.clone(),
            ))
            .await
            .unwrap();
        follower
            .apply_log_apply_commit(crate::log_apply::LogApplyCommit::put_resource(&later))
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
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

        let result = ds
            .apply_outbox_transactionally(
                "lease-renew-key",
                crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str(),
                &payload,
                "worker-1",
            )
            .await
            .unwrap();
        let crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv } = result else {
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
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

        let result = ds
            .apply_outbox_transactionally(
                "create-from-outbox-key",
                crate::kubelet::outbox::payload::OutboxOperation::NodeRegistration.as_str(),
                &payload,
                "worker-1",
            )
            .await
            .unwrap();
        let crate::kubelet::outbox::OutboxApplyResult::Applied { .. } = result else {
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
        use crate::datastore::command::StorageCommand;

        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let meta = crate::datastore::command::CommandMeta {
            command_id: crate::datastore::command::CommandId("test".into()),
            codec_version: crate::datastore::command::COMMAND_CODEC_VERSION,
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

    /// P3-11c4: in Raft mode with a RaftProposer attached, `create_resource`
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
        impl super::super::RaftProposer for InlineProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("inline-{}", uuid::Uuid::new_v4());
                crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "raft-inline",
                )
                .await
                .map_err(|e| anyhow::anyhow!("inline propose apply: {e}"))?;
                Ok(())
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string())
                    })?;
                let result = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::kubelet::outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(result.result)
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let proposer = Arc::new(InlineProposer {
            inner: inner.clone(),
            calls: Default::default(),
        });
        let proposer_dyn: Arc<dyn super::super::RaftProposer> = proposer.clone();
        ds.set_raft_proposer(proposer_dyn);

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
        impl super::super::RaftProposer for RecordingProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                self.calls.lock().unwrap().push(command);
                Ok(())
            }

            async fn propose_outbox_command(
                &self,
                _idempotency_key: &str,
                _operation: &str,
                _command: StorageCommand,
                _authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                unreachable!("resource batch routing should use propose_command")
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let db = ReplicatedDatastore::new(
            inner,
            ReplicationMode::Raft {
                node_name: "cp1".to_string(),
            },
        );
        let proposer = Arc::new(RecordingProposer {
            calls: Default::default(),
        });
        db.set_raft_proposer(proposer.clone());

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

    /// T7.2: without a raft proposer attached, writes must return a
    /// typed error and must not mutate local cluster.db.
    #[tokio::test]
    async fn raft_mode_without_proposer_create_resource_returns_error() {
        let inner = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let err = ds
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "no-proposer",
                json!({"metadata": {"name": "no-proposer"}}),
            )
            .await
            .expect_err("create_resource without proposer must fail");
        assert!(
            err.to_string().contains("proposer") || err.to_string().contains("Raft"),
            "expected missing-proposer error, got: {err}"
        );
        assert!(
            inner
                .get_resource("v1", "ConfigMap", Some("default"), "no-proposer")
                .await
                .unwrap()
                .is_none(),
            "inner backend must not be mutated without proposer"
        );
    }

    /// T7.2: delete without proposer also returns error.
    #[tokio::test]
    async fn raft_mode_without_proposer_delete_resource_returns_error() {
        let inner = Arc::new(crate::datastore::test_support::in_memory().await);
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
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let err = ds
            .delete_resource_with_preconditions_observed_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                "to-delete",
                ResourcePreconditions::default(),
            )
            .await
            .expect_err("delete without proposer must fail");
        assert!(
            err.to_string().contains("proposer") || err.to_string().contains("Raft"),
            "expected missing-proposer error, got: {err}"
        );
        assert!(
            inner
                .get_resource("v1", "ConfigMap", Some("default"), "to-delete")
                .await
                .unwrap()
                .is_some(),
            "inner backend must not be mutated without proposer"
        );
    }

    #[tokio::test]
    async fn raft_mode_apply_outbox_transactionally_routes_via_proposer() {
        use crate::datastore::backend::DatastoreHandle;

        struct InlineProposer {
            inner: DatastoreHandle,
            calls: std::sync::Mutex<Vec<StorageCommand>>,
        }

        #[async_trait]
        impl super::super::RaftProposer for InlineProposer {
            async fn propose_command(&self, _command: StorageCommand) -> anyhow::Result<()> {
                unreachable!("outbox routing test should use propose_outbox_command")
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.clone());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string())
                    })?;
                let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::kubelet::outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await?;
                Ok(outcome.result)
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let proposer = Arc::new(InlineProposer {
            inner: inner.clone(),
            calls: Default::default(),
        });
        ds.set_raft_proposer(proposer.clone());

        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(
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
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus.as_str(),
                &payload,
                "worker-1",
            )
            .await
            .expect("apply_outbox via proposer");
        let crate::kubelet::outbox::OutboxApplyResult::Applied { .. } = result else {
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

    /// T7.2: without a raft proposer, outbox apply must return a
    /// Retryable error and must not mutate local cluster.db.
    #[tokio::test]
    async fn raft_mode_without_proposer_apply_outbox_returns_retryable_error() {
        let inner = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );

        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(
            StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "no-proposer-outbox".into(),
                data: json!({"metadata": {"name": "no-proposer-outbox"}}),
            },
        )
        .encode_protobuf()
        .unwrap();

        let err = ds
            .apply_outbox_transactionally(
                "no-proposer-key",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus.as_str(),
                &payload,
                "worker-1",
            )
            .await
            .expect_err("outbox without proposer must fail");
        assert!(
            matches!(err, crate::kubelet::outbox::OutboxApplyError::Retryable(_)),
            "expected Retryable error, got: {err:?}"
        );
        assert!(
            inner
                .get_resource("v1", "ConfigMap", Some("default"), "no-proposer-outbox")
                .await
                .unwrap()
                .is_none(),
            "inner backend must not be mutated without proposer"
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
        impl super::super::RaftProposer for InlineProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                self.calls.lock().unwrap().push(command.variant_name());
                apply_command_to_backend(
                    self.inner.as_ref(),
                    command,
                    CommandMeta {
                        command_id: CommandId::new(),
                        codec_version: COMMAND_CODEC_VERSION,
                        resource_version: 0,
                        uid: None,
                        timestamp_ms: 0,
                        authoring_node: "raft-inline".into(),
                    },
                )
                .await
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                self.calls.lock().unwrap().push(command.variant_name());
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|e| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string())
                    })?;
                let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    crate::kubelet::outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()),
                    )?,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await
                .map_err(|e| crate::kubelet::outbox::OutboxApplyError::Retryable(e.to_string()))?;
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

        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let proposer = Arc::new(InlineProposer {
            inner: inner.clone(),
            calls: Default::default(),
        });
        let proposer_dyn: Arc<dyn super::super::RaftProposer> = proposer.clone();
        ds.set_raft_proposer(proposer_dyn);

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
            crate::datastore::errors::is_conflict_error(&err),
            "stale raft delete precondition must surface as conflict, got: {err:#}"
        );
        assert!(
            !err.to_string().contains("Query returned no rows"),
            "stale raft delete precondition must not leak sqlite no-rows as API/internal error: {err:#}"
        );
    }

    /// DSB-R-09a: RaftRequiresSnapshotter error is constructive.
    #[test]
    fn raft_requires_snapshotter_error_has_actionable_message() {
        let err = OpenError::RaftRequiresSnapshotter {
            backend: "redb".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Raft"), "error must mention Raft: {msg}");
        assert!(
            msg.contains("snapshot"),
            "error must mention snapshot: {msg}"
        );
        assert!(msg.contains("redb"), "error must name the backend: {msg}");
        assert!(
            msg.contains("DSB-R-09a"),
            "error must reference DSB-R-09a: {msg}"
        );
    }

    // ── T7.1: EnsureClusterMetadata command ──

    #[tokio::test]
    async fn ensure_cluster_metadata_command_applies_cluster_id_once() {
        use crate::datastore::command::{
            COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
        };
        use crate::datastore::replicated::apply_command_to_backend;

        let db = crate::datastore::test_support::in_memory().await;
        let meta = CommandMeta {
            command_id: CommandId::new(),
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
            db.get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
                .await
                .unwrap()
                .as_deref(),
            Some("test-uuid-001")
        );
        assert_eq!(
            db.get_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH)
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
            db.get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
                .await
                .unwrap()
                .as_deref(),
            Some("test-uuid-001"),
            "cluster_id must not be overwritten by a second proposal"
        );
    }

    #[test]
    fn ensure_cluster_metadata_protobuf_round_trip() {
        use crate::datastore::command::{StorageCommand, codec};

        let cmd = StorageCommand::EnsureClusterMetadata {
            cluster_id: "round-trip-uuid".into(),
        };
        let bytes = codec::encode_command_protobuf(&cmd).unwrap();
        let decoded = codec::decode_command_protobuf(&bytes).unwrap();
        assert_eq!(decoded, cmd);
    }

    // ── T7.3: follower proposer rejects before local mutation ──

    /// Helper: creates a ReplicatedDatastore in Raft mode with a
    /// proposer that always rejects (simulating a non-leader node).
    async fn make_ds_with_follower_proposer() -> (
        ReplicatedDatastore,
        std::sync::Arc<dyn crate::datastore::DatastoreBackend>,
    ) {
        let inner: std::sync::Arc<dyn crate::datastore::DatastoreBackend> =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "follower-1".into(),
            },
        );
        struct FollowerProposer;
        #[async_trait]
        impl super::super::RaftProposer for FollowerProposer {
            async fn propose_command(
                &self,
                _command: crate::datastore::command::StorageCommand,
            ) -> anyhow::Result<()> {
                Err(anyhow::anyhow!(
                    "not the leader; forward to current raft leader"
                ))
            }
            async fn propose_outbox_command(
                &self,
                _k: &str,
                _o: &str,
                _c: crate::datastore::command::StorageCommand,
                _a: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                    "not the leader".into(),
                ))
            }
        }
        ds.set_raft_proposer(std::sync::Arc::new(FollowerProposer));
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
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(
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
            .apply_outbox_transactionally("key", "PodStatus", &payload, "worker-1")
            .await
            .expect_err("follower outbox must reject");
        assert!(
            matches!(err, crate::kubelet::outbox::OutboxApplyError::Retryable(_)),
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

        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
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
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "worker-1".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
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

    /// Without a raft proposer, set_klights_meta must return an error
    /// and must not mutate the inner backend. This proves the metadata
    /// write cannot be a local-only escape hatch.
    #[tokio::test]
    async fn set_klights_meta_without_proposer_returns_error_no_local_mutation() {
        let inner = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "n1".into(),
            },
        );
        let err = ds
            .set_klights_meta("voters", r#"["mn-leader"]"#)
            .await
            .expect_err("set_klights_meta without proposer must fail");
        assert!(
            err.to_string().contains("proposer"),
            "expected missing-proposer error, got: {err}"
        );
        assert!(
            inner.get_klights_meta("voters").await.unwrap().is_none(),
            "inner backend must not be mutated without proposer"
        );
    }

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
        use crate::datastore::replicated::apply_command_to_backend;

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
                command_id: CommandId::new(),
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
        use crate::datastore::replicated::apply_command_to_backend;

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
                command_id: CommandId::new(),
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
}
