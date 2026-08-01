#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use crate::datastore::DatastoreBackend;
    use crate::datastore::node_local::NodeLocalStores;
    use klights_replication::activation::CommandCodecV3Activation;
    use klights_replication::state_machine::SqliteRaftStateMachine;
    use klights_replication::types::TypeConfig;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    use openraft::{Entry, EntryPayload, LeaderId, LogId};

    type TestRaftStateMachine =
        SqliteRaftStateMachine<klights_replication::snapshot::SqliteRaftSnapshotBuilder>;

    fn applied_durability(
        stores: &NodeLocalStores,
    ) -> Arc<dyn klights_node_store::RaftAppliedStateDurability> {
        Arc::new(
            klights_replication::node_durability::OpenRaftNodeDurabilityAdapter::new(
                stores.raft_log_persistence(),
                stores.raft_applied_state_persistence(),
            ),
        )
    }

    async fn state_machine(
        backend: Arc<crate::datastore::sqlite::Datastore>,
        applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        supervisor: Arc<TaskSupervisor>,
    ) -> TestRaftStateMachine {
        state_machine_with_activation(backend, applied_state, supervisor)
            .await
            .0
    }

    async fn state_machine_with_activation(
        backend: Arc<crate::datastore::sqlite::Datastore>,
        applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        supervisor: Arc<TaskSupervisor>,
    ) -> (TestRaftStateMachine, Arc<CommandCodecV3Activation>) {
        let materializer =
            crate::cluster_store_replication_adapter::DatastoreRaftCommitMaterializer::new(
                backend.clone(),
            );
        let activation = Arc::new(
            CommandCodecV3Activation::load(&materializer)
                .await
                .expect("load command codec activation"),
        );
        let stores =
            crate::cluster_store_replication_adapter::raft_state_machine_store_ports_for_test(
                backend.clone(),
            );
        let snapshot_builder = klights_replication::snapshot::SqliteRaftSnapshotBuilder::new(
            backend.focused_recovery_store(),
            backend.focused_read_store(),
            Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
                backend,
            )),
            applied_state.clone(),
            supervisor,
        );
        (
            SqliteRaftStateMachine::new_with_command_codec_activation(
                stores,
                applied_state,
                snapshot_builder,
                activation.clone(),
            ),
            activation,
        )
    }

    fn snapshot_watch_page_pause_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    struct PausedAppliedState {
        inner: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        reached: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }

    impl PausedAppliedState {
        fn new(inner: Arc<dyn klights_node_store::RaftAppliedStateDurability>) -> Self {
            Self {
                inner,
                reached: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            }
        }
    }

    impl klights_node_store::RaftAppliedStateDurability for PausedAppliedState {
        fn load_applied_state(
            &self,
        ) -> klights_node_store::RaftDurabilityFuture<'_, klights_node_store::EncodedRaftAppliedState>
        {
            Box::pin(async move {
                self.reached.notify_one();
                self.resume.notified().await;
                self.inner.load_applied_state().await
            })
        }

        fn store_applied_state(
            &self,
            state: klights_node_store::RaftAppliedStateWrite,
        ) -> klights_node_store::RaftDurabilityFuture<'_, ()> {
            self.inner.store_applied_state(state)
        }
    }

    async fn fresh_sm() -> TestRaftStateMachine {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-sm-test-node",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(NodeLocalStores::from_executor(node_executor).expect("create node-local db"));
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        state_machine(backend, applied_durability(&node_local), supervisor).await
    }

    async fn build_sm_with_backend(
        backend: Arc<crate::datastore::sqlite::Datastore>,
    ) -> TestRaftStateMachine {
        build_sm_with_backend_and_activation(backend).await.0
    }

    async fn build_sm_with_backend_and_activation(
        backend: Arc<crate::datastore::sqlite::Datastore>,
    ) -> (TestRaftStateMachine, Arc<CommandCodecV3Activation>) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-snapshot-test-node",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(NodeLocalStores::from_executor(node_executor).expect("create node-local db"));
        state_machine_with_activation(backend, applied_durability(&node_local), supervisor).await
    }

    async fn seed_snapshot_identity(backend: &dyn DatastoreBackend) {
        backend
            .set_klights_meta(
                klights_cluster_store::CLUSTER_ID_META_KEY,
                "state-machine-snapshot-cluster",
            )
            .await
            .unwrap();
        backend
            .set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn snapshot_round_trip_replays_namespaces_and_resources() {
        // Populate a "leader" backend with one namespace + one Pod.
        let backend_src: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let leader_membership = klights_cluster_core::ClusterMembership {
            cluster_id: "leader-snapshot-cluster".into(),
            voters: vec!["cp-leader".into()],
            term: 7,
            leader_hint: Some("cp-leader".into()),
        };
        backend_src
            .replace_replicated_resource_state(
                Vec::new(),
                0,
                Some(0),
                Some(Vec::new()),
                Some(crate::datastore::ReplicatedSnapshotMetadata {
                    cluster_id: "leader-snapshot-cluster".into(),
                    leader_epoch: 7,
                    membership: crate::datastore::ReplicatedMembershipState::Present(
                        leader_membership.clone(),
                    ),
                    command_codec_activation_version: Some(3),
                }),
            )
            .await
            .expect("seed leader cluster identity");
        backend_src
            .create_namespace(
                "snap-ns",
                serde_json::json!({
                    "metadata": {"name": "snap-ns", "uid": "uid-ns"}
                }),
            )
            .await
            .expect("create namespace");
        backend_src
            .create_resource(
                "v1",
                "Pod",
                Some("snap-ns"),
                "snap-pod",
                serde_json::json!({
                    "metadata": {"name": "snap-pod", "namespace": "snap-ns", "uid": "uid-pod"}
                }),
            )
            .await
            .expect("create resource");
        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        // Advance last_applied so the snapshot meta is non-trivial.
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(4, 10), 42),
            payload: EntryPayload::Blank,
        };
        sm_src.apply(vec![entry]).await.unwrap();

        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        let snapshot_bytes = snapshot.snapshot.clone().into_inner();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 42);
        assert!(
            !snapshot_bytes.is_empty(),
            "snapshot bytes must contain payload"
        );

        // Install on a fresh "follower" backend that starts empty.
        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_dst
            .replace_replicated_resource_state(
                Vec::new(),
                0,
                Some(0),
                Some(Vec::new()),
                Some(crate::datastore::ReplicatedSnapshotMetadata {
                    cluster_id: "divergent-snapshot-cluster".into(),
                    leader_epoch: 99,
                    membership: crate::datastore::ReplicatedMembershipState::Present(
                        klights_cluster_core::ClusterMembership {
                            cluster_id: "divergent-snapshot-cluster".into(),
                            voters: vec!["stale-cp".into()],
                            term: 99,
                            leader_hint: Some("stale-cp".into()),
                        },
                    ),
                    command_codec_activation_version: None,
                }),
            )
            .await
            .expect("seed divergent follower cluster identity");
        let (mut sm_dst, dst_activation) =
            build_sm_with_backend_and_activation(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install snapshot");
        assert_eq!(
            backend_dst
                .get_klights_meta(klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY,)
                .await
                .expect("read restored codec activation marker")
                .as_deref(),
            Some("3")
        );
        assert!(
            dst_activation.is_activated(),
            "snapshot install must atomically restore the persisted marker and reopen the shared gate"
        );

        // Verify the dst backend now carries the same namespace + pod.
        let namespaces = backend_dst.list_namespaces(None, None).await.unwrap();
        assert!(
            namespaces.items.iter().any(|ns| ns.name == "snap-ns"),
            "namespace must be replayed into dst backend"
        );
        let pod = backend_dst
            .get_resource("v1", "Pod", Some("snap-ns"), "snap-pod")
            .await
            .unwrap();
        assert!(pod.is_some(), "pod must be replayed into dst backend");
        let restored_metadata = backend_dst
            .read_cluster_metadata_observation()
            .await
            .expect("read restored cluster identity");
        assert_eq!(
            restored_metadata.metadata.cluster_id,
            "leader-snapshot-cluster"
        );
        assert_eq!(restored_metadata.metadata.leader_epoch, 7);
        assert_eq!(
            restored_metadata.membership,
            crate::datastore::ReplicatedMembershipState::Present(leader_membership),
            "streaming Raft envelope must preserve authoritative membership presence and value"
        );

        // last_applied must move forward on the destination, and the
        // current snapshot must be retrievable for outbound transfer.
        let (last_dst, _) = sm_dst.applied_state().await.unwrap();
        assert_eq!(last_dst.unwrap().index, 42);
        let cur = sm_dst.get_current_snapshot().await.unwrap();
        assert!(
            cur.is_some(),
            "installed snapshot must be cached for future outgoing transfer"
        );
        assert_eq!(cur.unwrap().meta.last_log_id.unwrap().index, 42);
    }

    #[tokio::test]
    async fn install_snapshot_restores_empty_watch_history_allocator_exactly() {
        let backend_src: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(backend_src.as_ref()).await;
        backend_src
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "leader-anchor",
                serde_json::json!({
                    "metadata": {"name": "leader-anchor", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        let leader_position = backend_src.current_watch_replay_position().await.unwrap();
        assert!(backend_src.gc_watch_events(0, -1).await.unwrap() > 0);

        let mut sm_src = build_sm_with_backend(backend_src).await;
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        for index in 0..8 {
            backend_dst
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    &format!("divergent-{index}"),
                    serde_json::json!({
                        "metadata": {
                            "name": format!("divergent-{index}"),
                            "namespace": "default"
                        }
                    }),
                )
                .await
                .unwrap();
        }
        assert!(
            backend_dst
                .current_watch_replay_position()
                .await
                .unwrap()
                .event_id
                > leader_position.event_id
        );

        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert_eq!(
            backend_dst
                .current_watch_replay_position()
                .await
                .unwrap()
                .event_id,
            leader_position.event_id,
            "Raft install must replace a divergent local allocator with the leader boundary"
        );
    }

    #[tokio::test]
    async fn snapshot_fence_excludes_post_anchor_resource_and_watch_event() {
        let _pause_guard = snapshot_watch_page_pause_test_lock().lock().await;
        let backend = crate::datastore::test_support::in_memory().await;
        seed_snapshot_identity(&backend).await;
        let entries = (1..=crate::datastore::snapshot_export::SNAPSHOT_EMIT_PAGE_SIZE as i64)
            .map(|event_id| {
                klights_cluster_core::SnapshotRestoreOperation::new(
                    1_000 + event_id,
                    None,
                    vec![klights_cluster_core::LogApplyMutation::PutWatchEvent(
                        klights_cluster_core::LogApplyWatchEventRow {
                            event_id: Some(event_id),
                            api_version: "v1".to_string(),
                            kind: "ConfigMap".to_string(),
                            namespace: Some("default".to_string()),
                            name: format!("seed-{event_id}"),
                            resource_version: 1_000 + event_id,
                            event_type: "ADDED".to_string(),
                            data: serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "ConfigMap",
                                "metadata": {
                                    "name": format!("seed-{event_id}"),
                                    "namespace": "default",
                                    "resourceVersion": (1_000 + event_id).to_string()
                                }
                            }),
                        },
                    )],
                )
            })
            .collect();
        backend
            .replace_replicated_resource_state(entries, 2_000, Some(512), Some(Vec::new()), None)
            .await
            .unwrap();

        let backend: Arc<crate::datastore::sqlite::Datastore> = Arc::new(backend);
        let mut state_machine = build_sm_with_backend(backend).await;
        let mut builder = state_machine.get_snapshot_builder().await;
        let pause =
            klights_cluster_datastore::sqlite::recovery::install_snapshot_capture_page_pause();
        let snapshot_task = tokio::spawn(async move { builder.build_snapshot().await });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.reached.notified())
            .await
            .expect("snapshot must reach the first watch page");

        let late_resource = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "late-resource",
                "namespace": "default",
                "uid": "late-resource-uid"
            }
        });
        let commit = klights_cluster_core::LogApplyCommit::try_new(vec![
            klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "late-resource".to_string(),
                    uid: "late-resource-uid".to_string(),
                    resource_version: 0,
                    data: late_resource.clone(),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            ),
            klights_cluster_core::LogApplyMutation::PutWatchEvent(
                klights_cluster_core::LogApplyWatchEventRow {
                    event_id: Some(513),
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "late-resource".to_string(),
                    resource_version: 0,
                    event_type: "ADDED".to_string(),
                    data: late_resource,
                },
            ),
        ])
        .expect("post-anchor live commit must be an RV-zero template");
        let payload = klights_replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
        let apply_task = tokio::spawn(async move {
            state_machine
                .apply(vec![Entry::<TypeConfig> {
                    log_id: LogId::new(LeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(
                        klights_replication::types::StorageCommandPayload::from_bytes(payload),
                    ),
                }])
                .await
        });
        tokio::task::yield_now().await;
        assert!(!apply_task.is_finished());
        pause.resume.notify_one();

        let snapshot = snapshot_task.await.unwrap().unwrap();
        apply_task.await.unwrap().unwrap();
        let destination: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut destination_state_machine = build_sm_with_backend(destination.clone()).await;
        destination_state_machine
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert_eq!(
            destination
                .current_watch_replay_position()
                .await
                .unwrap()
                .event_id,
            512
        );
        assert!(
            destination
                .get_resource("v1", "ConfigMap", Some("default"), "late-resource")
                .await
                .unwrap()
                .is_none(),
            "post-anchor materialized state and its event must both remain outside the snapshot"
        );
    }

    #[tokio::test]
    async fn disk_snapshot_fence_covers_applied_state_anchor_and_capture_pin() {
        let directory = tempfile::tempdir().expect("create disk snapshot fixture");
        std::fs::set_permissions(
            directory.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("secure disk snapshot fixture");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let backend = Arc::new(
            crate::datastore::sqlite::Datastore::new_persistent_paths(
                &directory.path().join("cluster.db"),
                supervisor.clone(),
                None,
            )
            .await
            .expect("open persistent snapshot fixture"),
        );
        seed_snapshot_identity(backend.as_ref()).await;
        let node_executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-snapshot-anchor-node",
        )
        .await
        .expect("open node-local snapshot fixture");
        let node_local =
            Arc::new(NodeLocalStores::from_executor(node_executor).expect("create node-local db"));
        let applied_state = applied_durability(&node_local);
        let paused_applied_state = Arc::new(PausedAppliedState::new(applied_state.clone()));
        let materializer =
            crate::cluster_store_replication_adapter::DatastoreRaftCommitMaterializer::new(
                backend.clone(),
            );
        let activation = Arc::new(
            CommandCodecV3Activation::load(&materializer)
                .await
                .expect("load command codec activation"),
        );
        let stores =
            crate::cluster_store_replication_adapter::raft_state_machine_store_ports_for_test(
                backend.clone(),
            );
        let snapshot_builder = klights_replication::snapshot::SqliteRaftSnapshotBuilder::new(
            backend.focused_recovery_store(),
            backend.focused_read_store(),
            Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
                backend,
            )),
            paused_applied_state.clone(),
            supervisor,
        );
        let mut state_machine = SqliteRaftStateMachine::new_with_command_codec_activation(
            stores,
            applied_state,
            snapshot_builder,
            activation,
        );
        let mut builder = state_machine.get_snapshot_builder().await;
        let snapshot_task = tokio::spawn(async move { builder.build_snapshot().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            paused_applied_state.reached.notified(),
        )
        .await
        .expect("snapshot must pause while reading its applied-state anchor");

        let commit = crate::datastore::test_support::test_live_commit(
            1,
            vec![klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "post-anchor".to_string(),
                    uid: "post-anchor-uid".to_string(),
                    resource_version: 0,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "post-anchor",
                            "namespace": "default",
                            "uid": "post-anchor-uid"
                        }
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            )],
        );
        let payload = klights_replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
        let mut apply_task = tokio::spawn(async move {
            state_machine
                .apply(vec![Entry::<TypeConfig> {
                    log_id: LogId::new(LeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(
                        klights_replication::types::StorageCommandPayload::from_bytes(payload),
                    ),
                }])
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(250), &mut apply_task)
                .await
                .is_err(),
            "committed apply must remain fenced until applied state and datastore snapshot share one anchor"
        );

        paused_applied_state.resume.notify_one();
        snapshot_task.await.unwrap().unwrap();
        apply_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn snapshot_fence_blocks_concurrent_authoritative_install() {
        let _pause_guard = snapshot_watch_page_pause_test_lock().lock().await;
        let source_backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(source_backend.as_ref()).await;
        let mut source_sm = build_sm_with_backend(source_backend).await;
        let replacement = source_sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();

        let destination = crate::datastore::test_support::in_memory().await;
        seed_snapshot_identity(&destination).await;
        let entries = (1..=crate::datastore::snapshot_export::SNAPSHOT_EMIT_PAGE_SIZE as i64)
            .map(|event_id| {
                klights_cluster_core::SnapshotRestoreOperation::new(
                    event_id,
                    None,
                    vec![klights_cluster_core::LogApplyMutation::PutWatchEvent(
                        klights_cluster_core::LogApplyWatchEventRow {
                        event_id: Some(event_id),
                        api_version: "v1".to_string(),
                        kind: "ConfigMap".to_string(),
                        namespace: Some("default".to_string()),
                        name: format!("seed-{event_id}"),
                        resource_version: event_id,
                        event_type: "ADDED".to_string(),
                        data: serde_json::json!({"metadata": {"name": format!("seed-{event_id}")}}),
                        },
                    )],
                )
            })
            .collect();
        destination
            .replace_replicated_resource_state(entries, 512, Some(512), Some(Vec::new()), None)
            .await
            .unwrap();
        let destination: Arc<crate::datastore::sqlite::Datastore> = Arc::new(destination);
        let mut destination_sm = build_sm_with_backend(destination).await;
        let mut builder = destination_sm.get_snapshot_builder().await;
        let pause =
            klights_cluster_datastore::sqlite::recovery::install_snapshot_capture_page_pause();
        let snapshot_task = tokio::spawn(async move { builder.build_snapshot().await });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.reached.notified())
            .await
            .expect("snapshot must reach the first watch page");

        let install_task = tokio::spawn(async move {
            destination_sm
                .install_snapshot(&replacement.meta, replacement.snapshot)
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !install_task.is_finished(),
            "authoritative install must wait until snapshot capture releases its fence"
        );
        pause.resume.notify_one();
        snapshot_task.await.unwrap().unwrap();
        install_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn install_snapshot_replaces_divergent_watch_replay_floors() {
        let backend_src: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(backend_src.as_ref()).await;
        for index in 0..5 {
            backend_src
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("floor-ns"),
                    &format!("leader-{index}"),
                    serde_json::json!({
                        "metadata": {
                            "name": format!("leader-{index}"),
                            "namespace": "floor-ns"
                        }
                    }),
                )
                .await
                .unwrap();
        }
        assert_eq!(backend_src.gc_watch_events(1, 100).await.unwrap(), 4);
        let target = [crate::datastore::WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "floor-ns",
        )];
        let leader_cursor = crate::datastore::WatchReplayPosition {
            resource_version: 4,
            event_id: 4,
            resource_version_filter_through_event_id: 0,
        };
        assert!(matches!(
            backend_src
                .list_watch_events_after_position_checked_bounded(
                    &target,
                    leader_cursor,
                    std::num::NonZeroUsize::new(10).unwrap(),
                )
                .await
                .unwrap(),
            crate::datastore::PositionedWatchReplayRead::Events(_)
        ));

        let mut sm_src = build_sm_with_backend(backend_src).await;
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        for index in 0..10 {
            backend_dst
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("floor-ns"),
                    &format!("follower-{index}"),
                    serde_json::json!({
                        "metadata": {
                            "name": format!("follower-{index}"),
                            "namespace": "floor-ns"
                        }
                    }),
                )
                .await
                .unwrap();
        }
        assert_eq!(backend_dst.gc_watch_events(1, 100).await.unwrap(), 9);

        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert!(
            matches!(
                backend_dst
                    .list_watch_events_after_position_checked_bounded(
                        &target,
                        leader_cursor,
                        std::num::NonZeroUsize::new(10).unwrap(),
                    )
                    .await
                    .unwrap(),
                crate::datastore::PositionedWatchReplayRead::Events(_)
            ),
            "a cursor valid against the leader floor must remain valid after snapshot install"
        );
    }

    /// finding.md H1 / P0 cluster.db divergence: installing a leader snapshot
    /// must atomically REPLACE the local replicated state, not merge snapshot
    /// commits over it. A follower/learner that holds a `stale` row the leader
    /// has already deleted must end up key-identical to the leader (stale row
    /// removed) after snapshot install. Previously this looped
    /// `apply_log_apply_commit` over the existing store (merge-only), so stale
    /// rows survived forever and members silently diverged under loss.
    #[tokio::test]
    async fn install_snapshot_replaces_local_state_and_removes_stale_rows() {
        // Leader: namespace snap-ns + ConfigMap `live`.
        let backend_src: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(backend_src.as_ref()).await;
        backend_src
            .create_namespace(
                "snap-ns",
                serde_json::json!({"metadata": {"name": "snap-ns"}}),
            )
            .await
            .expect("create leader namespace");
        backend_src
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "live",
                serde_json::json!({
                    "metadata": {"name": "live", "namespace": "snap-ns", "uid": "uid-live"}
                }),
            )
            .await
            .expect("create leader live resource");

        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        // Advance last_applied so the snapshot meta is non-trivial.
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(4, 10), 42),
            payload: EntryPayload::Blank,
        };
        sm_src.apply(vec![entry]).await.unwrap();
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder
            .build_snapshot()
            .await
            .expect("build leader snapshot");
        let snapshot_bytes = snapshot.snapshot.clone().into_inner();

        // Follower: same namespace + `live`, PLUS a stale `stale` ConfigMap
        // that the leader has already deleted. This is the divergent member.
        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_dst
            .create_namespace(
                "snap-ns",
                serde_json::json!({"metadata": {"name": "snap-ns"}}),
            )
            .await
            .expect("create follower namespace");
        backend_dst
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "live",
                serde_json::json!({
                    "metadata": {"name": "live", "namespace": "snap-ns", "uid": "uid-live"}
                }),
            )
            .await
            .expect("create follower live resource");
        backend_dst
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "stale",
                serde_json::json!({
                    "metadata": {"name": "stale", "namespace": "snap-ns", "uid": "uid-stale"}
                }),
            )
            .await
            .expect("seed stale follower resource absent from leader snapshot");

        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install leader snapshot onto divergent follower");

        // The destination must now be key-identical to the leader: the stale
        // row must be GONE, not merged. Compare by identity + resourceVersion,
        // ignoring `creationTimestamp` (a server-set field that legitimately
        // differs microsecond-to-microsecond when the same object is created
        // independently on two in-memory backends during the test setup).
        let dst_fp = resource_identity_fingerprint(backend_dst.as_ref()).await;
        let leader_fp_id = resource_identity_fingerprint(backend_src.as_ref()).await;
        assert_eq!(
            dst_fp, leader_fp_id,
            "install_snapshot must replace (not merge) local state: the stale row must be removed so the follower's resource identity set matches the leader snapshot"
        );
        // Spot-check the stale row is truly gone.
        let stale = backend_dst
            .get_resource("v1", "ConfigMap", Some("snap-ns"), "stale")
            .await
            .expect("get stale resource");
        assert!(
            stale.is_none(),
            "stale row absent from the leader snapshot must be removed on install_snapshot; got {stale:?}"
        );
    }

    /// Identity + resourceVersion fingerprint, ignoring server-set volatile
    /// fields (`creationTimestamp`) that can legitimately differ when the same
    /// object is created independently on two backends during test setup.
    async fn resource_identity_fingerprint(
        backend: &dyn DatastoreBackend,
    ) -> Vec<(String, String, Option<String>, String, String, i64)> {
        let full = resource_fingerprint(backend).await;
        full.into_iter()
            .map(|(api_version, kind, namespace, name, uid, rv, mut data)| {
                // Drop creationTimestamp so the comparison is over stable
                // identity + version, not the moment a backend wrote the row.
                if let Some(obj) = data.as_object_mut() {
                    if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                        meta.remove("creationTimestamp");
                    }
                }
                let _ = data; // creationTimestamp removed; remaining body ignored for identity
                (api_version, kind, namespace, name, uid, rv)
            })
            .collect()
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_resources_and_rv_counter() {
        let backend_src: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(backend_src.as_ref()).await;
        backend_src
            .create_namespace(
                "snap-rich",
                serde_json::json!({
                    "metadata": {
                        "name": "snap-rich",
                        "uid": "uid-rich-ns",
                        "creationTimestamp": "2026-05-14T00:00:00Z"
                    },
                    "labels": {"purpose": "snapshot"}
                }),
            )
            .await
            .expect("create namespace");
        let cm = backend_src
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-rich"),
                "settings",
                serde_json::json!({
                    "metadata": {
                        "name": "settings",
                        "namespace": "snap-rich",
                        "uid": "uid-settings"
                    },
                    "data": {"mode": "initial"}
                }),
            )
            .await
            .expect("create configmap");
        backend_src
            .update_resource(
                "v1",
                "ConfigMap",
                Some("snap-rich"),
                "settings",
                serde_json::json!({
                    "metadata": {
                        "name": "settings",
                        "namespace": "snap-rich",
                        "uid": cm.uid
                    },
                    "data": {"mode": "updated"}
                }),
                cm.resource_version,
            )
            .await
            .expect("update configmap");
        backend_src
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-snap",
                serde_json::json!({
                    "metadata": {"name": "worker-snap", "uid": "uid-worker-snap"},
                    "status": {"conditions": []}
                }),
            )
            .await
            .expect("create node");
        let current_rv = backend_src
            .advance_resource_version_after(64)
            .await
            .expect("advance rv past last object");
        let leader_fingerprint = resource_fingerprint(backend_src.as_ref()).await;

        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        sm_src
            .apply(vec![Entry::<TypeConfig> {
                log_id: LogId::new(LeaderId::new(7, 10), current_rv as u64),
                payload: EntryPayload::Blank,
            }])
            .await
            .expect("advance last_applied");
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        let snapshot_bytes = snapshot.snapshot.into_inner();

        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install snapshot");

        assert_eq!(
            resource_fingerprint(backend_dst.as_ref()).await,
            leader_fingerprint,
            "snapshot install must preserve live Kubernetes resources exactly as read through the API boundary"
        );
        assert_eq!(
            backend_dst.get_current_resource_version().await.unwrap(),
            current_rv,
            "snapshot install must restore the leader RV counter even when it is ahead of object RVs"
        );
    }

    /// Regression: `build_snapshot` must read the leader `current_rv` AFTER
    /// streaming the snapshot commits, not before. Reading it before streaming
    /// opens a TOCTOU window — commits applied during the (many) streaming
    /// awaits land in the snapshot with a `resourceVersion` higher than the
    /// already-captured `current_rv`, producing an internally inconsistent
    /// snapshot that a follower rejects in `replace_resource_state_in_conn`
    /// ("snapshot entry resourceVersion N is ahead of leader current_rv M").
    /// A rejected `install_snapshot` permanently breaks raft catch-up, which
    /// stalls replication, drops quorum, and flips leadership under load.
    ///
    /// This test applies commits concurrently while a snapshot is built and
    /// asserts the snapshot is internally consistent and installs cleanly on a
    /// fresh follower. On a current-thread test runtime the spawned applier
    /// runs cooperatively at each of the builder's await points.
    #[tokio::test]
    async fn build_snapshot_current_rv_not_behind_commits_applied_during_build() {
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        seed_snapshot_identity(backend.as_ref()).await;
        backend
            .create_namespace(
                "race-ns",
                serde_json::json!({"metadata": {"name": "race-ns", "uid": "uid-race-ns"}}),
            )
            .await
            .expect("seed namespace");
        for i in 0..5 {
            backend
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("race-ns"),
                    &format!("seed-{i}"),
                    serde_json::json!({
                        "metadata": {
                            "name": format!("seed-{i}"),
                            "namespace": "race-ns",
                            "uid": format!("uid-seed-{i}")
                        },
                        "data": {"k": "v"}
                    }),
                )
                .await
                .expect("seed configmap");
        }

        let mut sm = build_sm_with_backend(backend.clone()).await;
        sm.apply(vec![Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(1, 10), 1),
            payload: EntryPayload::Blank,
        }])
        .await
        .expect("advance last_applied");
        let mut builder = sm.get_snapshot_builder().await;

        // Concurrently apply commits while the snapshot is being built. Each
        // create_resource awaits, yielding back to the builder so the two
        // interleaving tasks race exactly like leader writes during a snapshot.
        let bg = backend.clone();
        let race = tokio::spawn(async move {
            for i in 0..60u32 {
                let _snapshot_mutation =
                    crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(
                        bg.as_ref(),
                    )
                    .await
                    .unwrap()
                    .expect("SQLite supplies a snapshot mutation fence");
                let _ = bg
                    .create_resource(
                        "v1",
                        "ConfigMap",
                        Some("race-ns"),
                        &format!("race-{i}"),
                        serde_json::json!({
                            "metadata": {
                                "name": format!("race-{i}"),
                                "namespace": "race-ns",
                                "uid": format!("uid-race-{i}")
                            },
                            "data": {"k": "v"}
                        }),
                    )
                    .await;
                tokio::task::yield_now().await;
            }
        });

        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        race.await.expect("concurrent applier finished");
        let snapshot_bytes = snapshot.snapshot.into_inner();

        // The snapshot must install cleanly on a fresh follower: a follower
        // applying replace_replicated_resource_state must not reject it.
        let backend_dst: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm_dst = build_sm_with_backend(backend_dst).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install_snapshot must not reject an internally-consistent snapshot");
    }

    #[tokio::test]
    async fn get_current_snapshot_builds_fresh_snapshot_when_missing() {
        let mut sm = fresh_sm().await;
        let first = sm
            .get_current_snapshot()
            .await
            .expect("fresh bootstrap should return snapshot")
            .expect("snapshot should be present");
        assert!(
            first.meta.snapshot_id.starts_with("raft-snapshot"),
            "snapshot should carry a synthetic id, got {}",
            first.meta.snapshot_id
        );
        let first_payload = first.snapshot.get_ref().clone();
        assert!(
            !first_payload.is_empty(),
            "generated snapshot should be non-empty"
        );

        let second = sm
            .get_current_snapshot()
            .await
            .expect("cached snapshot lookup should succeed")
            .expect("snapshot should still be present");
        assert_eq!(
            second.snapshot.get_ref(),
            &first_payload,
            "subsequent reads should rebuild deterministic snapshot bytes"
        );
    }

    async fn resource_fingerprint(
        backend: &dyn DatastoreBackend,
    ) -> Vec<(
        String,
        String,
        Option<String>,
        String,
        String,
        i64,
        serde_json::Value,
    )> {
        let mut rows = Vec::new();
        let namespaces = backend
            .list_namespaces(None, None)
            .await
            .expect("list namespaces");
        for namespace in namespaces.items {
            rows.push(resource_fingerprint_row(namespace));
        }
        for namespace in backend
            .list_namespaces(None, None)
            .await
            .expect("list namespaces for namespaced resources")
            .items
        {
            for resource in backend
                .list_namespace_resources(&namespace.name)
                .await
                .expect("list namespace resources")
            {
                rows.push(resource_fingerprint_row(resource));
            }
        }
        for resource in backend
            .list_cluster_resources()
            .await
            .expect("list cluster resources")
        {
            rows.push(resource_fingerprint_row(resource));
        }
        rows.sort_by(|a, b| (&a.0, &a.1, &a.2, &a.3).cmp(&(&b.0, &b.1, &b.2, &b.3)));
        rows
    }

    fn resource_fingerprint_row(
        resource: klights_cluster_core::Resource,
    ) -> (
        String,
        String,
        Option<String>,
        String,
        String,
        i64,
        serde_json::Value,
    ) {
        (
            resource.api_version,
            resource.kind,
            resource.namespace,
            resource.name,
            resource.uid,
            resource.resource_version,
            crate::api::inject_resource_version(resource.data, resource.resource_version),
        )
    }

    #[tokio::test]
    async fn apply_normal_entry_decodes_log_apply_commit_and_mutates_backend() {
        // T1.3: state machine apply must decode EntryPayload::Normal as a
        // `LogApplyCommit` protobuf and run it through
        // `backend.apply_log_apply_commit`. After apply, the cluster.db
        // row produced by the PutResource mutation must be visible to a
        // `get_resource` read on the same backend.
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm = build_sm_with_backend(backend.clone()).await;

        let commit = crate::datastore::test_support::test_live_commit(
            1,
            vec![klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "from-raft".to_string(),
                    uid: "cm-uid-1".to_string(),
                    resource_version: 1,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "from-raft",
                            "namespace": "default",
                            "uid": "cm-uid-1",
                            "resourceVersion": "1"
                        },
                        "data": {"k": "v"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            )],
        );
        let payload_bytes = klights_replication::log_apply_wire::encode_commit_protobuf(&commit)
            .expect("encode LogApplyCommit");

        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 1),
            payload: EntryPayload::Normal(
                klights_replication::types::StorageCommandPayload::from_bytes(payload_bytes),
            ),
        };
        let results = sm
            .apply(vec![entry])
            .await
            .expect("apply EntryPayload::Normal LogApplyCommit");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].applied_rv,
            Some(1),
            "state machine must return the commit's resource_version as applied_rv"
        );
        let applied_resource = match results[0].applied_mutation.as_ref() {
            Some(klights_cluster_store::AppliedMutation::Resource(resource)) => resource,
            None => panic!("state machine must return the exact committed resource"),
        };
        assert_eq!(applied_resource.name, "from-raft");
        assert_eq!(applied_resource.uid, "cm-uid-1");
        assert_eq!(applied_resource.resource_version, 1);

        let row = backend
            .get_resource("v1", "ConfigMap", Some("default"), "from-raft")
            .await
            .expect("get_resource ok")
            .expect("PutResource mutation must materialize the row");
        assert_eq!(row.uid, "cm-uid-1");
    }

    #[tokio::test]
    async fn apply_empty_live_commit_preserves_public_resource_version() {
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend.advance_resource_version_after(41).await.unwrap();
        let before_rv = backend.get_current_resource_version().await.unwrap();
        let mut sm = build_sm_with_backend(backend.clone()).await;
        let commit = crate::datastore::test_support::test_live_commit(0, Vec::new());
        let payload_bytes = klights_replication::log_apply_wire::encode_commit_protobuf(&commit)
            .expect("encode empty LogApplyCommit");
        let results = sm
            .apply(vec![Entry::<TypeConfig> {
                log_id: LogId::new(LeaderId::new(3, 10), 42),
                payload: EntryPayload::Normal(
                    klights_replication::types::StorageCommandPayload::from_bytes(payload_bytes),
                ),
            }])
            .await
            .expect("apply empty live commit");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].applied_rv, Some(before_rv));
        assert!(
            !results[0].public_resource_changed,
            "an empty live commit must not allocate a public resourceVersion"
        );
        assert!(results[0].applied_mutation.is_none());
        assert_eq!(
            backend.get_current_resource_version().await.unwrap(),
            before_rv,
            "empty live commit must preserve the datastore public resourceVersion"
        );
    }

    #[tokio::test]
    async fn apply_normal_entry_stamps_provisional_rv_after_current_store_rv() {
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let snapshot_rv = backend
            .advance_resource_version_after(100)
            .await
            .expect("establish a list snapshot rv above the raft log index");

        let commit = crate::datastore::test_support::test_live_commit(
            0,
            vec![klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "provisional-rv".to_string(),
                    uid: "cm-uid-provisional".to_string(),
                    resource_version: 0,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "provisional-rv",
                            "namespace": "default",
                            "uid": "cm-uid-provisional"
                        },
                        "data": {"k": "v"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            )],
        );
        let payload_bytes = klights_replication::log_apply_wire::encode_commit_protobuf(&commit)
            .expect("encode LogApplyCommit");
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 42),
            payload: EntryPayload::Normal(
                klights_replication::types::StorageCommandPayload::from_bytes(payload_bytes),
            ),
        };

        let mut sm = build_sm_with_backend(backend.clone()).await;
        let results = sm
            .apply(vec![entry])
            .await
            .expect("apply provisional LogApplyCommit");
        assert_eq!(results[0].applied_rv, Some(snapshot_rv + 1));
        let row = backend
            .get_resource("v1", "ConfigMap", Some("default"), "provisional-rv")
            .await
            .expect("get_resource ok")
            .expect("PutResource mutation must materialize the row");
        assert_eq!(row.resource_version, snapshot_rv + 1);
        let expected_rv = (snapshot_rv + 1).to_string();
        assert_eq!(
            row.data
                .pointer("/metadata/resourceVersion")
                .and_then(|value| value.as_str()),
            Some(expected_rv.as_str())
        );
    }

    #[tokio::test]
    async fn follower_selector_list_positive_watch_receives_v1_ready_transition() {
        let backend: Arc<crate::datastore::sqlite::Datastore> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend
            .create_resource(
                "v1",
                "Pod",
                Some("guestbook"),
                "guestbook-0",
                serde_json::json!({"metadata":{"name":"guestbook-0","namespace":"guestbook","uid":"guestbook-uid","labels":{"app":"guestbook"}},"status":{"phase":"Pending"}}),
            )
            .await
            .unwrap();
        backend.advance_resource_version_after(100).await.unwrap();
        let list = backend
            .list_resources(
                "v1",
                "Pod",
                Some("guestbook"),
                crate::datastore::ResourceListQuery::new(Some("app=guestbook"), None, None, None),
            )
            .await
            .unwrap();
        assert_eq!(
            list.items[0]
                .data
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            Some("Pending")
        );

        let commit = crate::datastore::test_support::test_live_commit(
            0,
            vec![klights_cluster_core::LogApplyMutation::PutResource(
                klights_cluster_core::LogApplyResourceRow {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("guestbook".into()),
                    name: "guestbook-0".into(),
                    uid: "guestbook-uid".into(),
                    resource_version: 0,
                    data: serde_json::json!({"metadata":{"name":"guestbook-0","namespace":"guestbook","uid":"guestbook-uid","labels":{"app":"guestbook"}},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: None,
                    precondition_resource_version: Some(1),
                    status_only: true,
                },
            )],
        );
        let payload = klights_replication::log_apply_wire::encode_commit_protobuf(&commit).unwrap();
        let mut sm = build_sm_with_backend(backend.clone()).await;
        let result = sm
            .apply(vec![Entry::<TypeConfig> {
                log_id: LogId::new(LeaderId::new(3, 10), 42),
                payload: EntryPayload::Normal(
                    klights_replication::types::StorageCommandPayload::from_bytes(payload),
                ),
            }])
            .await
            .unwrap();
        assert!(
            result[0].public_resource_changed,
            "a newly visible committed Pod status update must request downstream effects"
        );
        assert_eq!(
            result[0].pod_endpoint_effect(),
            klights_cluster_core::PodEndpointEffect::Changed,
            "real committed-Raft apply must carry the transaction-derived endpoint effect"
        );
        let applied_resource = match result[0].applied_mutation.as_ref() {
            Some(klights_cluster_store::AppliedMutation::Resource(resource)) => resource,
            None => panic!("visible status update must return its exact committed resource"),
        };
        assert_eq!(applied_resource.name, "guestbook-0");
        assert_eq!(
            applied_resource
                .data
                .pointer("/status/conditions/0/status")
                .and_then(|value| value.as_str()),
            Some("True")
        );
        let ready_rv = result[0].applied_rv.unwrap();
        assert!(ready_rv > list.resource_version);
        let events = backend
            .list_resources_modified_since("v1", "Pod", Some("guestbook"), list.resource_version)
            .await
            .unwrap();
        assert!(events.iter().any(|event| {
            event.event_type.as_ref() == "MODIFIED"
                && event
                    .resource
                    .data
                    .pointer("/status/conditions/0/status")
                    .and_then(|v| v.as_str())
                    == Some("True")
        }));
    }
}
