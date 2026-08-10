use super::*;

#[tokio::test]
async fn no_op_applied_outbox_gc_does_not_allocate_local_raft_rv() {
    let inner: crate::datastore::backend::DatastoreHandle = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
        remaining_keys.contains(&"recent-legacy-row"),
        "recent legacy row should survive GC"
    );
    assert!(
        !remaining_keys.contains(&"old-legacy-row"),
        "stale legacy row should be removed by raft-applied GC"
    );
    assert!(
        remaining
            .iter()
            .all(|row| row.subject_key != "GcAppliedOutbox"),
        "leader-authored GC is a regular raft proposal and must not fabricate an outbox ledger row"
    );
}

#[tokio::test]
async fn lease_renew_outbox_does_not_route_through_proposer() {
    let (ds, calls) = make_ds_with_inline_proposer().await;
    let inner = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
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
    let payload =
        crate::bootstrap::composition_tests::support::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

    let result = ds
        .apply_outbox_transactionally(
            "lease-renew-key",
            klights_kubelet::node_outbox::payload::OutboxOperation::LeaseRenew.as_str(),
            klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
            "worker-1",
        )
        .await
        .unwrap();
    let klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv } = result else {
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
    let payload =
        crate::bootstrap::composition_tests::support::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

    let result = ds
        .apply_outbox_transactionally(
            "create-from-outbox-key",
            klights_kubelet::node_outbox::payload::OutboxOperation::NodeRegistration.as_str(),
            klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
            "worker-1",
        )
        .await
        .unwrap();
    let klights_cluster_core::OutboxApplyOutcome::Applied { .. } = result else {
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
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
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
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            self.calls.lock().unwrap().push(command.clone());
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
                .await?;
            Ok(outcome.into_parts().0)
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
    let ds = SequencedDatastore::new(inner.clone(), proposer.clone());

    let payload = crate::bootstrap::composition_tests::support::OutboxPayload::from_command(
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
            klights_kubelet::node_outbox::payload::OutboxOperation::PodStatus.as_str(),
            klights_leader_rpc::storage_wire_codec::test_outbox_command(&payload),
            "worker-1",
        )
        .await
        .expect("apply_outbox via proposer");
    let klights_cluster_core::OutboxApplyOutcome::Applied { .. } = result else {
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

#[tokio::test]
async fn raft_mode_follower_proposer_rejects_outbox_apply_no_local_mutation() {
    let (ds, inner) = make_ds_with_follower_proposer().await;
    let payload = crate::bootstrap::composition_tests::support::OutboxPayload::from_command(
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
        matches!(err, klights_cluster_core::OutboxApplyError::Retryable(_)),
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
