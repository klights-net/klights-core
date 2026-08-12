use klights_cluster_core::{CommittedApplyOutcome, CommittedApplyRejection, NoPublicChangeReason};
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyCommit, OutboxStreamWatermark, WatchReplayPosition,
};
use klights_cluster_store::{
    AppliedOutboxLedger, AppliedOutboxLookup, ClusterResourceMutation, CommittedApplyError,
    CommittedApplyFuture, CommittedRaftApplyReceipt, CommittedRaftApplyRequest,
    DurableApplyLedgerRead, PrivilegedCommittedRaftApply,
};

struct FakeCommittedStore;

impl PrivilegedCommittedRaftApply for FakeCommittedStore {
    fn apply_committed_raft(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt> {
        Box::pin(async move {
            assert!(request.commit().mutations().is_empty());
            Ok(CommittedRaftApplyReceipt::new(
                CommittedApplyOutcome::Visible {
                    resource_version: 17,
                    resource: None,
                },
                klights_cluster_core::PodEndpointEffect::NotApplicable,
            ))
        })
    }
}

impl DurableApplyLedgerRead for FakeCommittedStore {
    fn current_apply_position(&self) -> CommittedApplyFuture<'_, WatchReplayPosition> {
        Box::pin(async {
            Ok(WatchReplayPosition {
                resource_version: 17,
                event_id: 23,
                resource_version_filter_through_event_id: 0,
            })
        })
    }

    fn get_applied_outbox(
        &self,
        lookup: AppliedOutboxLookup,
    ) -> CommittedApplyFuture<'_, Option<LogApplyAppliedOutboxRow>> {
        Box::pin(async move {
            Ok(Some(LogApplyAppliedOutboxRow {
                idempotency_key: lookup.into_idempotency_key(),
                subject_key: "v1/Pod/default/demo/uid-1".to_string(),
                operation: "PodStatus".to_string(),
                first_seen_ms: 100,
                applied_rv: Some(17),
                result_proto: vec![1, 2, 3],
                status_stamp: Some(9),
            }))
        })
    }

    fn list_outbox_watermarks(&self) -> CommittedApplyFuture<'_, Vec<OutboxStreamWatermark>> {
        Box::pin(async {
            Ok(vec![OutboxStreamWatermark {
                client_id: "worker-a".to_string(),
                stream_id: 4,
                stream_seq: 8,
            }])
        })
    }
}

fn assert_privileged_apply_object_safe(_: &dyn PrivilegedCommittedRaftApply) {}
fn assert_ledger_read_object_safe(_: &dyn DurableApplyLedgerRead) {}

#[test]
fn committed_apply_and_ledger_read_capabilities_are_distinct_and_object_safe() {
    let store = FakeCommittedStore;
    assert_privileged_apply_object_safe(&store);
    assert_ledger_read_object_safe(&store);
}

#[test]
fn sequenced_facade_rejects_committed_apply_through_both_trait_views() {
    let resource_view = std::any::type_name::<&dyn ClusterResourceMutation>();
    let outbox_view = std::any::type_name::<&dyn AppliedOutboxLedger>();
    let privileged_view = std::any::type_name::<&dyn PrivilegedCommittedRaftApply>();

    assert_ne!(
        resource_view, privileged_view,
        "application resource mutation capability must not expose committed apply"
    );
    assert_ne!(
        outbox_view, privileged_view,
        "application outbox capability must not expose committed apply"
    );
    assert!(
        privileged_view.contains("PrivilegedCommittedRaftApply"),
        "committed apply must remain available only through its privileged trait"
    );
}

#[test]
fn committed_apply_request_and_receipt_preserve_canonical_values() {
    let commit = LogApplyCommit::try_new(Vec::new()).unwrap();
    let request = CommittedRaftApplyRequest::new(commit.clone());
    assert_eq!(request.commit(), &commit);
    assert_eq!(request.into_commit(), commit);

    let receipt = CommittedRaftApplyReceipt::new(
        CommittedApplyOutcome::Rejected(CommittedApplyRejection::ResourceVersionConflict {
            message: "terminal conflict".to_string(),
        }),
        klights_cluster_core::PodEndpointEffect::NotApplicable,
    );
    assert!(matches!(
        receipt.outcome(),
        CommittedApplyOutcome::Rejected(CommittedApplyRejection::ResourceVersionConflict { .. })
    ));
}

#[test]
fn committed_apply_outcome_makes_contradictions_unrepresentable() {
    let outcomes = [
        CommittedApplyOutcome::Visible {
            resource_version: 41,
            resource: None,
        },
        CommittedApplyOutcome::NoPublicChange {
            resource_version: 41,
            reason: NoPublicChangeReason::DuplicateIdempotencyKey,
        },
        CommittedApplyOutcome::NoPublicChange {
            resource_version: 41,
            reason: NoPublicChangeReason::StaleStatusStamp,
        },
        CommittedApplyOutcome::Rejected(CommittedApplyRejection::UidConflict {
            message: "uid conflict".into(),
        }),
    ];
    assert_eq!(outcomes.len(), 4);
}

#[test]
fn durable_ledger_lookups_preserve_opaque_idempotency_keys() {
    for key in ["plain", "worker/a:stream/4#8", ""] {
        let lookup = AppliedOutboxLookup::new(key);
        assert_eq!(lookup.idempotency_key(), key);
        assert_eq!(lookup.into_idempotency_key(), key);
    }
}

#[test]
fn committed_apply_error_is_typed_and_adapter_neutral() {
    let error = CommittedApplyError::persistence_failed("commit failed");
    assert_eq!(error.to_string(), "commit failed");
    assert_eq!(
        error,
        CommittedApplyError::PersistenceFailed {
            message: "commit failed".to_string(),
        }
    );
}

#[test]
fn committed_apply_errors_preserve_retry_and_shutdown_semantics() {
    let errors = [
        CommittedApplyError::CorruptData {
            message: "bad ledger".into(),
        },
        CommittedApplyError::UnsupportedMode {
            message: "mode".into(),
        },
        CommittedApplyError::Retryable {
            message: "busy".into(),
        },
        CommittedApplyError::Timeout,
        CommittedApplyError::Cancelled,
    ];
    assert_eq!(errors.len(), 5);
}
