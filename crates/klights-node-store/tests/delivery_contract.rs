use klights_node_store::{
    DeadLetterEntry, DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest,
    DeadLetterStore, DeliveryError, DeliveryFuture, OUTBOX_DIAGNOSTIC_AGING_MS,
    OutboxAttemptFailure, OutboxBatchClaimRequest, OutboxClaimRequest, OutboxClassification,
    OutboxCompletion, OutboxDispatcherStore, OutboxEnqueue, OutboxLease, OutboxNow, OutboxPriority,
    OutboxProducerStore, OutboxRecord, OutboxSequence, OutboxSequencePolicy, OutboxStats,
    OutboxSubject, OutboxSupersedability, OutboxSupersedeRequest, PodCheckpointKey,
    PodStatusCheckpoint, PodStatusCheckpointApplied, PodStatusCheckpointStore,
    PodStatusCheckpointUpsert, RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration, TerminalDeleteClassification,
};
use klights_types::{PodIdentity, ResourceKey};

struct EmptyDeliveryStore;

impl OutboxProducerStore for EmptyDeliveryStore {
    fn enqueue_outbox(&self, _entry: OutboxEnqueue) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl OutboxDispatcherStore for EmptyDeliveryStore {
    fn claim_next_due_outbox(
        &self,
        _request: OutboxClaimRequest,
    ) -> DeliveryFuture<'_, Option<OutboxRecord>> {
        Box::pin(async { Ok(None) })
    }

    fn renew_outbox_lease(&self, _lease: OutboxLease) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn mark_outbox_attempt_failed(
        &self,
        _failure: OutboxAttemptFailure,
    ) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn complete_outbox(&self, _completion: OutboxCompletion) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn requeue_expired_outbox_leases(&self, _now: OutboxNow) -> DeliveryFuture<'_, usize> {
        Box::pin(async { Ok(0) })
    }

    fn next_outbox_wake_ms(&self, _now: OutboxNow) -> DeliveryFuture<'_, Option<i64>> {
        Box::pin(async { Ok(None) })
    }

    fn claim_due_outbox_batch(
        &self,
        _request: OutboxBatchClaimRequest,
    ) -> DeliveryFuture<'_, Vec<OutboxRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        _request: OutboxSupersedeRequest,
    ) -> DeliveryFuture<'_, usize> {
        Box::pin(async { Ok(0) })
    }
}

impl DeadLetterStore for EmptyDeliveryStore {
    fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        _request: DeadLetterMoveRequest,
    ) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn list_dead_letter(&self) -> DeliveryFuture<'_, Vec<DeadLetterEntry>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get_dead_letter(&self, _key: DeadLetterKey) -> DeliveryFuture<'_, Option<DeadLetterEntry>> {
        Box::pin(async { Ok(None) })
    }

    fn delete_dead_letter(&self, _key: DeadLetterKey) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn replay_dead_letter(&self, _request: DeadLetterReplayRequest) -> DeliveryFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn outbox_stats(&self) -> DeliveryFuture<'_, OutboxStats> {
        Box::pin(async { Ok(OutboxStats::try_new(0, 0.0, 0, 0, 0).unwrap()) })
    }
}

impl PodStatusCheckpointStore for EmptyDeliveryStore {
    fn upsert_pod_status_checkpoint(
        &self,
        _checkpoint: PodStatusCheckpointUpsert,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_pod_status_checkpoint(
        &self,
        _key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<PodStatusCheckpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn mark_pod_status_checkpoint_applied(
        &self,
        _applied: PodStatusCheckpointApplied,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete_pod_status_checkpoint(&self, _key: PodCheckpointKey) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl RuntimeObservationCheckpointStore for EmptyDeliveryStore {
    fn upsert_runtime_observation_checkpoint(
        &self,
        _checkpoint: RuntimeObservationCheckpoint,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_runtime_observation_checkpoint(
        &self,
        _key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<RuntimeObservationCheckpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn delete_runtime_observation_checkpoint(
        &self,
        _key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn assert_outbox_producer_object_safe(_: &dyn OutboxProducerStore) {}
fn assert_outbox_dispatcher_object_safe(_: &dyn OutboxDispatcherStore) {}
fn assert_dead_letter_object_safe(_: &dyn DeadLetterStore) {}
fn assert_pod_checkpoint_object_safe(_: &dyn PodStatusCheckpointStore) {}
fn assert_runtime_checkpoint_object_safe(_: &dyn RuntimeObservationCheckpointStore) {}

#[test]
fn delivery_capabilities_are_independently_object_safe() {
    let store = EmptyDeliveryStore;
    assert_outbox_producer_object_safe(&store);
    assert_outbox_dispatcher_object_safe(&store);
    assert_dead_letter_object_safe(&store);
    assert_pod_checkpoint_object_safe(&store);
    assert_runtime_checkpoint_object_safe(&store);
}

fn subject() -> OutboxSubject {
    OutboxSubject::new(
        "group/version/kind//raw-name/uid/raw",
        ResourceKey::new("group/version", "kind/raw", Some(String::new()), "raw-name"),
        Some("uid/raw".to_string()),
        "pod-uid/raw",
    )
}

fn workload_classification() -> OutboxClassification {
    OutboxClassification::try_new(
        OutboxPriority::Workload,
        OutboxSupersedability::PodStatus,
        TerminalDeleteClassification::NotTerminalDelete,
        OutboxSequencePolicy::PerSubject,
    )
    .unwrap()
}

fn terminal_delete_classification() -> OutboxClassification {
    OutboxClassification::try_new(
        OutboxPriority::Workload,
        OutboxSupersedability::Never,
        TerminalDeleteClassification::ActorOwnedPodDelete,
        OutboxSequencePolicy::PerSubject,
    )
    .unwrap()
}

#[test]
fn enqueue_preserves_opaque_identity_idempotency_and_payload_bytes() {
    let entry = OutboxEnqueue::try_new(
        "worker/a:stream/4#8",
        101,
        subject(),
        "Operation/Owned/Elsewhere",
        workload_classification(),
        vec![0, 255, 1, 0, 128],
        109,
    )
    .unwrap();

    assert_eq!(entry.idempotency_key(), "worker/a:stream/4#8");
    assert_eq!(entry.enqueued_ms(), 101);
    assert_eq!(
        entry.subject().subject_key(),
        "group/version/kind//raw-name/uid/raw"
    );
    assert_eq!(entry.subject().resource().api_version, "group/version");
    assert_eq!(entry.subject().resource().kind, "kind/raw");
    assert_eq!(entry.subject().resource().namespace.as_deref(), Some(""));
    assert_eq!(entry.subject().resource().name, "raw-name");
    assert_eq!(entry.subject().subject_uid(), Some("uid/raw"));
    assert_eq!(entry.subject().pod_uid(), "pod-uid/raw");
    assert_eq!(entry.operation(), "Operation/Owned/Elsewhere");
    assert_eq!(entry.classification(), workload_classification());
    assert_eq!(entry.payload(), &[0, 255, 1, 0, 128]);
    assert_eq!(entry.next_due_ms(), 109);
}

#[test]
fn claimed_row_preserves_stream_lease_and_retry_state_exactly() {
    let row = OutboxRecord::try_new(
        41,
        "client/raw",
        "idempotency/raw",
        101,
        subject(),
        "PodStatus",
        terminal_delete_classification(),
        OutboxSequence::try_new(9_223_372_036_854_775_000, 77).unwrap(),
        vec![9, 8, 7],
        3,
        120,
        130,
        Some("lease/raw".to_string()),
        Some("last/raw".to_string()),
    )
    .unwrap();

    assert_eq!(row.id(), 41);
    assert_eq!(row.client_id(), "client/raw");
    assert_eq!(row.idempotency_key(), "idempotency/raw");
    assert_eq!(row.enqueued_ms(), 101);
    assert_eq!(row.subject(), &subject());
    assert_eq!(row.operation(), "PodStatus");
    assert_eq!(row.classification(), terminal_delete_classification());
    assert_eq!(row.sequence().stream_id(), 9_223_372_036_854_775_000);
    assert_eq!(row.sequence().stream_seq(), 77);
    assert_eq!(row.payload(), &[9, 8, 7]);
    assert_eq!(row.attempt(), 3);
    assert_eq!(row.next_due_ms(), 120);
    assert_eq!(row.leased_until_ms(), 130);
    assert_eq!(row.lease_token(), Some("lease/raw"));
    assert_eq!(row.last_error(), Some("last/raw"));
}

#[test]
fn claim_lease_failure_completion_and_supersede_requests_are_exact() {
    let claim = OutboxClaimRequest::try_new(100, 9_999, "lease-token").unwrap();
    assert_eq!(claim.now_ms(), 100);
    assert_eq!(claim.lease_ms(), 9_999);
    assert_eq!(claim.lease_token(), "lease-token");

    let batch = OutboxBatchClaimRequest::try_new(101, 256, 7_777, "batch-token").unwrap();
    assert_eq!(batch.now_ms(), 101);
    assert_eq!(batch.limit(), 256);
    assert_eq!(batch.effective_limit(), 256);
    assert_eq!(batch.lease_ms(), 7_777);
    assert_eq!(batch.lease_token(), "batch-token");

    let hostile_but_supported_limit =
        OutboxBatchClaimRequest::try_new(101, usize::MAX, 7_777, "batch-token").unwrap();
    assert_eq!(hostile_but_supported_limit.limit(), usize::MAX);
    assert_eq!(hostile_but_supported_limit.effective_limit(), 256);

    let lease = OutboxLease::try_new(41, "renew-token", 999).unwrap();
    assert_eq!(lease.id(), 41);
    assert_eq!(lease.lease_token(), "renew-token");
    assert_eq!(lease.leased_until_ms(), 999);

    let failure = OutboxAttemptFailure::try_new(42, "failure-token", 1_234, "").unwrap();
    assert_eq!(failure.id(), 42);
    assert_eq!(failure.lease_token(), "failure-token");
    assert_eq!(failure.backoff_until_ms(), 1_234);
    assert_eq!(failure.error(), "");

    let completion = OutboxCompletion::try_new(43, "complete-token").unwrap();
    assert_eq!(completion.id(), 43);
    assert_eq!(completion.lease_token(), "complete-token");

    let supersede = OutboxSupersedeRequest::try_new("subject/raw", 44).unwrap();
    assert_eq!(supersede.subject_key(), "subject/raw");
    assert_eq!(supersede.terminal_delete_id(), 44);
}

#[test]
fn dead_letter_values_preserve_original_delivery_identity() {
    let move_request = DeadLetterMoveRequest::try_new("idempotency/raw", 0).unwrap();
    assert_eq!(move_request.idempotency_key(), "idempotency/raw");
    assert_eq!(move_request.max_attempts(), 0);

    let entry = DeadLetterEntry::try_new(
        8,
        41,
        "idempotency/raw",
        101,
        subject(),
        "RuntimeReconcile",
        workload_classification(),
        OutboxSequence::try_new(99, 7).unwrap(),
        vec![0, 1, 0, 255],
        17,
        "terminal/raw",
        202,
    )
    .unwrap();
    assert_eq!(entry.id(), 8);
    assert_eq!(entry.original_id(), 41);
    assert_eq!(entry.idempotency_key(), "idempotency/raw");
    assert_eq!(entry.enqueued_ms(), 101);
    assert_eq!(entry.subject(), &subject());
    assert_eq!(entry.operation(), "RuntimeReconcile");
    assert_eq!(entry.classification(), workload_classification());
    assert_eq!(entry.sequence(), OutboxSequence::try_new(99, 7).unwrap());
    assert_eq!(entry.payload(), &[0, 1, 0, 255]);
    assert_eq!(entry.attempts(), 17);
    assert_eq!(entry.last_error(), "terminal/raw");
    assert_eq!(entry.moved_at_ms(), 202);

    let stats = OutboxStats::try_new(11, 1.25, 2, 101, 7).unwrap();
    assert_eq!(stats.pending(), 11);
    assert_eq!(stats.oldest_age_seconds(), 1.25);
    assert_eq!(stats.dead_letter_count(), 2);
    assert_eq!(stats.dispatch_total(), 101);
    assert_eq!(stats.dispatch_errors_total(), 7);

    let key = DeadLetterKey::try_new(8).unwrap();
    let replay = DeadLetterReplayRequest::new(key);
    assert_eq!(replay.key().get(), 8);
}

#[test]
fn delivery_classification_is_typed_and_payload_independent() {
    let classes = [
        (OutboxPriority::Lease, 0),
        (OutboxPriority::NodeHealth, 1),
        (OutboxPriority::Workload, 2),
        (OutboxPriority::Diagnostic, 3),
    ];
    for (priority, persisted) in classes {
        assert_eq!(priority.persisted_value(), persisted);
    }
    assert_eq!(OutboxSupersedability::PodStatus.persisted_value(), 1);
    assert_eq!(
        TerminalDeleteClassification::ActorOwnedPodDelete.persisted_value(),
        1
    );
    assert_eq!(OutboxSequencePolicy::PerSubject.persisted_value(), 1);
    assert_eq!(workload_classification().persisted_values(), (2, 1, 0, 1));
    assert_eq!(
        OutboxClassification::try_from_persisted(2, 1, 0, 1).unwrap(),
        workload_classification()
    );
    assert_eq!(OUTBOX_DIAGNOSTIC_AGING_MS, 30_000);

    let opaque = OutboxEnqueue::try_new(
        "terminal-delete",
        7,
        subject(),
        "opaque-operation",
        terminal_delete_classification(),
        vec![0xff, 0x00, 0x80],
        7,
    )
    .unwrap();
    assert_eq!(
        opaque.classification().terminal_delete(),
        TerminalDeleteClassification::ActorOwnedPodDelete
    );
    assert_eq!(
        opaque.classification().supersedability(),
        OutboxSupersedability::Never
    );
    assert_eq!(
        opaque.classification().sequence_policy(),
        OutboxSequencePolicy::PerSubject
    );
    assert_eq!(opaque.payload(), [0xff, 0x00, 0x80]);
    assert_eq!(
        OutboxSequence::try_new(99, 0).unwrap().stream_seq(),
        0,
        "a persisted stream may await first-claim sequence assignment"
    );
}

#[test]
fn pod_status_checkpoint_preserves_uid_identity_and_opaque_status_bytes() {
    let identity = PodIdentity::new("namespace/raw", "pod/raw", "uid/raw");
    let payload = br#"{ "opaque" : [0, false, null] }"#.to_vec();
    let upsert =
        PodStatusCheckpointUpsert::try_new(identity.clone(), 0, payload.clone(), 101).unwrap();
    assert_eq!(upsert.pod(), &identity);
    assert_eq!(upsert.base_position(), 0);
    assert_eq!(upsert.status_payload(), payload);
    assert_eq!(upsert.updated_ms(), 101);

    let checkpoint =
        PodStatusCheckpoint::try_new(identity.clone(), 17, Some(19), payload.clone(), 102).unwrap();
    assert_eq!(checkpoint.pod(), &identity);
    assert_eq!(checkpoint.base_position(), 17);
    assert_eq!(checkpoint.applied_position(), Some(19));
    assert_eq!(checkpoint.status_payload(), payload);
    assert_eq!(checkpoint.updated_ms(), 102);

    let applied = PodStatusCheckpointApplied::try_new("uid/raw", 23, 103).unwrap();
    assert_eq!(applied.pod_uid(), "uid/raw");
    assert_eq!(applied.applied_position(), 23);
    assert_eq!(applied.updated_ms(), 103);

    let key = PodCheckpointKey::try_new("uid/raw").unwrap();
    assert_eq!(key.pod_uid(), "uid/raw");
}

#[test]
fn runtime_observation_checkpoint_preserves_order_duplicates_and_generation() {
    let checkpoint = RuntimeObservationCheckpoint::try_new(
        "uid/raw",
        vec![
            "container/b".into(),
            "container/a".into(),
            "container/b".into(),
        ],
        RuntimeObservationGeneration::try_from(i64::MAX).unwrap(),
        501,
    )
    .unwrap();
    assert_eq!(checkpoint.pod_uid(), "uid/raw");
    assert_eq!(
        checkpoint.container_ids(),
        ["container/b", "container/a", "container/b"]
    );
    assert_eq!(checkpoint.generation().get(), i64::MAX);
    assert_eq!(checkpoint.updated_ms(), 501);
}

#[test]
fn hostile_or_structurally_invalid_values_are_rejected_without_normalization() {
    let invalid = [
        OutboxEnqueue::try_new(
            "",
            0,
            subject(),
            "PodStatus",
            workload_classification(),
            vec![],
            0,
        )
        .unwrap_err(),
        OutboxEnqueue::try_new(
            "key",
            -1,
            subject(),
            "PodStatus",
            workload_classification(),
            vec![],
            0,
        )
        .unwrap_err(),
        OutboxEnqueue::try_new(
            "key",
            0,
            subject(),
            "",
            workload_classification(),
            vec![],
            0,
        )
        .unwrap_err(),
        OutboxClaimRequest::try_new(-1, 1, "token").unwrap_err(),
        OutboxClaimRequest::try_new(0, 0, "token").unwrap_err(),
        OutboxClaimRequest::try_new(0, 1, "").unwrap_err(),
        OutboxBatchClaimRequest::try_new(-1, 1, 1, "token").unwrap_err(),
        OutboxBatchClaimRequest::try_new(0, usize::MAX, -1, "token").unwrap_err(),
        OutboxLease::try_new(0, "token", 1).unwrap_err(),
        OutboxLease::try_new(1, "", 1).unwrap_err(),
        OutboxLease::try_new(1, "token", 0).unwrap_err(),
        OutboxAttemptFailure::try_new(-1, "token", 1, "error").unwrap_err(),
        OutboxCompletion::try_new(1, "").unwrap_err(),
        OutboxSupersedeRequest::try_new("", 1).unwrap_err(),
        DeadLetterMoveRequest::try_new("", 1).unwrap_err(),
        DeadLetterMoveRequest::try_new("key", -1).unwrap_err(),
        DeadLetterKey::try_new(0).unwrap_err(),
        OutboxNow::try_new(-1).unwrap_err(),
        OutboxSequence::try_new(0, 1).unwrap_err(),
        OutboxSequence::try_new(-1, 0).unwrap_err(),
        OutboxClassification::try_from_persisted(4, 0, 0, 0).unwrap_err(),
        OutboxClassification::try_new(
            OutboxPriority::Workload,
            OutboxSupersedability::PodStatus,
            TerminalDeleteClassification::ActorOwnedPodDelete,
            OutboxSequencePolicy::PerSubject,
        )
        .unwrap_err(),
        PodCheckpointKey::try_new("").unwrap_err(),
        PodStatusCheckpointUpsert::try_new(PodIdentity::new("ns", "pod", "uid"), -1, vec![], 0)
            .unwrap_err(),
        PodStatusCheckpointApplied::try_new("uid", -1, 0).unwrap_err(),
        RuntimeObservationGeneration::try_from(-1_i64).unwrap_err(),
        RuntimeObservationGeneration::try_from((i64::MAX as u64) + 1).unwrap_err(),
        RuntimeObservationGeneration::try_new(i128::from(i64::MAX) + 1).unwrap_err(),
        RuntimeObservationCheckpoint::try_new(
            "",
            vec![],
            RuntimeObservationGeneration::try_from(0_i64).unwrap(),
            0,
        )
        .unwrap_err(),
        OutboxStats::try_new(-1, 0.0, 0, 0, 0).unwrap_err(),
        OutboxStats::try_new(0, f64::NAN, 0, 0, 0).unwrap_err(),
        OutboxStats::try_new(0, -0.1, 0, 0, 0).unwrap_err(),
    ];
    assert!(
        invalid
            .iter()
            .all(|error| matches!(error, DeliveryError::InvalidInput { .. }))
    );
}

#[test]
fn delivery_errors_keep_retry_timeout_and_cancellation_distinct() {
    let errors = [
        DeliveryError::PersistenceFailed {
            message: "disk".into(),
        },
        DeliveryError::CorruptData {
            message: "row".into(),
        },
        DeliveryError::UnsupportedMode {
            message: "backend".into(),
        },
        DeliveryError::Retryable {
            message: "busy".into(),
        },
        DeliveryError::Timeout,
        DeliveryError::Cancelled,
    ];
    assert_eq!(errors.len(), 6);
    assert!(matches!(errors[3], DeliveryError::Retryable { .. }));
    assert_eq!(errors[4].to_string(), "node delivery persistence timed out");
    assert_eq!(
        errors[5].to_string(),
        "node delivery persistence was cancelled"
    );
    assert_ne!(errors[4], errors[5]);
}

#[derive(Clone)]
struct ReferenceRow {
    id: i64,
    subject: String,
    stream_id: i64,
    priority: OutboxPriority,
    supersedable_status: bool,
    enqueued_ms: i64,
    due_ms: i64,
    lease: Option<(String, i64)>,
    complete: bool,
}

#[derive(Default)]
struct ReferenceOutbox {
    rows: Vec<ReferenceRow>,
}

impl ReferenceOutbox {
    fn claim(&mut self, now_ms: i64, limit: usize, lease_token: &str, lease_ms: i64) -> Vec<i64> {
        let mut eligible: Vec<usize> = (0..self.rows.len())
            .filter(|&index| {
                let candidate = &self.rows[index];
                !candidate.complete
                    && candidate.due_ms <= now_ms
                    && candidate
                        .lease
                        .as_ref()
                        .is_none_or(|(_, leased_until_ms)| *leased_until_ms <= now_ms)
                    && !self.rows.iter().any(|older| {
                        !older.complete
                            && older.id < candidate.id
                            && (older.subject == candidate.subject
                                || (candidate.stream_id > 0
                                    && older.stream_id == candidate.stream_id))
                    })
            })
            .collect();
        let has_supersedable_status = eligible
            .iter()
            .any(|&index| self.rows[index].supersedable_status);
        eligible.sort_by_key(|&index| {
            let row = &self.rows[index];
            let priority = if row.priority == OutboxPriority::Diagnostic
                && (row.enqueued_ms <= now_ms - OUTBOX_DIAGNOSTIC_AGING_MS
                    || !has_supersedable_status)
            {
                OutboxPriority::Workload.persisted_value()
            } else {
                row.priority.persisted_value()
            };
            (priority, row.enqueued_ms, row.id)
        });
        eligible.truncate(limit.min(256));

        eligible
            .into_iter()
            .map(|index| {
                let row = &mut self.rows[index];
                row.lease = Some((lease_token.to_owned(), now_ms.saturating_add(lease_ms)));
                row.id
            })
            .collect()
    }

    fn complete(&mut self, id: i64, lease_token: &str) -> bool {
        let Some(row) = self.rows.iter_mut().find(|row| row.id == id) else {
            return false;
        };
        if row
            .lease
            .as_ref()
            .is_none_or(|(token, _)| token != lease_token)
        {
            return false;
        }
        row.complete = true;
        row.lease = None;
        true
    }
}

fn reference_row(
    id: i64,
    subject: impl Into<String>,
    stream_id: i64,
    priority: OutboxPriority,
) -> ReferenceRow {
    ReferenceRow {
        id,
        subject: subject.into(),
        stream_id,
        priority,
        supersedable_status: false,
        enqueued_ms: id,
        due_ms: 0,
        lease: None,
        complete: false,
    }
}

#[test]
fn reference_claim_contract_is_fifo_stream_ordered_and_priority_first() {
    let mut status = reference_row(1, "pod-a", 10, OutboxPriority::Workload);
    status.supersedable_status = true;
    let mut outbox = ReferenceOutbox {
        rows: vec![
            status,
            reference_row(2, "pod-a", 10, OutboxPriority::Lease),
            reference_row(3, "pod-b", 10, OutboxPriority::NodeHealth),
            reference_row(4, "pod-c", 20, OutboxPriority::Diagnostic),
            reference_row(5, "pod-d", 30, OutboxPriority::Lease),
        ],
    };

    assert_eq!(outbox.claim(100, 256, "claimer-a", 10), [5, 1, 4]);
    assert!(outbox.complete(1, "claimer-a"));
    assert_eq!(outbox.claim(101, 256, "claimer-b", 10), [2]);
    assert!(outbox.complete(2, "claimer-b"));
    assert_eq!(outbox.claim(102, 256, "claimer-c", 10), [3]);
}

#[test]
fn reference_batch_contract_caps_zero_exact_and_oversized_limits() {
    for (limit, expected) in [(0, 0), (256, 256), (257, 256), (usize::MAX, 256)] {
        let mut outbox = ReferenceOutbox {
            rows: (1..=300)
                .map(|id| reference_row(id, format!("subject-{id}"), id, OutboxPriority::Workload))
                .collect(),
        };
        assert_eq!(outbox.claim(10_000, limit, "batch", 10).len(), expected);
    }
}

#[test]
fn reference_leases_expire_and_cas_tokens_isolate_independent_claimers() {
    let mut outbox = ReferenceOutbox {
        rows: vec![
            reference_row(1, "pod-a", 10, OutboxPriority::Workload),
            reference_row(2, "pod-a", 10, OutboxPriority::Workload),
            reference_row(3, "pod-b", 20, OutboxPriority::Workload),
        ],
    };

    assert_eq!(outbox.claim(100, 1, "claimer-a", 10), [1]);
    assert_eq!(outbox.claim(101, 2, "claimer-b", 10), [3]);
    assert!(!outbox.complete(1, "wrong-token"));
    assert!(!outbox.complete(3, "claimer-a"));
    assert!(outbox.complete(3, "claimer-b"));
    assert_eq!(outbox.claim(109, 2, "claimer-c", 10), Vec::<i64>::new());
    assert_eq!(outbox.claim(110, 2, "claimer-c", 10), [1]);
    assert!(outbox.complete(1, "claimer-c"));
    assert_eq!(outbox.claim(111, 2, "claimer-d", 10), [2]);
}
