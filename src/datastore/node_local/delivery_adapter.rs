//! Focused node-delivery ports over the transitional node-local backend.
//!
//! Root composition constructs this adapter and injects only its node-store
//! capabilities into the outbox feature. The broad backend stays confined to
//! node persistence and composition until Phase 11 physically moves it.

use std::sync::Arc;

use klights_node_store::{
    DeliveryError, DeliveryFuture, OutboxAttemptFailure, OutboxAttemptFailureRecord,
    OutboxBatchClaimRequest, OutboxClaimRequest, OutboxCompletion, OutboxDispatchCounters,
    OutboxDispatcherStore, OutboxEnqueue, OutboxFailureDisposition, OutboxLease, OutboxNow,
    OutboxProducerStore, OutboxRecord, OutboxSequence, OutboxSequencePolicy,
    OutboxStatusStampStore, OutboxSupersedability, PodCheckpointKey, PodStatusCheckpoint,
    PodStatusCheckpointApplied, PodStatusCheckpointStore, PodStatusCheckpointUpsert,
    RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore, RuntimeObservationGeneration,
    TerminalDeleteClassification,
};
use klights_types::{PodIdentity, ResourceKey};

use super::{
    NodeLocalHandle, OutboxFailureDisposition as LegacyFailureDisposition, OutboxInsert, OutboxRow,
};

const STATUS_STAMP_META_KEY: &str = "pod_status_stamp_high_water";
const OUTBOX_DISPATCH_TOTAL_META_KEY: &str = "outbox_dispatch_total";
const OUTBOX_DISPATCH_ERRORS_META_KEY: &str = "outbox_dispatch_errors_total";

#[derive(Clone)]
pub(crate) struct NodeLocalDeliveryAdapter {
    backend: NodeLocalHandle,
}

impl NodeLocalDeliveryAdapter {
    pub(crate) fn new(backend: NodeLocalHandle) -> Arc<Self> {
        Arc::new(Self { backend })
    }
}

fn persistence(error: impl std::fmt::Display) -> DeliveryError {
    DeliveryError::persistence_failed(error.to_string())
}

fn classification_for_row(
    row: &OutboxRow,
) -> Result<klights_node_store::OutboxClassification, DeliveryError> {
    let supersedability = if row.supersedable_pod_status {
        OutboxSupersedability::PodStatus
    } else {
        OutboxSupersedability::Never
    };
    let terminal_delete = if row.is_terminal_pod_delete {
        TerminalDeleteClassification::ActorOwnedPodDelete
    } else {
        TerminalDeleteClassification::NotTerminalDelete
    };
    let sequence_policy = if row.stream_id > 0 || row.stream_seq > 0 {
        OutboxSequencePolicy::PerSubject
    } else {
        OutboxSequencePolicy::Unsequenced
    };
    klights_node_store::OutboxClassification::try_from_persisted(
        row.priority_class,
        supersedability.persisted_value(),
        terminal_delete.persisted_value(),
        sequence_policy.persisted_value(),
    )
}

fn outbox_record(row: OutboxRow) -> Result<OutboxRecord, DeliveryError> {
    let classification = classification_for_row(&row)?;
    let subject = klights_node_store::OutboxSubject::new(
        row.subject_key,
        ResourceKey::new(
            row.subject_api_version,
            row.subject_kind,
            row.subject_namespace,
            row.subject_name,
        ),
        row.subject_uid,
        row.pod_uid,
    );
    OutboxRecord::try_new(
        row.id,
        row.client_id,
        row.idempotency_key,
        row.enqueued_ms,
        subject,
        row.operation,
        classification,
        OutboxSequence::try_new(row.stream_id, row.stream_seq)?,
        row.payload_proto,
        row.attempt,
        row.next_due_ms,
        row.leased_until_ms,
        row.lease_token,
        row.last_error,
    )
}

impl OutboxProducerStore for NodeLocalDeliveryAdapter {
    fn enqueue_outbox(&self, entry: OutboxEnqueue) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (
                idempotency_key,
                enqueued_ms,
                subject,
                operation,
                classification,
                payload_proto,
                next_due_ms,
            ) = entry.into_parts();
            let (subject_key, resource, subject_uid, pod_uid) = subject.into_parts();
            self.backend
                .enqueue_outbox(OutboxInsert {
                    idempotency_key,
                    enqueued_ms,
                    subject_key,
                    subject_api_version: resource.api_version,
                    subject_kind: resource.kind,
                    subject_namespace: resource.namespace,
                    subject_name: resource.name,
                    subject_uid,
                    pod_uid,
                    operation,
                    classification,
                    payload_proto,
                    next_due_ms,
                })
                .await
                .map_err(persistence)
        })
    }
}

impl OutboxDispatcherStore for NodeLocalDeliveryAdapter {
    fn claim_next_due_outbox(
        &self,
        request: OutboxClaimRequest,
    ) -> DeliveryFuture<'_, Option<OutboxRecord>> {
        Box::pin(async move {
            self.backend
                .claim_next_due_outbox(request.now_ms(), request.lease_ms(), request.lease_token())
                .await
                .map_err(persistence)?
                .map(outbox_record)
                .transpose()
        })
    }

    fn renew_outbox_lease(&self, lease: OutboxLease) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.backend
                .renew_outbox_lease(lease.id(), lease.lease_token(), lease.leased_until_ms())
                .await
                .map_err(persistence)
        })
    }

    fn mark_outbox_attempt_failed(
        &self,
        failure: OutboxAttemptFailure,
    ) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.backend
                .mark_outbox_attempt_failed(
                    failure.id(),
                    failure.lease_token(),
                    failure.backoff_until_ms(),
                    failure.error(),
                )
                .await
                .map_err(persistence)
        })
    }

    fn record_outbox_failure(
        &self,
        failure: OutboxAttemptFailureRecord,
    ) -> DeliveryFuture<'_, OutboxFailureDisposition> {
        Box::pin(async move {
            let disposition = self
                .backend
                .record_outbox_failure(
                    failure.id(),
                    failure.lease_token(),
                    failure.backoff_until_ms(),
                    failure.error(),
                    failure.max_attempts(),
                )
                .await
                .map_err(persistence)?;
            Ok(match disposition {
                LegacyFailureDisposition::RetryScheduled => {
                    OutboxFailureDisposition::RetryScheduled
                }
                LegacyFailureDisposition::DeadLettered => OutboxFailureDisposition::DeadLettered,
                LegacyFailureDisposition::LeaseLost => OutboxFailureDisposition::LeaseLost,
            })
        })
    }

    fn complete_outbox(&self, completion: OutboxCompletion) -> DeliveryFuture<'_, bool> {
        Box::pin(async move {
            self.backend
                .complete_outbox(completion.id(), completion.lease_token())
                .await
                .map_err(persistence)
        })
    }

    fn requeue_expired_outbox_leases(&self, now: OutboxNow) -> DeliveryFuture<'_, usize> {
        Box::pin(async move {
            self.backend
                .requeue_expired_outbox_leases(now.get())
                .await
                .map_err(persistence)
        })
    }

    fn next_outbox_wake_ms(&self, now: OutboxNow) -> DeliveryFuture<'_, Option<i64>> {
        Box::pin(async move {
            self.backend
                .next_outbox_wake_ms(now.get())
                .await
                .map_err(persistence)
        })
    }

    fn claim_due_outbox_batch(
        &self,
        request: OutboxBatchClaimRequest,
    ) -> DeliveryFuture<'_, Vec<OutboxRecord>> {
        Box::pin(async move {
            self.backend
                .claim_due_outbox_batch(
                    request.now_ms(),
                    request.effective_limit(),
                    request.lease_ms(),
                    request.lease_token(),
                )
                .await
                .map_err(persistence)?
                .into_iter()
                .map(outbox_record)
                .collect()
        })
    }

    fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        request: klights_node_store::OutboxSupersedeRequest,
    ) -> DeliveryFuture<'_, usize> {
        Box::pin(async move {
            self.backend
                .complete_superseded_status_outbox_for_terminal_pod_delete(
                    request.subject_key(),
                    request.terminal_delete_id(),
                )
                .await
                .map_err(persistence)
        })
    }

    fn write_dispatch_counters(&self, counters: OutboxDispatchCounters) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .set_node_meta(
                    OUTBOX_DISPATCH_TOTAL_META_KEY,
                    &counters.dispatch_total().to_string(),
                )
                .await
                .map_err(persistence)?;
            self.backend
                .set_node_meta(
                    OUTBOX_DISPATCH_ERRORS_META_KEY,
                    &counters.dispatch_errors_total().to_string(),
                )
                .await
                .map_err(persistence)
        })
    }
}

impl OutboxStatusStampStore for NodeLocalDeliveryAdapter {
    fn read_status_stamp_high_water(&self) -> DeliveryFuture<'_, i64> {
        Box::pin(async move {
            let raw = self
                .backend
                .get_node_meta(STATUS_STAMP_META_KEY)
                .await
                .map_err(persistence)?;
            raw.map(|value| {
                value.parse::<i64>().map_err(|error| {
                    DeliveryError::corrupt_data(format!(
                        "invalid status-stamp high-water {value:?}: {error}"
                    ))
                })
            })
            .transpose()
            .map(|value| value.unwrap_or(0))
        })
    }

    fn write_status_stamp_high_water(&self, high_water: i64) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            if high_water < 0 {
                return Err(DeliveryError::corrupt_data(
                    "status-stamp high-water must be non-negative",
                ));
            }
            self.backend
                .set_node_meta(STATUS_STAMP_META_KEY, &high_water.to_string())
                .await
                .map_err(persistence)
        })
    }
}

impl PodStatusCheckpointStore for NodeLocalDeliveryAdapter {
    fn upsert_pod_status_checkpoint(
        &self,
        checkpoint: PodStatusCheckpointUpsert,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (pod, base_position, status_payload, updated_ms) = checkpoint.into_parts();
            let status = serde_json::from_slice(&status_payload).map_err(|error| {
                DeliveryError::corrupt_data(format!(
                    "invalid Pod status checkpoint payload: {error}"
                ))
            })?;
            self.backend
                .upsert_pod_status_checkpoint(
                    &pod.uid,
                    &pod.namespace,
                    &pod.name,
                    base_position,
                    status,
                    updated_ms,
                )
                .await
                .map_err(persistence)
        })
    }

    fn get_pod_status_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<PodStatusCheckpoint>> {
        Box::pin(async move {
            let checkpoint = self
                .backend
                .get_pod_status_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)?;
            checkpoint
                .map(|checkpoint| {
                    let status_payload =
                        serde_json::to_vec(&checkpoint.status).map_err(persistence)?;
                    PodStatusCheckpoint::try_new(
                        PodIdentity::new(
                            &checkpoint.namespace,
                            &checkpoint.pod_name,
                            &checkpoint.pod_uid,
                        ),
                        checkpoint.base_rv,
                        checkpoint.applied_rv,
                        status_payload,
                        checkpoint.updated_ms,
                    )
                })
                .transpose()
        })
    }

    fn mark_pod_status_checkpoint_applied(
        &self,
        applied: PodStatusCheckpointApplied,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .mark_pod_status_checkpoint_applied(
                    applied.pod_uid(),
                    applied.applied_position(),
                    applied.updated_ms(),
                )
                .await
                .map_err(persistence)
        })
    }

    fn delete_pod_status_checkpoint(&self, key: PodCheckpointKey) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .delete_pod_status_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)
        })
    }
}

impl RuntimeObservationCheckpointStore for NodeLocalDeliveryAdapter {
    fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            let (pod_uid, container_ids, generation, updated_ms) = checkpoint.into_parts();
            self.backend
                .upsert_runtime_observation_checkpoint(
                    super::sqlite::RuntimeObservationCheckpoint {
                        pod_uid,
                        container_ids,
                        generation: generation.get() as u64,
                        updated_ms,
                    },
                )
                .await
                .map_err(persistence)
        })
    }

    fn get_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<RuntimeObservationCheckpoint>> {
        Box::pin(async move {
            self.backend
                .get_runtime_observation_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)?
                .map(|checkpoint| {
                    RuntimeObservationCheckpoint::try_new(
                        checkpoint.pod_uid,
                        checkpoint.container_ids,
                        RuntimeObservationGeneration::try_from(checkpoint.generation)?,
                        checkpoint.updated_ms,
                    )
                })
                .transpose()
        })
    }

    fn delete_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .delete_runtime_observation_checkpoint(key.pod_uid())
                .await
                .map_err(persistence)
        })
    }
}
