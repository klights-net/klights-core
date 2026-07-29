//! Test-only compatibility for pre-Phase-11 delivery persistence tests.
//!
//! These adapters deliberately have `legacy_` names so the production focused
//! ports remain the only delivery API. They preserve the old test vocabulary
//! while exercising the extracted implementation through `klights-node-store`.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use klights_node_store::{
    DeadLetterEntry, DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest,
    DeadLetterStore, OutboxAttemptFailure, OutboxAttemptFailureRecord, OutboxBatchClaimRequest,
    OutboxClaimRequest, OutboxCompletion, OutboxDispatcherStore, OutboxEnqueue, OutboxLease,
    OutboxNow, OutboxProducerStore, OutboxRecord, OutboxStatusStampStore, OutboxSupersedeRequest,
    PodCheckpointKey, PodStatusCheckpointApplied, PodStatusCheckpointStore,
    PodStatusCheckpointUpsert, ReplicationCheckpointStore, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration,
};
use klights_types::{PodIdentity, ResourceKey};

use super::sqlite::RuntimeObservationCheckpoint;
use super::{
    DeadLetterRow, OutboxFailureDisposition, OutboxInsert, OutboxRow, OutboxStats,
    PodStatusCheckpoint, ReplicationCheckpoint,
};

fn persistence(error: impl std::fmt::Display) -> anyhow::Error {
    anyhow!(error.to_string())
}

fn enqueue(row: OutboxInsert) -> Result<OutboxEnqueue> {
    OutboxEnqueue::try_new(
        row.idempotency_key,
        row.enqueued_ms,
        klights_node_store::OutboxSubject::new(
            row.subject_key,
            ResourceKey::new(
                row.subject_api_version,
                row.subject_kind,
                row.subject_namespace,
                row.subject_name,
            ),
            row.subject_uid,
            row.pod_uid,
        ),
        row.operation,
        row.classification,
        row.payload_proto,
        row.next_due_ms,
    )
    .map_err(persistence)
}

fn outbox_row(row: OutboxRecord) -> OutboxRow {
    let subject = row.subject();
    let resource = subject.resource();
    let classification = row.classification();
    let sequence = row.sequence();
    OutboxRow {
        id: row.id(),
        client_id: row.client_id().to_string(),
        idempotency_key: row.idempotency_key().to_string(),
        enqueued_ms: row.enqueued_ms(),
        subject_key: subject.subject_key().to_string(),
        subject_api_version: resource.api_version.clone(),
        subject_kind: resource.kind.clone(),
        subject_namespace: resource.namespace.clone(),
        subject_name: resource.name.clone(),
        subject_uid: subject.subject_uid().map(str::to_string),
        pod_uid: subject.pod_uid().to_string(),
        operation: row.operation().to_string(),
        priority_class: classification.priority().persisted_value(),
        supersedable_pod_status: classification.supersedability()
            == klights_node_store::OutboxSupersedability::PodStatus,
        is_terminal_pod_delete: classification.terminal_delete()
            == klights_node_store::TerminalDeleteClassification::ActorOwnedPodDelete,
        stream_id: sequence.stream_id(),
        stream_seq: sequence.stream_seq(),
        payload_proto: row.payload().to_vec(),
        attempt: row.attempt(),
        next_due_ms: row.next_due_ms(),
        leased_until_ms: row.leased_until_ms(),
        lease_token: row.lease_token().map(str::to_string),
        last_error: row.last_error().map(str::to_string),
    }
}

fn dead_letter_row(row: DeadLetterEntry) -> DeadLetterRow {
    let subject = row.subject();
    let resource = subject.resource();
    let sequence = row.sequence();
    DeadLetterRow {
        id: row.id(),
        original_id: row.original_id(),
        client_id: row.client_id().to_string(),
        idempotency_key: row.idempotency_key().to_string(),
        enqueued_ms: row.enqueued_ms(),
        subject_key: subject.subject_key().to_string(),
        subject_api_version: resource.api_version.clone(),
        subject_kind: resource.kind.clone(),
        subject_namespace: resource.namespace.clone(),
        subject_name: resource.name.clone(),
        subject_uid: subject.subject_uid().map(str::to_string),
        pod_uid: subject.pod_uid().to_string(),
        operation: row.operation().to_string(),
        stream_id: sequence.stream_id(),
        stream_seq: sequence.stream_seq(),
        payload_proto: row.payload().to_vec(),
        attempts: row.attempts(),
        last_error: row.last_error().to_string(),
        moved_at_ms: row.moved_at_ms(),
    }
}

#[async_trait]
pub trait LegacyDeliveryTestStore:
    OutboxProducerStore
    + OutboxDispatcherStore
    + OutboxStatusStampStore
    + DeadLetterStore
    + PodStatusCheckpointStore
    + RuntimeObservationCheckpointStore
    + ReplicationCheckpointStore
    + Send
    + Sync
{
    async fn legacy_enqueue_outbox(&self, row: OutboxInsert) -> Result<()> {
        OutboxProducerStore::enqueue_outbox(self, enqueue(row)?)
            .await
            .map_err(persistence)
    }

    async fn legacy_claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Option<OutboxRow>> {
        let request =
            OutboxClaimRequest::try_new(now_ms, lease_ms, lease_token).map_err(persistence)?;
        OutboxDispatcherStore::claim_next_due_outbox(self, request)
            .await
            .map(|row| row.map(outbox_row))
            .map_err(persistence)
    }

    async fn legacy_renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> Result<bool> {
        let lease = OutboxLease::try_new(id, lease_token, leased_until_ms).map_err(persistence)?;
        OutboxDispatcherStore::renew_outbox_lease(self, lease)
            .await
            .map_err(persistence)
    }

    async fn legacy_mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> Result<bool> {
        let failure = OutboxAttemptFailure::try_new(id, lease_token, backoff_until_ms, error)
            .map_err(persistence)?;
        OutboxDispatcherStore::mark_outbox_attempt_failed(self, failure)
            .await
            .map_err(persistence)
    }

    async fn legacy_record_outbox_failure(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
        max_attempts: i64,
    ) -> Result<OutboxFailureDisposition> {
        let failure = OutboxAttemptFailureRecord::try_new(
            id,
            lease_token,
            backoff_until_ms,
            error,
            max_attempts,
        )
        .map_err(persistence)?;
        let disposition = OutboxDispatcherStore::record_outbox_failure(self, failure)
            .await
            .map_err(persistence)?;
        Ok(match disposition {
            klights_node_store::OutboxFailureDisposition::RetryScheduled => {
                OutboxFailureDisposition::RetryScheduled
            }
            klights_node_store::OutboxFailureDisposition::DeadLettered => {
                OutboxFailureDisposition::DeadLettered
            }
            klights_node_store::OutboxFailureDisposition::LeaseLost => {
                OutboxFailureDisposition::LeaseLost
            }
        })
    }

    async fn legacy_complete_outbox(&self, id: i64, lease_token: &str) -> Result<bool> {
        let completion = OutboxCompletion::try_new(id, lease_token).map_err(persistence)?;
        OutboxDispatcherStore::complete_outbox(self, completion)
            .await
            .map_err(persistence)
    }

    async fn legacy_claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Vec<OutboxRow>> {
        let request = OutboxBatchClaimRequest::try_new(now_ms, limit, lease_ms, lease_token)
            .map_err(persistence)?;
        OutboxDispatcherStore::claim_due_outbox_batch(self, request)
            .await
            .map(|rows| rows.into_iter().map(outbox_row).collect())
            .map_err(persistence)
    }

    async fn legacy_complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> Result<usize> {
        let request = OutboxSupersedeRequest::try_new(subject_key, terminal_delete_id)
            .map_err(persistence)?;
        OutboxDispatcherStore::complete_superseded_status_outbox_for_terminal_pod_delete(
            self, request,
        )
        .await
        .map_err(persistence)
    }

    async fn legacy_requeue_expired_outbox_leases(&self, now_ms: i64) -> Result<usize> {
        let now = OutboxNow::try_new(now_ms).map_err(persistence)?;
        OutboxDispatcherStore::requeue_expired_outbox_leases(self, now)
            .await
            .map_err(persistence)
    }

    async fn legacy_next_outbox_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
        let now = OutboxNow::try_new(now_ms).map_err(persistence)?;
        OutboxDispatcherStore::next_outbox_wake_ms(self, now)
            .await
            .map_err(persistence)
    }

    async fn legacy_move_outbox_to_dead_letter_if_max_attempts(
        &self,
        idempotency_key: &str,
        max_attempts: i64,
    ) -> Result<bool> {
        let request =
            DeadLetterMoveRequest::try_new(idempotency_key, max_attempts).map_err(persistence)?;
        DeadLetterStore::move_outbox_to_dead_letter_if_max_attempts(self, request)
            .await
            .map_err(persistence)
    }

    async fn legacy_list_dead_letter(&self) -> Result<Vec<DeadLetterRow>> {
        DeadLetterStore::list_dead_letter(self)
            .await
            .map(|rows| rows.into_iter().map(dead_letter_row).collect())
            .map_err(persistence)
    }

    async fn legacy_get_dead_letter(&self, id: i64) -> Result<Option<DeadLetterRow>> {
        let key = DeadLetterKey::try_new(id).map_err(persistence)?;
        DeadLetterStore::get_dead_letter(self, key)
            .await
            .map(|row| row.map(dead_letter_row))
            .map_err(persistence)
    }

    async fn legacy_delete_dead_letter(&self, id: i64) -> Result<bool> {
        let key = DeadLetterKey::try_new(id).map_err(persistence)?;
        DeadLetterStore::delete_dead_letter(self, key)
            .await
            .map_err(persistence)
    }

    async fn legacy_replay_dead_letter(
        &self,
        id: i64,
        classification: klights_node_store::OutboxClassification,
    ) -> Result<bool> {
        let key = DeadLetterKey::try_new(id).map_err(persistence)?;
        DeadLetterStore::replay_dead_letter(self, DeadLetterReplayRequest::new(key, classification))
            .await
            .map_err(persistence)
    }

    async fn legacy_outbox_stats(&self) -> Result<OutboxStats> {
        DeadLetterStore::outbox_stats(self)
            .await
            .map(|stats| OutboxStats {
                pending: stats.pending(),
                oldest_age_seconds: stats.oldest_age_seconds(),
                dead_letter_count: stats.dead_letter_count(),
                dispatch_total: stats.dispatch_total(),
                dispatch_errors_total: stats.dispatch_errors_total(),
            })
            .map_err(persistence)
    }

    async fn legacy_upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: serde_json::Value,
        updated_ms: i64,
    ) -> Result<()> {
        let status_payload = serde_json::to_vec(&status)?;
        let checkpoint = PodStatusCheckpointUpsert::try_new(
            PodIdentity::new(namespace, pod_name, pod_uid),
            base_rv,
            status_payload,
            updated_ms,
        )
        .map_err(persistence)?;
        PodStatusCheckpointStore::upsert_pod_status_checkpoint(self, checkpoint)
            .await
            .map_err(persistence)
    }

    async fn legacy_get_pod_status_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<PodStatusCheckpoint>> {
        let key = PodCheckpointKey::try_new(pod_uid).map_err(persistence)?;
        let checkpoint = PodStatusCheckpointStore::get_pod_status_checkpoint(self, key)
            .await
            .map_err(persistence)?;
        checkpoint
            .map(|checkpoint| {
                Ok(PodStatusCheckpoint {
                    pod_uid: checkpoint.pod().uid.clone(),
                    namespace: checkpoint.pod().namespace.clone(),
                    pod_name: checkpoint.pod().name.clone(),
                    base_rv: checkpoint.base_position(),
                    applied_rv: checkpoint.applied_position(),
                    status: serde_json::from_slice(checkpoint.status_payload())?,
                    updated_ms: checkpoint.updated_ms(),
                })
            })
            .transpose()
    }

    async fn legacy_mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> Result<()> {
        let applied = PodStatusCheckpointApplied::try_new(pod_uid, applied_rv, updated_ms)
            .map_err(persistence)?;
        PodStatusCheckpointStore::mark_pod_status_checkpoint_applied(self, applied)
            .await
            .map_err(persistence)
    }

    async fn legacy_delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        let key = PodCheckpointKey::try_new(pod_uid).map_err(persistence)?;
        PodStatusCheckpointStore::delete_pod_status_checkpoint(self, key)
            .await
            .map_err(persistence)
    }

    async fn legacy_upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> Result<()> {
        let generation =
            RuntimeObservationGeneration::try_from(checkpoint.generation).map_err(persistence)?;
        let checkpoint = klights_node_store::RuntimeObservationCheckpoint::try_new(
            checkpoint.pod_uid,
            checkpoint.container_ids,
            generation,
            checkpoint.updated_ms,
        )
        .map_err(persistence)?;
        RuntimeObservationCheckpointStore::upsert_runtime_observation_checkpoint(self, checkpoint)
            .await
            .map_err(persistence)
    }

    async fn legacy_get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<RuntimeObservationCheckpoint>> {
        let key = PodCheckpointKey::try_new(pod_uid).map_err(persistence)?;
        RuntimeObservationCheckpointStore::get_runtime_observation_checkpoint(self, key)
            .await
            .map(|checkpoint| {
                checkpoint.map(|checkpoint| RuntimeObservationCheckpoint {
                    pod_uid: checkpoint.pod_uid().to_string(),
                    container_ids: checkpoint.container_ids().to_vec(),
                    generation: checkpoint.generation().get() as u64,
                    updated_ms: checkpoint.updated_ms(),
                })
            })
            .map_err(persistence)
    }

    async fn legacy_delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        let key = PodCheckpointKey::try_new(pod_uid).map_err(persistence)?;
        RuntimeObservationCheckpointStore::delete_runtime_observation_checkpoint(self, key)
            .await
            .map_err(persistence)
    }

    async fn legacy_read_replication_checkpoint(&self) -> Result<Option<ReplicationCheckpoint>> {
        ReplicationCheckpointStore::read_replication_checkpoint(self)
            .await
            .map(|checkpoint| {
                checkpoint.map(|checkpoint| ReplicationCheckpoint {
                    last_applied_rv: checkpoint.last_applied_rv(),
                    leader_epoch: checkpoint.leader_epoch(),
                    cluster_id: checkpoint.cluster_id().to_string(),
                })
            })
            .map_err(persistence)
    }

    async fn legacy_write_replication_checkpoint(
        &self,
        last_applied_rv: i64,
        leader_epoch: i64,
        cluster_id: &str,
    ) -> Result<()> {
        ReplicationCheckpointStore::write_replication_checkpoint(
            self,
            klights_node_store::ReplicationCheckpoint::new(
                last_applied_rv,
                leader_epoch,
                cluster_id,
            ),
        )
        .await
        .map_err(persistence)
    }
}

impl<T> LegacyDeliveryTestStore for T where
    T: OutboxProducerStore
        + OutboxDispatcherStore
        + OutboxStatusStampStore
        + DeadLetterStore
        + PodStatusCheckpointStore
        + RuntimeObservationCheckpointStore
        + ReplicationCheckpointStore
        + Send
        + Sync
        + ?Sized
{
}
