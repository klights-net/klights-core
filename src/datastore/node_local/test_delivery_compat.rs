//! Test-only compatibility for pre-Phase-11 delivery persistence tests.
//!
//! These adapters deliberately have `legacy_` names so the production focused
//! ports remain the only delivery API. They preserve the old test vocabulary
//! while exercising the extracted implementation through `klights-node-store`.

#![cfg(test)]

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use klights_node_store::{
    DeadLetterEntry, DeadLetterMoveRequest, DeadLetterStore, OutboxClaimRequest,
    OutboxDispatcherStore, OutboxEnqueue, OutboxProducerStore, OutboxRecord,
};
use klights_types::ResourceKey;

use super::{DeadLetterRow, OutboxInsert, OutboxRow};

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
    OutboxProducerStore + OutboxDispatcherStore + DeadLetterStore + Send + Sync
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
}

impl<T> LegacyDeliveryTestStore for T where
    T: OutboxProducerStore + OutboxDispatcherStore + DeadLetterStore + Send + Sync + ?Sized
{
}
