//! Root composition for the passive node-delivery implementation.

use klights_node_store::{
    DeadLetterEntry, DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest,
    DeadLetterStore, DeliveryFuture, OutboxAttemptFailure, OutboxAttemptFailureRecord,
    OutboxBatchClaimRequest, OutboxClaimRequest, OutboxCompletion, OutboxDispatchCounters,
    OutboxDispatcherStore, OutboxEnqueue, OutboxFailureDisposition, OutboxLease, OutboxNow,
    OutboxProducerStore, OutboxRecord, OutboxStats, OutboxStatusStampStore, OutboxSupersedeRequest,
    PodCheckpointKey, PodStatusCheckpoint, PodStatusCheckpointApplied, PodStatusCheckpointStore,
    PodStatusCheckpointUpsert, RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore,
};

use super::NodeLocalStores;

impl OutboxProducerStore for NodeLocalStores {
    fn enqueue_outbox(&self, entry: OutboxEnqueue) -> DeliveryFuture<'_, ()> {
        self.delivery_ref().enqueue_outbox(entry)
    }
}

impl OutboxDispatcherStore for NodeLocalStores {
    fn claim_next_due_outbox(
        &self,
        request: OutboxClaimRequest,
    ) -> DeliveryFuture<'_, Option<OutboxRecord>> {
        self.delivery_ref().claim_next_due_outbox(request)
    }

    fn renew_outbox_lease(&self, lease: OutboxLease) -> DeliveryFuture<'_, bool> {
        self.delivery_ref().renew_outbox_lease(lease)
    }

    fn mark_outbox_attempt_failed(
        &self,
        failure: OutboxAttemptFailure,
    ) -> DeliveryFuture<'_, bool> {
        self.delivery_ref().mark_outbox_attempt_failed(failure)
    }

    fn record_outbox_failure(
        &self,
        failure: OutboxAttemptFailureRecord,
    ) -> DeliveryFuture<'_, OutboxFailureDisposition> {
        self.delivery_ref().record_outbox_failure(failure)
    }

    fn complete_outbox(&self, completion: OutboxCompletion) -> DeliveryFuture<'_, bool> {
        self.delivery_ref().complete_outbox(completion)
    }

    fn requeue_expired_outbox_leases(&self, now: OutboxNow) -> DeliveryFuture<'_, usize> {
        self.delivery_ref().requeue_expired_outbox_leases(now)
    }

    fn next_outbox_wake_ms(&self, now: OutboxNow) -> DeliveryFuture<'_, Option<i64>> {
        self.delivery_ref().next_outbox_wake_ms(now)
    }

    fn claim_due_outbox_batch(
        &self,
        request: OutboxBatchClaimRequest,
    ) -> DeliveryFuture<'_, Vec<OutboxRecord>> {
        self.delivery_ref().claim_due_outbox_batch(request)
    }

    fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        request: OutboxSupersedeRequest,
    ) -> DeliveryFuture<'_, usize> {
        self.delivery_ref()
            .complete_superseded_status_outbox_for_terminal_pod_delete(request)
    }

    fn write_dispatch_counters(&self, counters: OutboxDispatchCounters) -> DeliveryFuture<'_, ()> {
        self.delivery_ref().write_dispatch_counters(counters)
    }
}

impl OutboxStatusStampStore for NodeLocalStores {
    fn read_status_stamp_high_water(&self) -> DeliveryFuture<'_, i64> {
        self.delivery_ref().read_status_stamp_high_water()
    }

    fn write_status_stamp_high_water(&self, high_water: i64) -> DeliveryFuture<'_, ()> {
        self.delivery_ref()
            .write_status_stamp_high_water(high_water)
    }
}

impl DeadLetterStore for NodeLocalStores {
    fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        request: DeadLetterMoveRequest,
    ) -> DeliveryFuture<'_, bool> {
        self.delivery_ref()
            .move_outbox_to_dead_letter_if_max_attempts(request)
    }

    fn list_dead_letter(&self) -> DeliveryFuture<'_, Vec<DeadLetterEntry>> {
        self.delivery_ref().list_dead_letter()
    }

    fn get_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, Option<DeadLetterEntry>> {
        self.delivery_ref().get_dead_letter(key)
    }

    fn delete_dead_letter(&self, key: DeadLetterKey) -> DeliveryFuture<'_, bool> {
        self.delivery_ref().delete_dead_letter(key)
    }

    fn replay_dead_letter(&self, request: DeadLetterReplayRequest) -> DeliveryFuture<'_, bool> {
        self.delivery_ref().replay_dead_letter(request)
    }

    fn outbox_stats(&self) -> DeliveryFuture<'_, OutboxStats> {
        self.delivery_ref().outbox_stats()
    }
}

impl PodStatusCheckpointStore for NodeLocalStores {
    fn upsert_pod_status_checkpoint(
        &self,
        checkpoint: PodStatusCheckpointUpsert,
    ) -> DeliveryFuture<'_, ()> {
        self.delivery_ref().upsert_pod_status_checkpoint(checkpoint)
    }

    fn get_pod_status_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<PodStatusCheckpoint>> {
        self.delivery_ref().get_pod_status_checkpoint(key)
    }

    fn mark_pod_status_checkpoint_applied(
        &self,
        applied: PodStatusCheckpointApplied,
    ) -> DeliveryFuture<'_, ()> {
        self.delivery_ref()
            .mark_pod_status_checkpoint_applied(applied)
    }

    fn delete_pod_status_checkpoint(&self, key: PodCheckpointKey) -> DeliveryFuture<'_, ()> {
        self.delivery_ref().delete_pod_status_checkpoint(key)
    }
}

impl RuntimeObservationCheckpointStore for NodeLocalStores {
    fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: RuntimeObservationCheckpoint,
    ) -> DeliveryFuture<'_, ()> {
        self.delivery_ref()
            .upsert_runtime_observation_checkpoint(checkpoint)
    }

    fn get_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, Option<RuntimeObservationCheckpoint>> {
        self.delivery_ref().get_runtime_observation_checkpoint(key)
    }

    fn delete_runtime_observation_checkpoint(
        &self,
        key: PodCheckpointKey,
    ) -> DeliveryFuture<'_, ()> {
        self.delivery_ref()
            .delete_runtime_observation_checkpoint(key)
    }
}
