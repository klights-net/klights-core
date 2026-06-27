use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::datastore::{
    PodEndpointEvent, PodEndpointRow, PodNetworkAllocationRequest, PodNetworkEndpoint,
    PodSlotAdmissionEvent, PodWorkqueueEntry, PodWorkqueueKind,
};
use crate::pod_identity::PodIdentity;

use super::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodRuntimeRow, PodStatusCheckpoint,
    ProbeStateRow, ReplicationCheckpoint, SqliteNodeLocalDb,
};

#[async_trait]
pub trait NodeLocalBackend: Send + Sync {
    fn close(&self) {}
    fn backend_name(&self) -> &'static str;

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent>;
    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent>;

    async fn ensure_node_identity(&self, cluster_id: &str, node_uid: &str) -> Result<()>;
    async fn get_node_meta(&self, key: &str) -> Result<Option<String>>;
    async fn set_node_meta(&self, key: &str, value: &str) -> Result<()>;

    async fn enqueue_outbox(&self, row: OutboxInsert) -> Result<()>;
    async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Option<OutboxRow>>;
    async fn renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> Result<bool>;
    async fn mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> Result<bool>;
    async fn complete_outbox(&self, id: i64, lease_token: &str) -> Result<bool>;
    async fn requeue_expired_outbox_leases(&self, now_ms: i64) -> Result<usize>;
    async fn next_outbox_wake_ms(&self, now_ms: i64) -> Result<Option<i64>>;

    async fn claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Vec<OutboxRow>>;
    async fn complete_outbox_batch(&self, ids: &[i64]) -> Result<()>;
    async fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> Result<usize>;

    async fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        idempotency_key: &str,
        max_attempts: i64,
    ) -> Result<bool>;
    async fn list_dead_letter(&self) -> Result<Vec<DeadLetterRow>>;
    async fn get_dead_letter(&self, id: i64) -> Result<Option<DeadLetterRow>>;
    async fn delete_dead_letter(&self, id: i64) -> Result<bool>;
    async fn replay_dead_letter(&self, id: i64) -> Result<bool>;
    async fn outbox_stats(&self) -> Result<OutboxStats>;

    async fn admit_pod_runtime(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
    ) -> Result<()>;
    async fn record_sandbox(&self, pod_uid: &str, sandbox_id: &str) -> Result<()>;
    async fn record_cgroup(&self, pod_uid: &str, cgroup_path: &str) -> Result<()>;
    async fn delete_pod_runtime_for_uid(&self, pod_uid: &str) -> Result<()>;
    async fn get_pod_runtime(&self, pod_uid: &str) -> Result<Option<PodRuntimeRow>>;
    async fn list_pod_runtime(&self) -> Result<Vec<PodRuntimeRow>>;
    async fn list_pod_runtime_by_namespace(&self, namespace: &str) -> Result<Vec<PodRuntimeRow>>;
    async fn upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: Value,
        updated_ms: i64,
    ) -> Result<()>;
    async fn get_pod_status_checkpoint(&self, pod_uid: &str)
    -> Result<Option<PodStatusCheckpoint>>;
    async fn mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> Result<()>;
    async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()>;

    async fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: super::sqlite::RuntimeObservationCheckpoint,
    ) -> Result<()>;
    async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<super::sqlite::RuntimeObservationCheckpoint>>;
    async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()>;

    async fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest<'_>,
    ) -> Result<(String, u32)>;
    async fn get_network_for_uid(&self, pod_uid: &str) -> Result<Option<PodNetworkEndpoint>>;
    async fn get_network_for_sandbox(&self, sandbox_id: &str)
    -> Result<Option<PodNetworkEndpoint>>;
    async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()>;
    async fn list_networks(&self) -> Result<Vec<String>>;

    async fn upsert_endpoint(&self, row: PodEndpointRow) -> Result<()>;
    async fn delete_endpoint_for_uid(&self, pod_uid: &str) -> Result<()>;
    async fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>>;
    async fn list_endpoints_all(&self) -> Result<Vec<PodEndpointRow>>;
    async fn list_endpoints_for_node(&self, node_name: &str) -> Result<Vec<PodEndpointRow>>;

    async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()>;
    async fn peek_workqueue_next_due(&self) -> Result<Option<i64>>;
    async fn claim_workqueue_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>>;
    async fn complete_workqueue(&self, id: i64) -> Result<()>;

    async fn record_probe_result(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
        success: bool,
        ts_ms: i64,
    ) -> Result<()>;
    async fn get_probe_state(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
    ) -> Result<Option<ProbeStateRow>>;

    async fn read_replication_checkpoint(&self) -> Result<Option<ReplicationCheckpoint>>;
    async fn write_replication_checkpoint(
        &self,
        last_applied_rv: i64,
        leader_epoch: i64,
        cluster_id: &str,
    ) -> Result<()>;
}

#[async_trait]
impl NodeLocalBackend for SqliteNodeLocalDb {
    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        SqliteNodeLocalDb::subscribe_pod_endpoints(self)
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        SqliteNodeLocalDb::subscribe_pod_slot_admissions(self)
    }

    async fn ensure_node_identity(&self, cluster_id: &str, node_uid: &str) -> Result<()> {
        SqliteNodeLocalDb::ensure_node_identity(self, cluster_id, node_uid).await
    }

    async fn get_node_meta(&self, key: &str) -> Result<Option<String>> {
        SqliteNodeLocalDb::get_meta(self, key).await
    }

    async fn set_node_meta(&self, key: &str, value: &str) -> Result<()> {
        SqliteNodeLocalDb::set_meta(self, key, value).await
    }

    async fn enqueue_outbox(&self, row: OutboxInsert) -> Result<()> {
        SqliteNodeLocalDb::enqueue_outbox(self, row).await
    }

    async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Option<OutboxRow>> {
        SqliteNodeLocalDb::claim_next_due_outbox(self, now_ms, lease_ms, lease_token).await
    }

    async fn renew_outbox_lease(
        &self,
        id: i64,
        lease_token: &str,
        leased_until_ms: i64,
    ) -> Result<bool> {
        SqliteNodeLocalDb::renew_outbox_lease(self, id, lease_token, leased_until_ms).await
    }

    async fn mark_outbox_attempt_failed(
        &self,
        id: i64,
        lease_token: &str,
        backoff_until_ms: i64,
        error: &str,
    ) -> Result<bool> {
        SqliteNodeLocalDb::mark_outbox_attempt_failed(
            self,
            id,
            lease_token,
            backoff_until_ms,
            error,
        )
        .await
    }

    async fn complete_outbox(&self, id: i64, lease_token: &str) -> Result<bool> {
        SqliteNodeLocalDb::complete_outbox(self, id, lease_token).await
    }

    async fn requeue_expired_outbox_leases(&self, now_ms: i64) -> Result<usize> {
        SqliteNodeLocalDb::requeue_expired_outbox_leases(self, now_ms).await
    }

    async fn next_outbox_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
        SqliteNodeLocalDb::next_outbox_wake_ms(self, now_ms).await
    }

    async fn claim_due_outbox_batch(
        &self,
        now_ms: i64,
        limit: usize,
        lease_ms: i64,
        lease_token: &str,
    ) -> Result<Vec<OutboxRow>> {
        SqliteNodeLocalDb::claim_due_outbox_batch(self, now_ms, limit, lease_ms, lease_token).await
    }

    async fn complete_outbox_batch(&self, ids: &[i64]) -> Result<()> {
        SqliteNodeLocalDb::complete_outbox_batch(self, ids).await
    }

    async fn complete_superseded_status_outbox_for_terminal_pod_delete(
        &self,
        subject_key: &str,
        terminal_delete_id: i64,
    ) -> Result<usize> {
        SqliteNodeLocalDb::complete_superseded_status_outbox_for_terminal_pod_delete(
            self,
            subject_key,
            terminal_delete_id,
        )
        .await
    }

    async fn move_outbox_to_dead_letter_if_max_attempts(
        &self,
        idempotency_key: &str,
        max_attempts: i64,
    ) -> Result<bool> {
        SqliteNodeLocalDb::move_outbox_to_dead_letter_if_max_attempts(
            self,
            idempotency_key,
            max_attempts,
        )
        .await
    }

    async fn list_dead_letter(&self) -> Result<Vec<DeadLetterRow>> {
        SqliteNodeLocalDb::list_dead_letter(self).await
    }

    async fn get_dead_letter(&self, id: i64) -> Result<Option<DeadLetterRow>> {
        SqliteNodeLocalDb::get_dead_letter(self, id).await
    }

    async fn delete_dead_letter(&self, id: i64) -> Result<bool> {
        SqliteNodeLocalDb::delete_dead_letter(self, id).await
    }

    async fn replay_dead_letter(&self, id: i64) -> Result<bool> {
        SqliteNodeLocalDb::replay_dead_letter(self, id).await
    }

    async fn outbox_stats(&self) -> Result<OutboxStats> {
        SqliteNodeLocalDb::outbox_stats(self).await
    }

    async fn admit_pod_runtime(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
    ) -> Result<()> {
        SqliteNodeLocalDb::admit_pod_runtime(self, pod_uid, namespace, pod_name, node_name).await
    }

    async fn record_sandbox(&self, pod_uid: &str, sandbox_id: &str) -> Result<()> {
        SqliteNodeLocalDb::record_sandbox(self, pod_uid, sandbox_id).await
    }

    async fn record_cgroup(&self, pod_uid: &str, cgroup_path: &str) -> Result<()> {
        SqliteNodeLocalDb::record_cgroup(self, pod_uid, cgroup_path).await
    }

    async fn delete_pod_runtime_for_uid(&self, pod_uid: &str) -> Result<()> {
        SqliteNodeLocalDb::delete_pod_runtime_for_uid(self, pod_uid).await
    }

    async fn get_pod_runtime(&self, pod_uid: &str) -> Result<Option<PodRuntimeRow>> {
        SqliteNodeLocalDb::get_pod_runtime(self, pod_uid).await
    }

    async fn list_pod_runtime(&self) -> Result<Vec<PodRuntimeRow>> {
        SqliteNodeLocalDb::list_pod_runtime(self).await
    }

    async fn list_pod_runtime_by_namespace(&self, namespace: &str) -> Result<Vec<PodRuntimeRow>> {
        SqliteNodeLocalDb::list_pod_runtime_by_namespace(self, namespace).await
    }

    async fn upsert_pod_status_checkpoint(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        base_rv: i64,
        status: Value,
        updated_ms: i64,
    ) -> Result<()> {
        SqliteNodeLocalDb::upsert_pod_status_checkpoint(
            self, pod_uid, namespace, pod_name, base_rv, status, updated_ms,
        )
        .await
    }

    async fn get_pod_status_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<PodStatusCheckpoint>> {
        SqliteNodeLocalDb::get_pod_status_checkpoint(self, pod_uid).await
    }

    async fn mark_pod_status_checkpoint_applied(
        &self,
        pod_uid: &str,
        applied_rv: i64,
        updated_ms: i64,
    ) -> Result<()> {
        SqliteNodeLocalDb::mark_pod_status_checkpoint_applied(self, pod_uid, applied_rv, updated_ms)
            .await
    }

    async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        SqliteNodeLocalDb::delete_pod_status_checkpoint(self, pod_uid).await
    }

    async fn upsert_runtime_observation_checkpoint(
        &self,
        checkpoint: super::sqlite::RuntimeObservationCheckpoint,
    ) -> Result<()> {
        SqliteNodeLocalDb::upsert_runtime_observation_checkpoint(self, checkpoint).await
    }

    async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<super::sqlite::RuntimeObservationCheckpoint>> {
        SqliteNodeLocalDb::get_runtime_observation_checkpoint(self, pod_uid).await
    }

    async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        SqliteNodeLocalDb::delete_runtime_observation_checkpoint(self, pod_uid).await
    }

    async fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest<'_>,
    ) -> Result<(String, u32)> {
        SqliteNodeLocalDb::reserve_ip_and_insert_network(self, request).await
    }

    async fn get_network_for_uid(&self, pod_uid: &str) -> Result<Option<PodNetworkEndpoint>> {
        SqliteNodeLocalDb::get_network_for_uid(self, pod_uid).await
    }

    async fn get_network_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        SqliteNodeLocalDb::get_network_for_sandbox(self, sandbox_id).await
    }

    async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()> {
        SqliteNodeLocalDb::delete_network_for_sandbox(self, sandbox_id).await
    }

    async fn list_networks(&self) -> Result<Vec<String>> {
        SqliteNodeLocalDb::list_networks(self).await
    }

    async fn upsert_endpoint(&self, row: PodEndpointRow) -> Result<()> {
        SqliteNodeLocalDb::upsert_endpoint(self, row).await
    }

    async fn delete_endpoint_for_uid(&self, pod_uid: &str) -> Result<()> {
        SqliteNodeLocalDb::delete_endpoint_for_uid(self, pod_uid).await
    }

    async fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        SqliteNodeLocalDb::get_endpoint_by_pod_ip(self, pod_ip).await
    }

    async fn list_endpoints_all(&self) -> Result<Vec<PodEndpointRow>> {
        SqliteNodeLocalDb::list_endpoints_all(self).await
    }

    async fn list_endpoints_for_node(&self, node_name: &str) -> Result<Vec<PodEndpointRow>> {
        SqliteNodeLocalDb::list_endpoints_for_node(self, node_name).await
    }

    async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        SqliteNodeLocalDb::enqueue_workqueue(
            self,
            kind,
            pod,
            payload,
            attempt_count,
            min_delay_ms,
            last_error,
        )
        .await
    }

    async fn peek_workqueue_next_due(&self) -> Result<Option<i64>> {
        SqliteNodeLocalDb::peek_workqueue_next_due(self).await
    }

    async fn claim_workqueue_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        SqliteNodeLocalDb::claim_workqueue_due(self, now_ms).await
    }

    async fn complete_workqueue(&self, id: i64) -> Result<()> {
        SqliteNodeLocalDb::complete_workqueue(self, id).await
    }

    async fn record_probe_result(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
        success: bool,
        ts_ms: i64,
    ) -> Result<()> {
        SqliteNodeLocalDb::record_probe_result(
            self,
            pod_uid,
            container_name,
            probe_kind,
            success,
            ts_ms,
        )
        .await
    }

    async fn get_probe_state(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
    ) -> Result<Option<ProbeStateRow>> {
        SqliteNodeLocalDb::get_probe_state(self, pod_uid, container_name, probe_kind).await
    }

    async fn read_replication_checkpoint(&self) -> Result<Option<ReplicationCheckpoint>> {
        SqliteNodeLocalDb::read_replication_checkpoint(self).await
    }

    async fn write_replication_checkpoint(
        &self,
        last_applied_rv: i64,
        leader_epoch: i64,
        cluster_id: &str,
    ) -> Result<()> {
        SqliteNodeLocalDb::write_replication_checkpoint(
            self,
            last_applied_rv,
            leader_epoch,
            cluster_id,
        )
        .await
    }
}
