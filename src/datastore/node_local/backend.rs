use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::types::{
    PodSlotAdmissionEvent, PodSlotAdmissionResult, PodSlotClearResult, PodSlotMutationResult,
    PodWorkqueueEntry, PodWorkqueueKind,
};
use klights_node_store::{
    CacheNetworkFuture, DeadLetterStore, EndpointDeleteOutcome, EndpointUpsertOutcome,
    NodeIdentity, NodeKey, OutboxDispatcherStore, OutboxProducerStore, OutboxStatusStampStore,
    PodEndpointRecord, PodEndpointStore, PodEndpointStoreEventSource, PodEndpointStoreEventStream,
    PodIpamStore, PodNetworkAllocation,
    PodNetworkAllocationRequest as StorePodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint as StorePodNetworkEndpoint, PodStatusCheckpointStore,
    PodUidKey, ReplicationCheckpointStore, RuntimeObservationCheckpointStore, SandboxKey,
};
use klights_types::PodIdentity;

use super::{PodRuntimeRow, ProbeStateRow, SqliteNodeLocalDb};

#[async_trait]
pub trait NodeLocalBackend:
    NodeIdentity
    + OutboxProducerStore
    + OutboxDispatcherStore
    + OutboxStatusStampStore
    + DeadLetterStore
    + PodStatusCheckpointStore
    + RuntimeObservationCheckpointStore
    + ReplicationCheckpointStore
    + PodNetworkCache
    + PodIpamStore
    + PodEndpointStore
    + PodEndpointStoreEventSource
    + Send
    + Sync
{
    fn subscribe_pod_slot_admissions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<PodSlotAdmissionEvent>;
    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult>;
    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotMutationResult>;
    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<PodSlotClearResult>;

    async fn admit_pod_runtime(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
    ) -> Result<()>;
    async fn record_owned_sandbox(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
        sandbox_id: &str,
        created_ms: i64,
    ) -> std::result::Result<(), super::PodRuntimeOwnershipError>;
    async fn record_cgroup(&self, pod_uid: &str, cgroup_path: &str) -> Result<()>;
    async fn delete_pod_runtime_for_uid(&self, pod_uid: &str) -> Result<()>;
    async fn get_pod_runtime(&self, pod_uid: &str) -> Result<Option<PodRuntimeRow>>;
    async fn list_pod_runtime(&self) -> Result<Vec<PodRuntimeRow>>;
    async fn list_pod_runtime_by_namespace(&self, namespace: &str) -> Result<Vec<PodRuntimeRow>>;

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
}

impl PodNetworkCache for SqliteNodeLocalDb {
    fn get_network_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<StorePodNetworkEndpoint>> {
        self.network_state_ref().get_network_for_uid(pod_uid)
    }

    fn get_network_for_pod(
        &self,
        pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<StorePodNetworkEndpoint>> {
        self.network_state_ref().get_network_for_pod(pod)
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<StorePodNetworkEndpoint>> {
        self.network_state_ref().get_network_for_sandbox(sandbox_id)
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: SandboxKey,
        pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<StorePodNetworkEndpoint>> {
        self.network_state_ref()
            .get_network_for_assignment(sandbox_id, pod)
    }

    fn delete_network_for_sandbox(&self, sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()> {
        self.network_state_ref()
            .delete_network_for_sandbox(sandbox_id)
    }

    fn delete_network_if_matches(
        &self,
        request: StorePodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, bool> {
        self.network_state_ref().delete_network_if_matches(request)
    }

    fn list_network_assignments(
        &self,
    ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
        self.network_state_ref().list_network_assignments()
    }
}

impl PodIpamStore for SqliteNodeLocalDb {
    fn reserve_ip_and_insert_network(
        &self,
        request: StorePodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        self.network_state_ref()
            .reserve_ip_and_insert_network(request)
    }
}

impl PodEndpointStore for SqliteNodeLocalDb {
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome> {
        self.network_state_ref().upsert_endpoint(record)
    }

    fn delete_endpoint_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome> {
        self.network_state_ref().delete_endpoint_for_uid(pod_uid)
    }

    fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>> {
        self.network_state_ref().get_endpoint_by_pod_ip(pod_ip)
    }

    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        self.network_state_ref().list_endpoints_all()
    }

    fn list_endpoints_for_node(
        &self,
        node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        self.network_state_ref().list_endpoints_for_node(node_name)
    }
}

impl PodEndpointStoreEventSource for SqliteNodeLocalDb {
    fn subscribe_endpoint_events(&self) -> CacheNetworkFuture<'_, PodEndpointStoreEventStream> {
        self.network_state_ref().subscribe_endpoint_events()
    }
}

#[async_trait]
impl NodeLocalBackend for SqliteNodeLocalDb {
    fn subscribe_pod_slot_admissions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<PodSlotAdmissionEvent> {
        SqliteNodeLocalDb::subscribe_pod_slot_admissions(self)
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        SqliteNodeLocalDb::pod_slot_try_admit(self, namespace, pod_name, pod_uid, node_name).await
    }

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotMutationResult> {
        SqliteNodeLocalDb::pod_slot_mark_terminating(self, namespace, pod_name, pod_uid, node_name)
            .await
    }

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<PodSlotClearResult> {
        SqliteNodeLocalDb::pod_slot_clear_if_uid(self, namespace, pod_name, pod_uid).await
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

    async fn record_owned_sandbox(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
        sandbox_id: &str,
        created_ms: i64,
    ) -> std::result::Result<(), super::PodRuntimeOwnershipError> {
        SqliteNodeLocalDb::record_owned_sandbox(
            self, pod_uid, namespace, pod_name, node_name, sandbox_id, created_ms,
        )
        .await
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
}
