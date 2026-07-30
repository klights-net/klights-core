use async_trait::async_trait;

use super::SqliteNodeLocalDb;
use klights_node_store::{
    CacheNetworkFuture, DeadLetterStore, EndpointDeleteOutcome, EndpointUpsertOutcome,
    NodeIdentity, NodeKey, OutboxDispatcherStore, OutboxProducerStore, OutboxStatusStampStore,
    PodEndpointRecord, PodEndpointStore, PodEndpointStoreEventSource, PodEndpointStoreEventStream,
    PodIpamStore, PodNetworkAllocation,
    PodNetworkAllocationRequest as StorePodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint as StorePodNetworkEndpoint, PodRuntimeStore,
    PodSlotAdmissionEventSource, PodSlotAdmissionStore, PodStatusCheckpointStore, PodUidKey,
    PodWorkqueueStore, ProbeStateStore, ReplicationCheckpointStore,
    RuntimeObservationCheckpointStore, SandboxKey,
};
use klights_types::PodIdentity;

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
    + PodRuntimeStore
    + ProbeStateStore
    + PodWorkqueueStore
    + PodSlotAdmissionStore
    + PodSlotAdmissionEventSource
    + Send
    + Sync
{
    #[cfg(test)]
    async fn enqueue_workqueue(
        &self,
        kind: super::PodWorkqueueKind,
        pod: &PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        let identity = match kind {
            super::PodWorkqueueKind::Pod => {
                klights_node_store::PodWorkIdentity::try_pod(pod.clone())?
            }
            super::PodWorkqueueKind::Namespace => {
                klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
            }
        };
        let entry = klights_node_store::PodWorkqueueEnqueue::try_new(
            identity,
            serde_json::to_vec(&payload)?,
            attempt_count,
            min_delay_ms,
            last_error.map(str::to_string),
        )?;
        self.enqueue_work(entry).await.map_err(anyhow::Error::from)
    }

    #[cfg(test)]
    async fn peek_workqueue_next_due(&self) -> anyhow::Result<Option<i64>> {
        self.peek_next_due_ms().await.map_err(anyhow::Error::from)
    }

    #[cfg(test)]
    async fn claim_workqueue_due(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<Option<super::PodWorkqueueEntry>> {
        let row = self
            .claim_due_work(klights_node_store::DueTimeMs::try_new(now_ms)?)
            .await?;
        row.map(|row| {
            let (id, identity, payload, attempt_count, next_due_ms) = row.into_parts();
            let (kind, pod) = identity.into_persisted();
            Ok(super::PodWorkqueueEntry {
                id: id.get(),
                kind: match kind {
                    klights_node_store::PodWorkqueueKind::Pod => super::PodWorkqueueKind::Pod,
                    klights_node_store::PodWorkqueueKind::Namespace => {
                        super::PodWorkqueueKind::Namespace
                    }
                },
                namespace: pod.namespace,
                name: pod.name,
                uid: pod.uid,
                payload: serde_json::from_slice(&payload)?,
                attempt_count,
                next_attempt_at_ms: next_due_ms.get(),
            })
        })
        .transpose()
    }

    #[cfg(test)]
    async fn complete_workqueue(&self, _id: i64) -> anyhow::Result<()> {
        Ok(())
    }
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
impl NodeLocalBackend for SqliteNodeLocalDb {}
