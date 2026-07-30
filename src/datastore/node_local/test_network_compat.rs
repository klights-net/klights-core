use super::NodeLocalStores;
use klights_node_store::{
    CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey, PodEndpointRecord,
    PodEndpointStore, PodEndpointStoreEventSource, PodEndpointStoreEventStream, PodIpamStore,
    PodNetworkAllocation, PodNetworkAllocationRequest as StorePodNetworkAllocationRequest,
    PodNetworkAssignmentSnapshot, PodNetworkCache, PodNetworkEndpoint as StorePodNetworkEndpoint,
    PodUidKey, SandboxKey,
};
use klights_types::PodIdentity;

impl PodNetworkCache for NodeLocalStores {
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

impl PodIpamStore for NodeLocalStores {
    fn reserve_ip_and_insert_network(
        &self,
        request: StorePodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        self.network_state_ref()
            .reserve_ip_and_insert_network(request)
    }
}

impl PodEndpointStore for NodeLocalStores {
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

impl PodEndpointStoreEventSource for NodeLocalStores {
    fn subscribe_endpoint_events(&self) -> CacheNetworkFuture<'_, PodEndpointStoreEventStream> {
        self.network_state_ref().subscribe_endpoint_events()
    }
}
