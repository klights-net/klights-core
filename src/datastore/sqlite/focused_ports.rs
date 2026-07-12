use super::*;

#[async_trait::async_trait]
impl crate::datastore::ResourceListStore for Datastore {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> anyhow::Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources_page(
            self,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> anyhow::Result<Vec<(Option<String>, String)>> {
        crate::datastore::DatastoreBackend::list_resource_keys_for_scope(
            self,
            api_version,
            kind,
            namespaced,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::NamespaceContentStore for Datastore {
    async fn list_namespace_resources(&self, namespace: &str) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_of_kind(self, namespace, kind)
            .await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_excluding_kind(
            self, namespace, kind,
        )
        .await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::count_namespace_resources(self, namespace).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::NamespaceStore for Datastore {
    async fn create_namespace(&self, name: &str, data: Value) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::create_namespace(self, name, data).await
    }

    async fn get_namespace(&self, name: &str) -> anyhow::Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_namespace(self, name).await
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> anyhow::Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_namespaces_page(
            self,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace(&self, name: &str) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace(self, name).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace_contents(self, name).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::WatchHistoryStore for Datastore {
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> anyhow::Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_cluster_resources_modified_since(
            self,
            api_version,
            kind,
            since_rv,
        )
        .await
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> anyhow::Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_resources_modified_since(
            self,
            api_version,
            kind,
            namespace,
            since_rv,
        )
        .await
    }

    async fn list_all_watch_events_since(
        &self,
        since_rv: i64,
    ) -> anyhow::Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since(self, since_rv).await
    }

    async fn list_deleted_watch_events_since(
        &self,
        since_rv: i64,
    ) -> anyhow::Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_deleted_watch_events_since(self, since_rv).await
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::advance_resource_version_after(self, min_rv).await
    }

    async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> anyhow::Result<usize> {
        crate::datastore::DatastoreBackend::watch_events_gc_prunable_count(
            self, max_rows, batch_cap,
        )
        .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> anyhow::Result<usize> {
        crate::datastore::DatastoreBackend::gc_watch_events(self, max_rows, batch_cap).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::OwnershipStore for Datastore {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_resources_by_owner_uid(
            self,
            api_version,
            kind,
            namespace,
            owner_uid,
        )
        .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::StatusStore for Datastore {
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_status_only(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
        )
        .await
    }

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_status_only_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::NetworkStore for Datastore {
    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::record_sandbox(
            self, namespace, pod_name, pod_uid, sandbox_id,
        )
        .await
    }

    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_sandbox(self, namespace, pod_name).await
    }

    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_sandbox(self, namespace, pod_name).await
    }

    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_sandbox_for_uid(
            self, namespace, pod_name, pod_uid, sandbox_id,
        )
        .await
    }

    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_network(self, sandbox_id).await
    }

    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        crate::datastore::DatastoreBackend::get_pod_network(self, sandbox_id).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::NetworkMetadataStore for Datastore {
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_sandbox_for_uid(self, namespace, pod_name, pod_uid)
            .await
    }

    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        crate::datastore::DatastoreBackend::get_pod_network_for_pod(
            self, namespace, pod_name, pod_uid,
        )
        .await
    }

    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        crate::datastore::DatastoreBackend::ipam_allocate_and_record_pod_network(
            self,
            sandbox_id,
            pod,
            subnet_base_int,
            subnet_size,
            veth_host,
            netns_path,
        )
        .await
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        crate::datastore::DatastoreBackend::list_sandboxes(self).await
    }

    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        crate::datastore::DatastoreBackend::list_pod_network_sandbox_ids(self).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        crate::datastore::DatastoreBackend::allocate_node_subnet(
            self,
            node_name,
            cluster_cidr,
            node_ip,
        )
        .await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::update_node_peer_attributes(
            self,
            node_name,
            mode,
            hostport_range,
        )
        .await
    }

    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        crate::datastore::DatastoreBackend::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        crate::datastore::DatastoreBackend::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        crate::datastore::DatastoreBackend::list_peer_subnets(self, my_node_name).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_node_subnet(self, node_name).await
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        crate::datastore::DatastoreBackend::pod_endpoint_get_by_pod_ip(self, pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        crate::datastore::DatastoreBackend::pod_endpoint_list_all(self).await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        crate::datastore::DatastoreBackend::subscribe_pod_endpoints(self)
    }
}
