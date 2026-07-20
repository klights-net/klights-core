use super::*;

#[async_trait::async_trait]
impl crate::datastore::ResourceStore for Datastore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::create_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            expected_rv,
        )
        .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::CurrentResourceVersionStore for Datastore {
    async fn get_current_resource_version(&self) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::get_current_resource_version(self).await
    }
}

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

    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str) {
        crate::datastore::DatastoreBackend::seed_namespace_for_test(self, name).await
    }

    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> anyhow::Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_namespaces(self, label_selector, field_selector)
            .await
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

    async fn delete_namespace_observed_rv(&self, name: &str) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::delete_namespace_observed_rv(self, name).await
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

    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since_paged(
            self,
            since_rv,
            after_resource_version,
            after_id,
            limit,
        )
        .await
    }

    async fn list_watch_replay_floors(
        &self,
    ) -> anyhow::Result<Vec<crate::datastore::WatchReplayFloor>> {
        crate::datastore::DatastoreBackend::list_watch_replay_floors(self).await
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
        pod: &klights_types::PodIdentity,
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

#[async_trait::async_trait]
impl crate::datastore::PodWorkqueueStore for Datastore {
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_enqueue(
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

    async fn pod_workqueue_peek_next_due(&self) -> anyhow::Result<Option<i64>> {
        crate::datastore::DatastoreBackend::pod_workqueue_peek_next_due(self).await
    }

    async fn pod_workqueue_claim_due(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<Option<PodWorkqueueEntry>> {
        crate::datastore::DatastoreBackend::pod_workqueue_claim_due(self, now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_complete(self, id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_record_failure(
            self,
            row,
            min_delay_ms,
            error,
        )
        .await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_dead_letter(self, id, error).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::ReplicationStore for Datastore {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: crate::datastore::command::StorageCommand,
        meta: crate::datastore::command::CommandMeta,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::apply_replicated_command(self, command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::replace_replicated_resource_state(
            self,
            entries,
            current_rv,
            watch_event_high_water,
            watch_replay_floors,
            metadata,
        )
        .await
    }

    async fn apply_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit_outcome(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> anyhow::Result<klights_cluster_core::CommittedApplyOutcome> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit_outcome(self, commit).await
    }

    async fn current_log_apply_index(&self) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::current_log_apply_index(self).await
    }

    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: crate::datastore::types::ReplicatedCreateOptions,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::apply_replicated_create_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            options,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::DurableRecoveryStore for Datastore {
    async fn read_durable_allocator_observation(
        &self,
    ) -> anyhow::Result<crate::datastore::DurableAllocatorObservation> {
        crate::datastore::DatastoreBackend::read_durable_allocator_observation(self).await
    }

    async fn read_cluster_metadata_observation(
        &self,
    ) -> anyhow::Result<crate::datastore::ClusterMetadataObservation> {
        crate::datastore::DatastoreBackend::read_cluster_metadata_observation(self).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::BackendLifecycleStore for Datastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> anyhow::Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_exclusive_fence(self).await
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> anyhow::Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(self).await
    }

    fn close(&self) {
        crate::datastore::DatastoreBackend::close(self);
    }

    fn attach_raft_proposer(
        &self,
        proposer: std::sync::Arc<dyn crate::datastore::replicated::RaftProposer>,
    ) {
        crate::datastore::DatastoreBackend::attach_raft_proposer(self, proposer);
    }
}

#[cfg(test)]
impl crate::datastore::TestWatchStore for Datastore {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::datastore::DatastoreBackend::subscribe_watch_many(self, topics)
    }

    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        crate::datastore::DatastoreBackend::broadcast_watch_event(self, pending);
    }
}

#[async_trait::async_trait]
impl crate::datastore::ClusterResourceQueryStore for Datastore {
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> anyhow::Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources(
            self,
            api_version,
            kind,
            namespace,
            query,
        )
        .await
    }

    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> anyhow::Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources_for_watch_targets(
            self,
            targets,
            label_selector,
        )
        .await
    }

    async fn list_cluster_resources(&self) -> anyhow::Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_cluster_resources(self).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::LeaderResourceMutationStore for Datastore {
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::update_main_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn apply_resource_batch(
        &self,
        operations: Vec<ResourceBatchOperation>,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::apply_resource_batch(self, operations).await
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<i64> {
        crate::datastore::DatastoreBackend::delete_resource_with_preconditions_observed_rv(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> anyhow::Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::mark_for_delete_without_watch(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> anyhow::Result<Resource> {
        crate::datastore::DatastoreBackend::delete_resource_without_watch_with_tombstone(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await
    }

    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> anyhow::Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::patch_resource_latest(
            self,
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
        )
        .await
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> anyhow::Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::patch_resource_latest_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            request,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::WatchMaintenanceStore for Datastore {
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<crate::datastore::WatchReplayRead<RawWatchEvent>> {
        crate::datastore::DatastoreBackend::list_raw_watch_events_since_checked_bounded(
            self, targets, since_rv, limit,
        )
        .await
    }

    async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> anyhow::Result<SnapshotAtRv> {
        crate::datastore::DatastoreBackend::snapshot_resources_at_rv(
            self,
            api_version,
            kind,
            namespace,
            query,
            snapshot_rv,
        )
        .await
    }

    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_after_id_bounded(
            self, after_id, through_id, limit,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::datastore::PodCleanupStore for Datastore {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::move_pod_to_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> anyhow::Result<Vec<PodCleanupIntent>> {
        crate::datastore::DatastoreBackend::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intents_for_node(self, node_name)
            .await
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> anyhow::Result<PodSlotAdmissionResult> {
        crate::datastore::DatastoreBackend::pod_slot_try_admit(
            self, namespace, pod_name, pod_uid, node_name,
        )
        .await
    }

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_slot_mark_terminating(
            self, namespace, pod_name, pod_uid, node_name,
        )
        .await
    }

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::pod_slot_clear_if_uid(
            self, namespace, pod_name, pod_uid, node_name,
        )
        .await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        crate::datastore::DatastoreBackend::subscribe_pod_slot_admissions(self)
    }
}

#[async_trait::async_trait]
impl crate::datastore::AppliedOutboxStore for Datastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> anyhow::Result<usize> {
        crate::datastore::DatastoreBackend::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> anyhow::Result<Vec<crate::log_apply::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks(self).await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> anyhow::Result<Option<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> anyhow::Result<bool> {
        crate::datastore::DatastoreBackend::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> anyhow::Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> anyhow::Result<bool> {
        crate::datastore::DatastoreBackend::delete_uncommitted_applied_outbox_placeholder(
            self,
            idempotency_key,
            reserved_rv,
        )
        .await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark_effect(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn build_log_apply_commit_for_command(
        &self,
        command: crate::datastore::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> anyhow::Result<crate::log_apply::LogApplyCommit> {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_command(
            self,
            command,
            operation,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox_with_watermark(
            self,
            idempotency_key,
            operation,
            payload,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> anyhow::Result<usize> {
        crate::datastore::DatastoreBackend::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}
