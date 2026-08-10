//! `DatastoreBackend` compatibility impl for `SequencedDatastore`.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, OutboxApplyError as OutboxDeliveryError,
    OutboxApplyOutcome as OutboxDeliveryResult, PatchKind, Resource, ResourceBatchOperation,
    ResourcePatchRequest, ResourcePreconditions, StorageCommand,
};
use serde_json::Value;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use tokio::sync::broadcast;

#[cfg(any(test, feature = "pod-repository-test-support"))]
use crate::datastore::WatchTopic;
use crate::datastore::backend::DatastoreBackend;
use crate::datastore::types::{
    CatchUpResource, ListPageRequest, ResourceList, ResourceListQuery, SnapshotAtRv,
    WatchReplayFloor, WatchReplayRead, WatchTarget,
};
use klights_cluster_datastore::errors::DatastoreError;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_cluster_store::StagedPostCommit;

use super::SequencedDatastore;

const NODE_LEASE_RENEW_OPERATION: &str = "LeaseRenew";

fn command_resource(
    operation: &'static str,
    result: klights_leader_api::ResourceCommandResult,
) -> Result<Resource> {
    match result {
        klights_leader_api::ResourceCommandResult::Resource(resource) => Ok(resource),
        klights_leader_api::ResourceCommandResult::Ack { .. } => Err(anyhow::anyhow!(
            "{operation}: canonical resource command returned an acknowledgement"
        )),
    }
}

fn command_ack(
    operation: &'static str,
    result: klights_leader_api::ResourceCommandResult,
) -> Result<i64> {
    match result {
        klights_leader_api::ResourceCommandResult::Ack { resource_version } => Ok(resource_version),
        klights_leader_api::ResourceCommandResult::Resource(_) => Err(anyhow::anyhow!(
            "{operation}: canonical resource command returned a resource"
        )),
    }
}

fn legacy_outbox_operation(
    operation: &str,
) -> std::result::Result<klights_leader_api::OutboxDeliveryOperation, OutboxDeliveryError> {
    klights_leader_api::OutboxDeliveryOperation::try_from_wire_name(operation)
        .map_err(|error| OutboxDeliveryError::ConflictTerminal(error.to_string()))
}

fn legacy_outbox_error(error: klights_leader_api::OutboxDeliveryError) -> OutboxDeliveryError {
    match error {
        klights_leader_api::OutboxDeliveryError::NotFound(message) => {
            OutboxDeliveryError::NotFound(message)
        }
        klights_leader_api::OutboxDeliveryError::UidMismatch { expected, actual } => {
            OutboxDeliveryError::UidMismatch { expected, actual }
        }
        klights_leader_api::OutboxDeliveryError::InvalidRequest { field, message } => {
            OutboxDeliveryError::ConflictTerminal(format!("invalid {field}: {message}"))
        }
        klights_leader_api::OutboxDeliveryError::ConflictTerminal(message) => {
            OutboxDeliveryError::ConflictTerminal(message)
        }
        other => OutboxDeliveryError::Retryable(other.to_string()),
    }
}

#[async_trait]
impl DatastoreBackend for SequencedDatastore {
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    fn commit_observation_sink(
        &self,
    ) -> std::sync::Arc<dyn crate::datastore::CommitObservationSink> {
        self.passive.commit_observation_sink()
    }

    async fn read_durable_allocator_observation(
        &self,
    ) -> Result<crate::datastore::DurableAllocatorObservation> {
        self.passive.read_durable_allocator_observation().await
    }

    async fn read_cluster_metadata_observation(
        &self,
    ) -> Result<crate::datastore::ClusterMetadataObservation> {
        self.passive.read_cluster_metadata_observation().await
    }
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        self.passive.acquire_snapshot_exclusive_fence().await
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        self.passive.acquire_snapshot_mutation_fence().await
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<klights_watch::WatchEvent> {
        self.passive.subscribe_watch(topic)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> klights_watch::WatchReceiver {
        self.passive.subscribe_watch_many(topics)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        self.passive.broadcast_watch_event(pending);
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        let command = StorageCommand::CreateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
        };
        command_resource(
            "create_resource",
            self.submit_resource_command(command).await?,
        )
    }
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.passive
            .get_resource(api_version, kind, namespace, name)
            .await
    }
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        self.passive
            .list_resources(api_version, kind, namespace, query)
            .await
    }
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.passive
            .list_resources_page(
                api_version,
                kind,
                namespace,
                label_selector,
                field_selector,
                page,
            )
            .await
    }
    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.passive
            .list_resources_for_watch_targets(targets, label_selector)
            .await
    }
    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        self.passive
            .list_resource_keys_for_scope(api_version, kind, namespaced)
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
    ) -> Result<Resource> {
        let preconditions = ResourcePreconditions {
            uid: None,
            resource_version: Some(expected_rv),
        };
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        command_resource(
            "update_resource",
            self.submit_resource_command(command).await?,
        )
    }
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_rv = preconditions.resource_version.unwrap_or(0);
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        command_resource(
            "update_resource_with_preconditions",
            self.submit_resource_command(command).await?,
        )
    }
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_rv = preconditions.resource_version.unwrap_or(0);
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
            expected_rv,
            preconditions,
        };
        command_resource(
            "update_main_resource_with_preconditions",
            self.submit_resource_command(command).await?,
        )
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let command = StorageCommand::ApplyResourceBatch { operations };
        command_ack(
            "apply_resource_batch",
            self.submit_resource_command(command).await?,
        )?;
        Ok(())
    }

    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let command = StorageCommand::UpdateStatus {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            status,
            expected_rv,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: expected_rv,
            },
            observed_status_stamp: None,
        };
        command_resource(
            "update_status_only",
            self.submit_resource_command(command).await?,
        )
    }
    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_rv = preconditions.resource_version;
        let command = StorageCommand::UpdateStatus {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            status,
            expected_rv,
            preconditions,
            observed_status_stamp: None,
        };
        command_resource(
            "update_status_only_with_preconditions",
            self.submit_resource_command(command).await?,
        )
    }

    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>> {
        let Some(current) = self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
        else {
            return Err(DatastoreError::not_found(format!(
                "mark_for_delete_without_watch: {api_version}/{kind}/{name} not found"
            ))
            .into());
        };

        let mut current_data = (*current.data).clone();
        let Some(metadata) = current_data
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
        else {
            return Err(anyhow::anyhow!(
                "mark_for_delete_without_watch: {api_version}/{kind}/{name} is missing metadata"
            ));
        };
        if metadata
            .get("deletionTimestamp")
            .and_then(|timestamp| timestamp.as_str())
            .is_some_and(|timestamp| !timestamp.is_empty())
        {
            return Ok(Some(current));
        }

        crate::control_plane::client::local::ensure_mark_delete_timestamps(
            &mut current_data,
            grace_seconds,
            self.wall_clock.now_utc(),
        );
        let expected_rv = preconditions.resource_version.unwrap_or(0);
        let command = StorageCommand::UpdateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data: current_data,
            expected_rv,
            preconditions,
        };
        command_resource(
            "mark_for_delete_without_watch",
            self.submit_resource_command(command).await?,
        )
        .map(Some)
    }
    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        self.delete_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::default(),
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
    ) -> Result<()> {
        self.delete_resource_with_preconditions_observed_rv(
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
        .map(|_| ())
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        let command = StorageCommand::DeleteResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            preconditions,
        };
        if api_version == "v1" && kind == "Pod" {
            return command_ack(
                "delete_resource_with_preconditions_observed_rv",
                self.submit_resource_command(command).await?,
            );
        }
        command_ack(
            "delete_resource_with_preconditions_observed_rv",
            self.submit_resource_command(command).await?,
        )
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource> {
        let command = StorageCommand::DeleteResourceWithTombstone {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            preconditions,
            grace_seconds,
        };
        command_resource(
            "delete_resource_without_watch_with_tombstone",
            self.submit_resource_command(command).await?,
        )
    }

    async fn get_current_resource_version(&self) -> Result<i64> {
        self.passive.get_current_resource_version().await
    }
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        let command = StorageCommand::CreateNamespace {
            name: name.to_string(),
            data: data.clone(),
        };
        command_resource(
            "create_namespace",
            self.submit_resource_command(command).await?,
        )
    }
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        self.passive.get_namespace(name).await
    }
    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.passive
            .list_namespaces(label_selector, field_selector)
            .await
    }
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.passive
            .list_namespaces_page(label_selector, field_selector, page)
            .await
    }
    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let command = StorageCommand::UpdateNamespace {
            name: name.to_string(),
            data: data.clone(),
            expected_rv,
        };
        command_resource(
            "update_namespace",
            self.submit_resource_command(command).await?,
        )
    }
    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        let command = StorageCommand::DeleteNamespaceContents {
            name: name.to_string(),
        };
        command_ack(
            "delete_namespace_contents",
            self.submit_resource_command(command).await?,
        )
        .map(|_| ())
    }
    async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.delete_namespace_observed_rv(name).await.map(|_| ())
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        let command = StorageCommand::DeleteNamespace {
            name: name.to_string(),
        };
        command_ack(
            "delete_namespace_observed_rv",
            self.submit_resource_command(command).await?,
        )
    }
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.passive
            .find_owned_resources(owner_uid, namespace)
            .await
    }
    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        self.passive
            .list_resources_by_owner_uid(api_version, kind, namespace, owner_uid)
            .await
    }
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        self.passive
            .find_owned_by_name_kind_empty_uid(owner_api_version, owner_name, owner_kind, namespace)
            .await
    }
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.passive
            .list_cluster_resources_modified_since(api_version, kind, since_rv)
            .await
    }
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.passive.list_cluster_resources().await
    }
    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.passive
            .list_resources_modified_since(api_version, kind, namespace, since_rv)
            .await
    }
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        klights_cluster_store::ClusterWatchMaintenance::advance_resource_version_after(
            self.maintenance.as_ref(),
            min_rv,
        )
        .await
    }
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.passive.list_namespace_resources(namespace).await
    }
    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.passive
            .list_namespace_resources_of_kind(namespace, kind)
            .await
    }
    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.passive
            .list_namespace_resources_excluding_kind(namespace, kind)
            .await
    }
    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        self.passive.count_namespace_resources(namespace).await
    }
    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.passive
            .list_watch_events_since(targets, since_rv)
            .await
    }

    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        self.passive
            .list_watch_events_since_checked(targets, since_rv)
            .await
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        self.passive
            .list_watch_events_since_checked_bounded(targets, since_rv, limit)
            .await
    }

    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        self.passive
            .list_raw_watch_events_since_checked_bounded(targets, since_rv, limit)
            .await
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        self.passive.earliest_watch_event_rv().await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.passive.list_all_watch_events_since(since_rv).await
    }

    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.passive
            .list_all_watch_events_since_paged(since_rv, after_resource_version, after_id, limit)
            .await
    }

    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.passive
            .list_all_watch_events_after_id_bounded(after_id, through_id, limit)
            .await
    }

    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        self.passive.list_watch_replay_floors().await
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        self.passive
            .list_watch_replay_floors_paged(after, limit)
            .await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.passive.list_deleted_watch_events_since(since_rv).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        use klights_leader_api::LeaderNodeSubnetAllocation as _;
        let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            node_name,
            cluster_cidr,
            node_ip,
        )?;
        let result = self.network.allocate_node_subnet(request).await?;
        crate::control_plane::client::legacy_node_subnet(result.into_subnet())
            .map_err(anyhow::Error::new)
    }
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        self.network
            .update_node_peer_attributes(node_name, mode, hostport_range)
            .await
            .map_err(anyhow::Error::new)
    }
    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        use klights_leader_api::LeaderNetworkTopologyCommand as _;
        let metadata = crate::control_plane::client::focused_dataplane(metadata)?;
        self.network
            .register_node_dataplane(metadata)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        use klights_leader_api::LeaderNetworkTopologyQuery as _;
        let request = klights_leader_api::NodeDataplaneQuery::try_new(node_name)?;
        self.network
            .get_node_dataplane(request)
            .await?
            .into_option()
            .map(crate::control_plane::client::legacy_dataplane)
            .transpose()
            .map_err(anyhow::Error::new)
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        use klights_leader_api::LeaderNetworkTopologyQuery as _;
        let request = klights_leader_api::NodeSubnetQuery::try_new(node_name)?;
        self.network
            .get_node_subnet(request)
            .await?
            .into_option()
            .map(crate::control_plane::client::legacy_node_subnet)
            .transpose()
            .map_err(anyhow::Error::new)
    }
    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        use klights_leader_api::LeaderNetworkTopologyQuery as _;
        let node_name = request
            .excluded_node_name()
            .ok_or_else(|| anyhow::anyhow!("focused peer query requires an excluded node"))?;
        let request = klights_leader_api::PeerSubnetsQuery::try_new(node_name.as_str())?;
        self.network
            .list_peer_subnets(request)
            .await?
            .into_vec()
            .into_iter()
            .map(crate::control_plane::client::legacy_node_subnet)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)
    }
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        self.network
            .delete_node_subnet(node_name)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.pod_cleanup
            .move_intent(node_name, namespace, pod_name, pod_uid, reason)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        use klights_leader_api::LeaderPodCleanupIntents as _;
        let request = klights_leader_api::PodCleanupIntentListRequest::try_new(node_name)?;
        let intents = self
            .pod_cleanup
            .list_pod_cleanup_intents(request)
            .await?
            .into_iter()
            .map(crate::control_plane::client::local::legacy_pod_cleanup_intent)
            .collect::<Vec<_>>();
        Ok(intents)
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        use klights_leader_api::LeaderPodCleanupIntents as _;
        let request = klights_leader_api::PodCleanupIntentAckRequest::try_new(
            node_name, namespace, pod_name, pod_uid, reason,
        )?;
        self.pod_cleanup
            .acknowledge_pod_cleanup_intent(request)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        self.pod_cleanup
            .delete_all_for_node(node_name)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>> {
        let command = StorageCommand::PatchResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            patch_kind,
            patch: patch.clone(),
            preconditions: ResourcePreconditions::default(),
            strict_resource_version: false,
        };
        command_resource(
            "patch_resource_latest",
            self.submit_resource_command(command).await?,
        )
        .map(Some)
    }
    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let ResourcePatchRequest {
            patch_kind,
            patch,
            preconditions,
            strict_resource_version,
        } = request;
        let command = StorageCommand::PatchResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            patch_kind,
            patch: patch.clone(),
            preconditions: preconditions.clone(),
            strict_resource_version,
        };
        command_resource(
            "patch_resource_latest_with_preconditions",
            self.submit_resource_command(command).await?,
        )
        .map(Some)
    }
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        klights_cluster_store::ClusterWatchMaintenance::watch_events_gc_prunable_count(
            self.maintenance.as_ref(),
            max_rows,
            batch_cap,
        )
        .await
    }
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        klights_cluster_store::ClusterWatchMaintenance::gc_watch_events(
            self.maintenance.as_ref(),
            max_rows,
            batch_cap,
        )
        .await
    }
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        klights_cluster_store::ClusterMetadataMutation::get_klights_meta(
            self.maintenance.as_ref(),
            key,
        )
        .await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        klights_cluster_store::ClusterMetadataMutation::set_klights_meta(
            self.maintenance.as_ref(),
            key,
            value,
        )
        .await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.passive.list_outbox_stream_watermarks().await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.passive
            .list_outbox_stream_watermarks_paged(after, limit)
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        self.passive.get_applied_outbox(idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        self.passive.insert_applied_outbox(record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        self.passive.list_applied_outbox().await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
        if operation == NODE_LEASE_RENEW_OPERATION {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxDeliveryError::ConflictTerminal(err.to_string()))?;
            return Ok(OutboxDeliveryResult::Applied { applied_rv: 0 });
        }
        self.outbox_delivery
            .deliver_authenticated_outbox_command_effect(
                authoring_node.to_string(),
                idempotency_key.to_string(),
                legacy_outbox_operation(operation)?,
                Ok(command),
                None,
            )
            .await
            .map(|effect| effect.into_parts().0)
            .map_err(legacy_outbox_error)
    }

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
        if operation == NODE_LEASE_RENEW_OPERATION {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxDeliveryError::ConflictTerminal(err.to_string()))?;
            return Ok(OutboxDeliveryResult::Applied { applied_rv: 0 });
        }
        self.outbox_delivery
            .deliver_authenticated_outbox_command_effect(
                authoring_node.to_string(),
                idempotency_key.to_string(),
                legacy_outbox_operation(operation)?,
                Ok(command),
                watermark,
            )
            .await
            .map(|effect| effect.into_parts().0)
            .map_err(legacy_outbox_error)
    }

    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<crate::datastore::CommittedOutboxApply, OutboxDeliveryError> {
        if operation == NODE_LEASE_RENEW_OPERATION {
            klights_cluster_core::validate_lease_renew_command(&command, authoring_node)
                .map_err(|err| OutboxDeliveryError::ConflictTerminal(err.to_string()))?;
            return Ok(crate::datastore::CommittedOutboxApply::new(
                OutboxDeliveryResult::Applied { applied_rv: 0 },
                klights_cluster_core::ResourceMutationEffect::Unchanged,
                klights_cluster_core::PodEndpointEffect::NotApplicable,
            ));
        }
        let effect = self
            .outbox_delivery
            .deliver_authenticated_outbox_command_effect(
                authoring_node.to_string(),
                idempotency_key.to_string(),
                legacy_outbox_operation(operation)?,
                Ok(command),
                watermark,
            )
            .await
            .map_err(legacy_outbox_error)?;
        let (result, resource_effect, pod_endpoint_effect, committed_resource) =
            effect.into_parts();
        Ok(crate::datastore::CommittedOutboxApply::new(
            result,
            resource_effect,
            pod_endpoint_effect,
        )
        .with_committed_resource(committed_resource))
    }

    async fn build_log_apply_commit_for_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        self.passive
            .build_log_apply_commit_for_command(command, operation, authoring_node)
            .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<klights_cluster_core::BuildOutboxOutcome, OutboxDeliveryError> {
        self.passive
            .build_log_apply_commit_for_outbox(idempotency_key, operation, command, authoring_node)
            .await
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<klights_cluster_core::BuildOutboxOutcome, OutboxDeliveryError> {
        self.passive
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        self.maintenance.gc_applied_outbox(now_ms, ttl_ms).await
    }
}

#[async_trait]
impl crate::datastore::ResourceStore for SequencedDatastore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
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
    ) -> Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
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
    ) -> Result<()> {
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
    ) -> Result<Resource> {
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
    ) -> Result<Resource> {
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

#[async_trait]
impl crate::datastore::CurrentResourceVersionStore for SequencedDatastore {
    async fn get_current_resource_version(&self) -> Result<i64> {
        crate::datastore::DatastoreBackend::get_current_resource_version(self).await
    }
}

#[async_trait]
impl crate::datastore::ResourceListStore for SequencedDatastore {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
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
    ) -> Result<Vec<(Option<String>, String)>> {
        crate::datastore::DatastoreBackend::list_resource_keys_for_scope(
            self,
            api_version,
            kind,
            namespaced,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::NamespaceStore for SequencedDatastore {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        crate::datastore::DatastoreBackend::create_namespace(self, name, data).await
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        crate::datastore::DatastoreBackend::get_namespace(self, name).await
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    async fn seed_namespace_for_test(&self, name: &str) {
        crate::datastore::DatastoreBackend::seed_namespace_for_test(self, name).await
    }

    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_namespaces(self, label_selector, field_selector)
            .await
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
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
    ) -> Result<Resource> {
        crate::datastore::DatastoreBackend::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace(self, name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        crate::datastore::DatastoreBackend::delete_namespace_observed_rv(self, name).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_namespace_contents(self, name).await
    }
}

#[async_trait]
impl crate::datastore::WatchHistoryStore for SequencedDatastore {
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
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
    ) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_resources_modified_since(
            self,
            api_version,
            kind,
            namespace,
            since_rv,
        )
        .await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since(self, since_rv).await
    }

    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_since_paged(
            self,
            since_rv,
            after_resource_version,
            after_id,
            limit,
        )
        .await
    }

    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        crate::datastore::DatastoreBackend::list_watch_replay_floors(self).await
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        crate::datastore::DatastoreBackend::list_watch_replay_floors_paged(self, after, limit).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        crate::datastore::DatastoreBackend::list_deleted_watch_events_since(self, since_rv).await
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        crate::datastore::DatastoreBackend::advance_resource_version_after(self, min_rv).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::watch_events_gc_prunable_count(
            self, max_rows, batch_cap,
        )
        .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::gc_watch_events(self, max_rows, batch_cap).await
    }
}

#[async_trait]
impl crate::datastore::NamespaceContentStore for SequencedDatastore {
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_of_kind(self, namespace, kind)
            .await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_namespace_resources_excluding_kind(
            self, namespace, kind,
        )
        .await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        crate::datastore::DatastoreBackend::count_namespace_resources(self, namespace).await
    }
}

#[async_trait]
impl crate::datastore::OwnershipStore for SequencedDatastore {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
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
    ) -> Result<Vec<Resource>> {
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

#[async_trait]
impl crate::datastore::StatusStore for SequencedDatastore {
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
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
    ) -> Result<Resource> {
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

#[async_trait]
impl crate::datastore::MetaStore for SequencedDatastore {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_klights_meta(self, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::set_klights_meta(self, key, value).await
    }
}

#[async_trait]
impl crate::datastore::NetworkMetadataStore for SequencedDatastore {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
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
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
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
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        crate::datastore::DatastoreBackend::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        crate::datastore::DatastoreBackend::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        crate::datastore::DatastoreBackend::list_peer_subnets(self, request).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_node_subnet(self, node_name).await
    }
}

#[async_trait]
impl klights_cluster_store::BackendLifecycleStore for SequencedDatastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_exclusive_fence(self).await
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(self).await
    }

    fn close(&self) {
        crate::datastore::DatastoreBackend::close(self);
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl crate::datastore::TestWatchStore for SequencedDatastore {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> klights_watch::WatchReceiver {
        crate::datastore::DatastoreBackend::subscribe_watch_many(self, topics)
    }

    fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        crate::datastore::DatastoreBackend::broadcast_watch_event(self, pending);
    }
}

#[async_trait]
impl crate::datastore::ClusterResourceQueryStore for SequencedDatastore {
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
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
    ) -> Result<ResourceList> {
        crate::datastore::DatastoreBackend::list_resources_for_watch_targets(
            self,
            targets,
            label_selector,
        )
        .await
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        crate::datastore::DatastoreBackend::list_cluster_resources(self).await
    }
}

#[async_trait]
impl crate::datastore::LeaderResourceMutationStore for SequencedDatastore {
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
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

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_resource_batch(self, operations).await
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
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
    ) -> Result<Option<Resource>> {
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
    ) -> Result<Resource> {
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
    ) -> Result<Option<Resource>> {
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
    ) -> Result<Option<Resource>> {
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

#[async_trait]
impl crate::datastore::WatchMaintenanceStore for SequencedDatastore {
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::datastore::WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>>
    {
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
    ) -> Result<SnapshotAtRv> {
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
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        crate::datastore::DatastoreBackend::list_all_watch_events_after_id_bounded(
            self, after_id, through_id, limit,
        )
        .await
    }
}

#[async_trait]
impl crate::datastore::PodCleanupStore for SequencedDatastore {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::move_pod_to_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        crate::datastore::DatastoreBackend::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_pod_cleanup_intents_for_node(self, node_name)
            .await
    }
}

#[async_trait]
impl crate::datastore::AppliedOutboxStore for SequencedDatastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks(self).await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks_paged(self, after, limit)
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        crate::datastore::DatastoreBackend::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        crate::datastore::DatastoreBackend::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        crate::datastore::DatastoreBackend::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        crate::datastore::DatastoreBackend::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<crate::datastore::CommittedOutboxApply, OutboxDeliveryError> {
        crate::datastore::DatastoreBackend::apply_outbox_transactionally_with_watermark_effect(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn build_log_apply_commit_for_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
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
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<klights_cluster_core::BuildOutboxOutcome, OutboxDeliveryError> {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
        )
        .await
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<klights_cluster_core::BuildOutboxOutcome, OutboxDeliveryError> {
        crate::datastore::DatastoreBackend::build_log_apply_commit_for_outbox_with_watermark(
            self,
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}
