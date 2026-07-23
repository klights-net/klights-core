//! `DatastoreBackend` compatibility impl for `SequencedDatastore`.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::net::Ipv4Addr;
use tokio::sync::broadcast;

use crate::datastore::backend::DatastoreBackend;
#[cfg(test)]
use crate::datastore::command::CommandMeta;
use crate::datastore::command::StorageCommand;
use crate::datastore::errors::DatastoreError;
use crate::datastore::types::*;
use klights_watch::WatchTopic;

use super::SequencedDatastore;
#[cfg(test)]
use super::apply_command_to_backend;

fn ensure_mark_delete_timestamps(data: &mut Value, grace_seconds: i64) {
    let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    if metadata
        .get("deletionTimestamp")
        .and_then(|timestamp| timestamp.as_str())
        .is_none_or(str::is_empty)
    {
        metadata.insert(
            "deletionTimestamp".to_string(),
            Value::String(crate::utils::k8s_timestamp()),
        );
    }
    metadata
        .entry("deletionGracePeriodSeconds".to_string())
        .or_insert_with(|| Value::from(grace_seconds));
}

fn reject_application_committed_apply<T>(operation: &'static str) -> Result<T> {
    Err(anyhow::anyhow!(
        "sequenced datastore rejects application-side committed apply `{operation}`; \
         this operation is reserved for the private passive Raft state-machine backend"
    ))
}

#[async_trait]
impl DatastoreBackend for SequencedDatastore {
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

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        if true {
            self.passive.subscribe_watch_signals(topic)
        } else {
            klights_watch::WatchSignalReceiver::closed()
        }
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        self.passive.subscribe_watch(topic)
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        self.passive.subscribe_watch_many(topics)
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
        self.passive.broadcast_watch_event(pending);
    }

    async fn replace_replicated_resource_state(
        &self,
        _entries: Vec<crate::log_apply::LogApplyCommit>,
        _current_rv: i64,
        _watch_event_high_water: Option<i64>,
        _watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        _metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        reject_application_committed_apply("replace_replicated_resource_state")
    }

    async fn apply_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<()> {
        reject_application_committed_apply("apply_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        reject_application_committed_apply("apply_raft_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit_outcome(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<klights_cluster_core::CommittedApplyOutcome> {
        reject_application_committed_apply("apply_raft_log_apply_commit_outcome")
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
    ) -> Result<Resource> {
        if api_version == "v1"
            && kind == "Pod"
            && crate::datastore::pod_serviceaccount::should_inject_serviceaccount_volume(
                self.passive.as_ref(),
                &data,
                namespace,
            )
            .await
        {
            crate::datastore::pod_serviceaccount::inject_serviceaccount_volume(&mut data);
        }
        let command = StorageCommand::CreateResource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            data,
        };
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed create_resource: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_resource: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_resource_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_main_resource_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }
        let command = StorageCommand::ApplyResourceBatch { operations };
        self.propose_command(command).await?;
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
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_status_only: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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
        self.propose_command(command).await?;
        self
            .passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed update_status_only_with_preconditions: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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

        ensure_mark_delete_timestamps(&mut current_data, grace_seconds);
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
        self.propose_command(command).await?;
        self.passive
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed mark_for_delete_without_watch: row missing after commit for {api_version}/{kind}/{name}"
                )
            })
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
        self.propose_command(command).await?;
        Ok(self
            .passive
            .get_current_resource_version()
            .await
            .unwrap_or(0))
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
        let applied = self.propose_command(command).await?;
        match applied.applied_mutation {
            Some(crate::datastore::raft::types::AppliedMutation::Resource(resource)) => {
                Ok(resource)
            }
            None => Err(anyhow::anyhow!(
                "raft-routed delete_resource_without_watch_with_tombstone: committed tombstone result missing for {api_version}/{kind}/{name}"
            )),
        }
    }

    async fn get_current_resource_version(&self) -> Result<i64> {
        self.passive.get_current_resource_version().await
    }
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        let command = StorageCommand::CreateNamespace {
            name: name.to_string(),
            data: data.clone(),
        };
        self.propose_command(command).await?;
        self.passive.get_namespace(name).await?.ok_or_else(|| {
            anyhow::anyhow!("raft-routed create_namespace: row missing after commit for {name}")
        })
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
        self.propose_command(command).await?;
        self.passive.get_namespace(name).await?.ok_or_else(|| {
            anyhow::anyhow!("raft-routed update_namespace: row missing after commit for {name}")
        })
    }
    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        let command = StorageCommand::DeleteNamespaceContents {
            name: name.to_string(),
        };
        self.propose_command(command).await?;
        Ok(())
    }
    async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.delete_namespace_observed_rv(name).await.map(|_| ())
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        let command = StorageCommand::DeleteNamespace {
            name: name.to_string(),
        };
        self.propose_command(command).await?;
        self.passive.get_current_resource_version().await
    }
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.passive
            .pod_workqueue_enqueue(kind, pod, payload, attempt_count, min_delay_ms, last_error)
            .await
    }
    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.passive.pod_workqueue_peek_next_due().await
    }
    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.passive.pod_workqueue_claim_due(now_ms).await
    }
    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.passive.pod_workqueue_complete(id).await
    }
    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        self.passive
            .pod_workqueue_record_failure(row, min_delay_ms, error)
            .await
    }
    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()> {
        self.passive.pod_workqueue_dead_letter(id, error).await
    }
    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.passive
            .record_sandbox(namespace, pod_name, pod_uid, sandbox_id)
            .await
    }
    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        self.passive.get_sandbox(namespace, pod_name).await
    }
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        self.passive
            .get_sandbox_for_uid(namespace, pod_name, pod_uid)
            .await
    }
    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        self.passive.delete_sandbox(namespace, pod_name).await
    }
    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.passive
            .delete_sandbox_for_uid(namespace, pod_name, pod_uid, sandbox_id)
            .await
    }
    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        self.passive.delete_pod_network(sandbox_id).await
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
        let before_rv = self
            .passive
            .get_current_resource_version()
            .await
            .unwrap_or(0);
        let new_rv = before_rv.saturating_add(1).max(min_rv.saturating_add(1));
        self.propose_command(StorageCommand::AdvanceResourceVersion { min_rv, new_rv })
            .await?;
        Ok(self
            .passive
            .get_current_resource_version()
            .await
            .unwrap_or(new_rv))
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

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        self.passive
            .list_watch_events_after_position_checked_bounded(targets, position, limit)
            .await
    }

    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.passive.current_watch_replay_position().await
    }

    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        self.passive
            .snapshot_resources_at_position(targets, label_selector, field_selector, position)
            .await
    }

    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<RawWatchEvent>> {
        self.passive
            .list_raw_watch_events_since_checked_bounded(targets, since_rv, limit)
            .await
    }

    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        self.passive
            .list_raw_watch_events_after_position_checked_bounded(targets, position, limit)
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

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.passive.list_deleted_watch_events_since(since_rv).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.propose_command(StorageCommand::AllocateNodeSubnet {
            node_name: node_name.to_string(),
            subnet: cluster_cidr.to_string(),
            node_ip: node_ip.to_string(),
        })
        .await?;
        self.passive
            .get_node_subnet(node_name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "raft-routed allocate_node_subnet: row missing after commit for {node_name}"
                )
            })
    }
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        let mode_value = match mode {
            crate::controllers::annotations::NodePeerMode::Root => "root",
            crate::controllers::annotations::NodePeerMode::Rootless => "rootless",
        }
        .to_string();
        self.propose_command(StorageCommand::UpdateNodePeerAttributes {
            node_name: node_name.to_string(),
            mode: mode_value,
            hostport_range: hostport_range.map(|range| range.to_string()),
        })
        .await?;
        Ok(())
    }
    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        let command = StorageCommand::UpdateNodeDataplane {
            node_name: metadata.node_name.clone(),
            mode: metadata.mode.as_str().to_string(),
            encryption: metadata.encryption.as_str().to_string(),
            public_key: metadata.public_key.as_ref().map(ToString::to_string),
            endpoint: metadata.endpoint.to_string(),
            port: metadata.port,
        };
        self.propose_command(command).await?;
        Ok(())
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        self.passive.get_node_dataplane(node_name).await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.passive.get_node_subnet(node_name).await
    }
    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.passive.list_peer_subnets(my_node_name).await
    }
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        self.propose_command(StorageCommand::DeleteNodeSubnet {
            node_name: node_name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.propose_command(StorageCommand::MovePodToCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.passive
            .list_pod_cleanup_intents_for_node(node_name)
            .await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.propose_command(StorageCommand::DeletePodCleanupIntent {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        self.propose_command(StorageCommand::DeletePodCleanupIntentsForNode {
            node_name: node_name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        self.passive
            .pod_slot_try_admit(namespace, pod_name, pod_uid, node_name)
            .await
    }

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        self.passive
            .pod_slot_mark_terminating(namespace, pod_name, pod_uid, node_name)
            .await
    }

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()> {
        self.passive
            .pod_slot_clear_if_uid(namespace, pod_name, pod_uid, node_name)
            .await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        if true {
            self.passive.subscribe_pod_slot_admissions()
        } else {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
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
        self.propose_command(command).await?;
        self.passive
            .get_resource(api_version, kind, namespace, name)
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
        self.propose_command(command).await?;
        self.passive
            .get_resource(api_version, kind, namespace, name)
            .await
    }
    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        self.passive.get_pod_network(sandbox_id).await
    }
    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.passive
            .get_pod_network_for_pod(namespace, pod_name, pod_uid)
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
        self.passive
            .ipam_allocate_and_record_pod_network(
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
        self.passive.list_sandboxes().await
    }
    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.passive.list_pod_network_sandbox_ids().await
    }
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.passive
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await
    }
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        let prunable = self
            .passive
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.propose_command(StorageCommand::GcWatchEvents {
            max_rows,
            batch_cap,
        })
        .await?;
        Ok(prunable)
    }
    async fn pod_endpoint_get_by_pod_ip(&self, pod_ip: Ipv4Addr) -> Result<Option<PodEndpointRow>> {
        self.passive.pod_endpoint_get_by_pod_ip(pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.passive.pod_endpoint_list_all().await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        if true {
            self.passive.subscribe_pod_endpoints()
        } else {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.passive.get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        let command = StorageCommand::SetKlightsMeta {
            key: key.to_string(),
            value: value.to_string(),
        };
        self.propose_command(command).await?;
        Ok(())
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<crate::log_apply::OutboxStreamWatermark>> {
        self.passive.list_outbox_stream_watermarks().await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        self.passive.get_applied_outbox(idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        self.passive.insert_applied_outbox(record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        self.passive.list_applied_outbox().await
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> Result<bool> {
        self.passive
            .delete_uncommitted_applied_outbox_placeholder(idempotency_key, reserved_rv)
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
        let command = match crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(payload)
        {
            Ok(payload) => payload.command,
            Err(err) => {
                return Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
                    err.to_string(),
                ));
            }
        };
        if operation == crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&command, authoring_node)
                .map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(err.to_string())
                })?;
            return Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv: 0 });
        }
        self.proposal
            .propose_outbox_command(idempotency_key, operation, command, authoring_node, None)
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
        let payload_decoded = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(
            payload,
        )
        .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let command = payload_decoded.command;
        if operation == crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&command, authoring_node)
                .map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(err.to_string())
                })?;
            return Ok(crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv: 0 });
        }
        self.proposal
            .propose_outbox_command(
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
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let payload_decoded = crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(
            payload,
        )
        .map_err(|err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()))?;
        let command = payload_decoded.command;
        if operation == crate::kubelet::outbox::payload::OutboxOperation::LeaseRenew.as_str() {
            crate::node_lease_tracker::ensure_lease_renew_command(&command, authoring_node)
                .map_err(|err| {
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(err.to_string())
                })?;
            return Ok(crate::datastore::CommittedOutboxApply::new(
                crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv: 0 },
                crate::datastore::ResourceMutationEffect::Unchanged,
                crate::datastore::PodEndpointEffect::NotApplicable,
            ));
        }
        self.proposal
            .propose_outbox_command_effect(
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
    ) -> Result<crate::log_apply::LogApplyCommit> {
        self.passive
            .build_log_apply_commit_for_command(command, operation, authoring_node)
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
        self.passive
            .build_log_apply_commit_for_outbox(idempotency_key, operation, payload, authoring_node)
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
        self.passive
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation,
                payload,
                authoring_node,
                watermark,
            )
            .await
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        let cutoff_ms = now_ms.saturating_sub(ttl_ms);
        let prunable = self
            .passive
            .applied_outbox_gc_prunable_count(cutoff_ms)
            .await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.propose_command(StorageCommand::GcAppliedOutbox { cutoff_ms })
            .await?;
        Ok(prunable)
    }

    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        apply_command_to_backend(self.passive.as_ref(), command, meta).await
    }

    async fn current_log_apply_index(&self) -> Result<i64> {
        self.passive.current_log_apply_index().await
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

    #[cfg(test)]
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
impl crate::datastore::NetworkStore for SequencedDatastore {
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

    async fn get_pod_network(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<crate::datastore::PodNetworkEndpoint>> {
        crate::datastore::DatastoreBackend::get_pod_network(self, sandbox_id).await
    }
}

#[async_trait]
impl crate::datastore::NetworkMetadataStore for SequencedDatastore {
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
    ) -> Result<Option<crate::datastore::PodNetworkEndpoint>> {
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

    async fn list_sandboxes(&self) -> Result<Vec<crate::datastore::SandboxRef>> {
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
    ) -> Result<crate::datastore::NodeSubnet> {
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

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::datastore::NodeSubnet>> {
        crate::datastore::DatastoreBackend::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(
        &self,
        my_node_name: &str,
    ) -> Result<Vec<crate::datastore::NodeSubnet>> {
        crate::datastore::DatastoreBackend::list_peer_subnets(self, my_node_name).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::delete_node_subnet(self, node_name).await
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<crate::datastore::PodEndpointRow>> {
        crate::datastore::DatastoreBackend::pod_endpoint_get_by_pod_ip(self, pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<crate::datastore::PodEndpointRow>> {
        crate::datastore::DatastoreBackend::pod_endpoint_list_all(self).await
    }

    fn subscribe_pod_endpoints(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::datastore::PodEndpointEvent> {
        crate::datastore::DatastoreBackend::subscribe_pod_endpoints(self)
    }
}

#[async_trait]
impl crate::datastore::PodWorkqueueStore for SequencedDatastore {
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
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

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        crate::datastore::DatastoreBackend::pod_workqueue_peek_next_due(self).await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        crate::datastore::DatastoreBackend::pod_workqueue_claim_due(self, now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_complete(self, id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_record_failure(
            self,
            row,
            min_delay_ms,
            error,
        )
        .await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::pod_workqueue_dead_letter(self, id, error).await
    }
}

#[async_trait]
impl crate::datastore::ReplicationStore for SequencedDatastore {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_replicated_command(self, command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        _entries: Vec<crate::log_apply::LogApplyCommit>,
        _current_rv: i64,
        _watch_event_high_water: Option<i64>,
        _watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        _metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        reject_application_committed_apply("replace_replicated_resource_state")
    }

    async fn apply_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<()> {
        reject_application_committed_apply("apply_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        reject_application_committed_apply("apply_raft_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit_outcome(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<klights_cluster_core::CommittedApplyOutcome> {
        reject_application_committed_apply("apply_raft_log_apply_commit_outcome")
    }

    async fn current_log_apply_index(&self) -> Result<i64> {
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
        options: ReplicatedCreateOptions,
    ) -> Result<Resource> {
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

#[async_trait]
impl crate::datastore::DurableRecoveryStore for SequencedDatastore {
    async fn read_durable_allocator_observation(
        &self,
    ) -> Result<crate::datastore::DurableAllocatorObservation> {
        crate::datastore::DatastoreBackend::read_durable_allocator_observation(self).await
    }

    async fn read_cluster_metadata_observation(
        &self,
    ) -> Result<crate::datastore::ClusterMetadataObservation> {
        crate::datastore::DatastoreBackend::read_cluster_metadata_observation(self).await
    }
}

#[async_trait]
impl crate::datastore::BackendLifecycleStore for SequencedDatastore {
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

#[cfg(test)]
impl crate::datastore::TestWatchStore for SequencedDatastore {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::datastore::DatastoreBackend::subscribe_watch_many(self, topics)
    }

    fn broadcast_watch_event(&self, pending: PendingWatchEvent) {
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
    ) -> Result<crate::datastore::WatchReplayRead<RawWatchEvent>> {
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
    ) -> Result<Vec<PodCleanupIntent>> {
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

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
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
    ) -> Result<()> {
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
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::pod_slot_clear_if_uid(
            self, namespace, pod_name, pod_uid, node_name,
        )
        .await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        crate::datastore::DatastoreBackend::subscribe_pod_slot_admissions(self)
    }
}

#[async_trait]
impl crate::datastore::AppliedOutboxStore for SequencedDatastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<crate::log_apply::OutboxStreamWatermark>> {
        crate::datastore::DatastoreBackend::list_outbox_stream_watermarks(self).await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool> {
        crate::datastore::DatastoreBackend::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<AppliedOutboxRecord>> {
        crate::datastore::DatastoreBackend::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> Result<bool> {
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
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<crate::log_apply::LogApplyCommit> {
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

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        crate::datastore::DatastoreBackend::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}
