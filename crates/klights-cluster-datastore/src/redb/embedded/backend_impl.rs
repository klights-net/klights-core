//! Lower-owned focused persistence ports for the embedded redb backend.

use super::RedbDatastore;
use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions,
};
use klights_cluster_store::{
    AppliedOutboxLedger, BackendLifecycleStore, ClusterMetadataMutation, ClusterNamespaceMutation,
    ClusterPodCleanupStore, ClusterResourceMutation, ClusterStoreError, ClusterTopologyMutation,
    ClusterWatchMaintenance, DurableAllocatorRead, PersistenceBackend,
};
type ClusterResult<T> = klights_cluster_store::ClusterStoreResult<T>;

fn redb_port_error(error: anyhow::Error) -> ClusterStoreError {
    crate::errors::cluster_store_adapter_error(
        error,
        PersistenceBackend::Redb,
        "focused persistence port",
    )
}
use serde_json::Value;

impl RedbDatastore {
    async fn current_resource_version(&self) -> Result<i64> {
        self.read_store
            .read_allocator_state()
            .await
            .map(|state| state.position().resource_version)
            .map_err(anyhow::Error::from)
    }
}

#[async_trait]
impl ClusterResourceMutation for RedbDatastore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> ClusterResult<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            let committed = self
                .namespaces
                .create_ns(name, data)
                .await
                .map_err(redb_port_error)?;
            return Ok(self.finish_post_commit(committed));
        }
        let committed = self
            .resources
            .create_res(api_version, kind, namespace, name, data)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> ClusterResult<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            return self
                .namespaces
                .update_ns_impl(name, data, expected_rv)
                .await
                .map_err(redb_port_error);
        }
        let committed = self
            .resources
            .update_res(api_version, kind, namespace, name, data, expected_rv)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ClusterResult<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            return self
                .namespaces
                .update_ns_with_preconditions_impl(name, data, preconditions)
                .await
                .map_err(redb_port_error);
        }
        let committed = self
            .resources
            .update_res_with_preconditions(api_version, kind, namespace, name, data, preconditions)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> ClusterResult<Resource> {
        let committed = self
            .resources
            .update_main_res_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn apply_resource_batch(
        &self,
        operations: Vec<ResourceBatchOperation>,
    ) -> ClusterResult<()> {
        let _ = operations;
        Err(ClusterStoreError::unsupported(
            "redb backend does not support raft-backed resource batch writes",
        ))
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ClusterResult<()> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            return self
                .namespaces
                .delete_ns_impl(name)
                .await
                .map_err(redb_port_error);
        }
        let committed = self
            .resources
            .delete_res(api_version, kind, namespace, name)
            .await
            .map_err(redb_port_error)?;
        self.finish_post_commit(committed);
        Ok(())
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> ClusterResult<()> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            return self
                .namespaces
                .delete_ns_with_preconditions_impl(name, preconditions)
                .await
                .map_err(redb_port_error)
                .map(|_| ());
        }
        let committed = self
            .resources
            .delete_res_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await
            .map_err(redb_port_error)?;
        self.finish_post_commit(committed);
        Ok(())
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> ClusterResult<i64> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(ClusterStoreError::invalid_request(
                    "Namespace is cluster-scoped",
                ));
            }
            return self
                .namespaces
                .delete_ns_with_preconditions_impl(name, preconditions)
                .await
                .map_err(redb_port_error);
        }
        self.delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await?;
        self.current_resource_version()
            .await
            .map_err(redb_port_error)
    }

    async fn mark_for_delete_without_watch(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _preconditions: ResourcePreconditions,
        _grace_seconds: i64,
    ) -> ClusterResult<Option<Resource>> {
        Ok(None)
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> ClusterResult<Resource> {
        let committed = self
            .resources
            .delete_res_with_tombstone(
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                grace_seconds,
            )
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        _patch_kind: PatchKind,
        patch: Value,
    ) -> ClusterResult<Option<Resource>> {
        let committed = self
            .resources
            .patch(api_version, kind, namespace, name, patch)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> ClusterResult<Option<Resource>> {
        let committed = self
            .resources
            .patch_with_preconditions(api_version, kind, namespace, name, request)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }
}

#[async_trait]
impl ClusterNamespaceMutation for RedbDatastore {
    async fn create_namespace(&self, name: &str, data: Value) -> ClusterResult<Resource> {
        let committed = self
            .namespaces
            .create_ns(name, data)
            .await
            .map_err(redb_port_error)?;
        Ok(self.finish_post_commit(committed))
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> ClusterResult<Resource> {
        self.namespaces
            .update_ns_impl(name, data, expected_rv)
            .await
            .map_err(redb_port_error)
    }

    async fn delete_namespace(&self, name: &str) -> ClusterResult<()> {
        self.namespaces
            .delete_ns_impl(name)
            .await
            .map_err(redb_port_error)
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> ClusterResult<i64> {
        self.namespaces
            .delete_ns_observed_rv_impl(name)
            .await
            .map_err(redb_port_error)
    }

    async fn delete_namespace_contents(&self, name: &str) -> ClusterResult<()> {
        self.namespaces
            .delete_namespace_contents_impl(name)
            .await
            .map_err(redb_port_error)
    }
}

#[async_trait]
impl ClusterWatchMaintenance for RedbDatastore {
    async fn advance_resource_version_after(&self, min_rv: i64) -> ClusterResult<i64> {
        self.rv_store
            .advance_rv(min_rv)
            .await
            .map_err(redb_port_error)
    }

    async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> ClusterResult<usize> {
        self.watch_store
            .gc_watch_prunable_count(max_rows, batch_cap)
            .await
            .map_err(redb_port_error)
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> ClusterResult<usize> {
        self.watch_store
            .gc_watch(max_rows, batch_cap)
            .await
            .map_err(redb_port_error)
    }
}

#[async_trait]
impl ClusterTopologyMutation for RedbDatastore {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> ClusterResult<klights_cluster_store::StoredNodeSubnet> {
        self.network
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
            .map_err(redb_port_error)
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> ClusterResult<()> {
        self.network
            .update_peer_attrs(node_name, mode, hostport_range)
            .await
            .map_err(redb_port_error)
    }

    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> ClusterResult<()> {
        self.network
            .update_node_dataplane(metadata)
            .await
            .map_err(redb_port_error)
    }

    async fn delete_node_subnet(&self, node_name: &str) -> ClusterResult<()> {
        self.network
            .delete_node_subnet(node_name)
            .await
            .map_err(redb_port_error)
    }
}

#[async_trait]
impl ClusterPodCleanupStore for RedbDatastore {
    async fn move_pod_to_cleanup_intent(
        &self,
        _node_name: &str,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _reason: &str,
    ) -> ClusterResult<()> {
        Err(ClusterStoreError::unsupported(
            "redb backend does not support pod cleanup intents",
        ))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        _node_name: &str,
    ) -> ClusterResult<Vec<LogApplyPodCleanupIntentRow>> {
        Ok(Vec::new())
    }

    async fn delete_pod_cleanup_intent(
        &self,
        _node_name: &str,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _reason: &str,
    ) -> ClusterResult<()> {
        Ok(())
    }

    async fn delete_pod_cleanup_intents_for_node(&self, _node_name: &str) -> ClusterResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AppliedOutboxLedger for RedbDatastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> ClusterResult<usize> {
        self.live_committed_apply
            .applied_outbox_prunable_count(cutoff_ms)
            .await
            .map_err(redb_port_error)
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> ClusterResult<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply
            .list_outbox_watermarks()
            .await
            .map_err(redb_port_error)
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> ClusterResult<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply
            .list_outbox_watermarks_paged(after, limit)
            .await
            .map_err(redb_port_error)
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> ClusterResult<Option<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .get_applied_outbox_bytes(idempotency_key)
            .await
            .map_err(redb_port_error)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .transpose()
            .map_err(redb_port_error)
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> ClusterResult<bool> {
        let idempotency_key = record.idempotency_key.clone();
        let bytes = serde_json::to_vec(&record)
            .map_err(anyhow::Error::from)
            .map_err(redb_port_error)?;
        self.live_committed_apply
            .insert_applied_outbox_bytes(idempotency_key, bytes)
            .await
            .map_err(redb_port_error)
    }

    async fn list_applied_outbox(&self) -> ClusterResult<Vec<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .list_applied_outbox_bytes()
            .await
            .map_err(redb_port_error)?
            .into_iter()
            .map(|(_, bytes)| {
                serde_json::from_slice(&bytes)
                    .map_err(anyhow::Error::from)
                    .map_err(redb_port_error)
            })
            .collect()
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> ClusterResult<Vec<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .list_applied_outbox_bytes_paged(after_key, limit)
            .await
            .map_err(redb_port_error)?
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(anyhow::Error::from)
                    .map_err(redb_port_error)
            })
            .collect()
    }

    async fn build_log_apply_commit_for_command(
        &self,
        _command: klights_cluster_core::StorageCommand,
        _operation: &str,
        _authoring_node: &str,
    ) -> ClusterResult<klights_cluster_core::LogApplyCommit> {
        self.live_committed_apply
            .build_log_apply_commit_for_command()
            .map_err(redb_port_error)
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::StorageCommand,
        _authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply
            .build_log_apply_commit_for_outbox()
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply
            .build_log_apply_commit_for_outbox_with_watermark()
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> ClusterResult<usize> {
        self.live_committed_apply
            .gc_applied_outbox(now_ms, ttl_ms)
            .await
            .map_err(redb_port_error)
    }
}

#[async_trait]
impl ClusterMetadataMutation for RedbDatastore {
    async fn get_klights_meta(&self, key: &str) -> ClusterResult<Option<String>> {
        self.recovery
            .get_klights_meta(key)
            .await
            .map_err(redb_port_error)
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> ClusterResult<()> {
        self.live_committed_apply
            .set_klights_meta(key, value)
            .await
            .map_err(redb_port_error)
    }
}

#[async_trait]
impl BackendLifecycleStore for RedbDatastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> ClusterResult<Option<klights_cluster_store::SnapshotExclusiveFence>> {
        Ok(Some(klights_cluster_store::SnapshotExclusiveFence::new(
            self.accessor.acquire_snapshot_exclusive().await,
        )))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> ClusterResult<Option<klights_cluster_store::SnapshotMutationFence>> {
        Ok(Some(klights_cluster_store::SnapshotMutationFence::new(
            self.accessor.acquire_snapshot_mutation().await,
        )))
    }

    fn close(&self) {
        self.accessor.close();
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use klights_cluster_store::ClusterStoreErrorKind;

    use super::*;

    #[test]
    fn focused_redb_port_preserves_datastore_conflict_classification_and_source() {
        let error = redb_port_error(anyhow::Error::new(crate::errors::DatastoreError::conflict(
            "resourceVersion changed",
        )));

        assert_eq!(error.kind(), ClusterStoreErrorKind::Conflict);
        assert_eq!(error.backend(), Some(PersistenceBackend::Redb));
        assert_eq!(error.operation(), "focused persistence port");
        assert!(error.source().is_some());
    }
}
