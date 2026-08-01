//! Lower-owned focused persistence ports for the embedded redb backend.

use super::RedbDatastore;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions,
};
use klights_cluster_store::{
    AppliedOutboxLedger, BackendLifecycleStore, ClusterMetadataMutation, ClusterNamespaceMutation,
    ClusterPodCleanupStore, ClusterResourceMutation, ClusterTopologyMutation,
    ClusterWatchMaintenance, DurableAllocatorRead,
};
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
    ) -> Result<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            let committed = self.namespaces.create_ns(name, data).await?;
            return Ok(self.finish_post_commit(committed));
        }
        let committed = self
            .resources
            .create_res(api_version, kind, namespace, name, data)
            .await?;
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
    ) -> Result<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return self
                .namespaces
                .update_ns_impl(name, data, expected_rv)
                .await;
        }
        let committed = self
            .resources
            .update_res(api_version, kind, namespace, name, data, expected_rv)
            .await?;
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
    ) -> Result<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return self
                .namespaces
                .update_ns_with_preconditions_impl(name, data, preconditions)
                .await;
        }
        let committed = self
            .resources
            .update_res_with_preconditions(api_version, kind, namespace, name, data, preconditions)
            .await?;
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
    ) -> Result<Resource> {
        self.update_resource_with_preconditions(
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
        let _ = operations;
        Err(anyhow!(
            "redb backend does not support raft-backed resource batch writes"
        ))
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return self.namespaces.delete_ns_impl(name).await;
        }
        let committed = self
            .resources
            .delete_res(api_version, kind, namespace, name)
            .await?;
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
    ) -> Result<()> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return self
                .namespaces
                .delete_ns_with_preconditions_impl(name, preconditions)
                .await
                .map(|_| ());
        }
        let committed = self
            .resources
            .delete_res_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await?;
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
    ) -> Result<i64> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return self
                .namespaces
                .delete_ns_with_preconditions_impl(name, preconditions)
                .await;
        }
        self.delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await?;
        self.current_resource_version().await
    }

    async fn mark_for_delete_without_watch(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _preconditions: ResourcePreconditions,
        _grace_seconds: i64,
    ) -> Result<Option<Resource>> {
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
    ) -> Result<Resource> {
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
            .await?;
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
    ) -> Result<Option<Resource>> {
        let committed = self
            .resources
            .patch(api_version, kind, namespace, name, patch)
            .await?;
        Ok(self.finish_post_commit(committed))
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let committed = self
            .resources
            .patch_with_preconditions(api_version, kind, namespace, name, request)
            .await?;
        Ok(self.finish_post_commit(committed))
    }
}

#[async_trait]
impl ClusterNamespaceMutation for RedbDatastore {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        let committed = self.namespaces.create_ns(name, data).await?;
        Ok(self.finish_post_commit(committed))
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.namespaces
            .update_ns_impl(name, data, expected_rv)
            .await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.namespaces.delete_ns_impl(name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        self.namespaces.delete_ns_observed_rv_impl(name).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        self.namespaces.delete_namespace_contents_impl(name).await
    }
}

#[async_trait]
impl ClusterWatchMaintenance for RedbDatastore {
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.rv_store.advance_rv(min_rv).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.watch_store
            .gc_watch_prunable_count(max_rows, batch_cap)
            .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.watch_store.gc_watch(max_rows, batch_cap).await
    }
}

#[async_trait]
impl ClusterTopologyMutation for RedbDatastore {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        self.network
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        self.network
            .update_peer_attrs(node_name, mode, hostport_range)
            .await
    }

    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        self.network.update_node_dataplane(metadata).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        self.network.delete_node_subnet(node_name).await
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
    ) -> Result<()> {
        Err(anyhow!("redb backend does not support pod cleanup intents"))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        _node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        Ok(Vec::new())
    }

    async fn delete_pod_cleanup_intent(
        &self,
        _node_name: &str,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _reason: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_pod_cleanup_intents_for_node(&self, _node_name: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl AppliedOutboxLedger for RedbDatastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        self.live_committed_apply
            .applied_outbox_prunable_count(cutoff_ms)
            .await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply.list_outbox_watermarks().await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply
            .list_outbox_watermarks_paged(after, limit)
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .get_applied_outbox_bytes(idempotency_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .transpose()
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        let idempotency_key = record.idempotency_key.clone();
        let bytes = serde_json::to_vec(&record)?;
        self.live_committed_apply
            .insert_applied_outbox_bytes(idempotency_key, bytes)
            .await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .list_applied_outbox_bytes()
            .await?
            .into_iter()
            .map(|(_, bytes)| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .collect()
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply
            .list_applied_outbox_bytes_paged(after_key, limit)
            .await?
            .into_iter()
            .map(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .collect()
    }

    async fn build_log_apply_commit_for_command(
        &self,
        _command: klights_cluster_core::StorageCommand,
        _operation: &str,
        _authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        self.live_committed_apply
            .build_log_apply_commit_for_command()
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

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        self.live_committed_apply
            .gc_applied_outbox(now_ms, ttl_ms)
            .await
    }
}

#[async_trait]
impl ClusterMetadataMutation for RedbDatastore {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.recovery.get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        self.live_committed_apply.set_klights_meta(key, value).await
    }
}

#[async_trait]
impl BackendLifecycleStore for RedbDatastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<klights_cluster_store::SnapshotExclusiveFence>> {
        Ok(Some(klights_cluster_store::SnapshotExclusiveFence::new(
            self.accessor.acquire_snapshot_exclusive().await,
        )))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<klights_cluster_store::SnapshotMutationFence>> {
        Ok(Some(klights_cluster_store::SnapshotMutationFence::new(
            self.accessor.acquire_snapshot_mutation().await,
        )))
    }

    fn close(&self) {
        self.accessor.close();
    }
}
