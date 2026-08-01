//! Lower-owned focused persistence ports for the embedded SQLite backend.

use super::*;
use async_trait::async_trait;
use klights_cluster_store::{
    AppliedOutboxLedger, BackendLifecycleStore, ClusterMetadataMutation, ClusterNamespaceMutation,
    ClusterPodCleanupStore, ClusterResourceMutation, ClusterTopologyMutation,
    ClusterWatchMaintenance,
};

#[async_trait]
impl ClusterResourceMutation for Datastore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        Datastore::create_resource(self, api_version, kind, namespace, name, data).await
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
        Datastore::update_resource(self, api_version, kind, namespace, name, data, expected_rv)
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
        Datastore::update_resource_with_preconditions(
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

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        Datastore::update_main_resource_with_preconditions(
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
        Datastore::apply_resource_batch(self, operations).await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        Datastore::delete_resource(self, api_version, kind, namespace, name).await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        Datastore::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        Datastore::delete_resource_with_preconditions_observed_rv(
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
        Datastore::mark_resource_for_deletion_without_watch(
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
        let marked = Datastore::mark_resource_for_deletion_without_watch(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await?
        .ok_or_else(|| anyhow!("SQLite tombstone delete did not mark its target"))?;
        Datastore::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::uid_and_resource_version(
                marked.uid.clone(),
                marked.resource_version,
            ),
        )
        .await?;
        Ok(marked)
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
        Datastore::patch_resource_latest(
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
        Datastore::patch_resource_latest_with_preconditions(
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
impl ClusterNamespaceMutation for Datastore {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        Datastore::create_namespace(self, name, data).await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        Datastore::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        Datastore::delete_namespace(self, name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        Datastore::delete_namespace_observed_rv(self, name).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        Datastore::delete_namespace_contents(self, name).await
    }
}

#[async_trait]
impl ClusterWatchMaintenance for Datastore {
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        Datastore::advance_resource_version_after(self, min_rv).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        Datastore::watch_events_gc_prunable_count(self, max_rows, batch_cap).await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        Datastore::gc_watch_events(self, max_rows, batch_cap).await
    }
}

#[async_trait]
impl ClusterTopologyMutation for Datastore {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        Datastore::allocate_node_subnet(self, node_name, cluster_cidr, node_ip).await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        Datastore::update_node_peer_attributes(self, node_name, mode, hostport_range).await
    }

    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        Datastore::update_node_dataplane(self, metadata).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        Datastore::delete_node_subnet(self, node_name).await
    }
}

#[async_trait]
impl ClusterPodCleanupStore for Datastore {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        Datastore::move_pod_to_cleanup_intent(self, node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        Datastore::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        Datastore::delete_pod_cleanup_intent(self, node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        Datastore::delete_pod_cleanup_intents_for_node(self, node_name).await
    }
}

#[async_trait]
impl AppliedOutboxLedger for Datastore {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        Datastore::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        Datastore::list_outbox_stream_watermarks(self).await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        Datastore::list_outbox_stream_watermarks_paged(self, after, limit).await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        Datastore::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        Datastore::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        Datastore::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        Datastore::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn build_log_apply_commit_for_command(
        &self,
        command: klights_cluster_core::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        Datastore::build_log_apply_commit_for_command(self, command, operation, authoring_node)
            .await
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        Datastore::build_log_apply_commit_for_outbox(
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
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        Datastore::build_log_apply_commit_for_outbox_with_watermark(
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
        Datastore::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}

#[async_trait]
impl ClusterMetadataMutation for Datastore {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        let key = key.to_string();
        self.read_db_call("get_klights_meta", move |conn| {
            Ok(conn
                .query_row(queries::SELECT_KLIGHTS_META, [&key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?)
        })
        .await
        .map_err(|error| anyhow!("get_klights_meta failed: {error}"))
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        let key = key.to_string();
        let value = value.to_string();
        self.db_call("set_klights_meta", move |conn| {
            conn.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![&key, &value],
            )?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("set_klights_meta failed: {error}"))
    }
}

#[async_trait]
impl BackendLifecycleStore for Datastore {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<klights_cluster_store::SnapshotExclusiveFence>> {
        Ok(Some(klights_cluster_store::SnapshotExclusiveFence::new(
            self.snapshot_fence.clone().write_owned().await,
        )))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<klights_cluster_store::SnapshotMutationFence>> {
        Ok(Some(klights_cluster_store::SnapshotMutationFence::new(
            self.snapshot_fence.clone().read_owned().await,
        )))
    }

    fn close(&self) {}
}
