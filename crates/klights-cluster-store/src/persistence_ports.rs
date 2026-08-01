//! Focused passive cluster-persistence mutation ports.
//!
//! These capabilities are implemented by embedded datastore adapters and are
//! wired only by root composition. Normal application submission continues to
//! use `klights-leader-api`; committed Raft apply remains confined to
//! [`crate::PrivilegedCommittedRaftApply`].

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, PodEndpointEffect, Resource,
    ResourceBatchOperation, ResourceMutationEffect, ResourcePatchRequest, ResourcePreconditions,
    StorageCommand,
};
use serde_json::Value;

/// Transaction-derived metadata for an outbox apply.
///
/// Construction remains datastore-owned; transport and feature code cannot
/// synthesize persistence effects.
#[doc(hidden)]
pub struct CommittedOutboxApply {
    result: klights_cluster_core::OutboxApplyOutcome,
    resource_effect: ResourceMutationEffect,
    pod_endpoint_effect: PodEndpointEffect,
    committed_resource: Option<Resource>,
}

impl CommittedOutboxApply {
    #[doc(hidden)]
    pub const fn new(
        result: klights_cluster_core::OutboxApplyOutcome,
        resource_effect: ResourceMutationEffect,
        pod_endpoint_effect: PodEndpointEffect,
    ) -> Self {
        Self {
            result,
            resource_effect,
            pod_endpoint_effect,
            committed_resource: None,
        }
    }

    #[doc(hidden)]
    pub fn with_committed_resource(mut self, resource: Option<Resource>) -> Self {
        self.committed_resource = resource;
        self
    }

    #[doc(hidden)]
    pub fn into_parts(
        self,
    ) -> (
        klights_cluster_core::OutboxApplyOutcome,
        ResourceMutationEffect,
        PodEndpointEffect,
        Option<Resource>,
    ) {
        (
            self.result,
            self.resource_effect,
            self.pod_endpoint_effect,
            self.committed_resource,
        )
    }
}

/// Ordinary cluster resource mutation primitives.
///
/// The two no-watch deletion operations are persistence primitives, not
/// general Pod deletion authority. Root exposes them only through the
/// UID-qualified actor-owned or unscheduled-Pod-CAS paths.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait ClusterResourceMutation: Send + Sync {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource>;
    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource>;
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()>;
    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()>;
    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;
    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64>;
    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>>;
    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource>;
    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>>;
    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>>;
}

/// Ordinary Namespace mutation primitives.
#[async_trait]
pub trait ClusterNamespaceMutation: Send + Sync {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource>;
    async fn update_namespace(&self, name: &str, data: Value, expected_rv: i64)
    -> Result<Resource>;
    async fn delete_namespace(&self, name: &str) -> Result<()>;
    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64>;
    async fn delete_namespace_contents(&self, name: &str) -> Result<()>;
}

/// Mutating retention operations for durable watch history.
#[async_trait]
pub trait ClusterWatchMaintenance: Send + Sync {
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64>;
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
}

/// Cluster-owned node-subnet and dataplane metadata mutations.
#[async_trait]
pub trait ClusterTopologyMutation: Send + Sync {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<crate::StoredNodeSubnet>;
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()>;
    async fn update_node_dataplane(&self, metadata: crate::DataplanePeerMetadata) -> Result<()>;
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()>;
}

/// Durable cleanup-intent rows consumed by the Pod lifecycle actor.
#[async_trait]
pub trait ClusterPodCleanupStore: Send + Sync {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()>;
    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>>;
    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()>;
    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()>;
}

/// Applied-outbox ledger, proposal materialization, and retention operations.
///
/// Applying a built command is deliberately absent: committed apply is owned
/// exclusively by [`crate::PrivilegedCommittedRaftApply`].
#[async_trait]
pub trait AppliedOutboxLedger: Send + Sync {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize>;
    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>>;
    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&crate::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>>;
    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>>;
    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool>;
    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>>;
    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>>;
    async fn build_log_apply_commit_for_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit>;
    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize>;
}

/// Backend-local cluster metadata mutations.
#[async_trait]
pub trait ClusterMetadataMutation: Send + Sync {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>>;
    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()>;
}
