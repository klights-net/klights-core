//! Root composition adapter for the passive SQLite cluster datastore.

#[cfg(test)]
mod applier;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(any(test, feature = "integration-test-harness"))]
use super::backend::CommitObservationSink;
use super::backend::DatastoreBackend;
use super::types::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    PositionedWatchReplay, PositionedWatchReplayRead, ReplicatedMembershipState,
    ReplicatedSnapshotMetadata, ResourceList, ResourceListQuery, SnapshotAtRv, WatchReplayRead,
    WatchTarget, WatchTargetScope,
};
use klights_cluster_core::{
    BuildOutboxOutcome, LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
use klights_cluster_datastore::sqlite::embedded::Datastore as PassiveDatastore;
use klights_cluster_store::ClusterMetadataRead;
#[cfg(any(test, feature = "integration-test-harness"))]
use klights_cluster_store::StagedPostCommit;
#[cfg(test)]
use klights_watch::WatchTopic;

#[cfg(test)]
pub use klights_cluster_datastore::sqlite::embedded::ResourceMutationPauseOperation;

/// Root-owned composition identity around the passive SQLite implementation.
///
/// This wrapper exists only so root-local and sibling-crate feature traits can
/// be composed without violating Rust's orphan rules. Persistence state and
/// execution remain wholly owned by `PassiveDatastore`.
#[derive(Clone)]
pub struct Datastore(PassiveDatastore);

impl std::ops::Deref for Datastore {
    type Target = PassiveDatastore;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for Datastore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Datastore").finish_non_exhaustive()
    }
}

impl Datastore {
    pub async fn new_persistent_paths(
        cluster_db_path: &std::path::Path,
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
        key_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_persistent_paths_with_sink(
            cluster_db_path,
            supervisor,
            key_file,
            #[cfg(any(test, feature = "integration-test-harness"))]
            crate::watch_commit_observation_adapter::new_sink(),
            crate::outbox_response_codec_adapter::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    pub async fn new_persistent_paths_with_sink(
        cluster_db_path: &std::path::Path,
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
        key_file: Option<&std::path::Path>,
        #[cfg(any(test, feature = "integration-test-harness"))] commit_sink: std::sync::Arc<
            dyn CommitObservationSink,
        >,
        outbox_codec: std::sync::Arc<dyn klights_cluster_store::OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "integration-test-harness"))]
        let passive = PassiveDatastore::new_persistent_paths_with_sink(
            cluster_db_path,
            supervisor,
            key_file,
            commit_sink,
            outbox_codec,
            wall_clock,
        )
        .await?;
        #[cfg(not(any(test, feature = "integration-test-harness")))]
        let passive = PassiveDatastore::new_persistent_paths(
            cluster_db_path,
            supervisor,
            key_file,
            outbox_codec,
            wall_clock,
        )
        .await?;
        Ok(Self(passive))
    }

    pub async fn new_in_memory_with_watch_and_executor_with_sink(
        executor: klights_supervisor::DbExecutor,
        #[cfg(any(test, feature = "integration-test-harness"))] commit_sink: std::sync::Arc<
            dyn CommitObservationSink,
        >,
        outbox_codec: std::sync::Arc<dyn klights_cluster_store::OutboxResponseCodec>,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        #[cfg(any(test, feature = "integration-test-harness"))]
        let passive = PassiveDatastore::new_in_memory_with_watch_and_executor_with_sink(
            executor,
            commit_sink,
            outbox_codec,
            wall_clock,
        )
        .await?;
        #[cfg(not(any(test, feature = "integration-test-harness")))]
        let passive = PassiveDatastore::new_in_memory_with_watch_and_executor(
            executor,
            outbox_codec,
            wall_clock,
        )
        .await?;
        Ok(Self(passive))
    }

    #[cfg(any(test, feature = "integration-test-harness"))]
    pub async fn new_in_memory() -> Result<Self> {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = klights_cluster_datastore::sqlite::open_in_memory(
            supervisor,
            "sqlite:root-composition-test",
        )
        .await?;
        Self::new_in_memory_with_watch_and_executor_with_sink(
            executor,
            crate::watch_commit_observation_adapter::new_sink(),
            crate::outbox_response_codec_adapter::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_persistent(
        db_root: &std::path::Path,
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
        key_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::new_persistent_paths_with_sink(
            &db_root.join("sqlite").join("cluster.db"),
            supervisor,
            key_file,
            crate::watch_commit_observation_adapter::new_sink(),
            crate::outbox_response_codec_adapter::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_watch_and_executor(
        executor: klights_supervisor::DbExecutor,
    ) -> Result<Self> {
        Self::new_in_memory_with_watch_and_executor_with_sink(
            executor,
            crate::watch_commit_observation_adapter::new_sink(),
            crate::outbox_response_codec_adapter::new_codec(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    #[cfg(test)]
    pub fn subscribe_watch(
        &self,
        topic: klights_watch::WatchTopic,
    ) -> broadcast::Receiver<crate::watch::WatchEvent> {
        DatastoreBackend::subscribe_watch(self, topic)
    }

    #[cfg(test)]
    pub fn subscribe_watch_many(
        &self,
        topics: Vec<klights_watch::WatchTopic>,
    ) -> crate::watch::WatchReceiver {
        DatastoreBackend::subscribe_watch_many(self, topics)
    }

    #[cfg(test)]
    pub fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        DatastoreBackend::broadcast_watch_event(self, pending)
    }

    #[cfg(test)]
    pub fn install_list_resources_snapshot_pause_for_test(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> std::sync::Arc<klights_cluster_datastore::sqlite::ListResourcesSnapshotPause> {
        PassiveDatastore::install_list_resources_snapshot_pause_for_test(
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    }

    #[cfg(test)]
    pub async fn count_watch_events(&self) -> Result<i64> {
        PassiveDatastore::count_watch_events(self).await
    }

    pub async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        PassiveDatastore::create_namespace(self, name, data).await
    }

    pub async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_namespace(self, name).await
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl klights_kubelet::volume_sources::VolumeSourceReader for Datastore {
    async fn config_map(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "ConfigMap", Some(namespace), name).await
    }

    async fn secret(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "Secret", Some(namespace), name).await
    }

    async fn service_account(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "ServiceAccount", Some(namespace), name).await
    }

    async fn pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "Pod", Some(namespace), name).await
    }

    async fn node(&self, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "Node", None, name).await
    }

    async fn persistent_volume_claim(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, "v1", "PersistentVolume", None, name).await
    }
}

fn focused_watch_targets(
    targets: &[WatchTarget],
) -> Vec<klights_cluster_store::DurableWatchTarget> {
    targets
        .iter()
        .map(|target| match &target.scope {
            WatchTargetScope::Cluster => klights_cluster_store::DurableWatchTarget::cluster(
                &target.api_version,
                &target.kind,
            ),
            WatchTargetScope::Namespaced(None) => {
                klights_cluster_store::DurableWatchTarget::namespaced(
                    &target.api_version,
                    &target.kind,
                )
            }
            WatchTargetScope::Namespaced(Some(namespace)) => {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                    &target.api_version,
                    &target.kind,
                    namespace,
                )
            }
        })
        .collect()
}

fn focused_events_to_catchup(
    events: Vec<klights_cluster_store::DurableWatchEvent>,
) -> Vec<CatchUpResource> {
    events
        .into_iter()
        .map(|event| {
            let event_type = event.event_type().to_string();
            CatchUpResource {
                resource: event.into_resource(),
                event_type: std::borrow::Cow::Owned(event_type),
            }
        })
        .collect()
}

#[cfg(test)]
pub fn create_staged_post_commit(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    event_type: &str,
    data: impl Into<std::sync::Arc<Value>>,
) -> StagedPostCommit {
    crate::datastore::types::with_staged_test_resource_event(
        StagedPostCommit::new(api_version, kind, namespace, resource_version),
        event_type,
        name,
        data.into(),
    )
}

#[cfg(any(test, feature = "integration-test-harness"))]
pub fn staged_test_event(pending: &StagedPostCommit) -> Option<crate::watch::WatchEvent> {
    let staged = pending.test_event()?;
    let mut event = crate::watch::WatchEvent::from_type(
        staged.event_type(),
        staged.resource().data.as_ref().clone(),
    );
    event.encoded_payload =
        staged
            .encoded_json()
            .cloned()
            .map(|bytes| crate::watch::events::EncodedWatchPayload {
                content_type: crate::watch::WatchContentType::Json,
                bytes,
            });
    Some(event)
}

#[cfg(any(test, feature = "integration-test-harness"))]
pub fn staged_post_commit_from_event(event: crate::watch::WatchEvent) -> StagedPostCommit {
    let resource = Resource::try_from_data(event.object.clone())
        .expect("test watch event must carry canonical resource identity");
    let encoded_json = event
        .encoded_payload
        .as_ref()
        .filter(|payload| payload.content_type == crate::watch::WatchContentType::Json)
        .map(|payload| payload.bytes.clone());
    StagedPostCommit::new(
        &resource.api_version,
        &resource.kind,
        resource.namespace.as_deref(),
        resource.resource_version,
    )
    .with_test_event(event.event_type.to_string(), resource, encoded_json)
}

#[async_trait]
impl DatastoreBackend for Datastore {
    #[cfg(any(test, feature = "integration-test-harness"))]
    fn commit_observation_sink(&self) -> std::sync::Arc<dyn CommitObservationSink> {
        PassiveDatastore::commit_observation_sink(self)
            .expect("test datastore must install a commit observation sink")
    }

    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation> {
        let state = self
            .focused_read_store()
            .read_allocator_state()
            .await
            .map_err(anyhow::Error::new)?;
        Ok(DurableAllocatorObservation {
            position: state.position(),
        })
    }

    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation> {
        let observed = self
            .focused_recovery_store()
            .read_cluster_metadata()
            .await?;
        let (metadata, membership) = observed.into_parts();
        let membership = match membership {
            klights_cluster_store::SnapshotMembership::LegacyOmitted => {
                ReplicatedMembershipState::LegacyOmitted
            }
            klights_cluster_store::SnapshotMembership::AuthoritativeAbsent => {
                ReplicatedMembershipState::AuthoritativeAbsent
            }
            klights_cluster_store::SnapshotMembership::Present(membership) => {
                ReplicatedMembershipState::Present(membership)
            }
        };
        Ok(ClusterMetadataObservation {
            metadata,
            membership,
        })
    }

    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        Ok(Some(
            PassiveDatastore::acquire_snapshot_exclusive_fence(self).await,
        ))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        Ok(Some(
            PassiveDatastore::acquire_snapshot_mutation_fence(self).await,
        ))
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        klights_cluster_store::AuthoritativeSnapshotCapture::begin_capture_with_fence(
            self.focused_recovery_store().as_ref(),
            request,
            fence,
        )
        .await
        .map_err(anyhow::Error::from)
    }

    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str) {
        PassiveDatastore::seed_namespace_no_rv(self, name)
            .await
            .expect("seed namespace for test");
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        crate::watch_commit_observation_adapter::subscribe_test_events(
            PassiveDatastore::commit_observation_sink(self)
                .expect("test datastore must install a commit observation sink")
                .as_ref(),
            topic,
        )
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::watch_commit_observation_adapter::subscribe_test_events_many(
            PassiveDatastore::commit_observation_sink(self)
                .expect("test datastore must install a commit observation sink")
                .as_ref(),
            topics,
        )
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        let sink = PassiveDatastore::commit_observation_sink(self)
            .expect("test datastore must install a commit observation sink");
        sink.observe(&[pending]);
    }

    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        meta: klights_cluster_core::command::CommandMeta,
    ) -> Result<()> {
        self.0.apply_legacy_test_command(command, meta).await
    }

    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: crate::datastore::ReplicatedCreateOptions,
    ) -> Result<Resource> {
        self.0
            .apply_replicated_create_resource(api_version, kind, namespace, name, data, options)
            .await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        PassiveDatastore::replace_replicated_resource_state(
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
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        PassiveDatastore::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        let receipt = PassiveDatastore::apply_raft_log_apply_commit_receipt(self, commit).await?;
        Ok(crate::cluster_store_replication_adapter::storage_command_result_from_receipt(&receipt))
    }

    async fn apply_raft_log_apply_commit_receipt(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        PassiveDatastore::apply_raft_log_apply_commit_receipt(self, commit).await
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
                self, &data, namespace,
            )
            .await
        {
            crate::datastore::pod_serviceaccount::inject_serviceaccount_volume(&mut data);
        }
        PassiveDatastore::create_resource(self, api_version, kind, namespace, name, data).await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        PassiveDatastore::get_resource(self, api_version, kind, namespace, name).await
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        PassiveDatastore::list_resources(self, api_version, kind, namespace, query).await
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
        PassiveDatastore::list_resources_page(
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

    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        PassiveDatastore::list_resources_for_watch_targets(self, targets, label_selector).await
    }

    async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> Result<crate::datastore::types::SnapshotAtRv> {
        PassiveDatastore::snapshot_resources_at_rv(
            self,
            api_version,
            kind,
            namespace,
            query,
            snapshot_rv,
        )
        .await
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        PassiveDatastore::list_resource_keys_for_scope(self, api_version, kind, namespaced).await
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
        PassiveDatastore::update_resource(
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
        PassiveDatastore::update_resource_with_preconditions(
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
        PassiveDatastore::update_main_resource_with_preconditions(
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
        PassiveDatastore::apply_resource_batch(self, operations).await
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
        PassiveDatastore::update_status_only(
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
        PassiveDatastore::update_status_only_with_preconditions(
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

    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>> {
        PassiveDatastore::mark_resource_for_deletion_without_watch(
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
        let marked = PassiveDatastore::mark_resource_for_deletion_without_watch(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("SQLite tombstone delete did not mark its target"))?;
        PassiveDatastore::delete_resource_with_preconditions(
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

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        PassiveDatastore::delete_resource(self, api_version, kind, namespace, name).await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        PassiveDatastore::delete_resource_with_preconditions(
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
        PassiveDatastore::delete_resource_with_preconditions_observed_rv(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn get_current_resource_version(&self) -> Result<i64> {
        PassiveDatastore::get_current_resource_version(self).await
    }

    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        PassiveDatastore::create_namespace(self, name, data).await
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        PassiveDatastore::get_namespace(self, name).await
    }

    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        PassiveDatastore::list_namespaces(self, label_selector, field_selector).await
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        PassiveDatastore::list_namespaces_page(self, label_selector, field_selector, page).await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        PassiveDatastore::update_namespace(self, name, data, expected_rv).await
    }

    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        PassiveDatastore::delete_namespace_contents(self, name).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        PassiveDatastore::delete_namespace(self, name).await
    }

    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        PassiveDatastore::delete_namespace_observed_rv(self, name).await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        PassiveDatastore::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        PassiveDatastore::list_resources_by_owner_uid(self, api_version, kind, namespace, owner_uid)
            .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        PassiveDatastore::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }

    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::ModifiedClusterResourcesRequest::try_new(
            api_version,
            kind,
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_read_store()
            .list_cluster_resources_modified_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        PassiveDatastore::list_cluster_resources(self).await
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::ModifiedResourcesRequest::try_new(
            api_version,
            kind,
            namespace.map(str::to_string),
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_read_store()
            .list_resources_modified_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        PassiveDatastore::advance_resource_version_after(self, min_rv).await
    }

    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        PassiveDatastore::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        PassiveDatastore::list_namespace_resources_of_kind(self, namespace, kind).await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        PassiveDatastore::list_namespace_resources_excluding_kind(self, namespace, kind).await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        PassiveDatastore::count_namespace_resources(self, namespace).await
    }

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchEventsSinceRequest::try_new(
            focused_watch_targets(targets),
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_read_store()
            .list_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        PassiveDatastore::list_watch_events_since_checked(self, targets, since_rv).await
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        PassiveDatastore::list_watch_events_since_checked_bounded(self, targets, since_rv, limit)
            .await
    }

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        use klights_cluster_store::DurableWatchHistoryRead as _;

        let request = klights_cluster_store::WatchHistoryRequest::new(
            focused_watch_targets(targets),
            position,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_read_store()
            .replay_watch_history(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::WatchHistoryRead::Expired => {
                Ok(PositionedWatchReplayRead::Expired)
            }
            klights_cluster_store::WatchHistoryRead::Events(page) => {
                let next_position = page.next_position();
                let events = page
                    .into_events()
                    .into_iter()
                    .map(|event| {
                        let event_type = event.event.event_type().to_string();
                        klights_cluster_core::PositionedWatchEvent {
                            position: event.position,
                            event: CatchUpResource {
                                resource: event.event.into_resource(),
                                event_type: std::borrow::Cow::Owned(event_type),
                            },
                        }
                    })
                    .collect();
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events,
                    next_position,
                }))
            }
        }
    }

    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.focused_read_store()
            .read_allocator_state()
            .await
            .map(|state| state.position())
            .map_err(anyhow::Error::new)
    }

    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        PassiveDatastore::snapshot_resources_at_position(
            self,
            targets,
            label_selector,
            field_selector,
            position,
        )
        .await
    }

    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        use klights_cluster_store::DurableRawWatchHistoryRead as _;

        let request = klights_cluster_store::RawWatchEventsSinceRequest::try_new(
            focused_watch_targets(targets),
            since_rv,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_read_store()
            .list_raw_watch_events_since_checked_bounded(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::RawWatchHistoryRead::Expired => Ok(WatchReplayRead::Expired),
            klights_cluster_store::RawWatchHistoryRead::Events(page) => {
                Ok(WatchReplayRead::Events(page.into_events()))
            }
        }
    }

    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        use klights_cluster_store::DurableRawWatchHistoryRead as _;

        let request = klights_cluster_store::RawWatchEventsAfterPositionRequest::try_new(
            focused_watch_targets(targets),
            position,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_read_store()
            .list_raw_watch_events_after_position_checked_bounded(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::PositionedRawWatchHistoryRead::Expired => {
                Ok(PositionedWatchReplayRead::Expired)
            }
            klights_cluster_store::PositionedRawWatchHistoryRead::Events(page) => {
                let next_position = page.next_position();
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page.into_events(),
                    next_position,
                }))
            }
        }
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        self.focused_read_store()
            .earliest_watch_event_rv()
            .await
            .map_err(anyhow::Error::new)
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchRangeStart::try_new(since_rv)
            .map_err(anyhow::Error::new)?;
        self.focused_read_store()
            .list_all_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        PassiveDatastore::list_all_watch_events_since_paged(
            self,
            since_rv,
            after_resource_version,
            after_id,
            limit,
        )
        .await
    }

    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        PassiveDatastore::list_all_watch_events_after_id_bounded(self, after_id, through_id, limit)
            .await
    }

    async fn list_watch_replay_floors(&self) -> Result<Vec<crate::datastore::WatchReplayFloor>> {
        use klights_cluster_store::DurableWatchHistoryRead as _;

        self.focused_read_store()
            .list_replay_floors()
            .await
            .map(|floors| {
                floors
                    .into_iter()
                    .map(|floor| {
                        let (target, floor_resource_version, floor_event_id, position_is_exact) =
                            floor.into_parts();
                        let (api_version, kind, namespace_key) = match target {
                            klights_cluster_store::DurableReplayTarget::All => {
                                ("*".to_string(), "*".to_string(), "*".to_string())
                            }
                            klights_cluster_store::DurableReplayTarget::Cluster {
                                api_version,
                                kind,
                            } => (api_version, kind, "#cluster".to_string()),
                            klights_cluster_store::DurableReplayTarget::Namespaced {
                                api_version,
                                kind,
                                namespace,
                            } => (api_version, kind, namespace),
                        };
                        crate::datastore::WatchReplayFloor {
                            api_version,
                            kind,
                            namespace_key,
                            floor_resource_version,
                            floor_event_id,
                            position_is_exact,
                        }
                    })
                    .collect()
            })
            .map_err(anyhow::Error::new)
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<crate::datastore::WatchReplayFloor>> {
        PassiveDatastore::list_watch_replay_floors_paged(self, after, limit).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchRangeStart::try_new(since_rv)
            .map_err(anyhow::Error::new)?;
        self.focused_read_store()
            .list_deleted_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        PassiveDatastore::allocate_node_subnet(self, node_name, cluster_cidr, node_ip).await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        PassiveDatastore::update_node_peer_attributes(self, node_name, mode, hostport_range).await
    }

    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        PassiveDatastore::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        PassiveDatastore::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        PassiveDatastore::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        PassiveDatastore::list_peer_subnets(self, request).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        PassiveDatastore::delete_node_subnet(self, node_name).await
    }

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        PassiveDatastore::move_pod_to_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        PassiveDatastore::list_pod_cleanup_intents_for_node(self, node_name).await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        PassiveDatastore::delete_pod_cleanup_intent(
            self, node_name, namespace, pod_name, pod_uid, reason,
        )
        .await
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        PassiveDatastore::delete_pod_cleanup_intents_for_node(self, node_name).await
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
        PassiveDatastore::patch_resource_latest(
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
        PassiveDatastore::patch_resource_latest_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            request,
        )
        .await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        PassiveDatastore::watch_events_gc_prunable_count(self, max_rows, batch_cap).await
    }

    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        PassiveDatastore::applied_outbox_gc_prunable_count(self, cutoff_ms).await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        PassiveDatastore::gc_watch_events(self, max_rows, batch_cap).await
    }

    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        klights_cluster_store::ClusterMetadataMutation::get_klights_meta(&self.0, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        klights_cluster_store::ClusterMetadataMutation::set_klights_meta(&self.0, key, value).await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        PassiveDatastore::list_outbox_stream_watermarks(self).await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        PassiveDatastore::list_outbox_stream_watermarks_paged(self, after, limit).await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>> {
        PassiveDatastore::get_applied_outbox(self, idempotency_key).await
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        PassiveDatastore::insert_applied_outbox(self, record).await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        PassiveDatastore::list_applied_outbox(self).await
    }

    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        PassiveDatastore::list_applied_outbox_paged(self, after_key, limit).await
    }

    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        PassiveDatastore::apply_outbox_transactionally(
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        PassiveDatastore::apply_outbox_transactionally_with_watermark(
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
        PassiveDatastore::apply_outbox_transactionally_with_watermark_effect(
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
        command: klights_cluster_core::command::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        PassiveDatastore::build_log_apply_commit_for_command(
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<BuildOutboxOutcome, klights_cluster_core::OutboxApplyError> {
        PassiveDatastore::build_log_apply_commit_for_outbox(
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<BuildOutboxOutcome, klights_cluster_core::OutboxApplyError> {
        PassiveDatastore::build_log_apply_commit_for_outbox_with_watermark(
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
        PassiveDatastore::gc_applied_outbox(self, now_ms, ttl_ms).await
    }
}

#[async_trait::async_trait]
impl crate::datastore::MetaStore for Datastore {
    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_klights_meta(self, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        crate::datastore::DatastoreBackend::set_klights_meta(self, key, value).await
    }
}
