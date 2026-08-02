//! `DatastoreBackend` implementation for `RedbDatastore`.
//!
//! Every trait method delegates to the appropriate composed domain store.
//! Root datastore contracts are adapted here, while canonical reads and Redb
//! persistence algorithms remain owned by the focused lower modules.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::types::*;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
use klights_cluster_datastore::redb::read_core::RedbCheckedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbCollectionScope;
use klights_cluster_datastore::redb::read_core::RedbListQuery;
use klights_cluster_datastore::redb::read_core::RedbPositionedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbSnapshotRead;
#[cfg(test)]
use klights_cluster_datastore::redb::tables;
#[cfg(test)]
use klights_cluster_store::StagedPostCommit;
use klights_types::HostPortRange;
use klights_types::NodePeerMode;
#[cfg(test)]
use klights_watch::{WatchSignal, WatchTopic};

use super::RedbDatastore;

fn legacy_target_to_durable(target: &WatchTarget) -> klights_cluster_store::DurableWatchTarget {
    match &target.scope {
        WatchTargetScope::Cluster => {
            klights_cluster_store::DurableWatchTarget::cluster(&target.api_version, &target.kind)
        }
        WatchTargetScope::Namespaced(None) => {
            klights_cluster_store::DurableWatchTarget::namespaced(&target.api_version, &target.kind)
        }
        WatchTargetScope::Namespaced(Some(namespace)) => {
            klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                &target.api_version,
                &target.kind,
                namespace,
            )
        }
    }
}

fn durable_to_catchup(event: klights_cluster_store::DurableWatchEvent) -> CatchUpResource {
    let event_type = std::borrow::Cow::Owned(event.event_type().to_string());
    CatchUpResource {
        resource: event.into_resource(),
        event_type,
    }
}

fn durable_floor_to_legacy(floor: klights_cluster_store::DurableReplayFloor) -> WatchReplayFloor {
    let (target, floor_resource_version, floor_event_id, position_is_exact) = floor.into_parts();
    let (api_version, kind, namespace_key) = match target {
        klights_cluster_store::DurableReplayTarget::All => {
            ("*".to_string(), "*".to_string(), "*".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, "#cluster".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    };
    WatchReplayFloor {
        api_version,
        kind,
        namespace_key,
        floor_resource_version,
        floor_event_id,
        position_is_exact,
    }
}

#[cfg(test)]
use klights_cluster_datastore::redb::live_committed_apply::outbox_watermark_key;

#[async_trait]
impl DatastoreBackend for RedbDatastore {
    #[cfg(any(test, feature = "integration-test-harness"))]
    fn commit_observation_sink(
        &self,
    ) -> std::sync::Arc<dyn crate::datastore::CommitObservationSink> {
        klights_cluster_datastore::redb::embedded::RedbDatastore::commit_observation_sink(self)
            .expect("test datastore must install a commit observation sink")
    }

    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation> {
        use klights_cluster_store::DurableAllocatorRead;
        let state = self
            .focused_read_store()
            .read_allocator_state()
            .await
            .map_err(anyhow::Error::from)?;
        Ok(DurableAllocatorObservation {
            position: state.position(),
        })
    }

    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation> {
        let observed = self.recovery_store().read_cluster_metadata().await?;
        let membership = match observed.membership {
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
            metadata: observed.metadata,
            membership,
        })
    }

    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotExclusiveFence>> {
        Ok(Some(klights_cluster_store::SnapshotExclusiveFence::new(
            self.accessor.acquire_snapshot_exclusive().await,
        )))
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> Result<Option<crate::datastore::backend::SnapshotMutationFence>> {
        Ok(Some(klights_cluster_store::SnapshotMutationFence::new(
            self.accessor.acquire_snapshot_mutation().await,
        )))
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        self.recovery_store().begin_snapshot(request, fence).await
    }

    fn close(&self) {
        self.accessor.close();
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        crate::watch_commit_observation_adapter::subscribe_test_events(
            klights_cluster_datastore::redb::embedded::RedbDatastore::commit_observation_sink(self)
                .expect("test datastore must install a commit observation sink")
                .as_ref(),
            topic,
        )
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::watch_commit_observation_adapter::subscribe_test_events_many(
            klights_cluster_datastore::redb::embedded::RedbDatastore::commit_observation_sink(self)
                .expect("test datastore must install a commit observation sink")
                .as_ref(),
            topics,
        )
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        let event = crate::datastore::staged_test_event(&pending).expect("staged test watch event");
        let _ = WatchSignal::from_event(&event);
        crate::watch_commit_observation_adapter::publish_test_events(
            klights_cluster_datastore::redb::embedded::RedbDatastore::commit_observation_sink(self)
                .expect("test datastore must install a commit observation sink")
                .as_ref(),
            vec![event],
        );
    }

    async fn apply_log_apply_commit(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        self.live_committed_apply_store().apply_log_apply_commit()
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        self.live_committed_apply_store()
            .apply_raft_log_apply_commit()
    }

    async fn apply_raft_log_apply_commit_receipt(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        self.live_committed_apply_store()
            .apply_raft_log_apply_commit_receipt()
    }

    async fn create_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
    ) -> Result<Resource> {
        let committed = self.resources().create_res(a, k, n, m, d).await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn get_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
    ) -> Result<Option<Resource>> {
        self.focused_read_store()
            .core()
            .get_resource(a, k, n, m)
            .await
    }
    async fn update_resource(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
        e: i64,
    ) -> Result<Resource> {
        let committed = self.resources().update_res(a, k, n, m, d, e).await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn update_resource_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        d: Value,
        p: ResourcePreconditions,
    ) -> Result<Resource> {
        let committed = self
            .resources()
            .update_res_with_preconditions(a, k, n, m, d, p)
            .await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn delete_resource(&self, a: &str, k: &str, n: Option<&str>, m: &str) -> Result<()> {
        let committed = self.resources().delete_res(a, k, n, m).await?;
        self.finish_post_commit(committed);
        Ok(())
    }
    async fn delete_resource_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        p: ResourcePreconditions,
    ) -> Result<()> {
        let committed = self
            .resources()
            .delete_res_with_preconditions(a, k, n, m, p)
            .await?;
        self.finish_post_commit(committed);
        Ok(())
    }

    async fn delete_resource_without_watch_with_tombstone(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        p: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource> {
        let committed = self
            .resources()
            .delete_res_with_tombstone(a, k, n, m, p, grace_seconds)
            .await?;
        Ok(self.finish_post_commit(committed))
    }

    async fn list_resources(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        let cursor = query.continue_token.map(|name| {
            klights_cluster_store::ResourceCollectionKey::new(
                n.map(str::to_string),
                name.to_string(),
            )
        });
        let page = self
            .focused_read_store()
            .core()
            .list_resources(
                a,
                k,
                n.map_or(RedbCollectionScope::LegacyAny, |namespace| {
                    RedbCollectionScope::Namespace(namespace.to_string())
                }),
                RedbListQuery {
                    label_selector: query.label_selector.map(str::to_string),
                    field_selector: query.field_selector.map(str::to_string),
                    limit: query.limit,
                    cursor,
                },
            )
            .await?;
        Ok(ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
            continue_token: page
                .continuation
                .map(|continuation| continuation.name().to_string()),
            remaining_item_count: page.remaining_item_count,
        })
    }
    async fn list_resources_page(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        ls: Option<&str>,
        fs: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.resources().list_res_page(a, k, n, ls, fs, page).await
    }
    async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.resources()
            .list_resources_for_watch_targets(targets, label_selector)
            .await
    }
    async fn list_resource_keys_for_scope(
        &self,
        a: String,
        k: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        self.namespaces()
            .list_resource_keys_for_scope_impl(&a, &k, namespaced)
            .await
    }
    async fn update_status_only(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        s: Value,
        e: Option<i64>,
    ) -> Result<Resource> {
        let committed = self
            .resources()
            .update_status_only_impl(a, k, n, m, s, e)
            .await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn update_status_only_with_preconditions(
        &self,
        a: &str,
        k: &str,
        n: Option<&str>,
        m: &str,
        s: Value,
        p: ResourcePreconditions,
    ) -> Result<Resource> {
        let committed = self
            .resources()
            .update_status_only_with_preconditions_impl(a, k, n, m, s, p)
            .await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn get_current_resource_version(&self) -> Result<i64> {
        use klights_cluster_store::DurableAllocatorRead;
        self.focused_read_store()
            .read_allocator_state()
            .await
            .map(|state| state.position().resource_version)
            .map_err(anyhow::Error::from)
    }
    async fn create_namespace(&self, n: &str, d: Value) -> Result<Resource> {
        let committed = self.namespaces().create_ns(n, d).await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn get_namespace(&self, n: &str) -> Result<Option<Resource>> {
        self.focused_read_store()
            .core()
            .get_resource("v1", "Namespace", None, n)
            .await
    }
    async fn list_namespaces(&self, ls: Option<&str>, fs: Option<&str>) -> Result<ResourceList> {
        let page = self
            .focused_read_store()
            .core()
            .list_resources(
                "v1",
                "Namespace",
                RedbCollectionScope::Cluster,
                RedbListQuery {
                    label_selector: ls.map(str::to_string),
                    field_selector: fs.map(str::to_string),
                    limit: None,
                    cursor: None,
                },
            )
            .await?;
        Ok(ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
            continue_token: None,
            remaining_item_count: None,
        })
    }
    async fn list_namespaces_page(
        &self,
        ls: Option<&str>,
        fs: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let list = self.list_namespaces(ls, fs).await?;
        Ok(page.apply_to_sorted_resource_list(list))
    }
    async fn update_namespace(&self, n: &str, d: Value, e: i64) -> Result<Resource> {
        self.namespaces().update_ns_impl(n, d, e).await
    }
    async fn delete_namespace_contents(&self, n: &str) -> Result<()> {
        self.namespaces().delete_namespace_contents_impl(n).await
    }
    async fn delete_namespace(&self, n: &str) -> Result<()> {
        self.namespaces().delete_ns_impl(n).await
    }
    async fn find_owned_resources(&self, o: &str, ns: Option<&str>) -> Result<Vec<Resource>> {
        self.focused_read_store().core().find_owned(o, ns).await
    }
    async fn list_resources_by_owner_uid(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        o: &str,
    ) -> Result<Vec<Resource>> {
        let mut resources = self.focused_read_store().core().find_owned(o, ns).await?;
        resources.retain(|r| r.api_version == a && r.kind == k);
        Ok(resources)
    }
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        ns: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let candidates = self.focused_read_store().core().find_owned("", ns).await?;
        let filtered: Vec<Resource> = candidates
            .into_iter()
            .filter(|r| {
                let refs = r
                    .data
                    .get("metadata")
                    .and_then(|m| m.get("ownerReferences"))
                    .and_then(|v| v.as_array());
                match refs {
                    Some(refs) => refs.iter().any(|ore| {
                        ore.get("uid")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .is_empty()
                            && ore.get("apiVersion").and_then(|v| v.as_str())
                                == Some(owner_api_version)
                            && ore.get("kind").and_then(|v| v.as_str()) == Some(owner_kind)
                            && ore.get("name").and_then(|v| v.as_str()) == Some(owner_name)
                    }),
                    None => false,
                }
            })
            .collect();
        Ok(filtered)
    }
    async fn list_cluster_resources_modified_since(
        &self,
        a: &str,
        k: &str,
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.focused_read_store()
            .core()
            .watch_events_since(
                &[klights_cluster_store::DurableWatchTarget::cluster(a, k)],
                s,
            )
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.focused_read_store()
            .core()
            .list_cluster_resources()
            .await
    }
    async fn list_resources_modified_since(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let target = ns.map_or_else(
            || klights_cluster_store::DurableWatchTarget::cluster(a, k),
            |namespace| {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(a, k, namespace)
            },
        );
        self.focused_read_store()
            .core()
            .watch_events_since(&[target], s)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.rv_store().advance_rv(min_rv).await
    }
    async fn list_namespace_resources(&self, ns: &str) -> Result<Vec<Resource>> {
        self.focused_read_store()
            .core()
            .list_namespace_resources(ns, None, false)
            .await
    }
    async fn list_namespace_resources_of_kind(&self, ns: &str, k: &str) -> Result<Vec<Resource>> {
        self.focused_read_store()
            .core()
            .list_namespace_resources(ns, Some(k), false)
            .await
    }
    async fn list_namespace_resources_excluding_kind(
        &self,
        ns: &str,
        k: &str,
    ) -> Result<Vec<Resource>> {
        self.focused_read_store()
            .core()
            .list_namespace_resources(ns, Some(k), true)
            .await
    }
    async fn count_namespace_resources(&self, ns: &str) -> Result<i64> {
        self.focused_read_store()
            .core()
            .count_namespace_resources(ns)
            .await
    }
    async fn list_watch_events_since(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        self.focused_read_store()
            .core()
            .watch_events_since(&targets, s)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_watch_events_since_checked(
        &self,
        t: &[WatchTarget],
        s: i64,
    ) -> Result<WatchReplayRead> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .watch_events_since_checked(&targets, s, None)
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(
                events.into_iter().map(durable_to_catchup).collect(),
            )),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_watch_events_since_checked_bounded(
        &self,
        t: &[WatchTarget],
        s: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .watch_events_since_checked(&targets, s, Some(limit))
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(
                events.into_iter().map(durable_to_catchup).collect(),
            )),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .positioned_watch_events(&targets, position, limit)
            .await?
        {
            RedbPositionedWatchRead::Expired => Ok(PositionedWatchReplayRead::Expired),
            RedbPositionedWatchRead::Events(page) => {
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page
                        .events
                        .into_iter()
                        .map(|event| klights_cluster_core::PositionedWatchEvent {
                            position: event.position,
                            event: durable_to_catchup(event.event),
                        })
                        .collect(),
                    next_position: page.next_position,
                }))
            }
        }
    }
    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.focused_read_store().core().allocator_position().await
    }
    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .snapshot_at_position(&targets, label_selector, field_selector, position)
            .await?
        {
            RedbSnapshotRead::Expired => Ok(SnapshotAtRv::Expired),
            RedbSnapshotRead::Historical { items, position } => {
                Ok(SnapshotAtRv::List(ResourceList {
                    items,
                    resource_version: position.resource_version,
                    watch_replay_position: Some(position),
                    continue_token: None,
                    remaining_item_count: None,
                }))
            }
        }
    }
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        t: &[WatchTarget],
        s: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets: Vec<_> = t.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .raw_watch_events_since_checked(&targets, s, limit)
            .await?
        {
            RedbCheckedWatchRead::Events(events) => Ok(WatchReplayRead::Events(events)),
            RedbCheckedWatchRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }
    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets: Vec<_> = targets.iter().map(legacy_target_to_durable).collect();
        match self
            .focused_read_store()
            .core()
            .positioned_raw_watch_events(&targets, position, limit)
            .await?
        {
            RedbPositionedWatchRead::Expired => Ok(PositionedWatchReplayRead::Expired),
            RedbPositionedWatchRead::Events(page) => {
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page.events,
                    next_position: page.next_position,
                }))
            }
        }
    }
    async fn list_all_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.focused_read_store()
            .core()
            .all_watch_events_since(s, false)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn list_all_watch_events_since_paged(
        &self,
        s: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        let _ = after_resource_version;
        self.focused_read_store()
            .core()
            .all_watch_events_since_paged(s, after_id, None, limit)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|(id, event)| (id, durable_to_catchup(event)))
                    .collect()
            })
    }
    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.focused_read_store()
            .core()
            .all_watch_events_since_paged(0, after_id, Some(through_id), limit)
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|(id, event)| (id, durable_to_catchup(event)))
                    .collect()
            })
    }
    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        self.focused_read_store()
            .core()
            .replay_floors()
            .await
            .map(|floors| floors.into_iter().map(durable_floor_to_legacy).collect())
    }

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        self.focused_read_store()
            .core()
            .replay_floors_paged(after, limit)
            .await
            .map(|floors| floors.into_iter().map(durable_floor_to_legacy).collect())
    }
    async fn list_deleted_watch_events_since(&self, s: i64) -> Result<Vec<CatchUpResource>> {
        self.focused_read_store()
            .core()
            .all_watch_events_since(s, true)
            .await
            .map(|events| events.into_iter().map(durable_to_catchup).collect())
    }
    async fn allocate_node_subnet(
        &self,
        n: &str,
        c: &str,
        i: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        self.network_store().allocate_node_subnet(n, c, i).await
    }
    async fn update_node_peer_attributes(
        &self,
        n: &str,
        mode: NodePeerMode,
        hpr: Option<HostPortRange>,
    ) -> Result<()> {
        self.network_store().update_peer_attrs(n, mode, hpr).await
    }
    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        self.network_store().update_node_dataplane(metadata).await
    }
    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        self.focused_read_store()
            .core()
            .get_node_dataplane(node_name)
            .await
    }
    async fn get_node_subnet(
        &self,
        n: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        self.focused_read_store().core().get_node_subnet(n).await
    }
    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        self.focused_read_store()
            .core()
            .list_peer_subnets(request)
            .await
    }
    async fn delete_node_subnet(&self, n: &str) -> Result<()> {
        self.network_store().delete_node_subnet(n).await
    }
    async fn patch_resource_latest(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        n: &str,
        _pk: PatchKind,
        p: Value,
    ) -> Result<Option<Resource>> {
        let committed = self.resources().patch(a, k, ns, n, p).await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn patch_resource_latest_with_preconditions(
        &self,
        a: &str,
        k: &str,
        ns: Option<&str>,
        n: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let committed = self
            .resources()
            .patch_with_preconditions(a, k, ns, n, request)
            .await?;
        Ok(self.finish_post_commit(committed))
    }
    async fn watch_events_gc_prunable_count(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store().gc_watch_prunable_count(m, b).await
    }
    async fn gc_watch_events(&self, m: i64, b: i64) -> Result<usize> {
        self.watch_store().gc_watch(m, b).await
    }
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        self.live_committed_apply_store()
            .applied_outbox_prunable_count(cutoff_ms)
            .await
    }

    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply_store()
            .list_outbox_watermarks()
            .await
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.live_committed_apply_store()
            .list_outbox_watermarks_paged(after, limit)
            .await
    }

    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.recovery_store().get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.live_committed_apply_store()
            .set_klights_meta(key, value)
            .await
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> anyhow::Result<Option<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply_store()
            .get_applied_outbox_bytes(idempotency_key)
            .await?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .transpose()
    }

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool> {
        let idempotency_key = record.idempotency_key.clone();
        let bytes = serde_json::to_vec(&record)?;
        self.live_committed_apply_store()
            .insert_applied_outbox_bytes(idempotency_key, bytes)
            .await
    }

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        self.live_committed_apply_store()
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
        self.live_committed_apply_store()
            .list_applied_outbox_bytes_paged(after_key, limit)
            .await?
            .into_iter()
            .map(|bytes| serde_json::from_slice(&bytes).map_err(anyhow::Error::from))
            .collect()
    }

    async fn apply_outbox_transactionally(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply_store()
            .apply_outbox_transactionally()
    }

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply_store()
            .apply_outbox_transactionally_with_watermark()
    }

    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply_store()
            .apply_outbox_transactionally_with_watermark_effect()
    }

    async fn build_log_apply_commit_for_command(
        &self,
        _command: klights_cluster_core::command::StorageCommand,
        _operation: &str,
        _authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        self.live_committed_apply_store()
            .build_log_apply_commit_for_command()
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply_store()
            .build_log_apply_commit_for_outbox()
    }

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: klights_cluster_core::command::StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        self.live_committed_apply_store()
            .build_log_apply_commit_for_outbox_with_watermark()
    }

    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        self.live_committed_apply_store()
            .gc_applied_outbox(now_ms, ttl_ms)
            .await
    }
}

#[async_trait]
impl crate::datastore::ResourceStore for RedbDatastore {
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
impl crate::datastore::CurrentResourceVersionStore for RedbDatastore {
    async fn get_current_resource_version(&self) -> Result<i64> {
        crate::datastore::DatastoreBackend::get_current_resource_version(self).await
    }
}

#[async_trait]
impl crate::datastore::ResourceListStore for RedbDatastore {
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
impl crate::datastore::NamespaceStore for RedbDatastore {
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
impl crate::datastore::WatchHistoryStore for RedbDatastore {
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
impl crate::datastore::NamespaceContentStore for RedbDatastore {
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
impl crate::datastore::OwnershipStore for RedbDatastore {
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
impl crate::datastore::StatusStore for RedbDatastore {
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
impl crate::datastore::MetaStore for RedbDatastore {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        crate::datastore::DatastoreBackend::get_klights_meta(self, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        crate::datastore::DatastoreBackend::set_klights_meta(self, key, value).await
    }
}

#[async_trait]
impl crate::datastore::NetworkMetadataStore for RedbDatastore {
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
impl crate::datastore::ReplicationStore for RedbDatastore {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: klights_cluster_core::command::StorageCommand,
        meta: klights_cluster_core::command::CommandMeta,
    ) -> Result<()> {
        self.0.apply_legacy_test_command(command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
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
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        crate::datastore::DatastoreBackend::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit_receipt(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        crate::datastore::DatastoreBackend::apply_raft_log_apply_commit_receipt(self, commit).await
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
impl crate::datastore::DurableRecoveryStore for RedbDatastore {
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

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        crate::datastore::DatastoreBackend::begin_pinned_snapshot_capture(self, request, fence)
            .await
    }
}

#[async_trait]
impl klights_cluster_store::BackendLifecycleStore for RedbDatastore {
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
impl crate::datastore::TestWatchStore for RedbDatastore {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        crate::datastore::DatastoreBackend::subscribe_watch_many(self, topics)
    }

    fn broadcast_watch_event(&self, pending: StagedPostCommit) {
        crate::datastore::DatastoreBackend::broadcast_watch_event(self, pending);
    }
}

#[async_trait]
impl crate::datastore::ClusterResourceQueryStore for RedbDatastore {
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
impl crate::datastore::LeaderResourceMutationStore for RedbDatastore {
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
impl crate::datastore::WatchMaintenanceStore for RedbDatastore {
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
impl crate::datastore::PodCleanupStore for RedbDatastore {
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
impl crate::datastore::AppliedOutboxStore for RedbDatastore {
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::CommittedOutboxApply,
        klights_cluster_core::OutboxApplyError,
    > {
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
        command: klights_cluster_core::command::StorageCommand,
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
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
        command: klights_cluster_core::command::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
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

#[cfg(test)]
mod snapshot_paging_tests {
    use super::*;

    #[tokio::test]
    async fn watermark_keyset_pages_are_bounded_complete_and_exclusive() {
        let store = RedbDatastore::new_in_memory().await.unwrap();
        let db = store.accessor.db().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut watermarks = write.open_table(tables::OUTBOX_STREAM_WATERMARKS).unwrap();
            for index in 0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
                let client_id = format!("worker-{index:04}");
                let key = outbox_watermark_key(&client_id, 1).unwrap();
                watermarks.insert(key.as_slice(), index as i64 + 1).unwrap();
            }
        }
        write.commit().unwrap();

        let page_limit =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE).unwrap();
        let mut after = None;
        let mut delivered = Vec::new();
        let mut page_lengths = Vec::new();
        loop {
            let page = store
                .list_outbox_stream_watermarks_paged(after.as_ref(), page_limit)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            page_lengths.push(page.len());
            delivered.extend(page.iter().map(|row| row.client_id.clone()));
            after = Some(
                klights_cluster_store::SnapshotOutboxWatermarkCursor::from_watermark(
                    page.last().unwrap(),
                )
                .unwrap(),
            );
        }
        assert_eq!(
            page_lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
        assert_eq!(
            delivered.len(),
            klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1
        );
        assert!(delivered.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            store.list_outbox_stream_watermarks().await.unwrap().len(),
            delivered.len()
        );
    }
}
