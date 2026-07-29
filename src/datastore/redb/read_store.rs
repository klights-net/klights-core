//! Focused, passive Redb cluster-read capabilities.
//!
//! The facade and its read-core dependency form the mechanically movable
//! Phase 10B Redb slice. Root datastore compatibility lives outside it.

use anyhow::Result;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_cluster_datastore::errors;
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, ClusterOwnershipRead, ClusterResourceRead,
    ClusterResourceScopeRead, ClusterTopologyFuture, ClusterTopologyRead, ClusterTopologyReadError,
    DurableAllocatorRead, DurableAllocatorState, DurableRawWatchHistoryRead, DurableReplayFloor,
    DurableWatchEvent, DurableWatchHistoryRead, DurableWatchRangeRead,
    ModifiedClusterResourcesRequest, ModifiedResourcesRequest, NamespaceContentFuture,
    NamespaceContentRead, NamespaceKindRequest, NamespaceRequest, NodeTopologyRequest,
    OwnedKindRequest, OwnerNameKindRequest, OwnerUidRequest, OwnershipReadFuture,
    PeerTopologyRequest, PositionedRawWatchHistoryPage, PositionedRawWatchHistoryRead,
    RawWatchEventsAfterPositionRequest, RawWatchEventsSinceRequest, RawWatchHistoryPage,
    RawWatchHistoryRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceGetRequest, ResourceKeyScopeRequest, ResourceListPage, ResourceListRead,
    ResourceListRequest, ResourceListSnapshot, ResourceReadError, ResourceReadFuture,
    ResourceScopeSnapshot, ResourceSnapshotAtPositionRequest, ResourceSnapshotRead,
    ResourceVersionMatch, ResourceWatchTargetsRequest, StoredNodeSubnet, WatchEventsSinceRequest,
    WatchHistoryError, WatchHistoryFuture, WatchHistoryPage, WatchHistoryRead, WatchHistoryRequest,
    WatchRangeFuture, WatchRangeStart,
};

use super::read_core::{
    RedbCheckedWatchRead, RedbCollectionScope, RedbListQuery, RedbPositionedWatchRead,
    RedbReadCore, RedbResourceList, RedbSnapshotRead,
};

#[derive(Clone)]
pub struct RedbReadStore {
    core: RedbReadCore,
}

impl RedbReadStore {
    pub(super) fn new(accessor: std::sync::Arc<RedbAccessor>) -> Self {
        Self {
            core: RedbReadCore::new(accessor),
        }
    }

    pub(super) const fn core(&self) -> &RedbReadCore {
        &self.core
    }

    fn historical_list(
        &self,
        request: ResourceListRequest,
        position: WatchReplayPosition,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async move {
            let target = durable_target_for_collection(
                request.api_version(),
                request.kind(),
                request.scope(),
            );
            match self
                .core
                .snapshot_at_position(
                    &[target],
                    request.query().label_selector(),
                    request.query().field_selector(),
                    position,
                )
                .await
                .map_err(map_resource_error)?
            {
                RedbSnapshotRead::Expired => Err(ResourceReadError::Expired {
                    requested: position.resource_version,
                    oldest_available: 0,
                }),
                RedbSnapshotRead::Historical { items, position } => Ok(
                    ResourceListRead::Historical(page_items(items, request.query(), position)?),
                ),
            }
        })
    }

    fn current_list(
        &self,
        request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async move {
            let query = request.query();
            let scope = match request.scope() {
                ResourceCollectionScope::Cluster => RedbCollectionScope::Cluster,
                ResourceCollectionScope::AllNamespaces => RedbCollectionScope::AllNamespaces,
                ResourceCollectionScope::Namespace(namespace) => {
                    RedbCollectionScope::Namespace(namespace.clone())
                }
            };
            let page = self
                .core
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    scope,
                    RedbListQuery {
                        label_selector: query.label_selector().map(str::to_string),
                        field_selector: query.field_selector().map(str::to_string),
                        limit: query.limit(),
                        cursor: None,
                    },
                )
                .await
                .map_err(map_resource_error)?;
            if let ResourceVersionMatch::NotOlderThan(minimum) = query.resource_version_match()
                && page.position.resource_version < minimum
            {
                return Err(ResourceReadError::Conflict {
                    message: format!(
                        "current resourceVersion {} is older than requested {minimum}",
                        page.position.resource_version
                    ),
                });
            }
            Ok(ResourceListRead::Current(focused_page(page)?))
        })
    }
}

impl DurableAllocatorRead for RedbReadStore {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        Box::pin(async move {
            let position = self.core.allocator_position().await.map_err(|error| {
                AllocatorStateError::PersistenceFailed {
                    message: format!("{error:#}"),
                }
            })?;
            DurableAllocatorState::try_new(position)
        })
    }
}

impl ClusterResourceRead for RedbReadStore {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let key = request.into_key();
            self.core
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        if let Some(position) = request
            .query()
            .continuation()
            .map(|cursor| cursor.snapshot().position())
        {
            return self.historical_list(request, position);
        }
        match request.query().resource_version_match() {
            ResourceVersionMatch::Exact(resource_version) => self.historical_list(
                request,
                WatchReplayPosition {
                    resource_version,
                    event_id: 0,
                    resource_version_filter_through_event_id: 0,
                },
            ),
            ResourceVersionMatch::AtPosition(position) => self.historical_list(request, position),
            ResourceVersionMatch::Any | ResourceVersionMatch::NotOlderThan(_) => {
                self.current_list(request)
            }
        }
    }
}

impl ClusterResourceScopeRead for RedbReadStore {
    fn list_resources_for_watch_targets(
        &self,
        request: ResourceWatchTargetsRequest,
    ) -> ResourceReadFuture<'_, ResourceScopeSnapshot> {
        Box::pin(async move {
            let (items, position) = self
                .core
                .list_resources_for_watch_targets(request.targets(), request.label_selector())
                .await
                .map_err(map_resource_error)?;
            ResourceScopeSnapshot::try_new(items, position)
        })
    }

    fn list_resource_keys_for_scope(
        &self,
        request: ResourceKeyScopeRequest,
    ) -> ResourceReadFuture<'_, Vec<ResourceCollectionKey>> {
        Box::pin(async move {
            self.core
                .list_resource_keys(request.api_version(), request.kind(), request.namespaced())
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_cluster_resources(&self) -> ResourceReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.core
                .list_cluster_resources()
                .await
                .map_err(map_resource_error)
        })
    }

    fn snapshot_resources_at_position(
        &self,
        request: ResourceSnapshotAtPositionRequest,
    ) -> ResourceReadFuture<'_, ResourceSnapshotRead> {
        Box::pin(async move {
            match self
                .core
                .snapshot_at_position(
                    request.targets(),
                    request.label_selector(),
                    request.field_selector(),
                    request.position(),
                )
                .await
                .map_err(map_resource_error)?
            {
                RedbSnapshotRead::Expired => Ok(ResourceSnapshotRead::Expired),
                RedbSnapshotRead::Historical { items, position } => {
                    ResourceScopeSnapshot::try_new(items, position)
                        .map(ResourceSnapshotRead::Historical)
                }
            }
        })
    }
}

impl ClusterOwnershipRead for RedbReadStore {
    fn find_owned_resources(
        &self,
        request: OwnerUidRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.core
                .find_owned(request.owner_uid(), request.namespace())
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_resources_by_owner_uid(
        &self,
        request: OwnedKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            let mut resources = self
                .core
                .find_owned(request.owner_uid(), request.namespace())
                .await
                .map_err(map_resource_error)?;
            resources.retain(|resource| {
                resource.api_version == request.api_version() && resource.kind == request.kind()
            });
            Ok(resources)
        })
    }

    fn find_owned_by_name_kind_empty_uid(
        &self,
        request: OwnerNameKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            let mut resources = self
                .core
                .find_owned("", request.namespace())
                .await
                .map_err(map_resource_error)?;
            resources.retain(|resource| {
                resource
                    .data
                    .pointer("/metadata/ownerReferences")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|owners| {
                        owners.iter().any(|owner| {
                            owner
                                .get("uid")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .is_empty()
                                && owner.get("apiVersion").and_then(serde_json::Value::as_str)
                                    == Some(request.owner_api_version())
                                && owner.get("kind").and_then(serde_json::Value::as_str)
                                    == Some(request.owner_kind())
                                && owner.get("name").and_then(serde_json::Value::as_str)
                                    == Some(request.owner_name())
                        })
                    })
            });
            Ok(resources)
        })
    }
}

impl NamespaceContentRead for RedbReadStore {
    fn list_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.core
                .list_namespace_resources(request.namespace(), None, false)
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_namespace_resources_of_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.core
                .list_namespace_resources(request.namespace(), Some(request.kind()), false)
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_namespace_resources_excluding_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.core
                .list_namespace_resources(request.namespace(), Some(request.kind()), true)
                .await
                .map_err(map_resource_error)
        })
    }

    fn count_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, i64> {
        Box::pin(async move {
            self.core
                .count_namespace_resources(request.namespace())
                .await
                .map_err(map_resource_error)
        })
    }
}

impl DurableWatchHistoryRead for RedbReadStore {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        Box::pin(async move {
            match self
                .core
                .positioned_watch_events(request.targets(), request.position(), request.limit())
                .await
                .map_err(map_watch_error)?
            {
                RedbPositionedWatchRead::Expired => Ok(WatchHistoryRead::Expired),
                RedbPositionedWatchRead::Events(page) => {
                    WatchHistoryPage::try_new(page.events, page.next_position)
                        .map(WatchHistoryRead::Events)
                }
            }
        })
    }

    fn list_replay_floors(&self) -> WatchHistoryFuture<'_, Vec<DurableReplayFloor>> {
        Box::pin(async move { self.core.replay_floors().await.map_err(map_watch_error) })
    }
}

impl DurableWatchRangeRead for RedbReadStore {
    fn list_cluster_resources_modified_since(
        &self,
        request: ModifiedClusterResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        let target = klights_cluster_store::DurableWatchTarget::cluster(
            request.api_version(),
            request.kind(),
        );
        Box::pin(async move {
            self.core
                .watch_events_since(&[target], request.since_resource_version())
                .await
                .map_err(map_watch_error)
        })
    }

    fn list_resources_modified_since(
        &self,
        request: ModifiedResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        let target = request.namespace().map_or_else(
            || {
                klights_cluster_store::DurableWatchTarget::cluster(
                    request.api_version(),
                    request.kind(),
                )
            },
            |namespace| {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                    request.api_version(),
                    request.kind(),
                    namespace,
                )
            },
        );
        Box::pin(async move {
            self.core
                .watch_events_since(&[target], request.since_resource_version())
                .await
                .map_err(map_watch_error)
        })
    }

    fn list_watch_events_since(
        &self,
        request: WatchEventsSinceRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            self.core
                .watch_events_since(request.targets(), request.since_resource_version())
                .await
                .map_err(map_watch_error)
        })
    }

    fn earliest_watch_event_rv(&self) -> WatchRangeFuture<'_, Option<i64>> {
        Box::pin(async { Ok(None) })
    }

    fn list_all_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            self.core
                .all_watch_events_since(request.since_resource_version(), false)
                .await
                .map_err(map_watch_error)
        })
    }

    fn list_deleted_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            self.core
                .all_watch_events_since(request.since_resource_version(), true)
                .await
                .map_err(map_watch_error)
        })
    }
}

impl DurableRawWatchHistoryRead for RedbReadStore {
    fn list_raw_watch_events_since_checked_bounded(
        &self,
        request: RawWatchEventsSinceRequest,
    ) -> klights_cluster_store::RawWatchHistoryFuture<'_, RawWatchHistoryRead> {
        Box::pin(async move {
            match self
                .core
                .raw_watch_events_since_checked(
                    request.targets(),
                    request.since_resource_version(),
                    request.limit(),
                )
                .await
                .map_err(map_watch_error)?
            {
                RedbCheckedWatchRead::Expired => Ok(RawWatchHistoryRead::Expired),
                RedbCheckedWatchRead::Events(events) => {
                    RawWatchHistoryPage::try_new(events).map(RawWatchHistoryRead::Events)
                }
            }
        })
    }

    fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        request: RawWatchEventsAfterPositionRequest,
    ) -> klights_cluster_store::RawWatchHistoryFuture<'_, PositionedRawWatchHistoryRead> {
        Box::pin(async move {
            match self
                .core
                .positioned_raw_watch_events(request.targets(), request.position(), request.limit())
                .await
                .map_err(map_watch_error)?
            {
                RedbPositionedWatchRead::Expired => Ok(PositionedRawWatchHistoryRead::Expired),
                RedbPositionedWatchRead::Events(page) => {
                    PositionedRawWatchHistoryPage::try_new(page.events, page.next_position)
                        .map(PositionedRawWatchHistoryRead::Events)
                }
            }
        })
    }
}

impl ClusterTopologyRead for RedbReadStore {
    fn get_node_dataplane(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<klights_cluster_store::DataplanePeerMetadata>> {
        Box::pin(async move {
            self.core
                .get_node_dataplane(request.node_name().as_str())
                .await
                .map_err(map_topology_error)
        })
    }

    fn get_node_subnet(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<StoredNodeSubnet>> {
        Box::pin(async move {
            self.core
                .get_node_subnet(request.node_name().as_str())
                .await
                .map_err(map_topology_error)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Vec<StoredNodeSubnet>> {
        Box::pin(async move {
            self.core
                .list_peer_subnets(request)
                .await
                .map_err(map_topology_error)
        })
    }
}

fn focused_page(page: RedbResourceList) -> Result<ResourceListPage, ResourceReadError> {
    let snapshot = ResourceListSnapshot::try_new(page.position)?;
    let continuation = page
        .continuation
        .map(|after| ResourceContinuation::new(after, snapshot));
    ResourceListPage::try_new(
        page.items,
        snapshot,
        continuation,
        page.remaining_item_count,
    )
}

fn page_items(
    mut items: Vec<Resource>,
    query: &klights_cluster_store::ResourceListQuery,
    position: WatchReplayPosition,
) -> Result<ResourceListPage, ResourceReadError> {
    items
        .sort_by(|left, right| (&left.namespace, &left.name).cmp(&(&right.namespace, &right.name)));
    if let Some(cursor) = query.continuation() {
        items.retain(|item| {
            (item.namespace.as_deref(), item.name.as_str())
                > (cursor.after().namespace(), cursor.after().name())
        });
    }
    let limit = query.limit().and_then(|value| usize::try_from(value).ok());
    let total = items.len();
    let has_more = limit.is_some_and(|limit| total > limit);
    let remaining =
        if has_more && query.label_selector().is_none() && query.field_selector().is_none() {
            limit
                .and_then(|limit| total.checked_sub(limit))
                .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
        } else {
            None
        };
    if let Some(limit) = limit {
        items.truncate(limit);
    }
    let snapshot = ResourceListSnapshot::try_new(position)?;
    let continuation = has_more.then(|| {
        let item = items.last().expect("non-empty historical Redb page");
        ResourceContinuation::new(
            ResourceCollectionKey::new(item.namespace.clone(), item.name.clone()),
            snapshot,
        )
    });
    ResourceListPage::try_new(items, snapshot, continuation, remaining)
}

fn durable_target_for_collection(
    api_version: &str,
    kind: &str,
    scope: &ResourceCollectionScope,
) -> klights_cluster_store::DurableWatchTarget {
    match scope {
        ResourceCollectionScope::Cluster => {
            klights_cluster_store::DurableWatchTarget::cluster(api_version, kind)
        }
        ResourceCollectionScope::AllNamespaces => {
            klights_cluster_store::DurableWatchTarget::namespaced(api_version, kind)
        }
        ResourceCollectionScope::Namespace(namespace) => {
            klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                api_version,
                kind,
                namespace,
            )
        }
    }
}

fn map_resource_error(error: anyhow::Error) -> ResourceReadError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("selector") {
        ResourceReadError::InvalidSelector { message }
    } else if lower.contains("continu") || lower.contains("cursor") {
        ResourceReadError::InvalidContinuation { message }
    } else if lower.contains("older than requested") || errors::is_conflict_error(&error) {
        ResourceReadError::Conflict { message }
    } else if lower.contains("corrupt") || lower.contains("decode") || lower.contains("invalid") {
        ResourceReadError::CorruptData { message }
    } else {
        ResourceReadError::retryable(message)
    }
}

fn map_watch_error(error: anyhow::Error) -> WatchHistoryError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("corrupt") || lower.contains("malformed") || lower.contains("invalid") {
        WatchHistoryError::CorruptData { message }
    } else {
        WatchHistoryError::persistence_failed(message)
    }
}

fn map_topology_error(error: anyhow::Error) -> ClusterTopologyReadError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("invalid")
        || lower.contains("malformed")
        || lower.contains("corrupt")
        || lower.contains("bad ")
    {
        ClusterTopologyReadError::corrupt_data(message)
    } else {
        ClusterTopologyReadError::retryable(message)
    }
}
