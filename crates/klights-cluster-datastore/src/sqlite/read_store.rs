use std::net::Ipv4Addr;

use crate::errors;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, ClusterOwnershipRead, ClusterResourceRead,
    ClusterResourceScopeRead, ClusterTopologyFuture, ClusterTopologyRead, ClusterTopologyReadError,
    DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, DurableAllocatorRead,
    DurableAllocatorState, DurableRawWatchEvent, DurableRawWatchHistoryRead, DurableReplayFloor,
    DurableWatchEvent, DurableWatchHistoryRead, DurableWatchRangeRead, DurableWatchScope,
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
use klights_supervisor::DbExecutor;
use klights_types::{HostPortRange, LabelSelector, NodeName, NodePeerMode, PodSubnet};
use rusqlite::OptionalExtension;
use serde_json::Value;

use super::read_helpers::row_to_namespaced_resource;
use super::read_queries as queries;

/// Passive SQLite implementation of cluster resource/history/topology reads.
#[derive(Clone)]
pub struct SqliteReadStore {
    executor: DbExecutor,
    #[cfg(any(test, feature = "test-support"))]
    fail_next_watch_position_observation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(any(test, feature = "test-support"))]
    pub resource_get_call_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

pub struct SqliteResourceList {
    pub items: Vec<Resource>,
    pub resource_version: i64,
    pub watch_replay_position: Option<WatchReplayPosition>,
    pub continue_token: Option<String>,
    pub remaining_item_count: Option<i64>,
}

pub enum SqliteCheckedWatchRead {
    Events(Vec<DurableWatchEvent>),
    Expired,
}

#[derive(Clone, Copy)]
pub struct SqliteResourceListQuery<'a> {
    pub label_selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    pub limit: Option<i64>,
    pub continue_token: Option<&'a str>,
}

impl<'a> SqliteResourceListQuery<'a> {
    pub const fn new(
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
        continue_token: Option<&'a str>,
    ) -> Self {
        Self {
            label_selector,
            field_selector,
            limit,
            continue_token,
        }
    }
}

impl DurableAllocatorRead for SqliteReadStore {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        Box::pin(async move { SqliteReadStore::read_allocator_state(self).await })
    }
}

impl ClusterResourceRead for SqliteReadStore {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let key = request.into_key();
            SqliteReadStore::get_resource(
                self,
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
        Box::pin(async move {
            let query = request.query();
            let namespace = match request.scope() {
                ResourceCollectionScope::Cluster | ResourceCollectionScope::AllNamespaces => None,
                ResourceCollectionScope::Namespace(namespace) => Some(namespace.as_str()),
            };
            let all_namespaces = matches!(request.scope(), ResourceCollectionScope::AllNamespaces);
            let legacy_query = SqliteResourceListQuery::new(
                query.label_selector(),
                query.field_selector(),
                (!all_namespaces).then_some(query.limit()).flatten(),
                (!all_namespaces)
                    .then(|| query.start_after().map(ResourceCollectionKey::name))
                    .flatten(),
            );
            let continuation_position = query
                .continuation()
                .map(|cursor| cursor.snapshot().position());
            let requested_position = match query.resource_version_match() {
                ResourceVersionMatch::AtPosition(position) => Some(position),
                _ => continuation_position,
            };
            if requested_position.is_some()
                || matches!(
                    query.resource_version_match(),
                    ResourceVersionMatch::Exact(_)
                )
            {
                return self.list_historical(request).await;
            }
            // A selector can underfill several physical key windows. Pin the
            // canonical LIST port to one exact replay position and reuse the
            // bounded historical state machine instead of letting the legacy
            // compatibility reader scan every candidate inside one DB closure.
            if query.limit().is_some_and(|limit| limit > 0)
                && (query.label_selector().is_some() || query.field_selector().is_some())
            {
                let position = self
                    .read_db_call("cluster-read:selector-list-position", |connection| {
                        Ok(sqlite_replay_position(connection)?)
                    })
                    .await
                    .map_err(|error| map_resource_error(anyhow::Error::new(error)))?;
                let snapshot = ResourceListSnapshot::try_new(position)?;
                let continuation = query
                    .start_after()
                    .cloned()
                    .map(|after| ResourceContinuation::new(after, snapshot));
                let bounded_query = klights_cluster_store::ResourceListQuery::try_new(
                    query.label_selector().map(str::to_string),
                    query.field_selector().map(str::to_string),
                    query.limit(),
                    continuation,
                    ResourceVersionMatch::AtPosition(position),
                )?;
                return super::snapshot::bounded_historical_list(
                    self,
                    ResourceListRequest::new(
                        request.api_version().to_string(),
                        request.kind().to_string(),
                        request.scope().clone(),
                        bounded_query,
                    ),
                    position,
                )
                .await;
            }
            if all_namespaces
                && query.label_selector().is_none()
                && query.field_selector().is_none()
                && query.limit().is_some_and(|limit| limit > 0)
            {
                let av = request.api_version().to_string();
                let kind = request.kind().to_string();
                let after = query.start_after().cloned();
                let limit = usize::try_from(query.limit().unwrap()).unwrap_or(usize::MAX);
                let (mut items, position) = self.read_db_call("cluster-read:all-namespaces-keyset", move |connection| {
                    let mut sql = String::from("SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2");
                    let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(av), Box::new(kind)];
                    if let Some(after) = after {
                        sql.push_str(&format!(" AND (namespace > ?{} OR (namespace = ?{} AND name > ?{}))", values.len()+1, values.len()+2, values.len()+3));
                        let namespace = after.namespace().unwrap_or_default().to_string();
                        values.push(Box::new(namespace.clone())); values.push(Box::new(namespace)); values.push(Box::new(after.name().to_string()));
                    }
                    sql.push_str(&format!(" ORDER BY namespace, name LIMIT ?{}", values.len()+1));
                    values.push(Box::new(i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX)));
                    let refs = values.iter().map(|value| value.as_ref()).collect::<Vec<_>>();
                    let mut statement = connection.prepare(&sql)?;
                    let rows = statement.query_map(refs.as_slice(), row_to_namespaced_resource)?;
                    let items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok((items, sqlite_replay_position(connection)?))
                }).await.map_err(|error| map_resource_error(anyhow::Error::new(error)))?;
                let has_more = items.len() > limit;
                items.truncate(limit);
                let snapshot = ResourceListSnapshot::try_new(position)?;
                let continuation = if has_more {
                    let last = items
                        .last()
                        .expect("probe means a bounded page has a last item");
                    Some(ResourceContinuation::new(
                        ResourceCollectionKey::new(last.namespace.clone(), last.name.clone()),
                        snapshot,
                    ))
                } else {
                    None
                };
                return Ok(ResourceListRead::Current(ResourceListPage::try_new(
                    items,
                    snapshot,
                    continuation,
                    None,
                )?));
            }
            let mut page = SqliteReadStore::list_resources(
                self,
                request.api_version(),
                request.kind(),
                namespace,
                legacy_query,
            )
            .await
            .map_err(map_resource_error)?;
            if let ResourceVersionMatch::NotOlderThan(requested) = query.resource_version_match()
                && page.resource_version < requested
            {
                return Err(ResourceReadError::Conflict {
                    message: format!(
                        "current resourceVersion {} is older than requested {requested}",
                        page.resource_version
                    ),
                });
            }
            if all_namespaces {
                normalize_legacy_collection_page(&mut page, query.start_after(), true);
            }
            Ok(ResourceListRead::Current(legacy_port_page(
                page,
                None,
                query.limit(),
            )?))
        })
    }
}

impl ClusterResourceScopeRead for SqliteReadStore {
    fn list_resources_for_watch_targets(
        &self,
        request: ResourceWatchTargetsRequest,
    ) -> ResourceReadFuture<'_, ResourceScopeSnapshot> {
        Box::pin(async move {
            SqliteReadStore::list_resources_for_watch_targets(
                self,
                request.targets(),
                request.label_selector(),
            )
            .await
            .map_err(map_resource_error)
        })
    }

    fn list_resource_keys_for_scope(
        &self,
        request: ResourceKeyScopeRequest,
    ) -> ResourceReadFuture<'_, Vec<ResourceCollectionKey>> {
        Box::pin(async move {
            SqliteReadStore::list_resource_keys_for_scope(
                self,
                request.api_version().to_string(),
                request.kind().to_string(),
                request.namespaced(),
            )
            .await
            .map_err(map_resource_error)
        })
    }

    fn list_cluster_resources(&self) -> ResourceReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::list_cluster_resources(self)
                .await
                .map_err(map_resource_error)
        })
    }

    fn snapshot_resources_at_position(
        &self,
        request: ResourceSnapshotAtPositionRequest,
    ) -> ResourceReadFuture<'_, ResourceSnapshotRead> {
        Box::pin(async move {
            SqliteReadStore::snapshot_resources_at_position(
                self,
                request.targets(),
                request.label_selector(),
                request.field_selector(),
                request.position(),
            )
            .await
            .map_err(map_resource_error)
        })
    }
}

impl ClusterOwnershipRead for SqliteReadStore {
    fn find_owned_resources(
        &self,
        request: OwnerUidRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::find_owned_resources(self, request.owner_uid(), request.namespace())
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_resources_by_owner_uid(
        &self,
        request: OwnedKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::list_resources_by_owner_uid(
                self,
                request.api_version(),
                request.kind(),
                request.namespace(),
                request.owner_uid(),
            )
            .await
            .map_err(map_resource_error)
        })
    }

    fn find_owned_by_name_kind_empty_uid(
        &self,
        request: OwnerNameKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::find_owned_by_name_kind_empty_uid(
                self,
                request.owner_api_version(),
                request.owner_name(),
                request.owner_kind(),
                request.namespace(),
            )
            .await
            .map_err(map_resource_error)
        })
    }
}

impl NamespaceContentRead for SqliteReadStore {
    fn list_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::list_namespace_resources(self, request.namespace())
                .await
                .map_err(map_resource_error)
        })
    }

    fn list_namespace_resources_of_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::list_namespace_resources_of_kind(
                self,
                request.namespace(),
                request.kind(),
            )
            .await
            .map_err(map_resource_error)
        })
    }

    fn list_namespace_resources_excluding_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            SqliteReadStore::list_namespace_resources_excluding_kind(
                self,
                request.namespace(),
                request.kind(),
            )
            .await
            .map_err(map_resource_error)
        })
    }

    fn count_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, i64> {
        Box::pin(async move {
            SqliteReadStore::count_namespace_resources(self, request.namespace())
                .await
                .map_err(map_resource_error)
        })
    }
}

impl DurableWatchHistoryRead for SqliteReadStore {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        Box::pin(async move {
            let targets = request.targets().to_vec();
            let position = request.position();
            let limit = request.limit();
            self.db_call("cluster-read:watch-history", move |connection| {
                let transaction = connection.transaction()?;
                let current = sqlite_replay_position(&transaction)?;
                if position.event_id > current.event_id
                    || (position.event_id == 0
                        && position.resource_version > current.resource_version)
                {
                    return Ok(WatchHistoryRead::Expired);
                }
                let cursor_covers_current = position.event_id >= current.event_id
                    || (position.resource_version_filter_through_event_id >= current.event_id
                        && position.resource_version >= current.resource_version);
                if !cursor_covers_current
                    && sqlite_watch_position_expired(&transaction, &targets, position)?
                {
                    return Ok(WatchHistoryRead::Expired);
                }

                let (query, parameters) =
                    sqlite_positioned_watch_query(&targets, position, current.event_id, limit);
                let references: Vec<&dyn rusqlite::ToSql> = parameters
                    .iter()
                    .map(|parameter| parameter.as_ref())
                    .collect();
                let mut statement = transaction.prepare(&query)?;
                let rows = statement.query_map(&references[..], |row| {
                    let resource_version = row.get(4)?;
                    let event_id = row.get(7)?;
                    let event_type: String = row.get(5)?;
                    let data = decode_json_column(row, 6)?;
                    let resource = Resource {
                        id: 0,
                        api_version: row.get(0)?,
                        kind: row.get(1)?,
                        namespace: row.get(2)?,
                        name: row.get(3)?,
                        uid: Resource::uid_from_data(&data),
                        resource_version,
                        data: std::sync::Arc::new(data),
                    };
                    Ok(PositionedWatchEvent {
                        position: WatchReplayPosition {
                            resource_version,
                            event_id,
                            resource_version_filter_through_event_id: 0,
                        },
                        event: DurableWatchEvent::new(event_type, resource),
                    })
                })?;
                let events = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                let next_position =
                    WatchReplayPosition::after_page(position, &events, current.event_id, limit);
                let page = WatchHistoryPage::try_new(events, next_position)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                Ok(WatchHistoryRead::Events(page))
            })
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_replay_floors(&self) -> WatchHistoryFuture<'_, Vec<DurableReplayFloor>> {
        Box::pin(async move {
            self.db_call("cluster-read:watch-replay-floors", |connection| {
                let mut statement = connection.prepare(
                    "SELECT api_version, kind, namespace_key, floor_rv, floor_event_id,
                            floor_position_exact
                     FROM watch_replay_floors
                     ORDER BY api_version, kind, namespace_key",
                )?;
                let rows = statement.query_map([], |row| {
                    let api_version: String = row.get(0)?;
                    let kind: String = row.get(1)?;
                    let namespace: String = row.get(2)?;
                    let resource_version: i64 = row.get(3)?;
                    let event_id: i64 = row.get(4)?;
                    let exact: bool = row.get(5)?;
                    let result = if api_version == "*" && kind == "*" && namespace == "*" {
                        DurableReplayFloor::all(resource_version, event_id, exact)
                    } else if namespace == "#cluster" {
                        DurableReplayFloor::cluster(
                            api_version,
                            kind,
                            resource_version,
                            event_id,
                            exact,
                        )
                    } else {
                        DurableReplayFloor::namespaced(
                            api_version,
                            kind,
                            namespace,
                            resource_version,
                            event_id,
                            exact,
                        )
                    };
                    result.map_err(|_| rusqlite::Error::InvalidQuery)
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(map_watch_error)
        })
    }
}

impl DurableWatchRangeRead for SqliteReadStore {
    fn list_cluster_resources_modified_since(
        &self,
        request: ModifiedClusterResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            let api_version = request.api_version().to_string();
            let kind = request.kind().to_string();
            let since_resource_version = request.since_resource_version();
            self.db_call(
                "cluster-read:list-cluster-resources-modified-since",
                move |connection| {
                    let mut statement =
                        connection.prepare(queries::WATCH_EVENTS_LIST_CLUSTER_SINCE)?;
                    let rows = statement.query_map(
                        rusqlite::params![api_version, kind, since_resource_version],
                        watch_row_to_durable_event,
                    )?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(klights_supervisor::DbError::from)
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_resources_modified_since(
        &self,
        request: ModifiedResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            let api_version = request.api_version().to_string();
            let kind = request.kind().to_string();
            let namespace = request.namespace().map(str::to_string);
            let since_resource_version = request.since_resource_version();
            self.db_call(
                "cluster-read:list-resources-modified-since",
                move |connection| {
                    let mut query = queries::WATCH_EVENTS_LIST_NAMESPACED_SINCE_HEAD.to_string();
                    let mut parameters: Vec<Box<dyn rusqlite::ToSql>> = vec![
                        Box::new(api_version),
                        Box::new(kind),
                        Box::new(since_resource_version),
                    ];
                    if let Some(namespace) = namespace {
                        query.push_str(&format!(" AND namespace = ?{}", parameters.len() + 1));
                        parameters.push(Box::new(namespace));
                    }
                    query.push_str(" ORDER BY resource_version ASC, id ASC");
                    let references: Vec<&dyn rusqlite::ToSql> = parameters
                        .iter()
                        .map(|parameter| parameter.as_ref())
                        .collect();
                    let mut statement = connection.prepare(&query)?;
                    let rows = statement.query_map(&references[..], watch_row_to_durable_event)?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(klights_supervisor::DbError::from)
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_watch_events_since(
        &self,
        request: WatchEventsSinceRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            if request.targets().is_empty() {
                return Ok(Vec::new());
            }
            let targets = request.targets().to_vec();
            let since_resource_version = request.since_resource_version();
            self.db_call("cluster-read:list-watch-events-since", move |connection| {
                sqlite_list_watch_events_since(
                    connection,
                    &targets,
                    since_resource_version,
                    None,
                    watch_row_to_durable_event,
                )
                .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(map_watch_error)
        })
    }

    fn earliest_watch_event_rv(&self) -> WatchRangeFuture<'_, Option<i64>> {
        Box::pin(async move {
            self.db_call("cluster-read:earliest-watch-event-rv", |connection| {
                connection
                    .query_row(queries::WATCH_EVENTS_MIN_RV, [], |row| row.get(0))
                    .optional()
                    .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_all_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            let since_resource_version = request.since_resource_version();
            self.db_call(
                "cluster-read:list-all-watch-events-since",
                move |connection| {
                    let mut statement = connection.prepare(queries::WATCH_EVENTS_LIST_ALL_SINCE)?;
                    let rows = statement.query_map(
                        rusqlite::params![since_resource_version],
                        watch_row_to_durable_event,
                    )?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(klights_supervisor::DbError::from)
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_deleted_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>> {
        Box::pin(async move {
            let since_resource_version = request.since_resource_version();
            self.db_call(
                "cluster-read:list-deleted-watch-events-since",
                move |connection| {
                    let mut statement =
                        connection.prepare(queries::WATCH_EVENTS_LIST_DELETED_SINCE)?;
                    let rows = statement.query_map(
                        rusqlite::params![since_resource_version],
                        watch_row_to_durable_event,
                    )?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(klights_supervisor::DbError::from)
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }
}

impl DurableRawWatchHistoryRead for SqliteReadStore {
    fn list_raw_watch_events_since_checked_bounded(
        &self,
        request: RawWatchEventsSinceRequest,
    ) -> klights_cluster_store::RawWatchHistoryFuture<'_, RawWatchHistoryRead> {
        Box::pin(async move {
            if request.targets().is_empty() {
                return Ok(RawWatchHistoryRead::Events(RawWatchHistoryPage::empty()));
            }
            let targets = request.targets().to_vec();
            let since_resource_version = request.since_resource_version();
            let limit = request.limit();
            self.db_call(
                "cluster-read:list-raw-watch-events-since",
                move |connection| {
                    if since_resource_version > 0
                        && sqlite_watch_position_expired(
                            connection,
                            &targets,
                            WatchReplayPosition::from_resource_version(since_resource_version),
                        )?
                    {
                        return Ok(RawWatchHistoryRead::Expired);
                    }
                    let events = sqlite_list_watch_events_since(
                        connection,
                        &targets,
                        since_resource_version,
                        Some(limit),
                        watch_row_to_raw_watch_event,
                    )?;
                    let page = RawWatchHistoryPage::try_new(events)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    Ok(RawWatchHistoryRead::Events(page))
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }

    fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        request: RawWatchEventsAfterPositionRequest,
    ) -> klights_cluster_store::RawWatchHistoryFuture<'_, PositionedRawWatchHistoryRead> {
        Box::pin(async move {
            let position = request.position();
            if request.targets().is_empty() {
                let page = PositionedRawWatchHistoryPage::try_new(Vec::new(), position)?;
                return Ok(PositionedRawWatchHistoryRead::Events(page));
            }
            let targets = request.targets().to_vec();
            let limit = request.limit();
            self.db_call(
                "cluster-read:list-raw-watch-events-after-position",
                move |connection| {
                    let current = sqlite_replay_position(connection)?;
                    if position.event_id > current.event_id
                        || (position.event_id == 0
                            && position.resource_version > current.resource_version)
                    {
                        return Ok(PositionedRawWatchHistoryRead::Expired);
                    }
                    let cursor_covers_current = position.event_id >= current.event_id
                        || (position.resource_version_filter_through_event_id >= current.event_id
                            && position.resource_version >= current.resource_version);
                    if !cursor_covers_current
                        && sqlite_watch_position_expired(connection, &targets, position)?
                    {
                        return Ok(PositionedRawWatchHistoryRead::Expired);
                    }
                    let (query, parameters) =
                        sqlite_positioned_watch_query(&targets, position, current.event_id, limit);
                    let references: Vec<&dyn rusqlite::ToSql> = parameters
                        .iter()
                        .map(|parameter| parameter.as_ref())
                        .collect();
                    let mut statement = connection.prepare(&query)?;
                    let rows = statement.query_map(&references[..], |row| {
                        let event_id = row.get(7)?;
                        let event = watch_row_to_raw_watch_event(row)?;
                        Ok(PositionedWatchEvent {
                            position: WatchReplayPosition {
                                resource_version: event.resource_version,
                                event_id,
                                resource_version_filter_through_event_id: 0,
                            },
                            event,
                        })
                    })?;
                    let events = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                    let next_position =
                        WatchReplayPosition::after_page(position, &events, current.event_id, limit);
                    let page = PositionedRawWatchHistoryPage::try_new(events, next_position)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?;
                    Ok(PositionedRawWatchHistoryRead::Events(page))
                },
            )
            .await
            .map_err(map_watch_error)
        })
    }
}

fn sqlite_list_watch_events_since<T, F>(
    connection: &rusqlite::Connection,
    targets: &[klights_cluster_store::DurableWatchTarget],
    since_resource_version: i64,
    limit: Option<std::num::NonZeroUsize>,
    mapper: F,
) -> rusqlite::Result<Vec<T>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    let mut query = queries::WATCH_EVENTS_LIST_TARGETS_HEAD.to_string();
    let mut parameters: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(since_resource_version)];
    append_sqlite_watch_targets(&mut query, &mut parameters, targets);
    query.push_str(") ORDER BY resource_version ASC, id ASC");
    if let Some(limit) = limit {
        query.push_str(&format!(" LIMIT ?{}", parameters.len() + 1));
        parameters.push(Box::new(limit.get() as i64));
    }
    let references: Vec<&dyn rusqlite::ToSql> = parameters
        .iter()
        .map(|parameter| parameter.as_ref())
        .collect();
    let mut statement = connection.prepare(&query)?;
    statement
        .query_map(&references[..], mapper)?
        .collect::<rusqlite::Result<Vec<_>>>()
}

fn sqlite_positioned_watch_query(
    targets: &[klights_cluster_store::DurableWatchTarget],
    position: WatchReplayPosition,
    high_water_event_id: i64,
    limit: std::num::NonZeroUsize,
) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
    let mut query =
        "SELECT api_version, kind, namespace, name, resource_version, event_type, data, id
                     FROM watch_events WHERE "
            .to_string();
    let mut parameters: Vec<Box<dyn rusqlite::ToSql>>;
    if position.resource_version_filter_through_event_id > 0 {
        query.push_str("id > ?1 AND id <= ?2 AND (id > ?3 OR resource_version > ?4) AND (");
        parameters = vec![
            Box::new(position.event_id),
            Box::new(high_water_event_id),
            Box::new(position.resource_version_filter_through_event_id),
            Box::new(position.resource_version),
        ];
    } else {
        let boundary = if position.event_id == 0 {
            position.resource_version
        } else {
            position.event_id
        };
        query.push_str(if position.event_id == 0 {
            "resource_version > ?1 AND id <= ?2 AND ("
        } else {
            "id > ?1 AND id <= ?2 AND ("
        });
        parameters = vec![Box::new(boundary), Box::new(high_water_event_id)];
    }
    append_sqlite_watch_targets(&mut query, &mut parameters, targets);
    query.push_str(&format!(
        ") ORDER BY id ASC LIMIT ?{}",
        parameters.len() + 1
    ));
    parameters.push(Box::new(limit.get() as i64));
    (query, parameters)
}

fn append_sqlite_watch_targets(
    query: &mut String,
    parameters: &mut Vec<Box<dyn rusqlite::ToSql>>,
    targets: &[klights_cluster_store::DurableWatchTarget],
) {
    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            query.push_str(" OR ");
        }
        query.push('(');
        query.push_str(&format!(
            "api_version = ?{} AND kind = ?{}",
            parameters.len() + 1,
            parameters.len() + 2
        ));
        parameters.push(Box::new(target.api_version().to_string()));
        parameters.push(Box::new(target.kind().to_string()));
        match target.scope() {
            DurableWatchScope::Cluster => query.push_str(" AND namespace IS NULL"),
            DurableWatchScope::Namespaced(Some(namespace)) => {
                query.push_str(&format!(" AND namespace = ?{}", parameters.len() + 1));
                parameters.push(Box::new(namespace.to_string()));
            }
            DurableWatchScope::Namespaced(None) => {
                query.push_str(" AND namespace IS NOT NULL");
            }
        }
        query.push(')');
    }
}

fn watch_row_to_durable_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableWatchEvent> {
    let data = decode_json_column(row, 6)?;
    let resource = Resource {
        id: 0,
        api_version: row.get(0)?,
        kind: row.get(1)?,
        namespace: row.get(2)?,
        name: row.get(3)?,
        resource_version: row.get(4)?,
        uid: Resource::uid_from_data(&data),
        data: std::sync::Arc::new(data),
    };
    let event_type: String = row.get(5)?;
    Ok(DurableWatchEvent::new(event_type, resource))
}

fn watch_row_to_raw_watch_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<DurableRawWatchEvent> {
    let event_type: String = row.get(5)?;
    Ok(DurableRawWatchEvent {
        api_version: row.get(0)?,
        kind: row.get(1)?,
        namespace: row.get(2)?,
        name: row.get(3)?,
        resource_version: row.get(4)?,
        event_type: std::borrow::Cow::Owned(event_type),
        object_json: Bytes::from(row.get::<_, Vec<u8>>(6)?),
    })
}

fn sqlite_watch_position_expired(
    connection: &rusqlite::Connection,
    targets: &[klights_cluster_store::DurableWatchTarget],
    position: WatchReplayPosition,
) -> rusqlite::Result<bool> {
    for target in targets {
        if klights_cluster_store::ReplayRetentionBoundary::classify_all(
            super::replay_floor::target_replay_boundaries(
                connection,
                target.api_version(),
                target.kind(),
                target.scope(),
            )?,
            position,
        ) == klights_cluster_store::ReplayAvailability::Expired
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn map_watch_error(error: anyhow::Error) -> WatchHistoryError {
    let message = format!("{error:#}");
    if message.to_ascii_lowercase().contains("corrupt")
        || message.to_ascii_lowercase().contains("malformed")
        || message.to_ascii_lowercase().contains("invalid")
    {
        WatchHistoryError::CorruptData { message }
    } else {
        WatchHistoryError::persistence_failed(message)
    }
}

fn sqlite_replay_position(
    connection: &rusqlite::Connection,
) -> rusqlite::Result<WatchReplayPosition> {
    let resource_version = connection.query_row(
        "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'resource_version'",
        [],
        |row| row.get(0),
    )?;
    let event_id = connection.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
        [],
        |row| row.get(0),
    )?;
    Ok(WatchReplayPosition {
        resource_version,
        event_id,
        resource_version_filter_through_event_id: 0,
    })
}

fn decode_json_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Value> {
    serde_json::from_slice(&row.get::<_, Vec<u8>>(index)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

fn normalize_legacy_collection_page(
    page: &mut SqliteResourceList,
    continuation: Option<&ResourceCollectionKey>,
    all_namespaces: bool,
) {
    if all_namespaces {
        page.items.sort_by(|left, right| {
            (
                left.namespace.as_deref().unwrap_or_default(),
                left.name.as_str(),
            )
                .cmp(&(
                    right.namespace.as_deref().unwrap_or_default(),
                    right.name.as_str(),
                ))
        });
    } else {
        page.items.sort_by(|left, right| left.name.cmp(&right.name));
    }
    if let Some(cursor) = continuation {
        if all_namespaces && cursor.namespace().is_some() {
            let after = (cursor.namespace().unwrap_or_default(), cursor.name());
            page.items.retain(|item| {
                (
                    item.namespace.as_deref().unwrap_or_default(),
                    item.name.as_str(),
                ) > after
            });
        } else {
            page.items.retain(|item| item.name.as_str() > cursor.name());
        }
    }
    page.continue_token = None;
    page.remaining_item_count = None;
}

fn legacy_port_page(
    mut page: SqliteResourceList,
    pinned: Option<ResourceListSnapshot>,
    limit: Option<i64>,
) -> Result<ResourceListPage, ResourceReadError> {
    let snapshot = match (pinned, page.watch_replay_position) {
        (Some(snapshot), _) => snapshot,
        (None, Some(position)) => ResourceListSnapshot::try_new(position)?,
        (None, None) => ResourceListSnapshot::try_new(WatchReplayPosition {
            resource_version: page.resource_version,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        })?,
    };
    let has_more =
        limit.is_some_and(|limit| i64::try_from(page.items.len()).unwrap_or(i64::MAX) > limit);
    if let Some(limit) = limit.and_then(|limit| usize::try_from(limit).ok())
        && page.items.len() > limit
    {
        page.items.truncate(limit);
    }
    let continuation = if has_more || page.continue_token.is_some() {
        page.items.last().map(|item| {
            ResourceContinuation::new(
                ResourceCollectionKey::new(item.namespace.clone(), item.name.clone()),
                snapshot,
            )
        })
    } else {
        None
    };
    ResourceListPage::try_new(
        page.items,
        snapshot,
        continuation,
        page.remaining_item_count,
    )
}

fn map_resource_error(error: anyhow::Error) -> ResourceReadError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("selector") {
        ResourceReadError::InvalidSelector { message }
    } else if lower.contains("continu") || lower.contains("cursor") {
        ResourceReadError::InvalidContinuation { message }
    } else if errors::is_conflict_error(&error) {
        ResourceReadError::Conflict { message }
    } else if lower.contains("corrupt") || lower.contains("decode") || lower.contains("invalid") {
        ResourceReadError::CorruptData { message }
    } else {
        ResourceReadError::retryable(message)
    }
}

impl SqliteReadStore {
    pub fn new(executor: DbExecutor) -> Self {
        Self {
            executor,
            #[cfg(any(test, feature = "test-support"))]
            fail_next_watch_position_observation: std::sync::Arc::new(
                std::sync::atomic::AtomicBool::new(false),
            ),
            #[cfg(any(test, feature = "test-support"))]
            resource_get_call_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_with_test_instrumentation(
        executor: DbExecutor,
        fail_next_watch_position_observation: std::sync::Arc<std::sync::atomic::AtomicBool>,
        resource_get_call_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            executor,
            fail_next_watch_position_observation,
            resource_get_call_count,
        }
    }

    pub async fn read_db_call<T, F>(
        &self,
        label: &'static str,
        operation: F,
    ) -> klights_supervisor::DbCallResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> klights_supervisor::DbClosureResult<T>
            + Send
            + 'static,
    {
        self.executor.call_raw(label, operation).await
    }

    pub(super) fn current_resource_version_in_conn(
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<i64> {
        connection.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn current_resource_version_in_tx(
        transaction: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<i64> {
        transaction.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn current_watch_replay_position_in_tx(
        transaction: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<WatchReplayPosition> {
        let resource_version =
            transaction.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))?;
        let event_id = transaction.query_row(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
            [],
            |row| row.get(0),
        )?;
        Ok(WatchReplayPosition {
            resource_version,
            event_id,
            resource_version_filter_through_event_id: 0,
        })
    }

    pub async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        let name = name.to_string();
        let result = self
            .read_db_call("db_query", move |connection| {
                let mut statement = connection.prepare(queries::NAMESPACE_GET)?;
                let row = statement.query_row([name], |row| {
                    let data_bytes: Vec<u8> = row.get(3)?;
                    let data: Value = serde_json::from_slice(&data_bytes).ok().unwrap_or_default();
                    Ok(Resource {
                        id: 0,
                        api_version: "v1".to_string(),
                        kind: "Namespace".to_string(),
                        namespace: None,
                        name: row.get(0)?,
                        resource_version: row.get(1)?,
                        uid: row.get(2)?,
                        data: std::sync::Arc::new(data),
                    })
                });
                Ok(row)
            })
            .await;
        match result {
            Ok(Ok(resource)) => Ok(Some(resource)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(error)) => Err(anyhow!("Failed to get namespace: {error}")),
            Err(error) => Err(anyhow!("Failed to get namespace: {error}")),
        }
    }

    pub async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<SqliteResourceList> {
        let labels = label_selector
            .map(str::trim)
            .filter(|selector| !selector.is_empty())
            .map(LabelSelector::parse)
            .transpose()
            .map_err(|error| anyhow!("Invalid label selector: {error}"))?;
        let fields = field_selector
            .filter(|selector| !selector.is_empty())
            .map(klights_types::FieldSelector::parse)
            .transpose()
            .map_err(|error| anyhow!("Invalid field selector: {error}"))?;
        self.read_db_call("db_query", move |connection| {
            let transaction = connection.transaction()?;
            let connection = &transaction;
            let mut query = queries::NAMESPACES_LIST_HEAD.to_string();
            let mut parameters: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            if let Some(name) = fields.as_ref().and_then(|selector| {
                selector.requirements().iter().find_map(|requirement| {
                    (requirement.field() == "metadata.name"
                        && requirement.operator() == klights_types::FieldSelectorOperator::Equals
                        && !requirement.value().is_empty())
                    .then(|| requirement.value().to_string())
                })
            }) {
                query.push_str(" WHERE name = ?");
                parameters.push(Box::new(name));
            }
            query.push_str(" ORDER BY name ASC");
            let parameter_refs = parameters
                .iter()
                .map(|parameter| parameter.as_ref())
                .collect::<Vec<&dyn rusqlite::types::ToSql>>();
            let mut statement = connection.prepare(&query)?;
            let rows = statement.query_map(parameter_refs.as_slice(), |row| {
                let data_bytes: Vec<u8> = row.get(3)?;
                let data: Value = serde_json::from_slice(&data_bytes).ok().unwrap_or_default();
                Ok(Resource {
                    id: 0,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: row.get(0)?,
                    resource_version: row.get(1)?,
                    uid: row.get(2)?,
                    data: std::sync::Arc::new(data),
                })
            })?;
            let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            if let Some(selector) = &labels {
                items.retain(|item| selector.matches_resource(&item.data));
            }
            if let Some(selector) = &fields {
                items.retain(|item| {
                    selector.matches_resource_with_identity(
                        &item.api_version,
                        &item.kind,
                        &item.data,
                    )
                });
            }
            let watch_replay_position = Self::current_watch_replay_position_in_tx(&transaction)?;
            Ok(SqliteResourceList {
                items,
                resource_version: watch_replay_position.resource_version,
                watch_replay_position: Some(watch_replay_position),
                continue_token: None,
                remaining_item_count: None,
            })
        })
        .await
        .map_err(|error| anyhow!("Failed to list namespaces: {error}"))
    }

    pub async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<SqliteResourceList> {
        if label_selector.is_none()
            && field_selector.is_none()
            && limit.is_some_and(|value| value > 0)
        {
            let limit = usize::try_from(limit.unwrap()).unwrap_or(usize::MAX);
            let after = continue_token
                .filter(|token| !token.is_empty())
                .map(str::to_string);
            return self
                .read_db_call("cluster-read:namespaces-keyset", move |connection| {
                    let transaction = connection.transaction()?;
                    let mut query = queries::NAMESPACES_LIST_HEAD.to_string();
                    let mut parameters: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                    if let Some(after) = after {
                        query.push_str(" WHERE name > ?1");
                        parameters.push(Box::new(after));
                    }
                    query.push_str(&format!(
                        " ORDER BY name ASC LIMIT ?{}",
                        parameters.len() + 1
                    ));
                    parameters.push(Box::new(
                        i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX),
                    ));
                    let references = parameters
                        .iter()
                        .map(|parameter| parameter.as_ref())
                        .collect::<Vec<_>>();
                    let mut statement = transaction.prepare(&query)?;
                    let rows = statement.query_map(references.as_slice(), |row| {
                        let data_bytes: Vec<u8> = row.get(3)?;
                        let data: Value = serde_json::from_slice(&data_bytes).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?;
                        Ok(Resource {
                            id: 0,
                            api_version: "v1".to_string(),
                            kind: "Namespace".to_string(),
                            namespace: None,
                            name: row.get(0)?,
                            resource_version: row.get(1)?,
                            uid: row.get(2)?,
                            data: std::sync::Arc::new(data),
                        })
                    })?;
                    let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                    let has_more = items.len() > limit;
                    items.truncate(limit);
                    let watch_replay_position =
                        Self::current_watch_replay_position_in_tx(&transaction)?;
                    Ok(SqliteResourceList {
                        continue_token: has_more
                            .then(|| items.last().map(|item| item.name.clone()))
                            .flatten(),
                        items,
                        resource_version: watch_replay_position.resource_version,
                        watch_replay_position: Some(watch_replay_position),
                        remaining_item_count: None,
                    })
                })
                .await
                .map_err(|error| anyhow!("Failed to page namespaces: {error}"));
        }
        let mut list = self.list_namespaces(label_selector, field_selector).await?;
        if let Some(token) = continue_token.filter(|token| !token.is_empty()) {
            list.items.retain(|item| item.name.as_str() > token);
        }
        list.continue_token = None;
        list.remaining_item_count = None;
        if let Some(limit) = limit.filter(|limit| *limit > 0)
            && i64::try_from(list.items.len()).unwrap_or(i64::MAX) > limit
            && let Ok(limit) = usize::try_from(limit)
        {
            list.remaining_item_count =
                Some(i64::try_from(list.items.len() - limit).unwrap_or(i64::MAX));
            list.items.truncate(limit);
            list.continue_token = list.items.last().map(|item| item.name.clone());
        }
        Ok(list)
    }

    pub async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_namespace_resources_filtered(namespace, SqliteNamespaceKindFilter::All)
            .await
    }

    pub async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.list_namespace_resources_filtered(namespace, SqliteNamespaceKindFilter::OfKind(kind))
            .await
    }

    pub async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.list_namespace_resources_filtered(
            namespace,
            SqliteNamespaceKindFilter::ExcludingKind(kind),
        )
        .await
    }

    async fn list_namespace_resources_filtered(
        &self,
        namespace: &str,
        kind_filter: SqliteNamespaceKindFilter<'_>,
    ) -> Result<Vec<Resource>> {
        let namespace = namespace.to_string();
        let (sql, kind_param): (&'static str, Option<String>) = match kind_filter {
            SqliteNamespaceKindFilter::All => (queries::NAMESPACE_RESOURCES_LIST_ALL, None),
            SqliteNamespaceKindFilter::OfKind(kind) => (
                queries::NAMESPACE_RESOURCES_LIST_OF_KIND,
                Some(kind.to_string()),
            ),
            SqliteNamespaceKindFilter::ExcludingKind(kind) => (
                queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND,
                Some(kind.to_string()),
            ),
        };
        let rows = self
            .read_db_call("db_query", move |connection| {
                let mut statement = connection.prepare(sql)?;
                let mapper = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Resource> {
                    let data_bytes: Vec<u8> = row.get(7)?;
                    let data: Value = serde_json::from_slice(&data_bytes).unwrap_or(Value::Null);
                    Ok(Resource {
                        id: row.get(0)?,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: row.get(3)?,
                        name: row.get(4)?,
                        resource_version: row.get(5)?,
                        uid: row.get(6)?,
                        data: std::sync::Arc::new(data),
                    })
                };
                let mut output = Vec::new();
                match kind_param {
                    None => {
                        let mapped = statement.query_map(rusqlite::params![&namespace], mapper)?;
                        for item in mapped {
                            output.push(item?);
                        }
                    }
                    Some(kind) => {
                        let mapped =
                            statement.query_map(rusqlite::params![&namespace, &kind], mapper)?;
                        for item in mapped {
                            output.push(item?);
                        }
                    }
                }
                Ok(output)
            })
            .await
            .map_err(|error| anyhow!("Failed to list namespace resources: {error}"))?;
        Ok(rows)
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        let namespace = namespace.to_string();
        let count = self
            .read_db_call("db_query", move |connection| {
                let count: i64 = connection.query_row(
                    queries::NAMESPACE_RESOURCES_COUNT,
                    rusqlite::params![&namespace],
                    |row| row.get(0),
                )?;
                Ok(count)
            })
            .await
            .map_err(|error| anyhow!("Failed to count namespace resources: {error}"))?;
        Ok(count)
    }

    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<StoredNodeSubnet>> {
        let node_name = node_name.to_string();
        self.read_db_call("db_query", move |connection| {
            connection
                .query_row(
                    queries::NODE_SUBNET_SELECT_BY_NAME,
                    rusqlite::params![node_name],
                    row_to_node_subnet,
                )
                .optional()
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .map_err(|error| anyhow!("Failed to get node subnet: {error}"))
    }

    pub async fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> Result<Vec<StoredNodeSubnet>> {
        let excluded_node_name = request
            .excluded_node_name()
            .map_or_else(String::new, |node_name| node_name.as_str().to_string());
        self.read_db_call("db_query", move |connection| {
            let mut statement = connection.prepare(queries::NODE_SUBNET_LIST_PEERS)?;
            statement
                .query_map(rusqlite::params![excluded_node_name], row_to_node_subnet)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .map_err(|error| anyhow!("Failed to list peer subnets: {error}"))
    }

    pub async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<DataplanePeerMetadata>> {
        let node_name = node_name.to_string();
        self.read_db_call("db_query", move |connection| {
            connection
                .query_row(
                    queries::NODE_DATAPLANE_SELECT_BY_NAME,
                    rusqlite::params![node_name],
                    row_to_node_dataplane,
                )
                .optional()
                .map_err(klights_supervisor::DbError::from)
        })
        .await
        .map_err(|error| anyhow!("Failed to get node dataplane metadata: {error}"))
    }

    pub async fn read_allocator_state(
        &self,
    ) -> std::result::Result<DurableAllocatorState, AllocatorStateError> {
        #[cfg(any(test, feature = "test-support"))]
        if self
            .fail_next_watch_position_observation
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AllocatorStateError::PersistenceFailed {
                message: "injected watch-position observation failure".to_string(),
            });
        }
        let position = self
            .read_db_call("read_durable_allocator_observation", |connection| {
                let raw_resource_version: String = connection.query_row(
                    "SELECT value FROM metadata WHERE key = 'resource_version'",
                    [],
                    |row| row.get(0),
                )?;
                let resource_version = raw_resource_version.parse::<i64>().map_err(|_| {
                    klights_supervisor::DbError::Application(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid resource_version metadata {raw_resource_version:?}"),
                    )))
                })?;
                let event_id = connection.query_row(
                    "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
                    [],
                    |row| row.get(0),
                )?;
                if resource_version < 0 || event_id < 0 {
                    return Err(klights_supervisor::DbError::Application(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "allocator values must be non-negative",
                    ))));
                }
                Ok(WatchReplayPosition {
                    resource_version,
                    event_id,
                    resource_version_filter_through_event_id: 0,
                })
            })
            .await
            .map_err(|error| AllocatorStateError::PersistenceFailed {
                message: format!("atomic allocator observation failed: {error}"),
            })?;
        DurableAllocatorState::try_new(position)
    }

    pub async fn replay_watch_events_since_checked(
        &self,
        targets: &[klights_cluster_store::DurableWatchTarget],
        since_resource_version: i64,
        limit: Option<std::num::NonZeroUsize>,
    ) -> std::result::Result<SqliteCheckedWatchRead, WatchHistoryError> {
        if targets.is_empty() {
            return Ok(SqliteCheckedWatchRead::Events(Vec::new()));
        }
        let targets = targets.to_vec();
        self.db_call(
            "cluster-read:checked-watch-events-since",
            move |connection| {
                if since_resource_version > 0
                    && sqlite_watch_position_expired(
                        connection,
                        &targets,
                        WatchReplayPosition::from_resource_version(since_resource_version),
                    )?
                {
                    return Ok(SqliteCheckedWatchRead::Expired);
                }
                sqlite_list_watch_events_since(
                    connection,
                    &targets,
                    since_resource_version,
                    limit,
                    watch_row_to_durable_event,
                )
                .map(SqliteCheckedWatchRead::Events)
                .map_err(klights_supervisor::DbError::from)
            },
        )
        .await
        .map_err(map_watch_error)
    }

    async fn db_call<T, F>(&self, label: &'static str, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> klights_supervisor::DbClosureResult<T>
            + Send
            + 'static,
    {
        self.executor
            .call_raw(label, operation)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn list_historical(
        &self,
        request: ResourceListRequest,
    ) -> std::result::Result<ResourceListRead, ResourceReadError> {
        let query = request.query().clone();
        let position = if let Some(continuation) = query.continuation() {
            continuation.snapshot().position()
        } else {
            match query.resource_version_match() {
                ResourceVersionMatch::Exact(resource_version) => {
                    self.historical_exact_position(resource_version).await?
                }
                ResourceVersionMatch::AtPosition(position) => position,
                ResourceVersionMatch::Any | ResourceVersionMatch::NotOlderThan(_) => {
                    return Err(ResourceReadError::InvalidContinuation {
                        message: "historical LIST requires a pinned continuation".to_string(),
                    });
                }
            }
        };
        super::snapshot::bounded_historical_list(self, request, position).await
    }

    /// Convert an exact Kubernetes resourceVersion to the one durable apply
    /// position used by every bounded candidate/history window.  This happens
    /// once, before any public continuation exists; follow-ups use the typed
    /// snapshot verbatim and cannot drift to a later event at the same RV.
    async fn historical_exact_position(
        &self,
        resource_version: i64,
    ) -> std::result::Result<WatchReplayPosition, ResourceReadError> {
        self.read_db_call(
            "cluster-read:historical-exact-position",
            move |connection| {
                let head = sqlite_replay_position(connection)?;
                Ok(WatchReplayPosition {
                    resource_version,
                    event_id: 0,
                    resource_version_filter_through_event_id: head.event_id,
                })
            },
        )
        .await
        .map_err(|error| ResourceReadError::retryable(error.to_string()))
    }
}

enum SqliteNamespaceKindFilter<'a> {
    All,
    OfKind(&'a str),
    ExcludingKind(&'a str),
}

impl ClusterTopologyRead for SqliteReadStore {
    fn get_node_subnet(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<StoredNodeSubnet>> {
        Box::pin(async move {
            SqliteReadStore::get_node_subnet(self, request.node_name().as_str())
                .await
                .map_err(map_topology_error)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Vec<StoredNodeSubnet>> {
        Box::pin(async move {
            SqliteReadStore::list_peer_subnets(self, request)
                .await
                .map_err(map_topology_error)
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeTopologyRequest,
    ) -> ClusterTopologyFuture<'_, Option<DataplanePeerMetadata>> {
        Box::pin(async move {
            SqliteReadStore::get_node_dataplane(self, request.node_name().as_str())
                .await
                .map_err(map_topology_error)
        })
    }
}

fn row_to_node_subnet(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredNodeSubnet> {
    let node_name_text: String = row.get(0)?;
    let subnet_text: String = row.get(1)?;
    let gateway_ip_text: String = row.get(3)?;
    let node_ip_text: String = row.get(4)?;
    let mode_text: String = row.get(5).unwrap_or_else(|_| "root".to_string());
    let hostport_range_text: Option<String> = row.get(6).unwrap_or(None);

    let node_name = NodeName::parse(&node_name_text).map_err(parse_text_error(0))?;
    let subnet = PodSubnet::parse(&subnet_text).map_err(parse_text_error(1))?;
    let gateway_ip = gateway_ip_text
        .parse::<Ipv4Addr>()
        .map_err(|error| from_sql_error(3, error))?;
    let node_ip = node_ip_text
        .parse::<Ipv4Addr>()
        .map_err(|error| from_sql_error(4, error))?;
    let mode = klights_types::parse_node_peer_mode(Some(&mode_text)).unwrap_or(NodePeerMode::Root);
    let hostport_range = hostport_range_text
        .as_deref()
        .filter(|value| !value.is_empty())
        .and_then(|value| HostPortRange::parse(value).ok());

    Ok(StoredNodeSubnet {
        node_name,
        subnet,
        subnet_base_int: row.get::<_, i64>(2)? as u32,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    })
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

fn parse_text_error(index: usize) -> impl Fn(String) -> rusqlite::Error {
    move |message| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(NodeSubnetParseError(message)),
        )
    }
}

fn from_sql_error(
    index: usize,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(error))
}

#[derive(Debug)]
struct NodeSubnetParseError(String);

impl std::fmt::Display for NodeSubnetParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NodeSubnetParseError {}

fn row_to_node_dataplane(row: &rusqlite::Row<'_>) -> rusqlite::Result<DataplanePeerMetadata> {
    let node_name: String = row.get(0)?;
    let mode: String = row.get(1)?;
    let encryption: String = row.get(2)?;
    let public_key: Option<String> = row.get(3)?;
    let endpoint: String = row.get(4)?;
    let port = row
        .get::<_, Option<i64>>(5)?
        .map(u16::try_from)
        .transpose()
        .map_err(to_sql_error)?;

    DataplanePeerMetadata::try_new(
        node_name,
        DataplaneMode::parse(&mode).map_err(to_sql_error)?,
        DataplaneEncryption::parse(Some(&encryption)).map_err(to_sql_error)?,
        public_key,
        Some(endpoint),
        port,
    )
    .map_err(to_sql_error)
}

fn to_sql_error(error: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(error.to_string())))
}
