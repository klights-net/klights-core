//! Backend-private, root-independent Redb read algorithms.
//!
//! This is the mechanically movable Phase 10B implementation core. It owns
//! Redb-specific DTOs where the focused cluster-store contracts do not cover
//! legacy compatibility semantics.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::{RedbAccessor, tables};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_cluster_store::{
    DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, DurableRawWatchEvent,
    DurableReplayFloor, DurableReplayTarget, DurableWatchEvent, DurableWatchScope,
    DurableWatchTarget, PeerTopologyRequest, ResourceCollectionKey, StoredNodeSubnet,
};
use klights_types::{HostPortRange, NodeName, NodePeerMode, PodSubnet};
use redb::{ReadableDatabase, ReadableTable};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use super::key_codec::{decode_resource_key, lex_next, resource_key, resource_prefix};
use super::replay_floor::LegacyReplayFloor;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";

#[derive(Clone)]
pub struct RedbReadCore {
    accessor: Arc<RedbAccessor>,
}

#[derive(Clone, Debug)]
pub enum RedbCollectionScope {
    LegacyAny,
    Cluster,
    AllNamespaces,
    Namespace(String),
}

#[derive(Clone, Debug, Default)]
pub struct RedbListQuery {
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub cursor: Option<ResourceCollectionKey>,
}

#[derive(Clone, Debug)]
pub struct RedbResourceList {
    pub items: Vec<Resource>,
    pub position: WatchReplayPosition,
    pub continuation: Option<ResourceCollectionKey>,
    pub remaining_item_count: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum RedbCheckedWatchRead<T> {
    Events(Vec<T>),
    Expired,
}

#[derive(Clone, Debug)]
pub struct RedbPositionedWatchPage<T> {
    pub events: Vec<PositionedWatchEvent<T>>,
    pub next_position: WatchReplayPosition,
}

#[derive(Clone, Debug)]
pub enum RedbPositionedWatchRead<T> {
    Events(RedbPositionedWatchPage<T>),
    Expired,
}

#[derive(Clone, Debug)]
pub enum RedbSnapshotRead {
    Historical {
        items: Vec<Resource>,
        position: WatchReplayPosition,
    },
    Expired,
}

#[derive(Deserialize)]
struct StoredWatchEvent<'a> {
    #[serde(rename = "apiVersion")]
    api_version: Option<&'a str>,
    kind: Option<&'a str>,
    namespace: Option<&'a str>,
    name: Option<&'a str>,
    #[serde(rename = "eventType")]
    event_type: Option<&'a str>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<i64>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

impl RedbReadCore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    async fn call<T, F>(&self, label: &'static str, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, operation).await
    }

    pub async fn allocator_position(&self) -> Result<WatchReplayPosition> {
        self.call("redb-read:allocator-position", |database| {
            let read = database.begin_read()?;
            replay_position_in_read(&read)
        })
        .await
    }

    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace = namespace.map(str::to_string);
        let name = name.to_string();
        self.call("redb-read:get-resource", move |database| {
            let read = database.begin_read()?;
            if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
                let table = read.open_table(tables::NAMESPACES)?;
                return Ok(table.get(name.as_str())?.map(|body| {
                    resource_from_body("v1", "Namespace", None::<String>, &name, 0, body.value())
                }));
            }
            let key = resource_key(&api_version, &kind, namespace.as_deref(), &name);
            let table = read.open_table(if namespace.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            })?;
            Ok(table.get(key.as_slice())?.map(|value| {
                let (resource_version, body) = value.value();
                resource_from_body(
                    &api_version,
                    &kind,
                    namespace.clone(),
                    &name,
                    resource_version as i64,
                    body,
                )
            }))
        })
        .await
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        scope: RedbCollectionScope,
        query: RedbListQuery,
    ) -> Result<RedbResourceList> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let labels = query
            .label_selector
            .as_deref()
            .map(klights_types::parse_label_selector)
            .transpose()?
            .unwrap_or_default();
        let fields = query
            .field_selector
            .as_deref()
            .map(klights_types::FieldSelector::parse)
            .transpose()?;
        let has_selectors = !labels.is_empty() || fields.is_some();
        let limit = query
            .limit
            .filter(|value| *value > 0)
            .and_then(|value| usize::try_from(value).ok());
        let cursor = query.cursor;
        self.call("redb-read:list-resources", move |database| {
            let read = database.begin_read()?;
            let position = replay_position_in_read(&read)?;
            if api_version == "v1"
                && kind == "Namespace"
                && matches!(scope, RedbCollectionScope::Cluster)
            {
                return list_namespaces_in_read(&read, labels, fields, limit, cursor, position);
            }
            if matches!(scope, RedbCollectionScope::AllNamespaces) {
                let table = read.open_table(tables::RES_NS)?;
                let prefix = resource_prefix(&api_version, &kind, None);
                let mut start = prefix.clone();
                start.push(0);
                let end = lex_next(&prefix).unwrap_or_else(|| {
                    let mut end = prefix;
                    end.push(0xff);
                    end
                });
                let mut items = Vec::new();
                for entry in table.range(start.as_slice()..end.as_slice())? {
                    let (_, value) = entry?;
                    let (resource_version, body) = value.value();
                    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                    let resource =
                        resource_from_data(&api_version, &kind, resource_version as i64, data);
                    if labels.iter().all(|requirement| {
                        requirement.matches(
                            resource
                                .data
                                .pointer("/metadata/labels")
                                .and_then(Value::as_object),
                        )
                    }) && fields.as_ref().is_none_or(|selector| {
                        selector.matches_resource_with_identity(
                            &api_version,
                            &kind,
                            resource.data.as_ref(),
                        )
                    }) {
                        items.push(resource);
                    }
                }
                items.sort_by(|left, right| {
                    (&left.namespace, &left.name).cmp(&(&right.namespace, &right.name))
                });
                if let Some(cursor) = cursor.as_ref() {
                    items.retain(|item| {
                        (item.namespace.as_deref(), item.name.as_str())
                            > (cursor.namespace(), cursor.name())
                    });
                }
                let total = items.len();
                let has_more = limit.is_some_and(|limit| total > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
                let continuation = has_more.then(|| {
                    let item = items.last().expect("non-empty all-namespace page");
                    ResourceCollectionKey::new(item.namespace.clone(), item.name.clone())
                });
                return Ok(RedbResourceList {
                    items,
                    position,
                    continuation,
                    remaining_item_count: if has_more && !has_selectors {
                        limit
                            .and_then(|limit| total.checked_sub(limit))
                            .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
                    } else {
                        None
                    },
                });
            }

            let scans: Vec<(_, Option<String>)> = match &scope {
                RedbCollectionScope::LegacyAny => {
                    vec![(tables::RES_NS, None), (tables::RES_CLUSTER, None)]
                }
                RedbCollectionScope::Cluster => vec![(tables::RES_CLUSTER, None)],
                RedbCollectionScope::AllNamespaces => vec![(tables::RES_NS, None)],
                RedbCollectionScope::Namespace(namespace) => {
                    vec![(tables::RES_NS, Some(namespace.clone()))]
                }
            };
            let target = if has_selectors {
                limit.map_or(usize::MAX, |value| value.saturating_add(1))
            } else {
                usize::MAX
            };
            let mut items = Vec::with_capacity(target.min(4096));
            let mut remaining = 0_i64;
            let mut has_more = false;

            for (table_definition, namespace_filter) in scans {
                let table = read.open_table(table_definition)?;
                let prefix = resource_prefix(&api_version, &kind, namespace_filter.as_deref());
                let start = cursor
                    .as_ref()
                    .and_then(|cursor| {
                        let cursor_namespace = match &scope {
                            RedbCollectionScope::AllNamespaces => cursor.namespace(),
                            RedbCollectionScope::Namespace(namespace) => Some(namespace.as_str()),
                            RedbCollectionScope::LegacyAny | RedbCollectionScope::Cluster => None,
                        };
                        lex_next(&resource_key(
                            &api_version,
                            &kind,
                            cursor_namespace,
                            cursor.name(),
                        ))
                    })
                    .unwrap_or_else(|| {
                        let mut start = prefix.clone();
                        start.push(0);
                        start
                    });
                let end = lex_next(&prefix).unwrap_or_else(|| {
                    let mut end = prefix;
                    end.push(0xff);
                    end
                });
                for entry in table.range(start.as_slice()..end.as_slice())? {
                    let (_key, value) = entry?;
                    let (resource_version, body) = value.value();
                    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                    let resource =
                        resource_from_data(&api_version, &kind, resource_version as i64, data);
                    if !labels.iter().all(|requirement| {
                        requirement.matches(
                            resource
                                .data
                                .pointer("/metadata/labels")
                                .and_then(Value::as_object),
                        )
                    }) || fields.as_ref().is_some_and(|selector| {
                        !selector.matches_resource_with_identity(
                            &api_version,
                            &kind,
                            resource.data.as_ref(),
                        )
                    }) {
                        continue;
                    }
                    if has_selectors {
                        if items.len() == target {
                            break;
                        }
                        items.push(resource);
                    } else if limit.is_some_and(|limit| items.len() >= limit) {
                        has_more = true;
                        remaining = remaining.saturating_add(1);
                    } else {
                        items.push(resource);
                    }
                }
                if has_selectors && items.len() == target {
                    break;
                }
            }

            if has_selectors {
                has_more = limit.is_some_and(|limit| items.len() > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
            }
            let continuation = has_more.then(|| {
                let item = items.last().expect("non-empty limited Redb page");
                ResourceCollectionKey::new(item.namespace.clone(), item.name.clone())
            });
            Ok(RedbResourceList {
                items,
                position,
                continuation,
                remaining_item_count: (!has_selectors && has_more).then_some(remaining),
            })
        })
        .await
    }

    pub async fn list_resources_for_watch_targets(
        &self,
        targets: &[DurableWatchTarget],
        label_selector: Option<&str>,
    ) -> Result<(Vec<Resource>, WatchReplayPosition)> {
        let targets = targets.to_vec();
        let requirements = label_selector
            .map(klights_types::parse_label_selector)
            .transpose()?
            .unwrap_or_default();
        self.call("redb-read:list-watch-targets", move |database| {
            let read = database.begin_read()?;
            let mut items = read_current_targets(&read, &targets)?;
            items.retain(|resource| {
                requirements.iter().all(|requirement| {
                    requirement.matches(
                        resource
                            .data
                            .pointer("/metadata/labels")
                            .and_then(Value::as_object),
                    )
                })
            });
            sort_for_targets(&mut items, &targets);
            Ok((items, replay_position_in_read(&read)?))
        })
        .await
    }

    pub async fn list_resource_keys(
        &self,
        api_version: &str,
        kind: &str,
        namespaced: bool,
    ) -> Result<Vec<ResourceCollectionKey>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        self.call("redb-read:list-resource-keys", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(if namespaced {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            })?;
            let mut keys = Vec::new();
            for entry in table.iter()? {
                let (key, _value) = entry?;
                let Some((entry_api_version, entry_kind, namespace, name)) =
                    decode_resource_key(key.value(), namespaced)
                else {
                    continue;
                };
                if entry_api_version == api_version && entry_kind == kind {
                    keys.push(ResourceCollectionKey::new(namespace, name));
                }
            }
            keys.sort();
            Ok(keys)
        })
        .await
    }

    pub async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.call("redb-read:list-cluster-resources", |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::RES_CLUSTER)?;
            resources_from_table(&table, false, |_| true)
        })
        .await
    }

    pub async fn find_owned(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let owner_uid = owner_uid.to_string();
        let namespace = namespace.map(str::to_string);
        self.call("redb-read:find-owned", move |database| {
            let mut prefix = owner_uid.as_bytes().to_vec();
            prefix.push(0);
            let end = lex_next(&prefix).unwrap_or_else(|| {
                let mut end = prefix.clone();
                end.push(0xff);
                end
            });
            let read = database.begin_read()?;
            let table = read.open_table(tables::RESOURCES_BY_OWNER)?;
            let mut items = Vec::new();
            for entry in table.range(prefix.as_slice()..end.as_slice())? {
                let (_, value) = entry?;
                let (resource_version, body) = value.value();
                let resource = untyped_resource(resource_version as i64, body);
                if namespace
                    .as_deref()
                    .is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
                {
                    items.push(resource);
                }
            }
            Ok(items)
        })
        .await
    }

    pub async fn list_namespace_resources(
        &self,
        namespace: &str,
        kind: Option<&str>,
        excluding_kind: bool,
    ) -> Result<Vec<Resource>> {
        let namespace = namespace.to_string();
        let kind = kind.map(str::to_string);
        self.call("redb-read:list-namespace-content", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::RES_NS)?;
            resources_from_table(&table, true, |resource| {
                resource.namespace.as_deref() == Some(namespace.as_str())
                    && kind
                        .as_deref()
                        .is_none_or(|expected| (resource.kind == expected) != excluding_kind)
            })
        })
        .await
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        Ok(i64::try_from(
            self.list_namespace_resources(namespace, None, false)
                .await?
                .len(),
        )
        .unwrap_or(i64::MAX))
    }
}

fn list_namespaces_in_read(
    read: &redb::ReadTransaction,
    labels: Vec<klights_types::LabelRequirement>,
    fields: Option<klights_types::FieldSelector>,
    limit: Option<usize>,
    cursor: Option<ResourceCollectionKey>,
    position: WatchReplayPosition,
) -> Result<RedbResourceList> {
    let table = read.open_table(tables::NAMESPACES)?;
    let mut items = Vec::new();
    for entry in table.iter()? {
        let (name, body) = entry?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| name.value() <= cursor.name())
        {
            continue;
        }
        let resource = resource_from_body(
            "v1",
            "Namespace",
            None::<String>,
            name.value(),
            0,
            body.value(),
        );
        if labels.iter().all(|requirement| {
            requirement.matches(
                resource
                    .data
                    .pointer("/metadata/labels")
                    .and_then(Value::as_object),
            )
        }) && fields.as_ref().is_none_or(|selector| {
            selector.matches_resource_with_identity("v1", "Namespace", resource.data.as_ref())
        }) {
            items.push(resource);
        }
    }
    let total = items.len();
    let has_more = limit.is_some_and(|limit| total > limit);
    if let Some(limit) = limit {
        items.truncate(limit);
    }
    let continuation = has_more.then(|| {
        ResourceCollectionKey::new(
            None::<String>,
            items.last().expect("non-empty namespace page").name.clone(),
        )
    });
    Ok(RedbResourceList {
        items,
        position,
        continuation,
        remaining_item_count: if has_more && labels.is_empty() && fields.is_none() {
            limit
                .and_then(|limit| total.checked_sub(limit))
                .map(|remaining| i64::try_from(remaining).unwrap_or(i64::MAX))
        } else {
            None
        },
    })
}

fn resources_from_table(
    table: &redb::ReadOnlyTable<&[u8], (u64, &[u8])>,
    namespaced: bool,
    mut include: impl FnMut(&Resource) -> bool,
) -> Result<Vec<Resource>> {
    let mut items = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (resource_version, body) = value.value();
        let Some((api_version, kind, namespace, name)) =
            decode_resource_key(key.value(), namespaced)
        else {
            continue;
        };
        let resource = resource_from_body(
            api_version,
            kind,
            namespace.map(str::to_string),
            name,
            resource_version as i64,
            body,
        );
        if include(&resource) {
            items.push(resource);
        }
    }
    Ok(items)
}

fn resource_from_body(
    api_version: &str,
    kind: &str,
    namespace: Option<impl Into<String>>,
    name: &str,
    resource_version: i64,
    body: &[u8],
) -> Resource {
    let data = serde_json::from_slice(body).unwrap_or(Value::Null);
    Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(Into::into),
        name: name.to_string(),
        uid: Resource::uid_from_data(&data),
        resource_version,
        data: Arc::new(data),
    }
}

fn resource_from_data(
    api_version: &str,
    kind: &str,
    resource_version: i64,
    data: Value,
) -> Resource {
    let namespace = data
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .map(str::to_string);
    let name = data
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name,
        uid: Resource::uid_from_data(&data),
        resource_version,
        data: Arc::new(data),
    }
}

fn untyped_resource(resource_version: i64, body: &[u8]) -> Resource {
    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let api_version = data
        .get("apiVersion")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = data
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    resource_from_data(&api_version, &kind, resource_version, data)
}

fn replay_position_in_read(read: &redb::ReadTransaction) -> Result<WatchReplayPosition> {
    let metadata = read.open_table(tables::META)?;
    let parse = |key: &str| -> Result<i64> {
        let Some(value) = metadata.get(key)? else {
            return Ok(0);
        };
        let raw = std::str::from_utf8(value.value())
            .map_err(|error| anyhow!("invalid UTF-8 {key} metadata: {error}"))?;
        raw.parse::<i64>()
            .map_err(|_| anyhow!("invalid numeric {key} metadata {raw:?}"))
    };
    Ok(WatchReplayPosition {
        resource_version: parse("rv")?,
        event_id: parse("watch_event_id")?,
        resource_version_filter_through_event_id: 0,
    })
}

impl RedbReadCore {
    pub async fn watch_events_since(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
    ) -> Result<Vec<DurableWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:watch-events-since", move |database| {
            let read = database.begin_read()?;
            parsed_watch_events_in_read(&read, &targets, since_resource_version, None, None, None)
        })
        .await
    }

    pub async fn watch_events_since_checked(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
        limit: Option<NonZeroUsize>,
    ) -> Result<RedbCheckedWatchRead<DurableWatchEvent>> {
        if targets.is_empty() {
            return Ok(RedbCheckedWatchRead::Events(Vec::new()));
        }
        let targets = targets.to_vec();
        self.call("redb-read:watch-events-since-checked", move |database| {
            let read = database.begin_read()?;
            if since_resource_version > 0
                && targets.iter().try_fold(false, |expired, target| {
                    Ok::<_, anyhow::Error>(
                        expired
                            || target_rv_floor(&read, target)?
                                .is_some_and(|floor| since_resource_version < floor),
                    )
                })?
            {
                return Ok(RedbCheckedWatchRead::Expired);
            }
            parsed_watch_events_in_read(
                &read,
                &targets,
                since_resource_version,
                None,
                None,
                limit.map(NonZeroUsize::get),
            )
            .map(RedbCheckedWatchRead::Events)
        })
        .await
    }

    pub async fn positioned_watch_events(
        &self,
        targets: &[DurableWatchTarget],
        position: WatchReplayPosition,
        limit: NonZeroUsize,
    ) -> Result<RedbPositionedWatchRead<DurableWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:positioned-watch-events", move |database| {
            let read = database.begin_read()?;
            let current = replay_position_in_read(&read)?;
            if position.event_id > current.event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
                || position_expired_for_targets(&read, &targets, position)?
            {
                return Ok(RedbPositionedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let filter_through = if position.resource_version_filter_through_event_id > 0 {
                position.resource_version_filter_through_event_id
            } else if position.event_id == 0 {
                i64::MAX
            } else {
                0
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..=current.event_id.max(0) as u64)? {
                let (event_id, encoded) = entry?;
                let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())
                    .map_err(|error| anyhow!("malformed persisted watch event JSON: {error}"))?;
                let resource_version = stored.resource_version.unwrap_or_default();
                if event_id.value() as i64 <= filter_through
                    && resource_version <= position.resource_version
                {
                    continue;
                }
                if !targets.iter().any(|target| stored_matches(target, &stored)) {
                    continue;
                }
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version,
                        event_id: event_id.value() as i64,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: durable_event_from_stored(stored)?,
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, current.event_id, limit);
            Ok(RedbPositionedWatchRead::Events(RedbPositionedWatchPage {
                events,
                next_position,
            }))
        })
        .await
    }

    pub async fn raw_watch_events_since_checked(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
        limit: NonZeroUsize,
    ) -> Result<RedbCheckedWatchRead<DurableRawWatchEvent>> {
        if targets.is_empty() {
            return Ok(RedbCheckedWatchRead::Events(Vec::new()));
        }
        let targets = targets.to_vec();
        self.call("redb-read:raw-watch-events-since", move |database| {
            let read = database.begin_read()?;
            if since_resource_version > 0
                && targets.iter().try_fold(false, |expired, target| {
                    Ok::<_, anyhow::Error>(
                        expired
                            || target_rv_floor(&read, target)?
                                .is_some_and(|floor| since_resource_version < floor),
                    )
                })?
            {
                return Ok(RedbCheckedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.iter()? {
                let (_, encoded) = entry?;
                let Ok(stored) = serde_json::from_slice::<StoredWatchEvent<'_>>(encoded.value())
                else {
                    continue;
                };
                if stored.resource_version.unwrap_or_default() <= since_resource_version
                    || !targets.iter().any(|target| stored_matches(target, &stored))
                {
                    continue;
                }
                events.push(raw_event_from_stored(stored));
                if events.len() == limit.get() {
                    break;
                }
            }
            Ok(RedbCheckedWatchRead::Events(events))
        })
        .await
    }

    pub async fn positioned_raw_watch_events(
        &self,
        targets: &[DurableWatchTarget],
        position: WatchReplayPosition,
        limit: NonZeroUsize,
    ) -> Result<RedbPositionedWatchRead<DurableRawWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:positioned-raw-watch-events", move |database| {
            let read = database.begin_read()?;
            let current = replay_position_in_read(&read)?;
            if position.event_id > current.event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
                || position_expired_for_targets(&read, &targets, position)?
            {
                return Ok(RedbPositionedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let filter_through = if position.resource_version_filter_through_event_id > 0 {
                position.resource_version_filter_through_event_id
            } else if position.event_id == 0 {
                i64::MAX
            } else {
                0
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..=current.event_id.max(0) as u64)? {
                let (event_id, encoded) = entry?;
                let Ok(stored) = serde_json::from_slice::<StoredWatchEvent<'_>>(encoded.value())
                else {
                    continue;
                };
                let resource_version = stored.resource_version.unwrap_or_default();
                if (event_id.value() as i64) <= filter_through
                    && resource_version <= position.resource_version
                {
                    continue;
                }
                if !targets.iter().any(|target| stored_matches(target, &stored)) {
                    continue;
                }
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version,
                        event_id: event_id.value() as i64,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: raw_event_from_stored(stored),
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, current.event_id, limit);
            Ok(RedbPositionedWatchRead::Events(RedbPositionedWatchPage {
                events,
                next_position,
            }))
        })
        .await
    }

    pub async fn all_watch_events_since(
        &self,
        since_resource_version: i64,
        deleted_only: bool,
    ) -> Result<Vec<DurableWatchEvent>> {
        self.call("redb-read:all-watch-events-since", move |database| {
            let read = database.begin_read()?;
            parsed_watch_events_in_read(
                &read,
                &[],
                since_resource_version,
                deleted_only.then_some("DELETED"),
                None,
                None,
            )
        })
        .await
    }

    pub async fn all_watch_events_since_paged(
        &self,
        since_resource_version: i64,
        after_event_id: i64,
        through_event_id: Option<i64>,
        limit: NonZeroUsize,
    ) -> Result<Vec<(i64, DurableWatchEvent)>> {
        self.call("redb-read:all-watch-events-paged", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = after_event_id.saturating_add(1).max(0) as u64;
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..)? {
                let (event_id, encoded) = entry?;
                let event_id = event_id.value() as i64;
                if through_event_id.is_some_and(|through| event_id > through) {
                    break;
                }
                let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())
                    .unwrap_or(StoredWatchEvent {
                        api_version: None,
                        kind: None,
                        namespace: None,
                        name: None,
                        event_type: None,
                        resource_version: None,
                        data: None,
                    });
                if stored.resource_version.unwrap_or_default() <= since_resource_version {
                    continue;
                }
                events.push((event_id, durable_event_from_stored(stored)?));
                if events.len() == limit.get() {
                    break;
                }
            }
            Ok(events)
        })
        .await
    }

    pub async fn replay_floors(&self) -> Result<Vec<DurableReplayFloor>> {
        self.call("redb-read:replay-floors", |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut floors = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                    floors.push(floor);
                }
            }
            floors.sort_by(|left, right| {
                replay_target_sort_key(left.target()).cmp(&replay_target_sort_key(right.target()))
            });
            Ok(floors)
        })
        .await
    }

    pub async fn replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: NonZeroUsize,
    ) -> Result<Vec<DurableReplayFloor>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow!(
                "watch replay-floor page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after.map(|cursor| match cursor.target() {
            klights_cluster_store::DurableReplayTarget::All => floor_key("*", "*", "*"),
            klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
                floor_key(api_version, kind, CLUSTER_NAMESPACE_KEY)
            }
            klights_cluster_store::DurableReplayTarget::Namespaced {
                api_version,
                kind,
                namespace,
            } => floor_key(api_version, kind, namespace),
        });
        self.call("redb-read:replay-floors-paged", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut floors = Vec::with_capacity(limit.get());
            if let Some(after) = after {
                for entry in table.range(after.as_slice()..)? {
                    let (key, value) = entry?;
                    if key.value() <= after.as_slice() {
                        continue;
                    }
                    if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                        floors.push(floor);
                        if floors.len() == limit.get() {
                            break;
                        }
                    }
                }
            } else {
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                        floors.push(floor);
                        if floors.len() == limit.get() {
                            break;
                        }
                    }
                }
            }
            Ok(floors)
        })
        .await
    }
}

fn parsed_watch_events_in_read(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    since_resource_version: i64,
    required_event_type: Option<&str>,
    start_event_id: Option<i64>,
    limit: Option<usize>,
) -> Result<Vec<DurableWatchEvent>> {
    let table = read.open_table(tables::WATCH_EVENTS)?;
    let mut events = Vec::with_capacity(limit.unwrap_or_default().min(4096));
    let start = start_event_id.unwrap_or_default().max(0) as u64;
    for entry in table.range(start..)? {
        let (_, encoded) = entry?;
        let stored: StoredWatchEvent<'_> =
            serde_json::from_slice(encoded.value()).unwrap_or(StoredWatchEvent {
                api_version: None,
                kind: None,
                namespace: None,
                name: None,
                event_type: None,
                resource_version: None,
                data: None,
            });
        if stored.resource_version.unwrap_or_default() <= since_resource_version
            || required_event_type.is_some_and(|required| stored.event_type != Some(required))
            || (!targets.is_empty()
                && !targets.iter().any(|target| stored_matches(target, &stored)))
        {
            continue;
        }
        events.push(durable_event_from_stored(stored)?);
        if limit.is_some_and(|limit| events.len() == limit) {
            break;
        }
    }
    Ok(events)
}

fn stored_matches(target: &DurableWatchTarget, stored: &StoredWatchEvent<'_>) -> bool {
    target.api_version() == stored.api_version.unwrap_or_default()
        && target.kind() == stored.kind.unwrap_or_default()
        && target_matches_namespace(target, stored.namespace)
}

fn target_matches_namespace(target: &DurableWatchTarget, namespace: Option<&str>) -> bool {
    match target.scope() {
        DurableWatchScope::Cluster => namespace.is_none(),
        DurableWatchScope::Namespaced(Some(expected)) => namespace == Some(expected),
        DurableWatchScope::Namespaced(None) => namespace.is_some(),
    }
}

fn durable_event_from_stored(stored: StoredWatchEvent<'_>) -> Result<DurableWatchEvent> {
    let data = stored
        .data
        .map(|raw| serde_json::from_str(raw.get()))
        .transpose()?
        .unwrap_or(Value::Null);
    let resource = Resource {
        id: 0,
        api_version: stored.api_version.unwrap_or_default().to_string(),
        kind: stored.kind.unwrap_or_default().to_string(),
        namespace: stored.namespace.map(str::to_string),
        name: stored.name.unwrap_or_default().to_string(),
        uid: Resource::uid_from_data(&data),
        resource_version: stored.resource_version.unwrap_or_default(),
        data: Arc::new(data),
    };
    Ok(DurableWatchEvent::new(
        stored.event_type.unwrap_or_default().to_string(),
        resource,
    ))
}

fn raw_event_from_stored(stored: StoredWatchEvent<'_>) -> DurableRawWatchEvent {
    DurableRawWatchEvent {
        api_version: stored.api_version.unwrap_or_default().to_string(),
        kind: stored.kind.unwrap_or_default().to_string(),
        namespace: stored.namespace.map(str::to_string),
        name: stored.name.unwrap_or_default().to_string(),
        resource_version: stored.resource_version.unwrap_or_default(),
        event_type: Cow::Owned(stored.event_type.unwrap_or_default().to_string()),
        object_json: stored.data.map_or_else(
            || Bytes::from_static(b"null"),
            |data| Bytes::copy_from_slice(data.get().as_bytes()),
        ),
    }
}

fn target_rv_floor(
    read: &redb::ReadTransaction,
    target: &DurableWatchTarget,
) -> Result<Option<i64>> {
    let table = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let scoped = match target.scope() {
        DurableWatchScope::Cluster => table
            .get(floor_key(target.api_version(), target.kind(), CLUSTER_NAMESPACE_KEY).as_slice())?
            .map(|value| value.value() as i64),
        DurableWatchScope::Namespaced(Some(namespace)) => table
            .get(floor_key(target.api_version(), target.kind(), namespace).as_slice())?
            .map(|value| value.value() as i64),
        DurableWatchScope::Namespaced(None) => {
            namespaced_floor(read, target.api_version(), target.kind(), false)?
        }
    };
    Ok(LegacyReplayFloor::read(read)?
        .and_then(|legacy| legacy.merge_resource_version(scoped))
        .or(scoped))
}

fn position_expired_for_targets(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    position: WatchReplayPosition,
) -> Result<bool> {
    for target in targets {
        let floor = if position.event_id == 0 {
            target_rv_floor(read, target)?
        } else {
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let scoped = match target.scope() {
                DurableWatchScope::Cluster => table
                    .get(
                        floor_key(target.api_version(), target.kind(), CLUSTER_NAMESPACE_KEY)
                            .as_slice(),
                    )?
                    .and_then(|value| decode_position(value.value()).map(|(_, event_id)| event_id)),
                DurableWatchScope::Namespaced(Some(namespace)) => table
                    .get(floor_key(target.api_version(), target.kind(), namespace).as_slice())?
                    .and_then(|value| decode_position(value.value()).map(|(_, event_id)| event_id)),
                DurableWatchScope::Namespaced(None) => {
                    namespaced_floor(read, target.api_version(), target.kind(), true)?
                }
            };
            LegacyReplayFloor::read(read)?
                .and_then(|legacy| legacy.merge_event_id(scoped))
                .or(scoped)
        };
        let cursor = if position.event_id == 0 {
            position.resource_version
        } else {
            position.event_id
        };
        if floor.is_some_and(|floor| cursor < floor) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn namespaced_floor(
    read: &redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    positioned: bool,
) -> Result<Option<i64>> {
    let prefix = floor_prefix(api_version, kind);
    let mut floor = None;
    if positioned {
        let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                continue;
            };
            if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                continue;
            }
            if let Some((_, candidate)) = decode_position(value.value()) {
                floor = Some(floor.map_or(candidate, |current: i64| current.max(candidate)));
            }
        }
    } else {
        let table = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                continue;
            };
            if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                continue;
            }
            let candidate = value.value() as i64;
            floor = Some(floor.map_or(candidate, |current: i64| current.max(candidate)));
        }
    }
    Ok(floor)
}

fn floor_prefix(api_version: &str, kind: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(api_version.len() + kind.len() + 2);
    key.extend_from_slice(api_version.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    key
}

fn floor_key(api_version: &str, kind: &str, namespace: &str) -> Vec<u8> {
    let mut key = floor_prefix(api_version, kind);
    key.extend_from_slice(namespace.as_bytes());
    key
}

fn replay_target_sort_key(target: &DurableReplayTarget) -> (&str, &str, &str) {
    match target {
        DurableReplayTarget::All => ("*", "*", "*"),
        DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, CLUSTER_NAMESPACE_KEY)
        }
        DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    }
}

fn decode_position(encoded: &[u8]) -> Option<(i64, i64)> {
    (encoded.len() == 16).then(|| {
        (
            i64::try_from(u64::from_be_bytes(
                encoded[..8].try_into().expect("fixed floor prefix"),
            ))
            .unwrap_or(i64::MAX),
            i64::try_from(u64::from_be_bytes(
                encoded[8..].try_into().expect("fixed floor suffix"),
            ))
            .unwrap_or(i64::MAX),
        )
    })
}

fn decode_floor_key(key: &[u8]) -> Option<(String, String, String)> {
    let mut parts = key.splitn(3, |byte| *byte == 0);
    Some((
        String::from_utf8_lossy(parts.next()?).into_owned(),
        String::from_utf8_lossy(parts.next()?).into_owned(),
        String::from_utf8_lossy(parts.next()?).into_owned(),
    ))
}

fn decode_durable_floor(key: &[u8], value: &[u8]) -> Result<Option<DurableReplayFloor>> {
    let Some((api_version, kind, namespace)) = decode_floor_key(key) else {
        return Ok(None);
    };
    let Some((resource_version, event_id)) = decode_position(value) else {
        return Ok(None);
    };
    let floor = if api_version == "*" && kind == "*" && namespace == "*" {
        DurableReplayFloor::all(resource_version, event_id, true)
    } else if namespace == CLUSTER_NAMESPACE_KEY {
        DurableReplayFloor::cluster(api_version, kind, resource_version, event_id, true)
    } else {
        DurableReplayFloor::namespaced(
            api_version,
            kind,
            namespace,
            resource_version,
            event_id,
            true,
        )
    }
    .map_err(|error| anyhow!(error.to_string()))?;
    Ok(Some(floor))
}

impl RedbReadCore {
    pub async fn snapshot_at_position(
        &self,
        targets: &[DurableWatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<RedbSnapshotRead> {
        let targets = targets.to_vec();
        let sort_targets = targets.clone();
        let labels = label_selector.map(str::to_string);
        let fields = field_selector.map(str::to_string);
        self.call("redb-read:snapshot-at-position", move |database| {
            let read = database.begin_read()?;
            let current_position = replay_position_in_read(&read)?;
            if position.event_id > current_position.event_id
                || position.resource_version_filter_through_event_id > current_position.event_id
                || (position.event_id == 0
                    && position.resource_version_filter_through_event_id == 0
                    && position.resource_version > current_position.resource_version)
            {
                return Ok(RedbSnapshotRead::Expired);
            }
            let current = read_current_targets(&read, &targets)?;
            let raw = if cursor_covers_current(position, current_position) {
                ReconstructedMembership::Items(current)
            } else if position_expired_for_targets(&read, &targets, position)? {
                ReconstructedMembership::Expired
            } else {
                reconstruct_membership_in_read(&read, &targets, current, position)?
            };
            let ReconstructedMembership::Items(mut items) = raw else {
                return Ok(RedbSnapshotRead::Expired);
            };
            apply_selectors(&mut items, labels.as_deref(), fields.as_deref())?;
            sort_for_targets(&mut items, &sort_targets);
            Ok(RedbSnapshotRead::Historical { items, position })
        })
        .await
    }

    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<StoredNodeSubnet>> {
        let node_name = node_name.to_string();
        self.call("redb-read:get-node-subnet", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::NODE_SUBNETS)?;
            table
                .get(node_name.as_str())?
                .map(|value| parse_node_subnet(&node_name, value.value()))
                .transpose()
        })
        .await
    }

    pub async fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> Result<Vec<StoredNodeSubnet>> {
        let excluded = request
            .excluded_node_name()
            .map(|name| name.as_str().to_string());
        self.call("redb-read:list-peer-subnets", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::NODE_SUBNETS)?;
            let mut items = Vec::new();
            for entry in table.iter()? {
                let (name, value) = entry?;
                if (excluded.is_none() && name.value().is_empty())
                    || excluded.as_deref() == Some(name.value())
                {
                    continue;
                }
                items.push(parse_node_subnet(name.value(), value.value())?);
            }
            Ok(items)
        })
        .await
    }

    pub async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<DataplanePeerMetadata>> {
        let node_name = node_name.to_string();
        self.call("redb-read:get-node-dataplane", move |database| {
            let read = database.begin_read()?;
            let table = match read.open_table(tables::NODE_DATAPLANE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let Some(value) = table.get(node_name.as_str())? else {
                return Ok(None);
            };
            let body: Value = serde_json::from_slice(value.value())
                .map_err(|error| anyhow!("malformed persisted node dataplane JSON: {error}"))?;
            let mode = required_string(&body, "node dataplane", "mode")?;
            let encryption = required_string(&body, "node dataplane", "encryption")?;
            let public_key = optional_string(&body, "node dataplane", "public_key")?;
            let endpoint = optional_string(&body, "node dataplane", "endpoint")?;
            let port = optional_port(&body, "port")?;
            Ok(Some(DataplanePeerMetadata::try_new(
                node_name,
                DataplaneMode::parse(mode)?,
                DataplaneEncryption::parse(Some(encryption))?,
                public_key,
                endpoint,
                port,
            )?))
        })
        .await
    }
}

fn cursor_covers_current(position: WatchReplayPosition, current: WatchReplayPosition) -> bool {
    position.event_id >= current.event_id
        || (position.resource_version_filter_through_event_id >= current.event_id
            && position.resource_version >= current.resource_version)
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version >= current.resource_version)
}

fn read_current_targets(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
) -> Result<Vec<Resource>> {
    let mut resources = Vec::new();
    for target in targets {
        if target.api_version() == "v1"
            && target.kind() == "Namespace"
            && matches!(target.scope(), DurableWatchScope::Cluster)
        {
            let table = read.open_table(tables::NAMESPACES)?;
            for entry in table.iter()? {
                let (name, body) = entry?;
                let data: Value = serde_json::from_slice(body.value())?;
                let resource_version = data
                    .pointer("/metadata/resourceVersion")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default();
                resources.push(resource_from_data(
                    "v1",
                    "Namespace",
                    resource_version,
                    data,
                ));
                if let Some(last) = resources.last_mut() {
                    last.name = name.value().to_string();
                }
            }
            continue;
        }
        let namespaced = !matches!(target.scope(), DurableWatchScope::Cluster);
        let table = read.open_table(if namespaced {
            tables::RES_NS
        } else {
            tables::RES_CLUSTER
        })?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let (resource_version, body) = value.value();
            let Some((api_version, kind, namespace, name)) =
                decode_resource_key(key.value(), namespaced)
            else {
                continue;
            };
            if target.api_version() == api_version
                && target.kind() == kind
                && target_matches_namespace(target, namespace)
            {
                resources.push(resource_from_body(
                    api_version,
                    kind,
                    namespace.map(str::to_string),
                    name,
                    resource_version as i64,
                    body,
                ));
            }
        }
    }
    Ok(resources)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct MembershipKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl MembershipKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

struct MembershipHistoryEvent {
    event_id: i64,
    event_type: String,
    resource: Resource,
}

enum ReconstructedMembership {
    Expired,
    Items(Vec<Resource>),
}

struct MembershipReconstructor {
    state: BTreeMap<MembershipKey, Resource>,
    needs_predecessor: HashSet<MembershipKey>,
    position: WatchReplayPosition,
    expired: bool,
}

impl MembershipReconstructor {
    fn new(current: Vec<Resource>, position: WatchReplayPosition) -> Self {
        Self {
            state: current
                .into_iter()
                .map(|resource| (MembershipKey::from_resource(&resource), resource))
                .collect(),
            needs_predecessor: HashSet::new(),
            position,
            expired: false,
        }
    }

    fn observe(&mut self, event: &MembershipHistoryEvent) {
        if self.expired {
            return;
        }
        let key = MembershipKey::from_resource(&event.resource);
        if self.needs_predecessor.remove(&key) {
            if event.event_type == "DELETED" {
                self.expired = true;
                return;
            }
            self.state.insert(key.clone(), event.resource.clone());
        }
        if self
            .position
            .represents_event(event.event_id, event.resource.resource_version)
        {
            return;
        }
        match event.event_type.as_str() {
            "ADDED" => {
                self.state.remove(&key);
            }
            "MODIFIED" => {
                self.needs_predecessor.insert(key);
            }
            "DELETED" => {
                self.state.insert(key, event.resource.clone());
            }
            _ => self.expired = true,
        }
    }

    fn can_stop_before(&self, event_id: i64) -> bool {
        self.position.event_id > 0
            && self.position.resource_version_filter_through_event_id == 0
            && event_id <= self.position.event_id
            && self.needs_predecessor.is_empty()
    }

    fn finish(self) -> ReconstructedMembership {
        if self.expired || !self.needs_predecessor.is_empty() {
            ReconstructedMembership::Expired
        } else {
            ReconstructedMembership::Items(self.state.into_values().collect())
        }
    }
}

fn reconstruct_membership_in_read(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    current: Vec<Resource>,
    position: WatchReplayPosition,
) -> Result<ReconstructedMembership> {
    let table = read.open_table(tables::WATCH_EVENTS)?;
    let mut reconstructor = MembershipReconstructor::new(current, position);
    for entry in table.iter()?.rev() {
        let (event_id, encoded) = entry?;
        if reconstructor.can_stop_before(event_id.value() as i64) {
            break;
        }
        let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())?;
        if !targets.iter().any(|target| stored_matches(target, &stored)) {
            continue;
        }
        let event = durable_event_from_stored(stored)?;
        reconstructor.observe(&MembershipHistoryEvent {
            event_id: event_id.value() as i64,
            event_type: event.event_type().to_string(),
            resource: event.into_resource(),
        });
    }
    Ok(reconstructor.finish())
}

fn apply_selectors(
    items: &mut Vec<Resource>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<()> {
    let labels = label_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::LabelSelector::parse)
        .transpose()?;
    let fields = field_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::FieldSelector::parse)
        .transpose()?;
    items.retain(|resource| {
        labels
            .as_ref()
            .is_none_or(|selector| selector.matches_resource(resource.data.as_ref()))
            && fields.as_ref().is_none_or(|selector| {
                selector.matches_resource_with_identity(
                    &resource.api_version,
                    &resource.kind,
                    resource.data.as_ref(),
                )
            })
    });
    Ok(())
}

fn sort_for_targets(items: &mut [Resource], targets: &[DurableWatchTarget]) {
    items.sort_unstable_by(|left, right| {
        let order = |resource: &Resource| {
            targets
                .iter()
                .position(|target| {
                    target.api_version() == resource.api_version
                        && target.kind() == resource.kind
                        && target_matches_namespace(target, resource.namespace.as_deref())
                })
                .unwrap_or(usize::MAX)
        };
        (order(left), &left.namespace, &left.name).cmp(&(
            order(right),
            &right.namespace,
            &right.name,
        ))
    });
}

fn parse_node_subnet(name: &str, body: &[u8]) -> Result<StoredNodeSubnet> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow!("malformed persisted node subnet JSON: {error}"))?;
    let subnet_text = required_string(&value, "node subnet", "subnet")?;
    let subnet = PodSubnet::parse(subnet_text).map_err(|error| anyhow!("bad subnet: {error}"))?;
    if subnet.prefix() != 24 || subnet.to_string() != subnet_text {
        return Err(anyhow!("persisted node subnet CIDR must be canonical /24"));
    }
    let subnet_base_int = required_u32(&value, "node subnet", "subnet_base_int")?;
    if subnet_base_int != subnet.base() {
        return Err(anyhow!(
            "persisted node subnet base integer does not match its CIDR"
        ));
    }
    let gateway_ip = required_string(&value, "node subnet", "vtep_ip")?
        .parse::<Ipv4Addr>()
        .map_err(|error| anyhow!("bad vtep_ip: {error}"))?;
    if gateway_ip != subnet.base_ip() {
        return Err(anyhow!(
            "persisted node subnet gateway compatibility field does not match its CIDR"
        ));
    }
    let node_ip = required_string(&value, "node subnet", "node_ip")?
        .parse()
        .map_err(|error| anyhow!("bad node_ip: {error}"))?;
    let mode = match value.get("mode") {
        None => NodePeerMode::Root,
        Some(Value::String(mode)) if mode == "root" => NodePeerMode::Root,
        Some(Value::String(mode)) if mode == "rootless" => NodePeerMode::Rootless,
        Some(Value::String(mode)) => {
            return Err(anyhow!("unknown persisted node peer mode {mode:?}"));
        }
        Some(_) => return Err(anyhow!("node subnet field mode must be a string")),
    };
    let hostport_range = match value.get("hostport_range") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(
            HostPortRange::parse(value)
                .map_err(|error| anyhow!("invalid persisted hostport_range {value:?}: {error}"))?,
        ),
        Some(_) => {
            return Err(anyhow!(
                "node subnet field hostport_range must be a string or null"
            ));
        }
    };
    match (mode, hostport_range) {
        (NodePeerMode::Root, Some(_)) => {
            return Err(anyhow!(
                "persisted root node subnet must not carry a host-port range"
            ));
        }
        (NodePeerMode::Rootless, None) => {
            return Err(anyhow!(
                "persisted rootless node subnet requires a host-port range"
            ));
        }
        _ => {}
    }
    Ok(StoredNodeSubnet {
        node_name: NodeName::parse(name).map_err(|error| anyhow!("bad node name: {error}"))?,
        subnet,
        subnet_base_int,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    })
}

fn required_string<'a>(value: &'a Value, context: &str, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{context} field {field} must be a string"))
}

fn optional_string(value: &Value, context: &str, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(anyhow!("{context} field {field} must be a string or null")),
    }
}

fn required_u32(value: &Value, context: &str, field: &str) -> Result<u32> {
    let value = value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("{context} field {field} must be an unsigned integer"))?;
    u32::try_from(value).map_err(|_| anyhow!("{context} field {field} exceeds u32"))
}

fn optional_port(value: &Value, field: &str) -> Result<Option<u16>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let port = value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow!("node dataplane field {field} must be a non-zero u16"))?;
            Ok(Some(port))
        }
        Some(_) => Err(anyhow!(
            "node dataplane field {field} must be an integer or null"
        )),
    }
}
