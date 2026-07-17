//! `RedbWatchStore` — watch event history, catch-up, and GC.

use std::{collections::BTreeMap, sync::Arc};

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::{helpers, tables};
use crate::datastore::types::*;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";
const DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE: i64 = 1_024;
const MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES: usize = 16;

#[derive(Deserialize)]
struct StoredRawWatchEvent<'a> {
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

fn watch_events_min_scope_rows(max_rows: i64, scope_count: usize) -> usize {
    if max_rows <= 0 || scope_count == 0 {
        return 0;
    }
    let fair_share = (max_rows as usize) / scope_count;
    let dynamic_floor = if fair_share == 0 && scope_count <= MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES
    {
        1
    } else {
        fair_share
    };
    (max_rows.clamp(1, DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE) as usize).min(dynamic_floor)
}

fn watch_events_max_rows(max_rows: i64) -> usize {
    if max_rows <= 0 { 0 } else { max_rows as usize }
}

fn watch_events_batch_cap(batch_cap: i64) -> usize {
    if batch_cap < 0 {
        usize::MAX
    } else {
        batch_cap as usize
    }
}

pub struct RedbWatchStore {
    pub accessor: Arc<RedbAccessor>,
}

impl RedbWatchStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    pub async fn watch_list(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let targets_owned = targets.to_vec();
        self.db_call("watch_list", move |db| {
            let targets: &[WatchTarget] = &targets_owned;
            let r = db.begin_read()?;
            Self::watch_list_in_read(&r, targets, since_rv)
        })
        .await
    }

    pub async fn watch_list_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        if targets.is_empty() {
            return Ok(WatchReplayRead::Events(Vec::new()));
        }

        let targets_owned = targets.to_vec();
        self.db_call("watch_list_checked", move |db| {
            let targets: &[WatchTarget] = &targets_owned;
            let r = db.begin_read()?;
            if since_rv > 0 {
                for target in targets {
                    if let Some(floor_rv) = target_floor(&r, target)?
                        && since_rv < floor_rv
                    {
                        return Ok(WatchReplayRead::Expired);
                    }
                }
            }
            Self::watch_list_in_read(&r, targets, since_rv).map(WatchReplayRead::Events)
        })
        .await
    }

    pub async fn watch_list_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        match self.watch_list_checked(targets, since_rv).await? {
            WatchReplayRead::Events(mut events) => {
                events.truncate(limit.get());
                Ok(WatchReplayRead::Events(events))
            }
            WatchReplayRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }

    pub async fn watch_list_raw_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<RawWatchEvent>> {
        if targets.is_empty() {
            return Ok(WatchReplayRead::Events(Vec::new()));
        }

        let targets_owned = targets.to_vec();
        self.db_call("watch_list_raw_checked_bounded", move |db| {
            let targets: &[WatchTarget] = &targets_owned;
            let r = db.begin_read()?;
            if since_rv > 0 {
                for target in targets {
                    if let Some(floor_rv) = target_floor(&r, target)?
                        && since_rv < floor_rv
                    {
                        return Ok(WatchReplayRead::Expired);
                    }
                }
            }
            Self::watch_list_raw_in_read(&r, targets, since_rv, limit.get())
                .map(WatchReplayRead::Events)
        })
        .await
    }

    pub async fn watch_list_positioned_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        let targets = targets.to_vec();
        self.db_call("watch_list_positioned", move |db| {
            let read = db.begin_read()?;
            let current = helpers::watch_replay_position_in_read(&read)?;
            let high_water_event_id = current.event_id;
            if position.event_id > high_water_event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
            {
                return Ok(PositionedWatchReplayRead::Expired);
            }
            if position.event_id == 0 {
                for target in &targets {
                    if target_floor(&read, target)?
                        .is_some_and(|floor| position.resource_version < floor)
                    {
                        return Ok(PositionedWatchReplayRead::Expired);
                    }
                }
            } else if position_expired_for_targets(&read, &targets, position.event_id)? {
                return Ok(PositionedWatchReplayRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start_id = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start_id..=high_water_event_id.max(0) as u64)? {
                let (id, value) = entry?;
                let event_id = id.value() as i64;
                let body = value.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let rv_filter_through = if position.resource_version_filter_through_event_id > 0 {
                    position.resource_version_filter_through_event_id
                } else if position.event_id == 0 {
                    i64::MAX
                } else {
                    0
                };
                if event_id <= rv_filter_through && rv <= position.resource_version {
                    continue;
                }
                let av = event
                    .get("apiVersion")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let kind = event.get("kind").and_then(Value::as_str).unwrap_or("");
                let namespace = event.get("namespace").and_then(Value::as_str);
                if !targets
                    .iter()
                    .any(|target| watch_event_matches_target(target, av, kind, namespace))
                {
                    continue;
                }
                let data = event.get("data").cloned().unwrap_or(Value::Null);
                let name = event.get("name").and_then(Value::as_str).unwrap_or("");
                let event_type = event
                    .get("eventType")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version: rv,
                        event_id,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: CatchUpResource {
                        resource: Resource {
                            id: 0,
                            api_version: av.to_string(),
                            kind: kind.to_string(),
                            namespace: namespace.map(str::to_string),
                            name: name.to_string(),
                            uid: Resource::uid_from_data(&data),
                            resource_version: rv,
                            data: Arc::new(data),
                        },
                        event_type: std::borrow::Cow::Owned(event_type),
                    },
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, high_water_event_id, limit);
            Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                events,
                next_position,
            }))
        })
        .await
    }

    pub async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.db_call("current_watch_replay_position", |db| {
            let read = db.begin_read()?;
            helpers::watch_replay_position_in_read(&read)
        })
        .await
    }

    pub async fn watch_list_raw_positioned_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        let targets = targets.to_vec();
        self.db_call("watch_list_raw_positioned", move |db| {
            let read = db.begin_read()?;
            let current = helpers::watch_replay_position_in_read(&read)?;
            let high_water_event_id = current.event_id;
            if position.event_id > high_water_event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
            {
                return Ok(PositionedWatchReplayRead::Expired);
            }
            if position.event_id == 0 {
                for target in &targets {
                    if target_floor(&read, target)?
                        .is_some_and(|floor| position.resource_version < floor)
                    {
                        return Ok(PositionedWatchReplayRead::Expired);
                    }
                }
            } else if position_expired_for_targets(&read, &targets, position.event_id)? {
                return Ok(PositionedWatchReplayRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start_id = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start_id..=high_water_event_id.max(0) as u64)? {
                let (id, value) = entry?;
                let event_id = id.value() as i64;
                let Ok(event) = serde_json::from_slice::<StoredRawWatchEvent<'_>>(value.value())
                else {
                    continue;
                };
                let rv = event.resource_version.unwrap_or_default();
                let rv_filter_through = if position.resource_version_filter_through_event_id > 0 {
                    position.resource_version_filter_through_event_id
                } else if position.event_id == 0 {
                    i64::MAX
                } else {
                    0
                };
                if event_id <= rv_filter_through && rv <= position.resource_version {
                    continue;
                }
                let av = event.api_version.unwrap_or("");
                let kind = event.kind.unwrap_or("");
                if !targets
                    .iter()
                    .any(|target| watch_event_matches_target(target, av, kind, event.namespace))
                {
                    continue;
                }
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version: rv,
                        event_id,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: RawWatchEvent {
                        api_version: av.to_string(),
                        kind: kind.to_string(),
                        namespace: event.namespace.map(str::to_string),
                        name: event.name.unwrap_or("").to_string(),
                        resource_version: rv,
                        event_type: std::borrow::Cow::Owned(
                            event.event_type.unwrap_or("").to_string(),
                        ),
                        object_json: event.data.map_or_else(
                            || Bytes::from_static(b"null"),
                            |data| Bytes::copy_from_slice(data.get().as_bytes()),
                        ),
                    },
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, high_water_event_id, limit);
            Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                events,
                next_position,
            }))
        })
        .await
    }

    fn watch_list_raw_in_read(
        read_txn: &::redb::ReadTransaction,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: usize,
    ) -> Result<Vec<RawWatchEvent>> {
        let tbl = read_txn.open_table(tables::WATCH_EVENTS)?;
        let mut result = Vec::new();
        for e in tbl.iter()? {
            let (_, event_ref) = e?;
            let Ok(event) = serde_json::from_slice::<StoredRawWatchEvent<'_>>(event_ref.value())
            else {
                continue;
            };
            let rv = event.resource_version.unwrap_or_default();
            if rv <= since_rv {
                continue;
            }
            let ev_av = event.api_version.unwrap_or("");
            let ev_kind = event.kind.unwrap_or("");
            let ev_ns = event.namespace;
            if !targets
                .iter()
                .any(|target| watch_event_matches_target(target, ev_av, ev_kind, ev_ns))
            {
                continue;
            }

            result.push(RawWatchEvent {
                api_version: ev_av.to_string(),
                kind: ev_kind.to_string(),
                namespace: ev_ns.map(str::to_string),
                name: event.name.unwrap_or("").to_string(),
                resource_version: rv,
                event_type: std::borrow::Cow::Owned(event.event_type.unwrap_or("").to_string()),
                object_json: event.data.map_or_else(
                    || Bytes::from_static(b"null"),
                    |data| Bytes::copy_from_slice(data.get().as_bytes()),
                ),
            });
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }

    fn watch_list_in_read(
        read_txn: &::redb::ReadTransaction,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let tbl = read_txn.open_table(tables::WATCH_EVENTS)?;
        let mut result = Vec::new();
        for e in tbl.iter()? {
            let (_, event_ref) = e?;
            let body = event_ref.value().to_vec();
            let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let rv = event
                .get("resourceVersion")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            if rv <= since_rv {
                continue;
            }
            let ev_av = event
                .get("apiVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            let ev_ns = event.get("namespace").and_then(|v| v.as_str());
            if !targets
                .iter()
                .any(|target| watch_event_matches_target(target, ev_av, ev_kind, ev_ns))
            {
                continue;
            }

            let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
            let ev_type = event
                .get("eventType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
            result.push(CatchUpResource {
                resource: Resource {
                    id: 0,
                    api_version: ev_av.to_string(),
                    kind: ev_kind.to_string(),
                    namespace: ev_ns.map(|s| s.to_string()),
                    name: ev_name.to_string(),
                    uid: Resource::uid_from_data(&ev_data),
                    resource_version: rv,
                    data: std::sync::Arc::new(ev_data),
                },
                event_type: std::borrow::Cow::Owned(ev_type.to_string()),
            });
        }
        Ok(result)
    }

    pub async fn watch_list_deleted_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.db_call("watch_list_deleted_since", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::new();
            for e in tbl.iter()? {
                let (_, event_ref) = e?;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                if rv <= since_rv {
                    continue;
                }
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if ev_type != "DELETED" {
                    continue;
                }
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                result.push(CatchUpResource {
                    resource: Resource {
                        id: 0,
                        api_version: ev_av.to_string(),
                        kind: ev_kind.to_string(),
                        namespace: ev_ns.map(str::to_string),
                        name: ev_name.to_string(),
                        uid: Resource::uid_from_data(&ev_data),
                        resource_version: rv,
                        data: std::sync::Arc::new(ev_data),
                    },
                    event_type: std::borrow::Cow::Borrowed("DELETED"),
                });
            }
            Ok(result)
        })
        .await
    }

    pub async fn watch_list_all_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.db_call("watch_list_all_since", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::new();
            for e in tbl.iter()? {
                let (_, event_ref) = e?;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                if rv <= since_rv {
                    continue;
                }
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                result.push(CatchUpResource {
                    resource: Resource {
                        id: 0,
                        api_version: ev_av.to_string(),
                        kind: ev_kind.to_string(),
                        namespace: ev_ns.map(str::to_string),
                        name: ev_name.to_string(),
                        uid: Resource::uid_from_data(&ev_data),
                        resource_version: rv,
                        data: std::sync::Arc::new(ev_data),
                    },
                    event_type: std::borrow::Cow::Owned(ev_type.to_string()),
                });
            }
            Ok(result)
        })
        .await
    }

    pub async fn watch_list_all_since_paged(
        &self,
        since_rv: i64,
        _after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.db_call("watch_list_all_since_paged", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let limit = limit.get();
            let mut result = Vec::with_capacity(limit.min(4096));
            let start = after_id.saturating_add(1).max(0) as u64;
            for e in tbl.range(start..)? {
                let (id_guard, event_ref) = e?;
                let event_id = id_guard.value() as i64;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                if rv <= since_rv {
                    continue;
                }
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                result.push((
                    event_id,
                    CatchUpResource {
                        resource: Resource {
                            id: 0,
                            api_version: ev_av.to_string(),
                            kind: ev_kind.to_string(),
                            namespace: ev_ns.map(str::to_string),
                            name: ev_name.to_string(),
                            uid: Resource::uid_from_data(&ev_data),
                            resource_version: rv,
                            data: std::sync::Arc::new(ev_data),
                        },
                        event_type: std::borrow::Cow::Owned(ev_type.to_string()),
                    },
                ));
                if result.len() >= limit {
                    break;
                }
            }
            Ok(result)
        })
        .await
    }

    pub async fn watch_list_all_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        self.db_call("watch_list_all_after_id_bounded", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::with_capacity(limit.get().min(4096));
            let start = after_id.saturating_add(1).max(0) as u64;
            let end = through_id.max(0) as u64;
            for entry in tbl.range(start..=end)? {
                let (id, value) = entry?;
                let event: Value = serde_json::from_slice(value.value()).unwrap_or(Value::Null);
                let data = event.get("data").cloned().unwrap_or(Value::Null);
                result.push((
                    id.value() as i64,
                    CatchUpResource {
                        resource: Resource {
                            id: 0,
                            api_version: event
                                .get("apiVersion")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            kind: event
                                .get("kind")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            namespace: event
                                .get("namespace")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            name: event
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                            uid: Resource::uid_from_data(&data),
                            resource_version: event
                                .get("resourceVersion")
                                .and_then(Value::as_i64)
                                .unwrap_or_default(),
                            data: std::sync::Arc::new(data),
                        },
                        event_type: std::borrow::Cow::Owned(
                            event
                                .get("eventType")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                        ),
                    },
                ));
                if result.len() == limit.get() {
                    break;
                }
            }
            Ok(result)
        })
        .await
    }

    pub async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        self.db_call("list_watch_replay_floors", |db| {
            let read = db.begin_read()?;
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut floors = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                let mut parts = key.value().splitn(3, |byte| *byte == 0);
                let (Some(api_version), Some(kind), Some(namespace_key)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    continue;
                };
                let Some((floor_resource_version, floor_event_id)) =
                    decode_position_floor(value.value())
                else {
                    continue;
                };
                floors.push(WatchReplayFloor {
                    api_version: String::from_utf8_lossy(api_version).into_owned(),
                    kind: String::from_utf8_lossy(kind).into_owned(),
                    namespace_key: String::from_utf8_lossy(namespace_key).into_owned(),
                    floor_resource_version: floor_resource_version as i64,
                    floor_event_id: floor_event_id as i64,
                    position_is_exact: true,
                });
            }
            floors.sort_by(|left, right| {
                (&left.api_version, &left.kind, &left.namespace_key).cmp(&(
                    &right.api_version,
                    &right.kind,
                    &right.namespace_key,
                ))
            });
            Ok(floors)
        })
        .await
    }

    pub async fn modified_since(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let scope = if ns.is_some() {
            WatchTargetScope::Namespaced(ns.map(|s| s.to_string()))
        } else {
            WatchTargetScope::Cluster
        };
        let targets = vec![WatchTarget {
            api_version: av.to_string(),
            kind: kind.to_string(),
            scope,
        }];
        self.watch_list(&targets, since_rv).await
    }

    pub async fn gc_watch(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.db_call("gc_watch", move |db| {
            let w = db.begin_write()?;
            let entries: Vec<(u64, u64, Option<Vec<u8>>)> = {
                let tbl = w.open_table(tables::WATCH_EVENTS)?;
                let mut entries = Vec::new();
                for entry in tbl.iter()? {
                    let (key, event_ref) = entry?;
                    let event_id = key.value();
                    let body = event_ref.value().to_vec();
                    let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let rv = event
                        .get("resourceVersion")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    entries.push((event_id, rv, floor_key_for_event(&event)));
                }
                entries
            };
            let candidates = watch_gc_candidates(&entries, max_rows, batch_cap);
            if candidates.is_empty() {
                w.commit()?;
                return Ok(0);
            }

            let mut keys_to_remove = Vec::with_capacity(candidates.len());
            let mut floor_updates: BTreeMap<Vec<u8>, (u64, u64)> = BTreeMap::new();
            for (event_id, rv, floor_key) in candidates {
                if let Some(key) = floor_key {
                    floor_updates
                        .entry(key)
                        .and_modify(|floor| {
                            floor.0 = floor.0.max(rv);
                            floor.1 = floor.1.max(event_id);
                        })
                        .or_insert((rv, event_id));
                }
                keys_to_remove.push(event_id);
            }

            {
                let mut rv_floors = w.open_table(tables::WATCH_REPLAY_FLOORS)?;
                let mut position_floors = w.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
                for (key, (floor_rv, floor_event_id)) in floor_updates {
                    let existing = rv_floors.get(key.as_slice())?.map(|guard| guard.value());
                    if existing.is_none_or(|current| floor_rv > current) {
                        rv_floors.insert(key.as_slice(), floor_rv)?;
                    }
                    let (floor_rv, floor_event_id) = position_floors
                        .get(key.as_slice())?
                        .and_then(|value| decode_position_floor(value.value()))
                        .map_or((floor_rv, floor_event_id), |existing| {
                            (existing.0.max(floor_rv), existing.1.max(floor_event_id))
                        });
                    let encoded = encode_position_floor(floor_rv, floor_event_id);
                    position_floors.insert(key.as_slice(), encoded.as_slice())?;
                }
            }
            let removed = {
                let mut tbl2 = w.open_table(tables::WATCH_EVENTS)?;
                let n = keys_to_remove.len();
                for k in &keys_to_remove {
                    tbl2.remove(*k)?;
                }
                n
            };
            w.commit()?;
            Ok(removed)
        })
        .await
    }

    pub async fn gc_watch_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.db_call("gc_watch_prunable_count", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut entries = Vec::new();
            for entry in tbl.iter()? {
                let (key, event_ref) = entry?;
                let event_id = key.value();
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                entries.push((event_id, rv, floor_key_for_event(&event)));
            }
            Ok(watch_gc_candidates(&entries, max_rows, batch_cap).len())
        })
        .await
    }
}

fn watch_gc_candidates(
    entries: &[(u64, u64, Option<Vec<u8>>)],
    max_rows: i64,
    batch_cap: i64,
) -> Vec<(u64, u64, Option<Vec<u8>>)> {
    let max_rows = watch_events_max_rows(max_rows);
    if entries.len() <= max_rows {
        return Vec::new();
    }

    let global_prunable = entries.len() - max_rows;
    let batch_cap = watch_events_batch_cap(batch_cap);
    let mut scope_totals: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for (_, _, floor_key) in entries {
        if let Some(key) = floor_key {
            *scope_totals.entry(key.clone()).or_default() += 1;
        }
    }
    let min_scope_rows = watch_events_min_scope_rows(max_rows as i64, scope_totals.len());

    let mut seen_by_scope: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let mut candidates = Vec::new();
    for (idx, (event_id, rv, floor_key)) in entries.iter().enumerate() {
        if idx >= global_prunable || candidates.len() >= batch_cap {
            break;
        }

        if let Some(key) = floor_key {
            let seen = seen_by_scope.entry(key.clone()).or_default();
            let scope_rank = scope_totals.get(key).copied().unwrap_or(0) - *seen;
            *seen += 1;
            if scope_rank <= min_scope_rows {
                continue;
            }
        }

        candidates.push((*event_id, *rv, floor_key.clone()));
    }
    candidates
}

fn watch_event_matches_target(
    target: &WatchTarget,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> bool {
    if target.api_version != api_version || target.kind != kind {
        return false;
    }
    match &target.scope {
        WatchTargetScope::Cluster => namespace.is_none(),
        WatchTargetScope::Namespaced(Some(want)) => namespace == Some(want.as_str()),
        WatchTargetScope::Namespaced(None) => namespace.is_some(),
    }
}

fn target_floor(read_txn: &::redb::ReadTransaction, target: &WatchTarget) -> Result<Option<i64>> {
    let scoped = match &target.scope {
        WatchTargetScope::Cluster => read_floor(
            read_txn,
            &target.api_version,
            &target.kind,
            CLUSTER_NAMESPACE_KEY,
        ),
        WatchTargetScope::Namespaced(Some(namespace)) => {
            read_floor(read_txn, &target.api_version, &target.kind, namespace)
        }
        WatchTargetScope::Namespaced(None) => {
            read_namespaced_all_floor(read_txn, &target.api_version, &target.kind)
        }
    }?;
    Ok(super::replay_floor::LegacyReplayFloor::read(read_txn)?
        .and_then(|legacy| legacy.merge_resource_version(scoped))
        .or(scoped))
}

fn position_expired_for_targets(
    read_txn: &::redb::ReadTransaction,
    targets: &[WatchTarget],
    event_id: i64,
) -> Result<bool> {
    for target in targets {
        let scoped = match &target.scope {
            WatchTargetScope::Cluster => read_position_floor(
                read_txn,
                &target.api_version,
                &target.kind,
                CLUSTER_NAMESPACE_KEY,
            )?,
            WatchTargetScope::Namespaced(Some(namespace)) => {
                read_position_floor(read_txn, &target.api_version, &target.kind, namespace)?
            }
            WatchTargetScope::Namespaced(None) => {
                read_namespaced_all_position_floor(read_txn, &target.api_version, &target.kind)?
            }
        };
        let floor = super::replay_floor::LegacyReplayFloor::read(read_txn)?
            .and_then(|legacy| legacy.merge_event_id(scoped))
            .or(scoped);
        if floor.is_some_and(|floor| event_id < floor) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn encode_position_floor(resource_version: u64, event_id: u64) -> [u8; 16] {
    let mut encoded = [0_u8; 16];
    encoded[..8].copy_from_slice(&resource_version.to_be_bytes());
    encoded[8..].copy_from_slice(&event_id.to_be_bytes());
    encoded
}

fn decode_position_floor(encoded: &[u8]) -> Option<(u64, u64)> {
    if encoded.len() != 16 {
        return None;
    }
    Some((
        u64::from_be_bytes(encoded[..8].try_into().ok()?),
        u64::from_be_bytes(encoded[8..].try_into().ok()?),
    ))
}

fn read_position_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    namespace_key: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
    let key = floor_key(api_version, kind, namespace_key);
    Ok(floors
        .get(key.as_slice())?
        .and_then(|floor| decode_position_floor(floor.value()).map(|(_, id)| id as i64)))
}

fn read_namespaced_all_position_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
    let prefix = floor_key_prefix(api_version, kind);
    let mut floor = None;
    for entry in floors.iter()? {
        let (key, value) = entry?;
        let Some(namespace_key) = key.value().strip_prefix(prefix.as_slice()) else {
            continue;
        };
        if namespace_key.is_empty() || namespace_key == CLUSTER_NAMESPACE_KEY.as_bytes() {
            continue;
        }
        let Some((_, event_id)) = decode_position_floor(value.value()) else {
            continue;
        };
        floor = Some(floor.map_or(event_id as i64, |current: i64| current.max(event_id as i64)));
    }
    Ok(floor)
}

fn read_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    namespace_key: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let key = floor_key(api_version, kind, namespace_key);
    Ok(floors
        .get(key.as_slice())?
        .map(|floor| floor.value() as i64))
}

fn read_namespaced_all_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let prefix = floor_key_prefix(api_version, kind);
    let mut floor = None;
    for entry in floors.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        let namespace_key = match key.strip_prefix(prefix.as_slice()) {
            Some(namespace_key) if namespace_key != CLUSTER_NAMESPACE_KEY.as_bytes() => {
                namespace_key
            }
            _ => continue,
        };
        if namespace_key.is_empty() {
            continue;
        }
        let rv = value.value() as i64;
        floor = Some(floor.map_or(rv, |current: i64| current.max(rv)));
    }
    Ok(floor)
}

fn floor_key_for_event(event: &Value) -> Option<Vec<u8>> {
    let api_version = event.get("apiVersion").and_then(|value| value.as_str())?;
    let kind = event.get("kind").and_then(|value| value.as_str())?;
    let namespace_key = event
        .get("namespace")
        .and_then(|value| value.as_str())
        .unwrap_or(CLUSTER_NAMESPACE_KEY);
    Some(floor_key(api_version, kind, namespace_key))
}

fn floor_key(api_version: &str, kind: &str, namespace_key: &str) -> Vec<u8> {
    let mut key = floor_key_prefix(api_version, kind);
    key.extend_from_slice(namespace_key.as_bytes());
    key
}

fn floor_key_prefix(api_version: &str, kind: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(api_version.len() + kind.len() + 2);
    key.extend_from_slice(api_version.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    key
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::helpers;
    use crate::datastore::redb::open_boundary;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> RedbWatchStore {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbWatchStore::new(accessor)
    }

    fn insert_watch_event(
        s: &RedbWatchStore,
        rv: i64,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        event_type: &str,
    ) {
        let ev = serde_json::json!({"apiVersion":av,"kind":kind,"namespace":ns,"name":name,"eventType":event_type,"data":{}});
        let db = s.accessor.db().unwrap();
        let w = db.begin_write().unwrap();
        helpers::watch_insert(&w, rv, &ev).unwrap();
        w.commit().unwrap();
    }

    #[tokio::test]
    async fn watch_list_filters_by_target() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "p", "ADDED");
        insert_watch_event(&s, 2, "v1", "ConfigMap", Some("ns"), "cm", "ADDED");

        let targets = vec![WatchTarget {
            api_version: "v1".into(),
            kind: "Pod".into(),
            scope: WatchTargetScope::Namespaced(Some("ns".into())),
        }];
        let results = s.watch_list(&targets, 0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "p");
    }

    #[tokio::test]
    async fn watch_list_deleted_only_returns_deleted() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "p", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "p", "DELETED");
        insert_watch_event(&s, 3, "v1", "Pod", Some("ns"), "q", "MODIFIED");

        let results = s.watch_list_deleted_since(0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "p");
    }

    #[tokio::test]
    async fn watch_list_respects_since_rv() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "old", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "new", "ADDED");

        let results = s.watch_list_all_since(1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "new");
    }

    #[tokio::test]
    async fn watch_list_all_since_paged_keysets_across_resource_versions() {
        let s = store();
        for i in 1..=5 {
            insert_watch_event(&s, i, "v1", "Pod", Some("ns"), &format!("p{i}"), "ADDED");
        }
        let limit = std::num::NonZeroUsize::new(2).unwrap();

        let page1 = s.watch_list_all_since_paged(1, 0, 0, limit).await.unwrap();
        assert_eq!(
            page1
                .iter()
                .map(|(_, event)| event.resource.resource_version)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            page1.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let last = page1.last().unwrap();
        let page2 = s
            .watch_list_all_since_paged(1, last.1.resource.resource_version, last.0, limit)
            .await
            .unwrap();
        assert_eq!(
            page2
                .iter()
                .map(|(_, event)| event.resource.resource_version)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert_eq!(
            page2.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![4, 5]
        );
    }

    #[tokio::test]
    async fn positioned_replay_preserves_one_hundred_same_revision_siblings() {
        let s = store();
        for index in 0..100 {
            insert_watch_event(
                &s,
                7,
                "v1",
                "Pod",
                Some("ns"),
                &format!("same-rv-{index}"),
                "ADDED",
            );
        }
        let targets = [WatchTarget::namespaced_in_namespace("v1", "Pod", "ns")];
        let limit = std::num::NonZeroUsize::new(3).unwrap();
        let mut position = WatchReplayPosition::from_resource_version(6);
        let mut delivered = Vec::new();
        loop {
            let PositionedWatchReplayRead::Events(replay) = s
                .watch_list_positioned_checked_bounded(&targets, position, limit)
                .await
                .unwrap()
            else {
                panic!("fresh redb replay must not expire");
            };
            position = replay.next_position;
            let count = replay.events.len();
            delivered.extend(
                replay
                    .events
                    .into_iter()
                    .map(|event| event.event.resource.name),
            );
            if count < limit.get() {
                break;
            }
        }
        assert_eq!(delivered.len(), 100);
        assert_eq!(
            delivered
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            100
        );
    }

    #[tokio::test]
    async fn positioned_replay_gc_expiry_is_scope_specific() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Secret", Some("seed"), "anchor", "ADDED");
        let target = [WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "target",
        )];
        let quiet = [WatchTarget::namespaced_in_namespace(
            "v1", "Secret", "quiet",
        )];
        let limit = std::num::NonZeroUsize::new(3).unwrap();
        let PositionedWatchReplayRead::Events(target_start) = s
            .watch_list_positioned_checked_bounded(
                &target,
                WatchReplayPosition::from_resource_version(1),
                limit,
            )
            .await
            .unwrap()
        else {
            panic!("fresh target position must not expire");
        };
        let PositionedWatchReplayRead::Events(quiet_start) = s
            .watch_list_positioned_checked_bounded(
                &quiet,
                WatchReplayPosition::from_resource_version(1),
                limit,
            )
            .await
            .unwrap()
        else {
            panic!("fresh quiet position must not expire");
        };
        for index in 0..5 {
            insert_watch_event(
                &s,
                2 + index,
                "v1",
                "ConfigMap",
                Some("target"),
                &format!("item-{index}"),
                "ADDED",
            );
        }
        s.gc_watch(1, 100).await.unwrap();

        assert!(matches!(
            s.watch_list_positioned_checked_bounded(&target, target_start.next_position, limit,)
                .await
                .unwrap(),
            PositionedWatchReplayRead::Expired
        ));
        assert!(matches!(
            s.watch_list_positioned_checked_bounded(&quiet, quiet_start.next_position, limit,)
                .await
                .unwrap(),
            PositionedWatchReplayRead::Events(_)
        ));
    }

    #[tokio::test]
    async fn legacy_wildcard_floor_expires_unknown_positioned_scope() {
        let s = store();
        for rv in 1..=5 {
            insert_watch_event(
                &s,
                rv,
                "v1",
                "Secret",
                Some("seed"),
                &format!("seed-{rv}"),
                "ADDED",
            );
        }
        let db = s.accessor.db().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut rv_floors = write.open_table(tables::WATCH_REPLAY_FLOORS).unwrap();
            let mut position_floors = write
                .open_table(tables::WATCH_REPLAY_POSITION_FLOORS)
                .unwrap();
            let wildcard = floor_key("*", "*", "*");
            rv_floors.insert(wildcard.as_slice(), 5).unwrap();
            position_floors
                .insert(wildcard.as_slice(), encode_position_floor(5, 5).as_slice())
                .unwrap();
        }
        write.commit().unwrap();

        let target = [WatchTarget::namespaced_in_namespace(
            "example.test/v1",
            "Unknown",
            "default",
        )];
        assert!(matches!(
            s.watch_list_positioned_checked_bounded(
                &target,
                WatchReplayPosition {
                    resource_version: 4,
                    event_id: 4,
                    resource_version_filter_through_event_id: 0,
                },
                std::num::NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap(),
            PositionedWatchReplayRead::Expired
        ));
    }

    #[tokio::test]
    async fn gc_watch_trims_oldest() {
        let s = store();
        for i in 1..=10 {
            insert_watch_event(&s, i, "v1", "Pod", Some("ns"), &format!("p{i}"), "ADDED");
        }
        let removed = s.gc_watch(3, 100).await.unwrap();
        assert_eq!(removed, 7);
        // After GC, only 3 remain
        let all = s.watch_list_all_since(0).await.unwrap();
        assert_eq!(all.len(), 3);
    }
}
