use std::sync::Arc;

use anyhow::{Result, anyhow};
use rusqlite::ToSql;
use serde_json::Value;

use super::{Datastore, Resource, ResourceList, SnapshotAtRv, WatchReplayPosition, WatchTarget};
use crate::datastore::WatchTargetScope;
use crate::datastore::position_membership::{
    MembershipHistoryEvent, MembershipReconstructor, ReconstructedMembership,
    apply_membership_selectors, resource_from_history, sort_for_watch_targets,
};

impl Datastore {
    pub async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let sort_targets = targets.to_vec();
        let targets = sort_targets.clone();
        let label_selector = label_selector.map(str::to_string);
        let field_selector = field_selector.map(str::to_string);
        let raw = self
            .read_db_call("snapshot_resources_at_position", move |conn| {
                let tx = conn.transaction()?;
                let current_position = Self::current_watch_replay_position_in_tx(&tx)?;
                if position.event_id > current_position.event_id
                    || position.resource_version_filter_through_event_id > current_position.event_id
                    || (position.event_id == 0
                        && position.resource_version_filter_through_event_id == 0
                        && position.resource_version > current_position.resource_version)
                {
                    return Ok(PositionRawSnapshot::Expired);
                }
                if cursor_covers_current(position, current_position) {
                    return Ok(PositionRawSnapshot::Items(read_current_targets(
                        &tx, &targets,
                    )?));
                }
                if position_expired(&tx, &targets, position)? {
                    return Ok(PositionRawSnapshot::Expired);
                }

                let current = read_current_targets(&tx, &targets)?;
                Ok(
                    match reconstruct_in_conn(&tx, &targets, current, position)? {
                        ReconstructedMembership::Expired => PositionRawSnapshot::Expired,
                        ReconstructedMembership::Items(items) => PositionRawSnapshot::Items(items),
                    },
                )
            })
            .await
            .map_err(|error| anyhow!("failed to reconstruct watch-position snapshot: {error}"))?;

        let mut items = match raw {
            PositionRawSnapshot::Expired => return Ok(SnapshotAtRv::Expired),
            PositionRawSnapshot::Items(items) => apply_membership_selectors(
                items,
                label_selector.as_deref(),
                field_selector.as_deref(),
            )?,
        };
        sort_for_watch_targets(&mut items, &sort_targets);
        Ok(SnapshotAtRv::List(ResourceList {
            items,
            resource_version: position.resource_version,
            watch_replay_position: Some(position),
            continue_token: None,
            remaining_item_count: None,
        }))
    }
}

enum PositionRawSnapshot {
    Expired,
    Items(Vec<Resource>),
}

fn cursor_covers_current(position: WatchReplayPosition, current: WatchReplayPosition) -> bool {
    position.event_id >= current.event_id
        || (position.resource_version_filter_through_event_id >= current.event_id
            && position.resource_version >= current.resource_version)
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version >= current.resource_version)
}

fn position_expired(
    conn: &rusqlite::Connection,
    targets: &[WatchTarget],
    position: WatchReplayPosition,
) -> rusqlite::Result<bool> {
    for target in targets {
        if crate::datastore::replay_retention::ReplayRetentionBoundary::classify_all(
            super::replay_floor::target_replay_boundaries(conn, target)?,
            position,
        ) == crate::datastore::replay_retention::ReplayAvailability::Expired
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_current_targets(
    conn: &rusqlite::Connection,
    targets: &[WatchTarget],
) -> rusqlite::Result<Vec<Resource>> {
    let mut resources = Vec::new();
    for target in targets {
        if target.api_version == "v1"
            && target.kind == "Namespace"
            && matches!(&target.scope, WatchTargetScope::Cluster)
        {
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data FROM namespaces ORDER BY name",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(Resource {
                    id: 0,
                    api_version: "v1".into(),
                    kind: "Namespace".into(),
                    namespace: None,
                    name: row.get(0)?,
                    uid: row.get(1)?,
                    resource_version: row.get(2)?,
                    data: Arc::new(json_from_bytes(row.get(3)?)?),
                })
            })?;
            resources.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
            continue;
        }
        let (mut sql, namespace) = match &target.scope {
            WatchTargetScope::Cluster => (
                "SELECT id, api_version, kind, NULL, name, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2".to_string(),
                None,
            ),
            WatchTargetScope::Namespaced(namespace) => {
                let mut sql = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2".to_string();
                if namespace.is_some() {
                    sql.push_str(" AND namespace = ?3");
                }
                (sql, namespace.as_deref())
            }
        };
        sql.push_str(" ORDER BY 4, 5");
        let mut stmt = conn.prepare(&sql)?;
        let mut params: Vec<Box<dyn ToSql>> = vec![
            Box::new(target.api_version.clone()),
            Box::new(target.kind.clone()),
        ];
        if let Some(namespace) = namespace {
            params.push(Box::new(namespace.to_string()));
        }
        let refs = params
            .iter()
            .map(|param| param.as_ref())
            .collect::<Vec<_>>();
        let rows = stmt.query_map(refs.as_slice(), |row| {
            Ok(Resource {
                id: row.get(0)?,
                api_version: row.get(1)?,
                kind: row.get(2)?,
                namespace: row.get(3)?,
                name: row.get(4)?,
                resource_version: row.get(5)?,
                uid: row.get(6)?,
                data: Arc::new(json_from_bytes(row.get(7)?)?),
            })
        })?;
        resources.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(resources)
}

fn reconstruct_in_conn(
    conn: &rusqlite::Connection,
    targets: &[WatchTarget],
    current: Vec<Resource>,
    position: WatchReplayPosition,
) -> rusqlite::Result<ReconstructedMembership> {
    if targets.is_empty() {
        return Ok(ReconstructedMembership::Items(Vec::new()));
    }
    let mut sql = "SELECT id, api_version, kind, namespace, name, resource_version, event_type, data FROM watch_events WHERE (".to_string();
    let mut params = Vec::<Box<dyn ToSql>>::new();
    for (index, target) in targets.iter().enumerate() {
        if index > 0 {
            sql.push_str(" OR ");
        }
        sql.push_str(&format!(
            "(api_version = ?{} AND kind = ?{}",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(Box::new(target.api_version.clone()));
        params.push(Box::new(target.kind.clone()));
        match &target.scope {
            WatchTargetScope::Cluster => sql.push_str(" AND namespace IS NULL"),
            WatchTargetScope::Namespaced(Some(namespace)) => {
                sql.push_str(&format!(" AND namespace = ?{}", params.len() + 1));
                params.push(Box::new(namespace.clone()));
            }
            WatchTargetScope::Namespaced(None) => sql.push_str(" AND namespace IS NOT NULL"),
        }
        sql.push(')');
    }
    sql.push_str(") ORDER BY id DESC");
    let refs = params
        .iter()
        .map(|param| param.as_ref())
        .collect::<Vec<_>>();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(refs.as_slice(), |row| {
        Ok(MembershipHistoryEvent {
            event_id: row.get(0)?,
            event_type: row.get(6)?,
            resource: resource_from_history(
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                json_from_bytes(row.get(7)?)?,
            ),
        })
    })?;
    let mut reconstructor = MembershipReconstructor::new(current, position);
    for row in rows {
        let event = row?;
        if reconstructor.can_stop_before(event.event_id) {
            break;
        }
        reconstructor.observe(&event);
    }
    Ok(reconstructor.finish())
}

fn json_from_bytes(bytes: Vec<u8>) -> rusqlite::Result<Value> {
    serde_json::from_slice(&bytes)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}
