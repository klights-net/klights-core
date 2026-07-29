use super::super::crud::helpers::{
    WatchEventInsert, WatchEventPayload, insert_watch_event_in_conn,
};
use super::super::{create_staged_post_commit, queries};
use klights_cluster_core::LogApplyWatchEventRow;
use std::collections::HashMap;

const DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE: i64 = 1_024;
const MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES: i64 = 16;

pub(crate) fn watch_events_min_scope_rows(max_rows: i64) -> i64 {
    max_rows.clamp(1, DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE)
}

pub(crate) fn watch_events_min_scope_rows_for_scope_count(max_rows: i64, scope_count: i64) -> i64 {
    if max_rows <= 0 || scope_count <= 0 {
        return 0;
    }
    let fair_share = max_rows / scope_count;
    let dynamic_floor = if fair_share == 0 && scope_count <= MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES
    {
        1
    } else {
        fair_share
    };
    watch_events_min_scope_rows(max_rows).min(dynamic_floor)
}

pub(crate) fn watch_events_min_scope_rows_in_conn(
    conn: &rusqlite::Connection,
    max_rows: i64,
) -> rusqlite::Result<i64> {
    let scope_count =
        conn.query_row::<i64, _, _>(queries::WATCH_EVENTS_SCOPE_COUNT, [], |row| row.get(0))?;
    Ok(watch_events_min_scope_rows_for_scope_count(
        max_rows,
        scope_count,
    ))
}

pub(crate) fn gc_watch_events_in_tx(
    tx: &rusqlite::Transaction<'_>,
    max_rows: i64,
    batch_cap: i64,
) -> rusqlite::Result<usize> {
    let (ids, floors) = {
        let min_scope_rows = watch_events_min_scope_rows_in_conn(tx, max_rows)?;
        let mut stmt = tx.prepare(queries::WATCH_EVENTS_GC_CANDIDATES)?;
        let rows = stmt.query_map(
            rusqlite::params![max_rows, batch_cap, min_scope_rows],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )?;

        let mut ids = Vec::new();
        let mut floors: HashMap<(String, String, String), (i64, i64)> = HashMap::new();
        for row in rows {
            let (id, api_version, kind, namespace_key, resource_version) = row?;
            ids.push(id);
            floors
                .entry((api_version, kind, namespace_key))
                .and_modify(|floor| {
                    floor.0 = floor.0.max(resource_version);
                    floor.1 = floor.1.max(id);
                })
                .or_insert((resource_version, id));
        }
        (ids, floors)
    };

    for ((api_version, kind, namespace_key), (floor_rv, floor_event_id)) in floors {
        tx.execute(
            queries::WATCH_REPLAY_FLOOR_UPSERT,
            rusqlite::params![api_version, kind, namespace_key, floor_rv, floor_event_id],
        )?;
    }

    if ids.is_empty() {
        return Ok(0);
    }

    let mut delete = String::from("DELETE FROM watch_events WHERE id IN (");
    delete.push_str(
        &std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(","),
    );
    delete.push(')');
    tx.execute(&delete, rusqlite::params_from_iter(ids.iter()))
}

pub(super) struct WatchHistoryStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> WatchHistoryStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(super) fn apply_put_watch_event(
        &self,
        row: LogApplyWatchEventRow,
    ) -> tokio_rusqlite::Result<klights_cluster_store::StagedPostCommit> {
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        insert_watch_event_in_conn(
            self.tx,
            WatchEventInsert::preserve_committed_payload(
                row.event_id,
                WatchEventPayload {
                    api_version: &row.api_version,
                    kind: &row.kind,
                    namespace: row.namespace.as_deref(),
                    name: &row.name,
                    resource_version: row.resource_version,
                    event_type: &row.event_type,
                    data: &data_bytes,
                },
            ),
        )?;
        Ok(create_staged_post_commit(
            &row.api_version,
            &row.kind,
            row.namespace.as_deref(),
            &row.name,
            row.resource_version,
            &row.event_type,
            row.data,
        ))
    }

    pub(super) fn apply_gc_watch_events(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> tokio_rusqlite::Result<()> {
        let removed = gc_watch_events_in_tx(self.tx, max_rows, batch_cap)?;
        if removed > 0 {
            let _ = self.tx.execute("PRAGMA incremental_vacuum(1000)", []);
        }
        Ok(())
    }
}
