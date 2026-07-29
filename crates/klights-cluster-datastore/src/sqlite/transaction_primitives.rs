//! Root-independent SQLite transaction primitives shared by serial Phase 10 packets.

use super::mutation_queries as queries;
use rusqlite::OptionalExtension;

pub fn watch_event_allocator_high_water(conn: &rusqlite::Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
        [],
        |row| row.get(0),
    )
}

pub fn set_watch_event_allocator(
    conn: &rusqlite::Connection,
    high_water: i64,
) -> rusqlite::Result<()> {
    let updated = conn.execute(
        "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'watch_events'",
        rusqlite::params![high_water],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES ('watch_events', ?1)",
            rusqlite::params![high_water],
        )?;
    }
    Ok(())
}

pub fn next_resource_version_in_tx(conn: &rusqlite::Connection) -> rusqlite::Result<i64> {
    conn.execute(queries::METADATA_INCREMENT_RV, [])?;
    conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
}

pub fn current_resource_version(conn: &rusqlite::Connection) -> rusqlite::Result<i64> {
    conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
}

pub fn advance_resource_version_after(
    conn: &rusqlite::Connection,
    min_rv: i64,
) -> rusqlite::Result<i64> {
    let current = current_resource_version(conn)?;
    let next = current.saturating_add(1).max(min_rv.saturating_add(1));
    conn.execute(queries::METADATA_SET_RV, [next.to_string()])?;
    Ok(next)
}

pub fn resource_snapshot_for_key_at_rv(
    tx: &rusqlite::Transaction<'_>,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> tokio_rusqlite::Result<Option<serde_json::Value>> {
    let earliest: Option<i64> = tx
        .query_row(queries::WATCH_EVENTS_MIN_RV, [], |row| row.get(0))
        .optional()?;
    match earliest {
        Some(earliest) if resource_version + 1 >= earliest => {}
        _ => return Ok(None),
    }

    let namespace_key = namespace.unwrap_or("#cluster");
    let row: Option<(String, Vec<u8>)> = tx
        .query_row(
            "SELECT event_type, data FROM watch_events \
             WHERE api_version = ?1 \
               AND kind = ?2 \
               AND COALESCE(namespace, '#cluster') = ?3 \
               AND name = ?4 \
               AND resource_version <= ?5 \
             ORDER BY resource_version DESC, id DESC \
             LIMIT 1",
            rusqlite::params![api_version, kind, namespace_key, name, resource_version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((event_type, bytes)) = row else {
        return Ok(None);
    };
    if event_type == "DELETED" {
        return Ok(None);
    }
    serde_json::from_slice(&bytes).map(Some).map_err(|error| {
        tokio_rusqlite::Error::Rusqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })
}
