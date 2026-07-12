use super::queries;
use crate::datastore::replay_retention::ReplayRetentionBoundary;
use crate::datastore::{WatchTarget, WatchTargetScope};

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";
const LEGACY_WILDCARD: (&str, &str, &str) = ("*", "*", "*");

/// Authoritative retained-history boundary for one watch target.
///
/// Both scalar and positioned replay, including membership reconstruction,
/// must consult this value object so a restored legacy wildcard cannot be
/// honored by one replay path and ignored by another.
pub(super) fn target_replay_boundaries(
    conn: &rusqlite::Connection,
    target: &WatchTarget,
) -> rusqlite::Result<Vec<ReplayRetentionBoundary>> {
    let mut boundaries = match &target.scope {
        WatchTargetScope::Cluster => read_scope(
            conn,
            &target.api_version,
            &target.kind,
            CLUSTER_NAMESPACE_KEY,
        ),
        WatchTargetScope::Namespaced(Some(namespace)) => {
            read_scope(conn, &target.api_version, &target.kind, namespace)
        }
        WatchTargetScope::Namespaced(None) => {
            read_namespaced_all(conn, &target.api_version, &target.kind)
        }
    }?;
    boundaries.extend(read_scope(
        conn,
        LEGACY_WILDCARD.0,
        LEGACY_WILDCARD.1,
        LEGACY_WILDCARD.2,
    )?);
    Ok(boundaries)
}

fn read_scope(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace_key: &str,
) -> rusqlite::Result<Vec<ReplayRetentionBoundary>> {
    let mut stmt = conn.prepare(queries::WATCH_REPLAY_RETENTION_FLOOR_FOR_SCOPE)?;
    let rows = stmt.query_map(rusqlite::params![api_version, kind, namespace_key], |row| {
        row_to_boundary(row)
    })?;
    rows.collect()
}

fn read_namespaced_all(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
) -> rusqlite::Result<Vec<ReplayRetentionBoundary>> {
    let mut stmt = conn.prepare(queries::WATCH_REPLAY_RETENTION_FLOOR_FOR_NAMESPACED_ALL)?;
    let rows = stmt.query_map(rusqlite::params![api_version, kind], row_to_boundary)?;
    rows.collect()
}

fn row_to_boundary(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReplayRetentionBoundary> {
    let resource_version = row.get(0)?;
    let event_id = row.get(1)?;
    let position_is_exact: bool = row.get(2)?;
    Ok(if position_is_exact {
        ReplayRetentionBoundary::Exact(crate::datastore::WatchReplayPosition {
            resource_version,
            event_id,
            resource_version_filter_through_event_id: 0,
        })
    } else {
        ReplayRetentionBoundary::LegacyRvOnly { resource_version }
    })
}
