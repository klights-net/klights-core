use rusqlite::OptionalExtension;

use super::queries;
use crate::datastore::{WatchTarget, WatchTargetScope};

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";
const LEGACY_WILDCARD: (&str, &str, &str) = ("*", "*", "*");

/// Authoritative retained-history boundary for one watch target.
///
/// Both scalar and positioned replay, including membership reconstruction,
/// must consult this value object so a restored legacy wildcard cannot be
/// honored by one replay path and ignored by another.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ReplayRetentionFloor {
    pub resource_version: i64,
    pub event_id: i64,
}

impl ReplayRetentionFloor {
    fn combine(self, other: Self) -> Self {
        Self {
            resource_version: self.resource_version.max(other.resource_version),
            event_id: self.event_id.max(other.event_id),
        }
    }
}

pub(super) fn target_replay_floor(
    conn: &rusqlite::Connection,
    target: &WatchTarget,
) -> rusqlite::Result<Option<ReplayRetentionFloor>> {
    let scoped = match &target.scope {
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
    let wildcard = read_scope(
        conn,
        LEGACY_WILDCARD.0,
        LEGACY_WILDCARD.1,
        LEGACY_WILDCARD.2,
    )?;
    Ok(match (scoped, wildcard) {
        (Some(scoped), Some(wildcard)) => Some(scoped.combine(wildcard)),
        (scoped, wildcard) => scoped.or(wildcard),
    })
}

fn read_scope(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace_key: &str,
) -> rusqlite::Result<Option<ReplayRetentionFloor>> {
    conn.query_row(
        queries::WATCH_REPLAY_RETENTION_FLOOR_FOR_SCOPE,
        rusqlite::params![api_version, kind, namespace_key],
        |row| {
            Ok(ReplayRetentionFloor {
                resource_version: row.get(0)?,
                event_id: row.get(1)?,
            })
        },
    )
    .optional()
}

fn read_namespaced_all(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
) -> rusqlite::Result<Option<ReplayRetentionFloor>> {
    conn.query_row(
        queries::WATCH_REPLAY_RETENTION_FLOOR_FOR_NAMESPACED_ALL,
        rusqlite::params![api_version, kind],
        |row| {
            let resource_version = row.get::<_, Option<i64>>(0)?;
            let event_id = row.get::<_, Option<i64>>(1)?;
            Ok(resource_version
                .zip(event_id)
                .map(|(resource_version, event_id)| ReplayRetentionFloor {
                    resource_version,
                    event_id,
                }))
        },
    )
}
