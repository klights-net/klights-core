use super::Datastore;
use super::queries;

impl Datastore {
    pub(super) fn watch_event_allocator_high_water_in_conn(
        conn: &rusqlite::Connection,
    ) -> rusqlite::Result<i64> {
        conn.query_row(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
            [],
            |row| row.get(0),
        )
    }

    /// Set the watch allocator to an authoritative snapshot boundary.
    /// Unlike the upgrade helper above, replacement must be allowed to move a
    /// divergent follower's local sequence back to the leader's exact value.
    pub(super) fn set_watch_event_allocator_in_conn(
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

    pub(super) fn next_resource_version_in_conn(
        conn: &rusqlite::Connection,
    ) -> rusqlite::Result<i64> {
        conn.execute(queries::METADATA_INCREMENT_RV, [])?;
        conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn next_resource_version_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<i64> {
        tx.execute(queries::METADATA_INCREMENT_RV, [])?;
        tx.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn current_resource_version_in_conn(
        conn: &rusqlite::Connection,
    ) -> rusqlite::Result<i64> {
        conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn current_resource_version_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<i64> {
        tx.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))
    }

    pub(super) fn current_watch_replay_position_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<crate::datastore::WatchReplayPosition> {
        let resource_version = Self::current_resource_version_in_tx(tx)?;
        let event_id = Self::watch_event_allocator_high_water_in_conn(tx)?;
        Ok(crate::datastore::WatchReplayPosition {
            resource_version,
            event_id,
            resource_version_filter_through_event_id: 0,
        })
    }

    pub(super) fn advance_resource_version_after_in_conn(
        conn: &rusqlite::Connection,
        min_rv: i64,
    ) -> rusqlite::Result<i64> {
        let current: i64 = conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))?;
        let next = current.saturating_add(1).max(min_rv.saturating_add(1));
        conn.execute(queries::METADATA_SET_RV, [next.to_string()])?;
        Ok(next)
    }

    /// Normalize namespace for SQLite storage: None (cluster-scoped) → "" to allow UNIQUE constraint
    #[cfg(test)]
    pub fn normalize_namespace(ns: &Option<String>) -> String {
        ns.as_ref().map(|s| s.as_str()).unwrap_or("").to_string()
    }

    /// Denormalize namespace from SQLite: "" → None for cluster-scoped resources
    #[cfg(test)]
    pub fn denormalize_namespace(ns: String) -> Option<String> {
        if ns.is_empty() { None } else { Some(ns) }
    }
}
