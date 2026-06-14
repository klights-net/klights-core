use super::Datastore;
use super::queries;

impl Datastore {
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
