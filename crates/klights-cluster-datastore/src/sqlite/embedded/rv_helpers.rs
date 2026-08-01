use super::Datastore;
use super::transaction_primitives;

impl Datastore {
    #[cfg(test)]
    pub(super) fn next_resource_version_in_conn(
        conn: &rusqlite::Connection,
    ) -> rusqlite::Result<i64> {
        transaction_primitives::next_resource_version_in_tx(conn)
    }

    pub(super) fn current_resource_version_in_tx(
        tx: &rusqlite::Transaction<'_>,
    ) -> rusqlite::Result<i64> {
        transaction_primitives::current_resource_version(tx)
    }

    pub(super) fn advance_resource_version_after_in_conn(
        conn: &rusqlite::Connection,
        min_rv: i64,
    ) -> rusqlite::Result<i64> {
        transaction_primitives::advance_resource_version_after(conn, min_rv)
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
