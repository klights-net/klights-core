//! Schema fingerprint check for early-fail on schema mismatch.
//!
//! DSB-02 implements fingerprint-based detection so an operator gets one clear
//! startup error instead of confusing runtime SQL failures. The fingerprint is
//! a SHA256 hash of all CREATE TABLE statements in `init_schema()`. A fresh DB
//! writes the fingerprint; an existing DB must match exactly.
//!
//! During development, the operator action on mismatch is to delete the DB and
//! restart with the current schema.
//!
//! **Fingerprint integrity:** The constant `SCHEMA_FINGERPRINT` is verified by
//! `fingerprint_matches_live_schema` which runs `init_schema_in_conn` on an
//! in-memory DB and hashes the actual CREATE TABLE SQL from `sqlite_master`.
//! Adding, removing, or modifying a table in `schema.rs` without bumping the
//! constant fails the test — no hand-synced DDL duplication required.

use std::path::Path;

use super::queries;
use crate::datastore::errors::OpenError;
use rusqlite::OptionalExtension;

/// SHA256 hash of the current schema DDL. Regenerated whenever the schema
/// changes — the test `fingerprint_matches_live_schema` enforces this so a
/// developer cannot forget to bump it.
///
/// Computed from the actual CREATE TABLE SQL that `init_schema_in_conn`
/// writes to `sqlite_master`, sorted by table name for stability.
/// Indexes are excluded; only the core data model (tables) is fingerprinted.
pub(super) const SCHEMA_FINGERPRINT: &str =
    "ed9d64b90b224c9ad1aa82e6823767c717bf5fbca9e92c1b22cabe44b8c327b0";

/// Verify the fingerprint matches or initialize it for a fresh DB.
///
/// Returns `Ok(())` if:
/// - The DB is fresh (no fingerprint row) — writes the current fingerprint.
/// - The DB exists and the stored fingerprint matches.
///
/// Returns `Err(OpenError::SchemaMismatch)` if the stored fingerprint differs.
pub(super) fn check_or_init(conn: &rusqlite::Connection, db_path: &Path) -> Result<(), OpenError> {
    let stored: Option<String> = conn
        .query_row(queries::META_SELECT, ["schema_fingerprint"], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|e| OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("failed to read schema_fingerprint: {}", e),
        })?;

    match stored {
        None => {
            // Fresh install: init_schema already ran; record fingerprint.
            conn.execute(
                queries::META_INSERT,
                ("schema_fingerprint", SCHEMA_FINGERPRINT),
            )
            .map_err(|e| OpenError::Corrupt {
                path: db_path.display().to_string(),
                details: format!("failed to write schema_fingerprint: {}", e),
            })?;
            Ok(())
        }
        Some(v) if v == SCHEMA_FINGERPRINT => Ok(()),
        Some(actual) => Err(OpenError::SchemaMismatch {
            path: db_path.display().to_string(),
            expected: SCHEMA_FINGERPRINT.to_string(),
            actual,
            hint: "schema changed since this DB was created — delete the DB and restart"
                .to_string(),
        }),
    }
}

/// Run `PRAGMA integrity_check` and fail if the result is not "ok".
///
/// Corruption detection runs once at boot before any `DatastoreBackend`
/// method is reachable. A non-"ok" result produces `OpenError::Corrupt`.
pub(super) fn check_integrity(
    conn: &rusqlite::Connection,
    db_path: &Path,
) -> Result<(), OpenError> {
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("integrity_check query failed: {}", e),
        })?;

    if result != "ok" {
        return Err(OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("integrity_check returned: {}", result),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    /// Compute the SHA256 fingerprint from the actual schema created by
    /// `schema::init_schema_in_conn`.
    ///
    /// Creates an in-memory SQLite DB, runs the full schema init, then hashes
    /// all CREATE TABLE statements read from `sqlite_master` (sorted by name
    /// for stability). This is the single source of truth — no hand-synced DDL.
    fn compute_fingerprint_from_live_schema() -> String {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        super::super::schema::init_schema_in_conn(&mut conn).expect("init_schema");

        let mut ddl: Vec<String> = conn
            .prepare(
                "SELECT sql FROM sqlite_master \
                 WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )
            .expect("prepare")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .filter_map(|r| r.ok())
            .collect();

        // Normalize whitespace so formatting differences don't change the hash.
        for sql in &mut ddl {
            *sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        }

        let mut hasher = Sha256::new();
        for stmt in &ddl {
            hasher.update(stmt.as_bytes());
        }
        let bytes = hasher.finalize();
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    #[test]
    fn fingerprint_matches_live_schema() {
        // This test is the fingerprint-bump gate. It runs the actual
        // init_schema_in_conn, reads CREATE TABLE SQL from sqlite_master,
        // and hashes it. If you add/remove/modify a table in schema.rs,
        // this test fails and tells you the new hash to put in
        // SCHEMA_FINGERPRINT.
        let computed = compute_fingerprint_from_live_schema();
        assert_eq!(
            computed, SCHEMA_FINGERPRINT,
            "SCHEMA_FINGERPRINT is stale. The live schema produces hash \
             '{computed}' but the constant is '{SCHEMA_FINGERPRINT}'. \
             Update SCHEMA_FINGERPRINT in fingerprint.rs to match."
        );
    }

    #[test]
    fn fingerprint_is_stable() {
        let a = compute_fingerprint_from_live_schema();
        let b = compute_fingerprint_from_live_schema();
        assert_eq!(a, b, "same schema must produce same fingerprint");
    }

    #[test]
    fn live_schema_has_expected_table_count() {
        // Spot-check that init_schema creates the expected number of tables.
        // This catches cases where a table is accidentally dropped.
        let mut conn = rusqlite::Connection::open_in_memory().expect("open");
        super::super::schema::init_schema_in_conn(&mut conn).expect("init_schema");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |row| row.get(0),
            )
            .expect("count");

        assert_eq!(count, 14, "expecting 14 cluster tables in schema");
    }

    #[test]
    fn cluster_schema_excludes_node_local_tables() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open");
        super::super::schema::init_schema_in_conn(&mut conn).expect("init_schema");

        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )
            .expect("prepare")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("collect");

        for forbidden in [
            "pod_workqueue",
            "pod_sandboxes",
            "pod_networks",
            "pod_endpoints",
            "pod_slot_admissions",
        ] {
            assert!(
                !tables.contains(&forbidden.to_string()),
                "cluster.db schema must not contain node-local table {forbidden}"
            );
        }
    }
}
