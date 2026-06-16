//! DSB-02 schema fingerprint tests.
//!
//! Tests for schema fingerprint check and corruption fail-fast.

use crate::datastore::errors::OpenError;
use crate::datastore::sqlite::Datastore;
use crate::log_apply::{
    LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow,
    LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow, LogApplyPodCleanupIntentRow,
    LogApplyResourceRow, LogApplyWatchEventRow,
};
use rusqlite::OptionalExtension;
use serde_json::json;
use sha2::{Digest, Sha256};

// opener is a sibling module of tests/
use super::super::opener;

#[test]
fn fresh_db_initializes_schema_and_writes_fingerprint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.db");
    let mut conn = rusqlite::Connection::open(&path).expect("open");

    // Apply pragmas and init schema.
    opener::apply_pragmas(&conn, opener::PragmaProfile::Plaintext).expect("pragmas");
    opener::init_schema(&mut conn).expect("init schema");

    // check_db_health writes the fingerprint on fresh DB.
    opener::check_db_health(&mut conn, &path).expect("check_db_health on fresh");

    // Check fingerprint is written.
    let fp: Option<String> = conn
        .query_row(
            "SELECT value FROM _klights_meta WHERE key = 'schema_fingerprint'",
            [],
            |row| row.get(0),
        )
        .optional()
        .expect("query fingerprint");
    assert!(fp.is_some(), "fingerprint should be written to fresh DB");
    let fp = fp.unwrap();
    assert!(!fp.is_empty(), "fingerprint should not be empty");
    assert_eq!(fp.len(), 64, "SHA256 produces 64 hex chars");
}

#[test]
fn existing_db_with_matching_fingerprint_opens_cleanly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.db");
    let mut conn = rusqlite::Connection::open(&path).expect("open");

    // Set up DB with current schema and fingerprint.
    opener::apply_pragmas(&conn, opener::PragmaProfile::Plaintext).expect("pragmas");
    opener::init_schema(&mut conn).expect("init schema");
    opener::check_db_health(&mut conn, &path).expect("check_db_health on fresh");

    // Close and reopen — fingerprint check should pass.
    drop(conn);
    let mut conn2 = rusqlite::Connection::open(&path).expect("reopen");
    opener::apply_pragmas(&conn2, opener::PragmaProfile::Plaintext).expect("pragmas");
    opener::check_db_health(&mut conn2, &path).expect("check_db_health on reopen");
}

#[test]
fn existing_db_with_different_fingerprint_fails_with_actionable_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.db");
    let mut conn = rusqlite::Connection::open(&path).expect("open");

    // Set up DB with current schema.
    opener::apply_pragmas(&conn, opener::PragmaProfile::Plaintext).expect("pragmas");
    opener::init_schema(&mut conn).expect("init schema");

    // Write a stale fingerprint.
    conn.execute(
        "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES ('schema_fingerprint', 'deadbeef')",
        [],
    ).expect("insert stale fingerprint");

    // Fingerprint check should fail with SchemaMismatch.
    let err =
        opener::check_db_health(&mut conn, &path).expect_err("should fail with stale fingerprint");
    let OpenError::SchemaMismatch {
        path: err_path,
        expected,
        actual,
        hint,
    } = err
    else {
        panic!("expected SchemaMismatch, got: {:?}", err);
    };
    assert_eq!(
        err_path,
        path.display().to_string(),
        "path should be included"
    );
    assert!(
        !expected.is_empty(),
        "expected fingerprint should be present"
    );
    assert_eq!(actual, "deadbeef", "actual should be the stale fingerprint");
    assert!(
        hint.contains("delete the DB"),
        "hint should mention operator action"
    );
    assert!(hint.contains("restart"), "hint should mention restart");
}

#[test]
fn corrupt_main_db_fails_open_with_path_and_sqlite_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.db");

    // Create a valid DB first
    {
        let mut conn = rusqlite::Connection::open(&path).expect("open");
        opener::apply_pragmas(&conn, opener::PragmaProfile::Plaintext).expect("pragmas");
        opener::init_schema(&mut conn).expect("init schema");
        opener::check_db_health(&mut conn, &path).expect("first check");

        // Add some data
        conn.execute(
            "INSERT INTO metadata (key, value) VALUES ('test', 'value')",
            [],
        )
        .expect("insert data");
    }

    // Corrupt the database file by writing garbage at offset 100
    // (after the SQLite header but within the first page)
    {
        use std::io::Seek;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for write");
        file.seek(std::io::SeekFrom::Start(100)).expect("seek");
        file.write_all(b"CORRUPTED_DATA_HERE!!!")
            .expect("write corrupt data");
    }

    // Opening should succeed (SQLite is permissive) but integrity check should fail
    let mut conn =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open may succeed");

    let err = opener::check_db_health(&mut conn, &path).expect_err("should detect corruption");
    let OpenError::Corrupt { path: p, details } = err else {
        panic!("expected Corrupt error, got: {:?}", err);
    };
    assert_eq!(p, path.display().to_string(), "path should be included");
    assert!(
        details.contains("integrity_check"),
        "details should mention integrity_check"
    );
}

#[test]
fn wal_present_but_main_missing_fails_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.db");

    // Create a WAL file without a main DB
    {
        use std::io::Write;
        let wal_path = path.with_extension("db-wal");
        let mut f = std::fs::File::create(&wal_path).expect("create wal");
        f.write_all(b"this is a wal file without main db")
            .expect("write wal");
    }

    // The opener should detect the orphaned WAL and refuse to open.
    // SQLite would silently create a new empty DB, masking potential data loss.
    let err = opener::check_orphaned_wal(&path).expect_err("should detect orphaned WAL");
    let OpenError::Corrupt { path: p, details } = err else {
        panic!("expected Corrupt, got: {:?}", err);
    };
    assert_eq!(p, path.display().to_string());
    assert!(
        details.contains("orphaned WAL"),
        "details should mention orphaned WAL: {}",
        details
    );
    assert!(
        details.contains("missing"),
        "details should mention missing main DB: {}",
        details
    );
}

#[test]
fn schema_domain_map_is_deferred_to_dsb_ha_00() {
    // Marker: DSB-HA-00 complete (left for historical reference).
}

fn hash_table_marker(hasher: &mut Sha256, table: &str) {
    hasher.update(b"TABLE:");
    hash_str(hasher, table);
}

fn hash_row_separator(hasher: &mut Sha256) {
    hasher.update([0x1F]);
}

fn hash_i64(hasher: &mut Sha256, value: i64) {
    hasher.update([0x49]);
    hasher.update(value.to_le_bytes());
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hasher.update([0x53]);
    hash_slice(hasher, value.as_bytes());
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update([0x42]);
    hash_slice(hasher, value);
}

fn hash_slice(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hash_optional_str(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(v) => {
            hasher.update([0xFF]);
            hash_str(hasher, v);
        }
        None => {
            hasher.update([0x00]);
        }
    }
}

fn hash_optional_i64(hasher: &mut Sha256, value: Option<i64>) {
    match value {
        Some(v) => {
            hasher.update([0xFF]);
            hash_i64(hasher, v);
        }
        None => {
            hasher.update([0x00]);
        }
    }
}

async fn fingerprint_db_family_state(db: &Datastore) -> String {
    let mut hasher = Sha256::new();

    async fn append_namespaces(hasher: &mut Sha256, db: &Datastore) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-namespaces", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT name, uid, resource_version, data FROM namespaces ORDER BY name",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "namespaces");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_i64(hasher, row.2);
            hash_bytes(hasher, &row.3);
        }
        Ok(())
    }

    async fn append_namespaced_resources(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-namespaced-resources", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, api_version, kind, namespace, name, uid, resource_version, created_rv, data \
                     FROM namespaced_resources ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, Vec<u8>>(8)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "namespaced_resources");
        for row in rows {
            hash_row_separator(hasher);
            hash_i64(hasher, row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_str(hasher, &row.3);
            hash_str(hasher, &row.4);
            hash_str(hasher, &row.5);
            hash_i64(hasher, row.6);
            hash_i64(hasher, row.7);
            hash_bytes(hasher, &row.8);
        }
        Ok(())
    }

    async fn append_cluster_resources(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-cluster-resources", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, api_version, kind, name, uid, resource_version, created_rv, data \
                     FROM cluster_resources ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, Vec<u8>>(7)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "cluster_resources");
        for row in rows {
            hash_row_separator(hasher);
            hash_i64(hasher, row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_str(hasher, &row.3);
            hash_str(hasher, &row.4);
            hash_i64(hasher, row.5);
            hash_i64(hasher, row.6);
            hash_bytes(hasher, &row.7);
        }
        Ok(())
    }

    async fn append_watch_events(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-watch-events", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, api_version, kind, namespace, name, resource_version, event_type, data \
                     FROM watch_events ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, Vec<u8>>(7)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "watch_events");
        for row in rows {
            hash_row_separator(hasher);
            hash_i64(hasher, row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_optional_str(hasher, row.3.as_deref());
            hash_str(hasher, &row.4);
            hash_i64(hasher, row.5);
            hash_str(hasher, &row.6);
            hash_bytes(hasher, &row.7);
        }
        Ok(())
    }

    async fn append_watch_replay_floors(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-watch-replay-floors", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT api_version, kind, namespace_key, floor_rv FROM watch_replay_floors \
                     ORDER BY api_version, kind, namespace_key",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "watch_replay_floors");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_i64(hasher, row.3);
        }
        Ok(())
    }

    async fn append_node_subnets(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-node-subnets", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT node_name, subnet, subnet_base_int, vtep_ip, node_ip, mode, \
                     hostport_range, created_at FROM node_subnets ORDER BY node_name",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, i64>(7)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "node_subnets");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_i64(hasher, row.2);
            hash_str(hasher, &row.3);
            hash_str(hasher, &row.4);
            hash_str(hasher, &row.5);
            hash_optional_str(hasher, row.6.as_deref());
            hash_i64(hasher, row.7);
        }
        Ok(())
    }

    async fn append_node_dataplane(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-node-dataplane", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT node_name, mode, encryption, public_key, endpoint, port, updated_at \
                     FROM node_dataplane ORDER BY node_name",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, Option<i64>>(5)?,
                            row.get::<_, i64>(6)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "node_dataplane");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_optional_str(hasher, row.3.as_deref());
            hash_str(hasher, &row.4);
            hash_optional_i64(hasher, row.5);
            hash_i64(hasher, row.6);
        }
        Ok(())
    }

    async fn append_applied_outbox(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-applied-outbox", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT idempotency_key, subject_key, operation, first_seen_ms, applied_rv, \
                     result_proto, status_stamp, reserved_rv \
                     FROM applied_outbox ORDER BY idempotency_key",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Vec<u8>>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                            row.get::<_, Option<i64>>(7)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "applied_outbox");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_i64(hasher, row.3);
            hash_optional_i64(hasher, row.4);
            hash_bytes(hasher, &row.5);
            hash_optional_i64(hasher, row.6);
            hash_optional_i64(hasher, row.7);
        }
        Ok(())
    }

    async fn append_pod_cleanup_intents(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-pod-cleanup-intents", |conn| {
                let mut stmt = conn.prepare(
                    "SELECT node_name, namespace, pod_name, pod_uid, reason, resource_version, \
                     created_at_ms, pod_data FROM pod_cleanup_intents ORDER BY node_name, namespace, pod_name, pod_uid, reason",
                )?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, Vec<u8>>(7)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "pod_cleanup_intents");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
            hash_str(hasher, &row.2);
            hash_str(hasher, &row.3);
            hash_str(hasher, &row.4);
            hash_i64(hasher, row.5);
            hash_i64(hasher, row.6);
            hash_bytes(hasher, &row.7);
        }
        Ok(())
    }

    async fn append_klights_meta(
        hasher: &mut Sha256,
        db: &Datastore,
    ) -> tokio_rusqlite::Result<()> {
        let rows = db
            .db_call("family-fingerprint-klights-meta", |conn| {
                let mut stmt = conn.prepare("SELECT key, value FROM _klights_meta ORDER BY key")?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();

        hash_table_marker(hasher, "_klights_meta");
        for row in rows {
            hash_row_separator(hasher);
            hash_str(hasher, &row.0);
            hash_str(hasher, &row.1);
        }
        Ok(())
    }

    append_namespaces(&mut hasher, db).await.unwrap();
    append_namespaced_resources(&mut hasher, db).await.unwrap();
    append_cluster_resources(&mut hasher, db).await.unwrap();
    append_watch_events(&mut hasher, db).await.unwrap();
    append_watch_replay_floors(&mut hasher, db).await.unwrap();
    append_node_subnets(&mut hasher, db).await.unwrap();
    append_node_dataplane(&mut hasher, db).await.unwrap();
    append_applied_outbox(&mut hasher, db).await.unwrap();
    append_pod_cleanup_intents(&mut hasher, db).await.unwrap();
    append_klights_meta(&mut hasher, db).await.unwrap();

    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

#[tokio::test]
async fn raft_mixed_family_apply_converges_to_identical_fingerprint() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();
    let derived_resource_commit = LogApplyCommit::new(
        30,
        vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("mixed-family-ns".to_string()),
            name: "mixed-family-derived".to_string(),
            uid: "mixed-family-derived-uid".to_string(),
            resource_version: 30,
            data: json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "mixed-family-derived",
                    "namespace": "mixed-family-ns",
                    "uid": "mixed-family-derived-uid",
                    "resourceVersion": "30",
                },
                "data": {"seed": "derived"},
            }),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        })],
    );

    let seed_watch_10 = LogApplyCommit::new(
        10,
        vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("mixed-family-ns".to_string()),
            name: "seed-watch-10".to_string(),
            resource_version: 10,
            event_type: "ADDED".to_string(),
            data: json!({"type": "ADDED", "object": {"metadata": {"name": "seed-watch-10"}}}),
        })],
    );

    let seed_watch_20 = LogApplyCommit::new(
        20,
        vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("mixed-family-ns".to_string()),
            name: "seed-watch-20".to_string(),
            resource_version: 20,
            event_type: "ADDED".to_string(),
            data: json!({"type": "ADDED", "object": {"metadata": {"name": "seed-watch-20"}}}),
        })],
    );

    let mixed_commit = LogApplyCommit::new(
        60,
        vec![
            LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                name: "mixed-family-ns".to_string(),
                uid: "mixed-family-ns-uid".to_string(),
                resource_version: 60,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "mixed-family-ns",
                        "uid": "mixed-family-ns-uid",
                        "resourceVersion": "60"
                    },
                }),
            }),
            LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("mixed-family-ns".to_string()),
                name: "mixed-family-main".to_string(),
                uid: "mixed-family-main-uid".to_string(),
                resource_version: 60,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "mixed-family-main",
                        "namespace": "mixed-family-ns",
                        "uid": "mixed-family-main-uid",
                        "resourceVersion": "60"
                    },
                    "data": {"seed": "main"},
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            }),
            LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: "mixed-family-node-a".to_string(),
                subnet: "10.42.0.0/24".to_string(),
                subnet_base_int: 1_762_000_000,
                vtep_ip: "10.42.0.1".to_string(),
                node_ip: "192.0.2.10".to_string(),
                mode: "root".to_string(),
                hostport_range: Some("30000-30010".to_string()),
            }),
            LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                node_name: "mixed-family-node-a".to_string(),
                mode: "root".to_string(),
                encryption: "enabled".to_string(),
                public_key: Some("AAAAAA==".to_string()),
                endpoint: "192.0.2.10".to_string(),
                port: Some(51_820),
            }),
            LogApplyMutation::PutKlightsMeta {
                key: "mixed-family-fingerprint-seed".to_string(),
                value: "v1".to_string(),
            },
            LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "mixed-family-outbox".to_string(),
                subject_key: "family.main".to_string(),
                operation: "Apply".to_string(),
                first_seen_ms: 1_234,
                applied_rv: Some(60),
                result_proto: b"ok-result".to_vec(),
                status_stamp: Some(7),
            }),
            LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                node_name: "mixed-family-node-a".to_string(),
                namespace: "mixed-family-ns".to_string(),
                pod_name: "family-schedulable-pod".to_string(),
                pod_uid: "family-pod-uid".to_string(),
                reason: "evicted".to_string(),
                resource_version: 60,
                created_at_ms: 1_357,
                pod_data: json!({"metadata": {"name": "family-schedulable-pod"}}),
            }),
            LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("mixed-family-ns".to_string()),
                name: "mixed-family-main-watch".to_string(),
                resource_version: 60,
                event_type: "MODIFIED".to_string(),
                data: json!({
                    "type": "MODIFIED",
                    "object": {
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "mixed-family-main",
                            "namespace": "mixed-family-ns",
                            "uid": "mixed-family-main-uid",
                            "resourceVersion": "60",
                        },
                        "data": {
                            "seed": "main"
                        },
                    },
                }),
            }),
        ],
    );

    let gc_commit = LogApplyCommit::new(
        61,
        vec![LogApplyMutation::GcWatchEvents {
            max_rows: 2,
            batch_cap: 10_000,
        }],
    );

    for (leader_commit, follower_commit) in [
        (seed_watch_10.clone(), seed_watch_10),
        (seed_watch_20.clone(), seed_watch_20),
    ] {
        leader
            .apply_log_apply_commit(leader_commit)
            .await
            .expect("leader seed explicit watch should apply");
        follower
            .apply_log_apply_commit(follower_commit)
            .await
            .expect("follower seed explicit watch should apply");
    }

    leader
        .apply_log_apply_commit(derived_resource_commit.clone())
        .await
        .expect("leader derived watch resource should apply");
    follower
        .apply_log_apply_commit(derived_resource_commit)
        .await
        .expect("follower derived watch resource should apply");

    leader
        .apply_log_apply_commit(mixed_commit.clone())
        .await
        .expect("leader mixed mutation family commit should apply");
    follower
        .apply_log_apply_commit(mixed_commit)
        .await
        .expect("follower mixed mutation family commit should apply");

    leader
        .apply_log_apply_commit(gc_commit.clone())
        .await
        .expect("leader watch GC commit should apply");
    follower
        .apply_log_apply_commit(gc_commit)
        .await
        .expect("follower watch GC commit should apply");

    let leader_fingerprint = fingerprint_db_family_state(&leader).await;
    let follower_fingerprint = fingerprint_db_family_state(&follower).await;
    assert_eq!(leader_fingerprint, follower_fingerprint);

    let leader_derived_watch: Vec<u8> = leader
        .db_call("mixed-family-derived-watch-bytes-leader", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "mixed-family-ns", "mixed-family-derived", 30],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await
        .unwrap();
    let follower_derived_watch: Vec<u8> = follower
        .db_call("mixed-family-derived-watch-bytes-follower", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "mixed-family-ns", "mixed-family-derived", 30],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await
        .unwrap();

    assert_eq!(leader_derived_watch, follower_derived_watch);

    let explicit_watch_payload = serde_json::to_vec(&json!({
        "type": "MODIFIED",
        "object": {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "mixed-family-main",
                "namespace": "mixed-family-ns",
                "uid": "mixed-family-main-uid",
                "resourceVersion": "60",
            },
            "data": {
                "seed": "main"
            },
        },
    }))
    .unwrap();

    let leader_explicit_watch: Vec<u8> = leader
        .db_call("mixed-family-explicit-watch-bytes-leader", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "mixed-family-ns", "mixed-family-main-watch", 60],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await
        .unwrap();
    let follower_explicit_watch: Vec<u8> = follower
        .db_call("mixed-family-explicit-watch-bytes-follower", |conn| {
            Ok(conn.query_row(
                "SELECT data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5",
                rusqlite::params!["v1", "ConfigMap", "mixed-family-ns", "mixed-family-main-watch", 60],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await
        .unwrap();

    assert_eq!(leader_explicit_watch, follower_explicit_watch);
    assert_eq!(leader_explicit_watch, explicit_watch_payload);
}
