//! DSB-02 schema fingerprint tests.
//!
//! Tests for schema fingerprint check and corruption fail-fast.

use crate::datastore::errors::OpenError;
use rusqlite::OptionalExtension;

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
