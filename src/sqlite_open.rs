//! Centralized open-time configuration for SQLite-backed datastores.
//!
//! This module owns only schema-neutral connection policy: paths, permissions,
//! SQLCipher key application, PRAGMAs, integrity checks, and file metrics.
//! Cluster and node-local owners compose it with their own schema and
//! fingerprint adapters.
//!
//! All filesystem mutations route through `TaskSupervisor` file-category
//! helpers — opener never blocks the reactor and never bypasses the
//! supervisor (HR #2).

use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow};

use crate::datastore::errors::OpenError;
use klights_supervisor::TaskSupervisor;

const PRAGMA_JOURNAL_MODE: &str = "journal_mode";
const PRAGMA_SYNCHRONOUS: &str = "synchronous";
const PRAGMA_AUTO_VACUUM: &str = "auto_vacuum";
const PRAGMA_CACHE_SIZE: &str = "cache_size";
const PRAGMA_TEMP_STORE: &str = "temp_store";
const PRAGMA_MMAP_SIZE: &str = "mmap_size";
const PRAGMA_FOREIGN_KEYS: &str = "foreign_keys";
const PRAGMA_BUSY_TIMEOUT: &str = "busy_timeout";
const PRAGMA_VALUE_JOURNAL_MODE_WAL: &str = "WAL";
const PRAGMA_VALUE_SYNCHRONOUS_NORMAL: &str = "NORMAL";
const PRAGMA_VALUE_AUTO_VACUUM_INCREMENTAL: &str = "INCREMENTAL";
const PRAGMA_VALUE_CACHE_SIZE: &str = "-40000";
const PRAGMA_VALUE_TEMP_STORE_MEMORY: &str = "MEMORY";
const PRAGMA_VALUE_MMAP_SIZE: &str = "134217728";
const PRAGMA_VALUE_FOREIGN_KEYS_ON: &str = "ON";
const PRAGMA_VALUE_BUSY_TIMEOUT_MS: &str = "5000";

/// PRAGMA + key application profile selected at open time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PragmaProfile {
    /// Standard SQLite, no encryption. Today's only callable profile.
    Plaintext,
    /// SQLCipher whole-file encryption — DSB-06 implements; DSB-01 does
    /// not call it. Defined here so the opener has one type to grow.
    Encrypted,
}

/// Source of the SQLCipher key. Stub today; DSB-06 fills in `File`.
/// Only `None` is callable from DSB-01.
#[derive(Debug, Clone)]
pub enum KeySource {
    /// Read the key bytes from a root-only file (mode 0600, parent 0700).
    /// Implementation lives in DSB-06.
    File(PathBuf),
}

/// Where the connection lives.
#[derive(Debug, Clone)]
pub enum OpenPath {
    SharedMemory(PathBuf),
    Disk(PathBuf),
}

/// Bundled options for opening a connection.
#[derive(Debug, Clone)]
pub struct OpenOpts {
    pub path: OpenPath,
    pub profile: PragmaProfile,
    /// `None` in DSB-01 (plaintext + in-memory only). DSB-06 plumbs the
    /// real `KeySource::File` value through the encrypted profile.
    pub key_source: Option<KeySource>,
    /// Default `false` — the opener refuses to open a disk DB whose
    /// parent directory exists with permissions wider than `0700`. Tests
    /// running on shared `/tmp` (mode `1777`) flip this to `true` to
    /// stay scoped to the test fixture.
    pub allow_existing_perms: bool,
}

impl OpenOpts {
    pub fn in_memory() -> Self {
        Self {
            path: OpenPath::SharedMemory(shared_memory_uri("raw")),
            profile: PragmaProfile::Plaintext,
            key_source: None,
            allow_existing_perms: false,
        }
    }

    pub fn disk(path: impl Into<PathBuf>) -> Self {
        Self {
            path: OpenPath::Disk(path.into()),
            profile: PragmaProfile::Plaintext,
            key_source: None,
            allow_existing_perms: false,
        }
    }

    pub fn shared_memory(scope: &str) -> Self {
        Self {
            path: OpenPath::SharedMemory(shared_memory_uri(scope)),
            profile: PragmaProfile::Plaintext,
            key_source: None,
            allow_existing_perms: false,
        }
    }

    pub fn with_key_file(self, key_file: Option<&Path>) -> Result<Self> {
        #[cfg(not(feature = "sqlcipher"))]
        {
            if key_file.is_some() {
                return Err(anyhow!(
                    "SQLCipher encryption requested but the 'sqlcipher' cargo feature is not enabled. \
                     Rebuild with --features sqlcipher"
                ));
            }
            Ok(self)
        }

        #[cfg(feature = "sqlcipher")]
        {
            let mut opts = self;
            if let Some(kf) = key_file {
                opts.profile = PragmaProfile::Encrypted;
                opts.key_source = Some(KeySource::File(kf.to_path_buf()));
            }
            Ok(opts)
        }
    }
}

fn shared_memory_uri(scope: &str) -> PathBuf {
    PathBuf::from(format!(
        "file:klights-{scope}-{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    ))
}

/// Apply the PRAGMA list for `profile` to a freshly-opened connection.
/// Idempotent — re-applying on an existing DB does not change values.
///
/// PRAGMAs are issued via `execute_batch` so SQLite parses values as
/// keyword tokens (e.g. `WAL`, `INCREMENTAL`) rather than quoted strings;
/// `pragma_update` with a `&str` quotes the value, which `auto_vacuum`
/// silently rejects.
pub fn apply_pragmas(conn: &rusqlite::Connection, profile: PragmaProfile) -> rusqlite::Result<()> {
    // Sanity: DSB-01 only ships `Plaintext`; `Encrypted` is a forward
    // declaration. If a caller smuggles `Encrypted` through without the
    // sqlcipher feature or a key, fail loudly.
    if matches!(profile, PragmaProfile::Encrypted) {
        #[cfg(not(feature = "sqlcipher"))]
        {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Encrypted profile requires sqlcipher cargo feature".into()),
            ));
        }
        // Under sqlcipher: key was already applied before this call.
        // Fall through to apply remaining PRAGMAs with mmap_size=0.
    }

    // auto_vacuum is a persistent file-header flag and can only be
    // toggled when the file has zero pages. Issue it first, then VACUUM
    // to materialise the header before journal_mode=WAL writes any
    // pages of its own. After this batch a fresh disk DB has the flag
    // baked in; an existing DB no-ops because the flag is already set.
    let mmap_val: String = if matches!(profile, PragmaProfile::Encrypted) {
        "0".to_string()
    } else {
        PRAGMA_VALUE_MMAP_SIZE.to_string()
    };

    let batch = format!(
        "PRAGMA {av} = {av_v}; \
         VACUUM; \
         PRAGMA {jm} = {jm_v}; \
         PRAGMA {sync} = {sync_v}; \
         PRAGMA {cs} = {cs_v}; \
         PRAGMA {ts} = {ts_v}; \
         PRAGMA {mm} = {mm_v}; \
         PRAGMA {fk} = {fk_v}; \
         PRAGMA {bt} = {bt_v};",
        jm = PRAGMA_JOURNAL_MODE,
        jm_v = PRAGMA_VALUE_JOURNAL_MODE_WAL,
        sync = PRAGMA_SYNCHRONOUS,
        sync_v = PRAGMA_VALUE_SYNCHRONOUS_NORMAL,
        av = PRAGMA_AUTO_VACUUM,
        av_v = PRAGMA_VALUE_AUTO_VACUUM_INCREMENTAL,
        cs = PRAGMA_CACHE_SIZE,
        cs_v = PRAGMA_VALUE_CACHE_SIZE,
        ts = PRAGMA_TEMP_STORE,
        ts_v = PRAGMA_VALUE_TEMP_STORE_MEMORY,
        mm = PRAGMA_MMAP_SIZE,
        mm_v = mmap_val,
        fk = PRAGMA_FOREIGN_KEYS,
        fk_v = PRAGMA_VALUE_FOREIGN_KEYS_ON,
        bt = PRAGMA_BUSY_TIMEOUT,
        bt_v = PRAGMA_VALUE_BUSY_TIMEOUT_MS,
    );
    conn.execute_batch(&batch)
}

/// Apply connection-local settings for a read-only datastore connection.
///
/// The write connection owns persistent PRAGMAs, schema initialization, and
/// fingerprint creation. Read connections stay query-only and only apply
/// connection-scoped tuning that does not mutate the database file.
pub fn apply_read_pragmas(
    conn: &rusqlite::Connection,
    profile: PragmaProfile,
) -> rusqlite::Result<()> {
    if matches!(profile, PragmaProfile::Encrypted) {
        #[cfg(not(feature = "sqlcipher"))]
        {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some("Encrypted profile requires sqlcipher cargo feature".into()),
            ));
        }
    }

    let mmap_val: String = if matches!(profile, PragmaProfile::Encrypted) {
        "0".to_string()
    } else {
        PRAGMA_VALUE_MMAP_SIZE.to_string()
    };

    let batch = format!(
        "PRAGMA query_only = ON; \
         PRAGMA {cs} = {cs_v}; \
         PRAGMA {ts} = {ts_v}; \
         PRAGMA {mm} = {mm_v}; \
         PRAGMA {fk} = {fk_v}; \
         PRAGMA {bt} = {bt_v};",
        cs = PRAGMA_CACHE_SIZE,
        cs_v = PRAGMA_VALUE_CACHE_SIZE,
        ts = PRAGMA_TEMP_STORE,
        ts_v = PRAGMA_VALUE_TEMP_STORE_MEMORY,
        mm = PRAGMA_MMAP_SIZE,
        mm_v = mmap_val,
        fk = PRAGMA_FOREIGN_KEYS,
        fk_v = PRAGMA_VALUE_FOREIGN_KEYS_ON,
        bt = PRAGMA_BUSY_TIMEOUT,
        bt_v = PRAGMA_VALUE_BUSY_TIMEOUT_MS,
    );
    conn.execute_batch(&batch)
}

/// Ensure the parent directory exists with `0700` and chmod the DB file
/// (and its WAL/SHM siblings, when present) to `0600`.
///
/// Runs entirely on the file-category blocking pool; never touches the
/// reactor thread.
pub async fn ensure_root_only(
    supervisor: &Arc<TaskSupervisor>,
    db_path: &Path,
    allow_existing_perms: bool,
) -> Result<()> {
    let db_path = db_path.to_path_buf();
    let supervisor = supervisor.clone();
    supervisor
        .clone()
        .run_blocking_file("opener:ensure_root_only", move || {
            ensure_root_only_blocking(&db_path, allow_existing_perms)
        })
        .await
        .map_err(|e| anyhow!("ensure_root_only supervisor error: {e}"))?
}

fn ensure_root_only_blocking(db_path: &Path, allow_existing_perms: bool) -> Result<()> {
    let parent = db_path
        .parent()
        .ok_or_else(|| anyhow!("db path has no parent: {}", db_path.display()))?;

    if parent.exists() {
        let meta = std::fs::metadata(parent)
            .map_err(|e| anyhow!("stat parent dir {} failed: {}", parent.display(), e))?;
        let mode = meta.mode() & 0o777;
        // Loose-perm fixtures (shared /tmp) opt in via allow_existing_perms;
        // the parent gets tightened to 0700 below.
        if mode != 0o700 && !allow_existing_perms {
            return Err(anyhow!(
                "parent dir {} has mode {:o}; opener requires 0700 (set allow_existing_perms for tests)",
                parent.display(),
                mode
            ));
        }
    } else {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(parent)
            .map_err(|e| anyhow!("create parent dir {} failed: {}", parent.display(), e))?;
    }

    // Tighten parent dir to 0700 (no-op if already correct).
    chmod(parent, 0o700)?;

    // Tighten db file + WAL/SHM siblings (when present) to 0600.
    // SQLite may remove WAL/SHM sidecars while a previous connection is
    // closing, so missing files here are an acceptable no-op.
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = db_path.as_os_str().to_owned();
        candidate.push(suffix);
        let candidate = std::path::PathBuf::from(candidate);
        chmod_if_exists(&candidate, 0o600)?;
    }
    Ok(())
}

fn chmod_if_exists(path: &Path, mode: u32) -> Result<()> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(anyhow!("stat {} failed: {}", path.display(), err)),
    };
    let mut perms = meta.permissions();
    if perms.mode() & 0o777 == mode {
        return Ok(());
    }
    perms.set_mode(mode);
    match std::fs::set_permissions(path, perms) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow!(
            "chmod {} to {:o} failed: {}",
            path.display(),
            mode,
            err
        )),
    }
}

fn chmod(path: &Path, mode: u32) -> Result<()> {
    let mut perms = std::fs::metadata(path)
        .map_err(|e| anyhow!("stat {} failed: {}", path.display(), e))?
        .permissions();
    if perms.mode() & 0o777 == mode {
        return Ok(());
    }
    perms.set_mode(mode);
    std::fs::set_permissions(path, perms)
        .map_err(|e| anyhow!("chmod {} to {:o} failed: {}", path.display(), mode, e))
}

/// Apply the SQLCipher key to a freshly-opened connection.
///
/// Must be called **before** `apply_pragmas` so the encrypted pages are
/// readable.  The key is applied via `pragma_update` (bound parameter)
/// which never places the key in the SQL text.
#[cfg(feature = "sqlcipher")]
pub fn apply_key(conn: &rusqlite::Connection, key: &[u8]) -> rusqlite::Result<()> {
    conn.pragma_update(None, "key", key)
}

/// Read the SQLCipher key from a file via the supervisor's file-category
/// blocking pool.
#[cfg(feature = "sqlcipher")]
pub async fn read_key_file(
    supervisor: &std::sync::Arc<TaskSupervisor>,
    path: &std::path::Path,
) -> Result<Vec<u8>> {
    let path = path.to_path_buf();
    let path_for_err = path.clone();
    let supervisor = supervisor.clone();
    let key = supervisor
        .run_blocking_file("opener:read_key_file", move || {
            std::fs::read(&path)
                .map_err(|e| anyhow!("failed to read key file {}: {}", path.display(), e))
        })
        .await
        .map_err(|e| anyhow!("read_key_file supervisor error: {e}"))??;

    if key.is_empty() {
        return Err(anyhow!(
            "SQLCipher key file {} is empty",
            path_for_err.display()
        ));
    }

    Ok(key)
}

/// Check for orphaned WAL file (WAL exists but main DB does not).
///
/// This is a safety check that detects an inconsistent state where
/// the WAL file is present but the main database file is missing.
/// SQLite would silently create a new empty DB, potentially masking
/// data loss. The opener must fail explicitly so the operator knows.
pub async fn check_orphaned_wal(supervisor: &Arc<TaskSupervisor>, db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    supervisor
        .clone()
        .run_blocking_file("opener:check_orphaned_wal", move || {
            check_orphaned_wal_blocking(&db_path)
        })
        .await
        .map_err(|error| anyhow!("check_orphaned_wal supervisor error: {error}"))??;
    Ok(())
}

fn check_orphaned_wal_blocking(db_path: &Path) -> Result<(), OpenError> {
    let wal_path = {
        let mut s = db_path.as_os_str().to_owned();
        s.push("-wal");
        PathBuf::from(s)
    };

    // If WAL exists but main DB does not, this is an orphaned WAL.
    if wal_path.exists() && !db_path.exists() {
        return Err(OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!(
                "orphaned WAL file {} exists but main DB {} is missing — possible data loss",
                wal_path.display(),
                db_path.display()
            ),
        });
    }
    Ok(())
}

pub async fn persistent_datastore_sizes(
    supervisor: &Arc<TaskSupervisor>,
    db_path: &Path,
) -> Result<(u64, u64)> {
    let db_path = db_path.to_path_buf();
    supervisor
        .clone()
        .run_blocking_file("opener:persistent_datastore_sizes", move || {
            let db_size = std::fs::metadata(&db_path)
                .map(|meta| meta.len())
                .unwrap_or(0);
            let mut wal_path = db_path.as_os_str().to_owned();
            wal_path.push("-wal");
            let wal_path = PathBuf::from(wal_path);
            let wal_size = std::fs::metadata(wal_path)
                .map(|meta| meta.len())
                .unwrap_or(0);
            (db_size, wal_size)
        })
        .await
        .map_err(|error| anyhow!("persistent_datastore_sizes supervisor error: {error}"))
}

#[cfg(test)]
pub fn check_orphaned_wal_for_test(db_path: &Path) -> Result<(), OpenError> {
    check_orphaned_wal_blocking(db_path)
}

/// Run the schema-neutral SQLite integrity check.
pub fn check_integrity(conn: &rusqlite::Connection, db_path: &Path) -> Result<(), OpenError> {
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|error| OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("integrity_check query failed: {error}"),
        })?;
    if result != "ok" {
        return Err(OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("integrity_check returned: {result}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_supervisor::TaskCategoryConfig;
    use std::sync::Arc;

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn open_temp_conn() -> rusqlite::Connection {
        rusqlite::Connection::open_in_memory().expect("open in-memory")
    }

    /// Open a disk-backed connection in a fixture dir so journal_mode=WAL
    /// isn't a silent no-op (SQLite refuses WAL on in-memory DBs).
    fn open_disk_conn() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.db");
        let conn = rusqlite::Connection::open(&path).expect("open disk");
        (dir, conn)
    }

    fn pragma_text(conn: &rusqlite::Connection, name: &str) -> String {
        conn.pragma_query_value(None, name, |row| row.get::<_, String>(0))
            .unwrap_or_default()
    }

    fn pragma_int(conn: &rusqlite::Connection, name: &str) -> i64 {
        conn.pragma_query_value(None, name, |row| row.get::<_, i64>(0))
            .unwrap_or_default()
    }

    #[test]
    fn open_persistent_applies_pragmas() {
        let (_dir, conn) = open_disk_conn();
        apply_pragmas(&conn, PragmaProfile::Plaintext).expect("apply_pragmas");
        // SQLite only writes the file header (auto_vacuum flag lives there)
        // after the first page is created, so create a table to materialise
        // the header before checking persistent flags.
        conn.execute("CREATE TABLE pragma_probe (id INTEGER)", [])
            .expect("create probe table");

        // journal_mode echoes "wal" lowercase; SQLite normalizes the value.
        assert_eq!(pragma_text(&conn, "journal_mode").to_uppercase(), "WAL");
        // synchronous returns the integer code: NORMAL = 1
        assert_eq!(pragma_int(&conn, "synchronous"), 1);
        // auto_vacuum: INCREMENTAL = 2
        assert_eq!(pragma_int(&conn, "auto_vacuum"), 2);
        assert_eq!(pragma_int(&conn, "cache_size"), -40_000);
        // temp_store: MEMORY = 2
        assert_eq!(pragma_int(&conn, "temp_store"), 2);
        // foreign_keys: ON = 1
        assert_eq!(pragma_int(&conn, "foreign_keys"), 1);
        assert_eq!(pragma_int(&conn, "busy_timeout"), 5_000);
        assert_eq!(pragma_int(&conn, "mmap_size"), 134_217_728);
    }

    #[test]
    fn apply_pragmas_is_idempotent() {
        let (_dir, conn) = open_disk_conn();
        apply_pragmas(&conn, PragmaProfile::Plaintext).expect("first apply");
        let mode_before = pragma_text(&conn, "journal_mode");
        let cache_before = pragma_int(&conn, "cache_size");
        apply_pragmas(&conn, PragmaProfile::Plaintext).expect("second apply");
        assert_eq!(pragma_text(&conn, "journal_mode"), mode_before);
        assert_eq!(pragma_int(&conn, "cache_size"), cache_before);
    }

    #[test]
    fn apply_pragmas_rejects_encrypted_profile_without_sqlcipher_feature() {
        let conn = open_temp_conn();
        let err =
            apply_pragmas(&conn, PragmaProfile::Encrypted).expect_err("Encrypted must not apply");
        assert!(format!("{err}").contains("sqlcipher"));
    }

    #[tokio::test]
    async fn open_persistent_sets_parent_dir_0700_and_file_mode_0600() {
        // /tmp itself is mode 1777, so the fixture passes
        // allow_existing_perms=true and creates a private subdir which
        // ensure_root_only then tightens to 0700.
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("klights-data");
        std::fs::create_dir(&nested).expect("create nested");
        let db_path = nested.join("state.db");

        // Touch the db + WAL + SHM so we can verify all three get 0600.
        for suffix in ["", "-wal", "-shm"] {
            let mut p = db_path.as_os_str().to_owned();
            p.push(suffix);
            std::fs::File::create(std::path::PathBuf::from(p)).expect("create file");
        }

        let supervisor = supervisor();
        ensure_root_only(&supervisor, &db_path, /* allow_existing_perms */ true)
            .await
            .expect("ensure_root_only");

        let dir_meta = std::fs::metadata(&nested).expect("stat dir");
        assert_eq!(dir_meta.mode() & 0o777, 0o700, "parent dir must be 0700");

        for suffix in ["", "-wal", "-shm"] {
            let mut p = db_path.as_os_str().to_owned();
            p.push(suffix);
            let path = std::path::PathBuf::from(p);
            let meta = std::fs::metadata(&path).expect("stat db file");
            assert_eq!(
                meta.mode() & 0o777,
                0o600,
                "{} must be 0600",
                path.display()
            );
        }
    }

    #[tokio::test]
    async fn ensure_root_only_creates_missing_parent_with_0700() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("klights-fresh");
        // do NOT pre-create nested — opener creates with 0700.
        let db_path = nested.join("state.db");
        // Touch the db file but only after parent exists.
        let supervisor = supervisor();
        ensure_root_only(&supervisor, &db_path, false)
            .await
            .expect("ensure_root_only");
        let dir_meta = std::fs::metadata(&nested).expect("stat dir");
        assert_eq!(dir_meta.mode() & 0o777, 0o700);
    }

    #[test]
    fn chmod_if_exists_treats_missing_optional_sidecar_as_noop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing_sidecar = dir.path().join("state.db-shm");

        chmod_if_exists(&missing_sidecar, 0o600).expect("missing sidecar is optional");
    }

    #[tokio::test]
    async fn ensure_root_only_rejects_loose_parent_perms_by_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("klights-loose");
        std::fs::DirBuilder::new()
            .mode(0o755)
            .create(&nested)
            .expect("create loose");
        let db_path = nested.join("state.db");
        let supervisor = supervisor();
        let err = ensure_root_only(&supervisor, &db_path, false)
            .await
            .expect_err("must reject 0755 parent");
        assert!(format!("{err}").contains("0700"));
    }

    #[test]
    fn check_integrity_detects_corruption() {
        use std::io::{Seek, Write};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.db");
        {
            let conn = rusqlite::Connection::open(&path).expect("open");
            apply_pragmas(&conn, PragmaProfile::Plaintext).expect("pragmas");
            conn.execute("CREATE TABLE integrity_probe (value TEXT NOT NULL)", [])
                .expect("create probe table");
            conn.execute("INSERT INTO integrity_probe VALUES ('value')", [])
                .expect("insert data");
            check_integrity(&conn, &path).expect("first check");
        }

        // Reopen and corrupt the first page
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for write");
        // SQLite header is 100 bytes; corrupt a byte in the first page
        file.seek(std::io::SeekFrom::Start(50)).expect("seek");
        file.write_all(b"CORRUPT").expect("write corrupt data");

        // Opening should succeed but integrity check should fail
        let conn = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("open may succeed");

        let err = check_integrity(&conn, &path).expect_err("should detect corruption");
        let OpenError::Corrupt { path: p, details } = err else {
            panic!("expected Corrupt, got: {:?}", err);
        };
        assert_eq!(p, path.display().to_string());
        assert!(
            details.contains("integrity_check") || details.contains("corrupt"),
            "details should mention integrity check or corruption"
        );
    }
}
