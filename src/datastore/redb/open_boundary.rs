//! Supervised blocking boundary for redb database open and table setup.

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use ::redb::Database;

use crate::datastore::errors::OpenError;
use crate::task_supervisor::TaskSupervisor;

use super::meta;
use super::opener::RedbOpenOpts;
use super::tables;

const REDB_OPEN_RETRY_ATTEMPTS: usize = 50;
const REDB_OPEN_RETRY_DELAY: Duration = Duration::from_millis(500);

pub(super) async fn open_persistent(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
) -> Result<Database, OpenError> {
    let mut last_retry_error = String::new();

    for attempt in 0..REDB_OPEN_RETRY_ATTEMPTS {
        match open_persistent_once(supervisor, opts.clone(), open_persistent_blocking).await {
            Ok(db) => return Ok(db),
            Err(err)
                if is_retryable_already_open(&err) && attempt + 1 < REDB_OPEN_RETRY_ATTEMPTS =>
            {
                last_retry_error = err.to_string();
                supervisor
                    .sleep("redb_open_retry_delay", REDB_OPEN_RETRY_DELAY)
                    .await
                    .map_err(|sleep_err| OpenError::Corrupt {
                        path: opts.path.display().to_string(),
                        details: format!("redb open retry timer failed: {sleep_err}"),
                    })?;
            }
            Err(err) => return Err(err),
        }
    }

    Err(OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to open redb database after retries: {last_retry_error}"),
    })
}

#[cfg(test)]
pub(super) async fn open_persistent_with<F>(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
    opener: F,
) -> Result<Database, OpenError>
where
    F: FnOnce(&RedbOpenOpts) -> Result<Database, OpenError> + Send + 'static,
{
    open_persistent_once(supervisor, opts, opener).await
}

async fn open_persistent_once<F>(
    supervisor: &TaskSupervisor,
    opts: RedbOpenOpts,
    opener: F,
) -> Result<Database, OpenError>
where
    F: FnOnce(&RedbOpenOpts) -> Result<Database, OpenError> + Send + 'static,
{
    let path = opts.path.clone();
    supervisor
        .run_db_blocking("redb_open_persistent", "redb", move || opener(&opts))
        .await
        .map_err(|err| OpenError::Corrupt {
            path: path.display().to_string(),
            details: format!("supervised redb open task failed: {err}"),
        })?
}

pub(super) async fn open_in_memory(supervisor: &TaskSupervisor) -> anyhow::Result<Database> {
    supervisor
        .run_db_blocking("redb_open_in_memory", "redb", open_in_memory_blocking)
        .await
        .map_err(|err| anyhow::anyhow!("supervised in-memory redb open task failed: {err}"))?
}

pub(super) fn open_in_memory_blocking() -> anyhow::Result<Database> {
    let db = ::redb::Database::builder()
        .create_with_backend(::redb::backends::InMemoryBackend::new())
        .map_err(|e| anyhow::anyhow!("in-memory redb: {e}"))?;
    initialize_tables(&db).map_err(|e| anyhow::anyhow!("in-memory redb table init: {e}"))?;
    Ok(db)
}

pub fn open_persistent_blocking(opts: &RedbOpenOpts) -> Result<Database, OpenError> {
    ensure_parent_dir(&opts.path)?;
    let db = try_open_db(opts).map_err(|e| OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to create/open redb database: {e}"),
    })?;
    initialize_tables(&db).map_err(|e| OpenError::Corrupt {
        path: opts.path.display().to_string(),
        details: format!("failed to initialize redb tables: {e}"),
    })?;
    std::fs::set_permissions(&opts.path, PermissionsExt::from_mode(0o600)).map_err(|e| {
        OpenError::Filesystem {
            path: opts.path.clone(),
            source: e,
        }
    })?;
    meta::schema_check(&db).map_err(|e| attach_path(opts, e))?;
    Ok(db)
}

fn ensure_parent_dir(path: &std::path::Path) -> Result<(), OpenError> {
    if let Some(parent) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .create(parent)
            .map_err(|e| OpenError::Filesystem {
                path: parent.to_path_buf(),
                source: e,
            })?;
        std::fs::set_permissions(parent, PermissionsExt::from_mode(0o700)).map_err(|e| {
            OpenError::Filesystem {
                path: parent.to_path_buf(),
                source: e,
            }
        })?;
    }
    Ok(())
}

fn try_open_db(opts: &RedbOpenOpts) -> std::result::Result<Database, redb::DatabaseError> {
    if opts.path.exists() {
        Database::builder()
            .set_cache_size(opts.cache_size)
            .open(&opts.path)
    } else {
        Database::builder()
            .set_cache_size(opts.cache_size)
            .create(&opts.path)
    }
}

fn initialize_tables(db: &Database) -> anyhow::Result<()> {
    let w = db.begin_write()?;
    {
        let _ = w.open_table(tables::RES_CLUSTER);
        let _ = w.open_table(tables::RES_NS);
        let _ = w.open_table(tables::NAMESPACES);
        let _ = w.open_table(tables::WATCH_EVENTS);
        let _ = w.open_table(tables::APPLIED_OUTBOX);
        let _ = w.open_table(tables::RESOURCES_BY_OWNER);
        let _ = w.open_table(tables::RV_TO_KEY);
        let _ = w.open_table(tables::POD_SANDBOXES);
        let _ = w.open_table(tables::POD_NETWORKS);
        let _ = w.open_table(tables::NODE_SUBNETS);
        let _ = w.open_table(tables::POD_SLOT_ADMISSIONS);
        let _ = w.open_table(tables::POD_ENDPOINTS);
        let _ = w.open_table(tables::POD_WORKQUEUE);
        let _ = w.open_table(tables::META);
        let _ = w.open_table(tables::KLIGHTS_META);
    }
    w.commit()?;
    Ok(())
}

fn attach_path(opts: &RedbOpenOpts, err: OpenError) -> OpenError {
    match err {
        OpenError::SchemaMismatch {
            expected,
            actual,
            hint,
            ..
        } => OpenError::SchemaMismatch {
            path: opts.path.display().to_string(),
            expected,
            actual,
            hint,
        },
        other => other,
    }
}

fn is_retryable_already_open(err: &OpenError) -> bool {
    err.to_string().contains("already open")
}
