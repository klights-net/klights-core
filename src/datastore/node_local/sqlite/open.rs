use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use klights_supervisor::TaskSupervisor;

use crate::datastore::errors::OpenError;
use crate::sqlite_boundary::DbExecutor;
use crate::sqlite_open::{self, OpenOpts, OpenPath};

fn display_path(opts: &OpenOpts) -> String {
    match &opts.path {
        OpenPath::SharedMemory(_) => "<in-memory>".to_string(),
        OpenPath::Disk(path) => path.display().to_string(),
    }
}

pub async fn open_with_opts(
    opts: OpenOpts,
    supervisor: Arc<TaskSupervisor>,
    connection_key: impl Into<String>,
) -> Result<DbExecutor> {
    let display = display_path(&opts);
    let executor = DbExecutor::open_with_opts(opts, supervisor, connection_key).await?;
    executor
        .call_raw("node-schema:init-and-check", move |conn| {
            init_schema(conn)?;
            check_db_health(conn, Path::new(&display))?;
            Ok(())
        })
        .await?;
    Ok(executor)
}

pub fn in_memory_opts() -> OpenOpts {
    OpenOpts::shared_memory("node")
}

pub fn disk_opts(path: impl Into<std::path::PathBuf>) -> OpenOpts {
    OpenOpts::disk(path)
}

fn init_schema(conn: &mut rusqlite::Connection) -> Result<(), OpenError> {
    super::schema::init_schema_in_conn(conn).map_err(|error| OpenError::Corrupt {
        path: "<unknown>".to_string(),
        details: format!("node-local schema initialization failed: {error}"),
    })
}

fn check_db_health(conn: &mut rusqlite::Connection, db_path: &Path) -> Result<(), OpenError> {
    sqlite_open::check_integrity(conn, db_path)?;
    super::schema::check_or_init_fingerprint(conn, db_path)
}
