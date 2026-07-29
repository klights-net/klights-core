use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use klights_node_store::NodeStoreOpenError;
use klights_supervisor::TaskSupervisor;

use klights_supervisor::DbExecutor;
use klights_supervisor::sqlite_open::{self, OpenOpts, OpenPath};

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
            init_schema(conn).map_err(db_call_error)?;
            check_db_health(conn, Path::new(&display)).map_err(db_call_error)?;
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

fn db_call_error(error: NodeStoreOpenError) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(error))
}

fn init_schema(conn: &mut rusqlite::Connection) -> Result<(), NodeStoreOpenError> {
    crate::schema::init_schema_in_conn(conn).map_err(|error| {
        NodeStoreOpenError::corrupt(
            "<unknown>",
            format!("node-local schema initialization failed: {error}"),
        )
    })
}

fn check_db_health(
    conn: &mut rusqlite::Connection,
    db_path: &Path,
) -> Result<(), NodeStoreOpenError> {
    sqlite_open::check_integrity(conn, db_path).map_err(|error| match error {
        klights_supervisor::SqliteOpenError::Corrupt { path, details } => {
            NodeStoreOpenError::corrupt(path, details)
        }
    })?;
    crate::schema::check_or_init_fingerprint(conn, db_path)
}
