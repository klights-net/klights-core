use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use klights_supervisor::TaskSupervisor;

use crate::datastore::errors::OpenError;
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
        .call_raw("cluster-schema:init-and-check", move |conn| {
            init_schema(conn)?;
            check_db_health(conn, Path::new(&display))?;
            Ok(())
        })
        .await?;
    Ok(executor)
}

pub async fn open_read_only_with_opts(
    opts: OpenOpts,
    supervisor: Arc<TaskSupervisor>,
    connection_key: impl Into<String>,
) -> Result<DbExecutor> {
    let display = display_path(&opts);
    let executor = DbExecutor::open_read_only_with_opts(opts, supervisor, connection_key).await?;
    executor
        .call_raw("cluster-schema:check-read-only", move |conn| {
            sqlite_open::check_integrity(conn, Path::new(&display))?;
            super::fingerprint::check_or_init(conn, Path::new(&display))?;
            Ok(())
        })
        .await?;
    Ok(executor)
}

pub async fn open_in_memory(
    supervisor: Arc<TaskSupervisor>,
    connection_key: impl Into<String>,
) -> Result<DbExecutor> {
    open_with_opts(
        OpenOpts::shared_memory("cluster"),
        supervisor,
        connection_key,
    )
    .await
}

#[cfg(test)]
pub async fn open_in_memory_with_default_supervisor(
    connection_key: impl Into<String>,
) -> Result<DbExecutor> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    open_in_memory(supervisor, connection_key).await
}

pub fn init_schema(conn: &mut rusqlite::Connection) -> Result<(), OpenError> {
    super::schema::init_schema_in_conn(conn).map_err(|error| OpenError::Corrupt {
        path: "<unknown>".to_string(),
        details: format!("schema initialization failed: {error}"),
    })
}

pub fn check_db_health(conn: &mut rusqlite::Connection, db_path: &Path) -> Result<(), OpenError> {
    sqlite_open::check_integrity(conn, db_path)?;
    super::fingerprint::check_or_init(conn, db_path)
}
