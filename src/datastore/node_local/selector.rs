use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::datastore::backend_kind::BackendKind;
use crate::datastore::node_local::{NodeLocalHandle, SqliteNodeLocalDb};
use crate::sqlite_boundary::DbExecutor;
use crate::sqlite_open as opener;
use klights_node_store::{RaftAppliedStateDurability, RaftLogDurability};
use klights_supervisor::TaskSupervisor;

pub struct LeaderNodeLocalStores {
    pub node: NodeLocalHandle,
    pub raft_log: Arc<dyn RaftLogDurability>,
    pub raft_applied_state: Arc<dyn RaftAppliedStateDurability>,
}

pub async fn open_node_local(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<NodeLocalHandle> {
    match kind {
        BackendKind::Sqlite => {
            let sqlite = open_sqlite(path, supervisor, key_file, connection_key).await?;
            Ok(sqlite as NodeLocalHandle)
        }
        BackendKind::Redb => crate::datastore::node_local::redb::open().await,
    }
}

/// Opens both forms of the node-local handle for the SQLite backend:
/// the trait-object `NodeLocalHandle` for the existing callers, and the
/// concrete `Arc<SqliteNodeLocalDb>` for components that need direct
/// SQLite access (P3-11: `SqliteRaftLogStorage` + `SqliteRaftStateMachine`
/// both work against the concrete handle so they can use raft-specific
/// tables in the same SQLite file).
///
/// Returns `None` for the `Arc<SqliteNodeLocalDb>` slot when the
/// backend isn't SQLite.
pub async fn open_leader_node_local(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<LeaderNodeLocalStores> {
    match kind {
        BackendKind::Sqlite => {
            let sqlite = open_sqlite(path, supervisor, key_file, connection_key).await?;
            Ok(LeaderNodeLocalStores {
                node: sqlite.clone(),
                raft_log: sqlite.clone(),
                raft_applied_state: sqlite,
            })
        }
        BackendKind::Redb => {
            anyhow::bail!("the redb node-local backend does not implement Raft durability")
        }
    }
}

#[cfg(test)]
pub async fn open_node_local_with_sqlite(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<(NodeLocalHandle, Option<Arc<SqliteNodeLocalDb>>)> {
    match kind {
        BackendKind::Sqlite => {
            let sqlite = open_sqlite(path, supervisor, key_file, connection_key).await?;
            Ok((sqlite.clone(), Some(sqlite)))
        }
        BackendKind::Redb => Ok((crate::datastore::node_local::redb::open().await?, None)),
    }
}

async fn open_sqlite(
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<Arc<SqliteNodeLocalDb>> {
    let opts = match path {
        Some(path) => opener::OpenOpts::node_disk(path.to_path_buf()),
        None => opener::OpenOpts::node_in_memory(),
    }
    .with_key_file(key_file)?;
    let executor = DbExecutor::open_with_opts(opts, supervisor, connection_key).await?;
    Ok(Arc::new(SqliteNodeLocalDb::from_executor(executor)?))
}
