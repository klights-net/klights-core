use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::datastore::backend_kind::BackendKind;
use crate::datastore::node_local::NodeLocalStores;
use klights_node_store::{RaftAppliedStateDurability, RaftLogDurability};
use klights_supervisor::TaskSupervisor;

pub(crate) struct LeaderNodeLocalStores {
    pub(crate) node: NodeLocalStores,
    pub(crate) raft_log: Arc<dyn RaftLogDurability>,
    pub(crate) raft_applied_state: Arc<dyn RaftAppliedStateDurability>,
}

pub(crate) async fn open_node_local(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<NodeLocalStores> {
    match kind {
        BackendKind::Sqlite => open_sqlite(path, supervisor, key_file, connection_key).await,
        BackendKind::Redb => crate::datastore::node_local::redb::open().await,
    }
}

/// Opens focused node-local ports plus the root-only OpenRaft conversion.
pub(crate) async fn open_leader_node_local(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<LeaderNodeLocalStores> {
    match kind {
        BackendKind::Sqlite => {
            let node = open_sqlite(path, supervisor, key_file, connection_key).await?;
            let raft = Arc::new(
                crate::datastore::node_local::raft_adapter::OpenRaftNodeDurabilityAdapter::new(
                    node.raft_log_persistence(),
                    node.raft_applied_state_persistence(),
                ),
            );
            Ok(LeaderNodeLocalStores {
                node,
                raft_log: raft.clone(),
                raft_applied_state: raft,
            })
        }
        BackendKind::Redb => {
            anyhow::bail!("the redb node-local backend does not implement Raft durability")
        }
    }
}

#[cfg(test)]
pub(crate) async fn open_node_local_with_sqlite(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<(
    NodeLocalStores,
    Option<Arc<crate::datastore::node_local::NodeLocalStores>>,
)> {
    match kind {
        BackendKind::Sqlite => {
            let node = open_sqlite(path, supervisor, key_file, connection_key).await?;
            let legacy = Arc::new(
                crate::datastore::node_local::NodeLocalStores::from_executor(
                    node.executor_for_test(),
                )?,
            );
            Ok((node, Some(legacy)))
        }
        BackendKind::Redb => Ok((crate::datastore::node_local::redb::open().await?, None)),
    }
}

async fn open_sqlite(
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    key_file: Option<&Path>,
    connection_key: &'static str,
) -> Result<NodeLocalStores> {
    let opts = match path {
        Some(path) => klights_node_datastore::open::disk_opts(path.to_path_buf()),
        None => klights_node_datastore::open::in_memory_opts(),
    }
    .with_key_file(key_file)?;
    let executor =
        klights_node_datastore::open::open_with_opts(opts, supervisor, connection_key).await?;
    NodeLocalStores::from_executor_with_clock(
        executor,
        Arc::new(klights_supervisor::SystemWallClock),
    )
}
