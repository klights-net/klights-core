use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};

use crate::bootstrap::cluster_store::backend_kind::BackendKind;
use crate::bootstrap::node_store::NodeLocalStores;
use klights_supervisor::TaskSupervisor;

pub(crate) async fn open_node_local(
    kind: BackendKind,
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    connection_key: &'static str,
) -> Result<NodeLocalStores> {
    match kind {
        BackendKind::Sqlite => open_sqlite(path, supervisor, connection_key).await,
        BackendKind::Redb => match open_redb().await? {},
    }
}

async fn open_redb() -> Result<std::convert::Infallible> {
    bail!("node-local redb backend not implemented yet")
}

async fn open_sqlite(
    path: Option<&Path>,
    supervisor: Arc<TaskSupervisor>,
    connection_key: &'static str,
) -> Result<NodeLocalStores> {
    let opts = match path {
        Some(path) => klights_node_datastore::open::disk_opts(path.to_path_buf()),
        None => klights_node_datastore::open::in_memory_opts(),
    };
    let executor =
        klights_node_datastore::open::open_with_opts(opts, supervisor, connection_key).await?;
    NodeLocalStores::from_executor_with_clock(
        executor,
        Arc::new(klights_supervisor::SystemWallClock),
    )
}
