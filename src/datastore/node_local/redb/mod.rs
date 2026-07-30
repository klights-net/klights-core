pub mod tables;

use anyhow::{Result, bail};

use crate::datastore::node_local::NodeLocalStores;

pub(crate) async fn open() -> Result<NodeLocalStores> {
    bail!("node-local redb backend not implemented yet")
}
