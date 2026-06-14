pub mod tables;

use anyhow::{Result, bail};

use crate::datastore::node_local::NodeLocalHandle;

pub async fn open() -> Result<NodeLocalHandle> {
    bail!("node-local redb backend not implemented yet")
}
