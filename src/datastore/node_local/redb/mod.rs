pub mod tables;

use anyhow::{Result, bail};

pub(crate) async fn open() -> Result<std::convert::Infallible> {
    bail!("node-local redb backend not implemented yet")
}
