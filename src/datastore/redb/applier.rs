#![cfg(test)]
//! Root test adapter for the backend-neutral legacy command fixture.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::command::{CommandMeta, StorageCommand};

use crate::bootstrap::sequenced_datastore::DatastoreApplier;

use super::RedbDatastore;

#[async_trait]
impl DatastoreApplier for RedbDatastore {
    async fn apply_command(&self, command: StorageCommand, meta: CommandMeta) -> Result<()> {
        self.0.apply_legacy_test_command(command, meta).await
    }
}
