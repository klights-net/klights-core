//! Root composition wrapper for the focused Redb backend snapshot port.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_store::{DatastoreSnapshotter, SnapshotEnvelope, SnapshotExclusiveFence};

use super::RedbDatastore;

#[async_trait]
impl DatastoreSnapshotter for RedbDatastore {
    fn backend_kind(&self) -> &'static str {
        self.recovery.backend_kind()
    }

    fn schema_fingerprint(&self) -> String {
        self.recovery.schema_fingerprint()
    }

    async fn snapshot(&self, fence: SnapshotExclusiveFence) -> Result<SnapshotEnvelope> {
        self.recovery.snapshot(fence).await
    }

    async fn restore(
        &self,
        envelope: &SnapshotEnvelope,
        fence: SnapshotExclusiveFence,
    ) -> Result<()> {
        self.recovery.restore(envelope, fence).await
    }
}
