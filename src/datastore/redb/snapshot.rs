//! Root composition wrapper for the focused Redb backend snapshot port.

use async_trait::async_trait;
use klights_cluster_store::{
    ClusterStoreResult, DatastoreSnapshotter, SnapshotEnvelope, SnapshotExclusiveFence,
};

use super::RedbDatastore;

#[async_trait]
impl DatastoreSnapshotter for RedbDatastore {
    fn backend_kind(&self) -> &'static str {
        self.recovery_store().backend_kind()
    }

    fn schema_fingerprint(&self) -> String {
        self.recovery_store().schema_fingerprint()
    }

    async fn snapshot(
        &self,
        fence: SnapshotExclusiveFence,
    ) -> ClusterStoreResult<SnapshotEnvelope> {
        self.recovery_store().snapshot(fence).await
    }

    async fn restore(
        &self,
        envelope: &SnapshotEnvelope,
        fence: SnapshotExclusiveFence,
    ) -> ClusterStoreResult<()> {
        self.recovery_store().restore(envelope, fence).await
    }
}
