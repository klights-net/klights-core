//! Composition-owned cluster watch/meta maintenance capability.
//!
//! The compatibility sequencer may still need these focused maintenance
//! operations, but their concrete local implementation belongs to bootstrap
//! composition rather than the broad local API client.

use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::command::StorageCommand;
use klights_replication::proposal::RaftProposal;

use crate::bootstrap::authority::AuthorityHandle;
use crate::datastore::DatastoreHandle;

pub(crate) struct ClusterStoreLeaderMaintenance {
    db: DatastoreHandle,
    proposal: Arc<dyn RaftProposal>,
    authority: AuthorityHandle,
}

impl ClusterStoreLeaderMaintenance {
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        db: DatastoreHandle,
        proposal: Arc<dyn RaftProposal>,
        authority: A,
    ) -> Self {
        Self {
            db,
            proposal,
            authority: authority.into(),
        }
    }

    fn require_leader(&self) -> anyhow::Result<()> {
        self.authority
            .local_permit()
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!("operation requires current raft leader: {error}"))
    }
}

#[async_trait]
impl klights_cluster_store::ClusterWatchMaintenance for ClusterStoreLeaderMaintenance {
    async fn advance_resource_version_after(&self, min_rv: i64) -> anyhow::Result<i64> {
        self.require_leader()?;
        let before = self.db.get_current_resource_version().await?;
        let new_rv = before.saturating_add(1).max(min_rv.saturating_add(1));
        self.proposal
            .propose_command(StorageCommand::AdvanceResourceVersion { min_rv, new_rv })
            .await?;
        self.db.get_current_resource_version().await.or(Ok(new_rv))
    }

    async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> anyhow::Result<usize> {
        self.db
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> anyhow::Result<usize> {
        self.require_leader()?;
        let prunable = self
            .db
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.proposal
            .propose_command(StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            })
            .await?;
        Ok(prunable)
    }
}

#[async_trait]
impl klights_cluster_store::ClusterMetadataMutation for ClusterStoreLeaderMaintenance {
    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.db.get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::SetKlightsMeta {
                key: key.to_string(),
                value: value.to_string(),
            })
            .await?;
        Ok(())
    }
}
