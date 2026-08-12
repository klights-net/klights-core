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

    #[cfg(test)]
    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> anyhow::Result<usize> {
        self.require_leader()?;
        let cutoff_ms = now_ms.saturating_sub(ttl_ms);
        let prunable = self.db.applied_outbox_gc_prunable_count(cutoff_ms).await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.proposal
            .propose_command(StorageCommand::GcAppliedOutbox { cutoff_ms })
            .await?;
        Ok(prunable)
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use klights_cluster_core::{
        LogApplyAppliedOutboxRow, OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark,
    };
    use klights_cluster_store::{
        ClusterMetadataMutation, ClusterWatchMaintenance, StorageCommandResult,
    };
    use klights_replication::proposal::RaftProposal;
    use std::sync::Mutex;

    struct RecordingApplyingProposal {
        inner: crate::bootstrap::outbox_apply_adapter::BackendProposalFixture,
        commands: Mutex<Vec<StorageCommand>>,
    }

    #[async_trait]
    impl RaftProposal for RecordingApplyingProposal {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<StorageCommandResult> {
            self.commands.lock().unwrap().push(command.clone());
            self.inner.propose_command(command).await
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<OutboxStreamWatermark>,
        ) -> Result<OutboxApplyOutcome, OutboxApplyError> {
            unreachable!("maintenance commands do not use the outbox proposal entrypoint")
        }
    }

    fn authority(is_leader: bool) -> Arc<dyn klights_leader_api::LeaderAuthority> {
        klights_replication::authority::WatchLeaderAuthority::channel(is_leader, None).0
    }

    async fn fixture(
        is_leader: bool,
    ) -> (
        crate::datastore::sqlite::Datastore,
        Arc<RecordingApplyingProposal>,
        ClusterStoreLeaderMaintenance,
    ) {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let handle: DatastoreHandle = Arc::new(db.clone());
        let proposal = Arc::new(RecordingApplyingProposal {
            inner: crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                handle.clone(),
            ),
            commands: Default::default(),
        });
        let adapter =
            ClusterStoreLeaderMaintenance::new(handle, proposal.clone(), authority(is_leader));
        (db, proposal, adapter)
    }

    #[tokio::test]
    async fn raft_mode_advance_resource_version_routes_through_proposer() {
        let (db, proposal, adapter) = fixture(true).await;

        let advanced = adapter.advance_resource_version_after(10).await.unwrap();

        assert_eq!(advanced, 1);
        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::AdvanceResourceVersion {
                min_rv: 10,
                new_rv: 11
            }]
        ));
        assert_eq!(db.get_current_resource_version().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn raft_mode_applied_outbox_gc_routes_through_proposer_and_prunes_via_apply() {
        let (db, proposal, adapter) = fixture(true).await;
        db.insert_applied_outbox(LogApplyAppliedOutboxRow {
            idempotency_key: "old-outbox".to_string(),
            subject_key: "v1/Pod/default/web/pod-uid".to_string(),
            operation: "PodStatus".to_string(),
            first_seen_ms: 100,
            applied_rv: Some(1),
            result_proto: Vec::new(),
            status_stamp: None,
        })
        .await
        .unwrap();

        let pruned = adapter.gc_applied_outbox(1_000, 500).await.unwrap();

        assert_eq!(pruned, 1);
        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::GcAppliedOutbox { cutoff_ms: 500 }]
        ));
        assert!(db.get_applied_outbox("old-outbox").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raft_mode_watch_events_gc_routes_through_proposer_and_prunes_via_apply() {
        let (db, proposal, adapter) = fixture(true).await;
        for name in ["first", "second", "third"] {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": name}
                }),
            )
            .await
            .unwrap();
        }
        let before = db.watch_events_gc_prunable_count(1, 10).await.unwrap();

        let pruned = adapter.gc_watch_events(1, 10).await.unwrap();

        assert_eq!(pruned, before);
        assert!(pruned > 0);
        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::GcWatchEvents {
                max_rows: 1,
                batch_cap: 10
            }]
        ));
        assert_eq!(db.watch_events_gc_prunable_count(1, 10).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn set_klights_meta_with_proposer_routes_through_raft() {
        let (db, proposal, adapter) = fixture(true).await;

        adapter
            .set_klights_meta("cluster-id", "cluster-a")
            .await
            .unwrap();

        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::SetKlightsMeta { key, value }]
                if key == "cluster-id" && value == "cluster-a"
        ));
        assert_eq!(
            db.get_klights_meta("cluster-id").await.unwrap().as_deref(),
            Some("cluster-a")
        );
    }

    #[tokio::test]
    async fn set_klights_meta_follower_proposer_rejects_no_local_mutation() {
        let (db, proposal, adapter) = fixture(false).await;

        let error = adapter
            .set_klights_meta("cluster-id", "cluster-a")
            .await
            .expect_err("follower metadata write must be rejected");

        assert!(error.to_string().contains("current raft leader"));
        assert!(proposal.commands.lock().unwrap().is_empty());
        assert!(db.get_klights_meta("cluster-id").await.unwrap().is_none());
    }
}
