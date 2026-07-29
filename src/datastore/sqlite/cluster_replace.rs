//! Root SQLite compatibility and post-commit composition facade.
//!
//! Transaction ownership lives in the corrected 10C.2 live-apply and 10D
//! recovery packets. This facade only supplies the app-owned executor, codec,
//! and test-only observation hook.

use super::Datastore;
use crate::datastore::types::ReplicatedSnapshotMetadata;
use anyhow::{Result, anyhow};
use klights_cluster_core::{LogApplyCommit, SnapshotRestoreOperation};

#[cfg(test)]
#[derive(Clone)]
pub(super) struct PostCommitPublishPause {
    pub(crate) reached: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) published: std::sync::Arc<tokio::sync::Notify>,
    gate: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

#[cfg(test)]
impl PostCommitPublishPause {
    pub(crate) fn resume(&self) {
        let (lock, condition) = &*self.gate;
        *lock.lock().unwrap() = true;
        condition.notify_one();
    }
}

#[cfg(test)]
fn pause_after_commit_before_publish(
    slot: &std::sync::Mutex<Option<PostCommitPublishPause>>,
) -> Option<std::sync::Arc<tokio::sync::Notify>> {
    let pause = slot.lock().unwrap().take()?;
    pause.reached.notify_one();
    let (lock, condition) = &*pause.gate;
    let mut resumed = lock.lock().unwrap();
    while !*resumed {
        resumed = condition.wait(resumed).unwrap();
    }
    Some(pause.published)
}

impl Datastore {
    #[cfg(test)]
    pub(super) fn install_post_commit_publish_pause(&self) -> PostCommitPublishPause {
        let pause = PostCommitPublishPause {
            reached: std::sync::Arc::new(tokio::sync::Notify::new()),
            published: std::sync::Arc::new(tokio::sync::Notify::new()),
            gate: std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())),
        };
        *self.post_commit_publish_pause.lock().unwrap() = Some(pause.clone());
        pause
    }

    /// Replace cluster-replicated state from one authoritative leader snapshot.
    pub async fn replace_replicated_resource_state(
        &self,
        entries: Vec<SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        let watch_replay_floors = watch_replay_floors.map(|floors| {
            floors
                .into_iter()
                .map(|floor| super::recovery::SnapshotReplayFloor {
                    api_version: floor.api_version,
                    kind: floor.kind,
                    namespace_key: floor.namespace_key,
                    floor_resource_version: floor.floor_resource_version,
                    floor_event_id: floor.floor_event_id,
                    position_is_exact: floor.position_is_exact,
                })
                .collect()
        });
        let metadata = metadata.map(|metadata| super::recovery::SnapshotMetadata {
            cluster_id: metadata.cluster_id,
            leader_epoch: metadata.leader_epoch,
            membership: match metadata.membership {
                crate::datastore::ReplicatedMembershipState::LegacyOmitted => {
                    super::recovery::SnapshotMembership::LegacyOmitted
                }
                crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent => {
                    super::recovery::SnapshotMembership::AuthoritativeAbsent
                }
                crate::datastore::ReplicatedMembershipState::Present(membership) => {
                    super::recovery::SnapshotMembership::Present(membership)
                }
            },
            command_codec_activation_version: metadata.command_codec_activation_version,
        });
        #[cfg(test)]
        let watch_bus = self.commit_sink.clone();
        let outbox_codec = self.outbox_codec.clone();
        #[cfg(test)]
        let post_commit_publish_pause = self.post_commit_publish_pause.clone();
        self.db_call_with_post_commit(
            "replace_replicated_resource_state",
            move |conn| {
                let context = super::live_apply::TransactionContext::new(outbox_codec.as_ref());
                let pending = super::recovery::replace_resource_state_in_conn(
                    conn,
                    entries,
                    current_rv,
                    watch_event_high_water,
                    watch_replay_floors,
                    metadata,
                    &context,
                )?;
                Ok(((), pending))
            },
            move |pending| {
                #[cfg(not(test))]
                let _ = pending;
                #[cfg(test)]
                let published =
                    pause_after_commit_before_publish(post_commit_publish_pause.as_ref());
                #[cfg(test)]
                super::watch::publish_pending_batch(pending, watch_bus.as_ref());
                #[cfg(test)]
                if let Some(published) = published {
                    published.notify_one();
                }
            },
        )
        .await
        .map_err(|err| anyhow!("failed to replace replicated resource state: {err}"))?;
        Ok(())
    }

    pub async fn apply_log_apply_commit(&self, commit: LogApplyCommit) -> Result<()> {
        let pending = self
            .live_committed_apply
            .apply_log_apply_commit(commit)
            .await
            .map_err(|err| anyhow!("failed to apply log_apply commit: {err}"))?;

        #[cfg(not(test))]
        let _ = pending;
        #[cfg(test)]
        self.publish_watch_events(pending);
        Ok(())
    }

    pub async fn apply_raft_log_apply_commit_receipt(
        &self,
        commit: LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        self.apply_raft_log_apply_commit_atomically(commit).await
    }

    async fn apply_raft_log_apply_commit_atomically(
        &self,
        commit: LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        #[cfg(test)]
        let post_commit_publish_pause = self.post_commit_publish_pause.clone();
        let (receipt, pending) = self
            .live_committed_apply
            .apply_committed_with_pending(commit)
            .await?;
        #[cfg(not(test))]
        let _ = pending;
        #[cfg(test)]
        {
            let published = pause_after_commit_before_publish(post_commit_publish_pause.as_ref());
            super::watch::publish_pending_batch(pending, self.commit_sink.as_ref());
            if let Some(published) = published {
                published.notify_one();
            }
        }
        Ok(receipt)
    }
}
