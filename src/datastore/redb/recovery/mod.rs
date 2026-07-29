//! Phase 10D Redb recovery-only metadata ownership.
//!
//! Live cluster-meta mutation stays with `live_committed_apply`. Snapshot
//! capture/restore remains in the adjacent recovery modules and root facade
//! adaptation remains in `backend_impl`.

use std::collections::HashSet;
use std::sync::Arc;

use ::redb::ReadableDatabase;
use anyhow::{Result, anyhow};
use klights_cluster_core::{ClusterMembership, ClusterMetadata};
use klights_cluster_datastore::redb::{RedbAccessor, tables};
use klights_cluster_store::SnapshotMembership;

mod backend_snapshot;
mod capture;

pub(super) struct RedbClusterMetadataObservation {
    pub(super) metadata: ClusterMetadata,
    pub(super) membership: SnapshotMembership,
}

#[derive(Clone)]
pub(super) struct RedbRecoveryStore {
    accessor: Arc<RedbAccessor>,
    snapshot_sessions: Arc<tokio::sync::Semaphore>,
}

impl RedbRecoveryStore {
    pub(super) fn new(
        accessor: Arc<RedbAccessor>,
        snapshot_sessions: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            accessor,
            snapshot_sessions,
        }
    }

    pub(super) async fn read_cluster_metadata(&self) -> Result<RedbClusterMetadataObservation> {
        self.accessor
            .call("redb_atomic_cluster_metadata_observation", |db| {
                let read = db.begin_read()?;
                let klights = read.open_table(tables::KLIGHTS_META)?;
                let get = |key: &str| -> Result<Option<String>> {
                    Ok(klights.get(key)?.map(|value| value.value().to_string()))
                };
                let cluster_id = get(klights_cluster_store::CLUSTER_ID_META_KEY)?
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("cluster_id is missing or empty"))?;
                let raw_epoch = get(klights_cluster_store::LEADER_EPOCH_META_KEY)?
                    .ok_or_else(|| anyhow!("leader_epoch is missing"))?;
                let leader_epoch = raw_epoch
                    .parse::<i64>()
                    .map_err(|_| anyhow!("invalid leader_epoch {raw_epoch:?}"))?;
                let meta = read.open_table(tables::META)?;
                let current_rv = match meta.get("rv")? {
                    None => 0,
                    Some(value) => {
                        let raw = std::str::from_utf8(value.value())
                            .map_err(|error| anyhow!("invalid resource_version UTF-8: {error}"))?;
                        raw.parse::<i64>()
                            .map_err(|_| anyhow!("invalid resource_version {raw:?}"))?
                    }
                };
                if leader_epoch < 0 || current_rv < 0 {
                    return Err(anyhow!(
                        "cluster metadata numeric values must be non-negative"
                    ));
                }
                let membership = match (
                    get(klights_cluster_store::RAFT_VOTERS_META_KEY)?,
                    get(klights_cluster_store::RAFT_TERM_META_KEY)?,
                    get(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?,
                ) {
                    (None, None, None) => SnapshotMembership::AuthoritativeAbsent,
                    (Some(raw_voters), Some(raw_term), Some(raw_hint)) => {
                        let voters: Vec<String> = serde_json::from_str(&raw_voters)?;
                        let term = raw_term
                            .parse::<i64>()
                            .map_err(|_| anyhow!("invalid raft term {raw_term:?}"))?;
                        let mut unique = HashSet::with_capacity(voters.len());
                        if term < 0
                            || voters.is_empty()
                            || voters
                                .iter()
                                .any(|voter| voter.is_empty() || !unique.insert(voter.as_str()))
                        {
                            return Err(anyhow!(
                                "membership contains an invalid term or voter set"
                            ));
                        }
                        SnapshotMembership::Present(ClusterMembership {
                            cluster_id: cluster_id.clone(),
                            voters,
                            term,
                            leader_hint: (!raw_hint.is_empty()).then_some(raw_hint),
                        })
                    }
                    _ => return Err(anyhow!("membership metadata is incomplete")),
                };
                Ok(RedbClusterMetadataObservation {
                    metadata: ClusterMetadata {
                        cluster_id,
                        leader_epoch,
                        current_rv,
                    },
                    membership,
                })
            })
            .await
    }

    pub(super) async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        let key = key.to_string();
        self.accessor
            .call("redb_get_klights_meta", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::KLIGHTS_META)?;
                Ok(table
                    .get(key.as_str())?
                    .map(|value| value.value().to_string()))
            })
            .await
    }
}
