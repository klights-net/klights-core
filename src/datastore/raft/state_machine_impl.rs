//! Raft state machine wrapping the cluster `DatastoreBackend`.
//!
//! Implements openraft 0.9 storage-v2 `RaftStateMachine`. T1.3 unified
//! the apply path so every committed `EntryPayload::Normal` carries a
//! `LogApplyCommit` protobuf (built by the leader's proposer via
//! `backend.build_log_apply_commit_for_outbox`). The state machine decodes
//! the commit and calls `backend.apply_log_apply_commit` — the same code
//! every voter follower and learner runs — so cluster.db is byte-identical
//! across the cluster.
//!
//! Snapshot APIs are wired via `snapshot::SqliteRaftSnapshotBuilder`,
//! which streams from `replication::snapshot` so leader build and follower
//! install share one source of truth for "what makes up a cluster snapshot".
//! `install_snapshot` replays the bundled `LogApplyCommit` entries through
//! `apply_log_apply_commit` and `get_current_snapshot` rebuilds the snapshot
//! when openraft asks for an outbound transfer.

use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};

use crate::datastore::DatastoreBackend;
use crate::datastore::node_local::SqliteNodeLocalDb;
use crate::datastore::raft::snapshot::{RaftSnapshotData, SqliteRaftSnapshotBuilder};
use crate::datastore::raft::types::{NodeId, StorageCommandResult, TypeConfig};

const META_KEY_LAST_APPLIED: &str = "last_applied";
const META_KEY_LAST_MEMBERSHIP: &str = "last_membership";
#[cfg(test)]
const META_KEY_CURRENT_SNAPSHOT: &str = "current_snapshot";

#[derive(Clone)]
pub struct SqliteRaftStateMachine {
    backend: Arc<dyn DatastoreBackend>,
    node_local: Arc<SqliteNodeLocalDb>,
}

impl SqliteRaftStateMachine {
    pub fn new(
        backend: Arc<dyn DatastoreBackend>,
        node_local: Arc<SqliteNodeLocalDb>,
        // T1.3: authoring_node is no longer carried by the state machine
        // because the apply path decodes `LogApplyCommit` directly and
        // calls `backend.apply_log_apply_commit`. Kept in the signature
        // so existing call sites compile unchanged; the value is unused.
        _authoring_node: String,
    ) -> Self {
        Self {
            backend,
            node_local,
        }
    }
}

fn ioerr_read(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_state_machine(AnyError::error(e.to_string())),
    }
}

fn ioerr_write(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_state_machine(AnyError::error(e.to_string())),
    }
}

fn apply_err(log_id: LogId<NodeId>, e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::apply(log_id, AnyError::error(e.to_string())),
    }
}

impl SqliteRaftStateMachine {
    async fn read_last_applied(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self
            .node_local
            .raft_meta_get(META_KEY_LAST_APPLIED)
            .await
            .map_err(ioerr_read)?
        {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).map_err(ioerr_read)?),
            None => Ok(None),
        }
    }

    async fn write_last_applied(&self, id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(&Some(id)).map_err(ioerr_write)?;
        self.node_local
            .raft_meta_set(META_KEY_LAST_APPLIED, bytes)
            .await
            .map_err(ioerr_write)
    }

    async fn read_membership(
        &self,
    ) -> Result<StoredMembership<NodeId, openraft::BasicNode>, StorageError<NodeId>> {
        match self
            .node_local
            .raft_meta_get(META_KEY_LAST_MEMBERSHIP)
            .await
            .map_err(ioerr_read)?
        {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).map_err(ioerr_read)?),
            None => Ok(StoredMembership::default()),
        }
    }

    async fn write_membership(
        &self,
        m: &StoredMembership<NodeId, openraft::BasicNode>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(m).map_err(ioerr_write)?;
        self.node_local
            .raft_meta_set(META_KEY_LAST_MEMBERSHIP, bytes)
            .await
            .map_err(ioerr_write)
    }

    async fn build_current_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut builder = self.get_snapshot_builder().await;
        builder.build_snapshot().await
    }
}

impl RaftStateMachine<TypeConfig> for SqliteRaftStateMachine {
    type SnapshotBuilder = SqliteRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let last = self.read_last_applied().await?;
        let m = self.read_membership().await?;
        Ok((last, m))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<StorageCommandResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        // P3-8: defensive fence against stale-leader writes. openraft
        // already refuses to dispatch an entry whose term is below the
        // current term at the consensus layer, but the state machine
        // double-checks against `last_applied` so a buggy lower layer
        // (or a manual `Raft::initialize` from an operator) cannot
        // silently apply commits at an older term and rewrite history.
        let mut last_applied_term = self.read_last_applied().await?.map(|id| id.leader_id.term);
        for entry in entries {
            let log_id = entry.log_id;
            if let Some(prev_term) = last_applied_term
                && log_id.leader_id.term < prev_term
            {
                return Err(apply_err(
                    log_id,
                    format!(
                        "stale-term apply rejected: entry term {} < last_applied term {}",
                        log_id.leader_id.term, prev_term
                    ),
                ));
            }
            last_applied_term = Some(log_id.leader_id.term);
            match entry.payload {
                EntryPayload::Blank => {
                    out.push(StorageCommandResult::default());
                }
                EntryPayload::Membership(m) => {
                    let stored = StoredMembership::new(Some(log_id), m);
                    self.write_membership(&stored).await?;
                    out.push(StorageCommandResult::default());
                }
                EntryPayload::Normal(payload) => {
                    // T1.3: raft entry payloads carry a `LogApplyCommit`
                    // protobuf (built by the leader's proposer via
                    // `backend.build_log_apply_commit_for_outbox`). Every
                    // node — leader, voter follower, learner — applies
                    // through the same `apply_log_apply_commit` →
                    // `apply_commit_in_tx` path so cluster.db state is
                    // byte-identical across the cluster.
                    let commit = crate::log_apply::decode_commit_protobuf(payload.as_slice())
                        .map_err(|e| apply_err(log_id, e))?;
                    let result = self
                        .backend
                        .apply_raft_log_apply_commit(commit)
                        .await
                        .map_err(|e| apply_err(log_id, e))?;
                    out.push(result);
                }
            }
            self.write_last_applied(log_id).await?;
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let last_applied = self.read_last_applied().await.unwrap_or(None);
        let membership = self.read_membership().await.unwrap_or_default();
        SqliteRaftSnapshotBuilder {
            backend: self.backend.clone(),
            last_applied,
            membership,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let data = RaftSnapshotData::deserialize_from_bytes(&bytes).map_err(ioerr_write)?;
        // Raft snapshot install semantics: the destination state machine
        // must become byte/key-identical to the leader snapshot at the
        // snapshot index. Applying snapshot commits over the existing local
        // store (merge) is a correctness bug — rows the leader has deleted
        // but a lagging follower/learner still holds are never removed, so
        // the member silently diverges (observed after lossy Sonobuoy:
        // followers/learner carry more rows than the leader). Use the
        // authoritative replace primitive, which deletes all replicated
        // tables first, then replays the snapshot commits and restores the
        // leader RV. (finding.md H1 / P0 cluster.db divergence.)
        self.backend
            .replace_replicated_resource_state(data.commits, data.current_rv, None)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_state_machine(AnyError::error(e.to_string())),
            })?;
        if let Some(id) = meta.last_log_id {
            self.write_last_applied(id).await?;
        }
        let stored =
            StoredMembership::new(meta.last_log_id, meta.last_membership.membership().clone());
        self.write_membership(&stored).await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(Some(self.build_current_snapshot().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::{DbExecutor, opener};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::{Entry, EntryPayload, LeaderId, Membership};
    use std::collections::BTreeSet;

    async fn fresh_sm() -> SqliteRaftStateMachine {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor.clone(),
            "sqlite:raft-sm-test-node",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(
            SqliteNodeLocalDb::from_executor(node_executor).expect("create node-local db"),
        );
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        SqliteRaftStateMachine::new(backend, node_local, "test-node".into())
    }

    #[tokio::test]
    async fn applied_state_starts_empty() {
        let mut sm = fresh_sm().await;
        let (last, m) = sm.applied_state().await.unwrap();
        assert!(last.is_none());
        assert!(m.log_id().is_none());
    }

    #[tokio::test]
    async fn apply_blank_entry_advances_last_applied() {
        let mut sm = fresh_sm().await;
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(1, 10), 1),
            payload: EntryPayload::Blank,
        };
        let out = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(out.len(), 1);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn apply_rejects_entry_with_lower_term_than_last_applied() {
        // P3-8: defensive fence so a stale leader cannot rewrite history
        // even if the consensus layer failed to filter it out.
        let mut sm = fresh_sm().await;
        let high_term_entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(5, 10), 1),
            payload: EntryPayload::Blank,
        };
        sm.apply(vec![high_term_entry])
            .await
            .expect("first apply at term 5 succeeds");
        let stale_entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 2),
            payload: EntryPayload::Blank,
        };
        let err = sm
            .apply(vec![stale_entry])
            .await
            .expect_err("stale-term entry must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("stale-term apply rejected"),
            "error should mention stale-term fence, got: {msg}"
        );
        // last_applied remains at the term-5 entry.
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().leader_id.term, 5);
    }

    #[tokio::test]
    async fn apply_accepts_same_or_higher_term_entries() {
        let mut sm = fresh_sm().await;
        let t1 = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(2, 10), 1),
            payload: EntryPayload::Blank,
        };
        sm.apply(vec![t1]).await.unwrap();
        let t1_same = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(2, 10), 2),
            payload: EntryPayload::Blank,
        };
        sm.apply(vec![t1_same])
            .await
            .expect("same-term entry must be accepted");
        let t1_higher = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(7, 20), 3),
            payload: EntryPayload::Blank,
        };
        sm.apply(vec![t1_higher])
            .await
            .expect("higher-term entry must be accepted");
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().leader_id.term, 7);
    }

    async fn build_sm_with_backend(backend: Arc<dyn DatastoreBackend>) -> SqliteRaftStateMachine {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_executor = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor.clone(),
            "sqlite:raft-snapshot-test-node",
        )
        .await
        .expect("open node-local executor");
        let node_local = Arc::new(
            SqliteNodeLocalDb::from_executor(node_executor).expect("create node-local db"),
        );
        SqliteRaftStateMachine::new(backend, node_local, "snap-test".into())
    }

    #[tokio::test]
    async fn snapshot_round_trip_replays_namespaces_and_resources() {
        // Populate a "leader" backend with one namespace + one Pod.
        let backend_src: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_src
            .create_namespace(
                "snap-ns",
                serde_json::json!({
                    "metadata": {"name": "snap-ns", "uid": "uid-ns"}
                }),
            )
            .await
            .expect("create namespace");
        backend_src
            .create_resource(
                "v1",
                "Pod",
                Some("snap-ns"),
                "snap-pod",
                serde_json::json!({
                    "metadata": {"name": "snap-pod", "namespace": "snap-ns", "uid": "uid-pod"}
                }),
            )
            .await
            .expect("create resource");
        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        // Advance last_applied so the snapshot meta is non-trivial.
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(4, 10), 42),
            payload: EntryPayload::Blank,
        };
        sm_src.apply(vec![entry]).await.unwrap();

        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        let snapshot_bytes = snapshot.snapshot.clone().into_inner();
        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 42);
        assert!(
            !snapshot_bytes.is_empty(),
            "snapshot bytes must contain payload"
        );

        // Install on a fresh "follower" backend that starts empty.
        let backend_dst: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install snapshot");

        // Verify the dst backend now carries the same namespace + pod.
        let namespaces = backend_dst.list_namespaces(None, None).await.unwrap();
        assert!(
            namespaces.items.iter().any(|ns| ns.name == "snap-ns"),
            "namespace must be replayed into dst backend"
        );
        let pod = backend_dst
            .get_resource("v1", "Pod", Some("snap-ns"), "snap-pod")
            .await
            .unwrap();
        assert!(pod.is_some(), "pod must be replayed into dst backend");

        // last_applied must move forward on the destination, and the
        // current snapshot must be retrievable for outbound transfer.
        let (last_dst, _) = sm_dst.applied_state().await.unwrap();
        assert_eq!(last_dst.unwrap().index, 42);
        let cur = sm_dst.get_current_snapshot().await.unwrap();
        assert!(
            cur.is_some(),
            "installed snapshot must be cached for future outgoing transfer"
        );
        assert_eq!(cur.unwrap().meta.last_log_id.unwrap().index, 42);
    }

    /// finding.md H1 / P0 cluster.db divergence: installing a leader snapshot
    /// must atomically REPLACE the local replicated state, not merge snapshot
    /// commits over it. A follower/learner that holds a `stale` row the leader
    /// has already deleted must end up key-identical to the leader (stale row
    /// removed) after snapshot install. Previously this looped
    /// `apply_log_apply_commit` over the existing store (merge-only), so stale
    /// rows survived forever and members silently diverged under loss.
    #[tokio::test]
    async fn install_snapshot_replaces_local_state_and_removes_stale_rows() {
        // Leader: namespace snap-ns + ConfigMap `live`.
        let backend_src: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_src
            .create_namespace(
                "snap-ns",
                serde_json::json!({"metadata": {"name": "snap-ns"}}),
            )
            .await
            .expect("create leader namespace");
        backend_src
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "live",
                serde_json::json!({
                    "metadata": {"name": "live", "namespace": "snap-ns", "uid": "uid-live"}
                }),
            )
            .await
            .expect("create leader live resource");

        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        // Advance last_applied so the snapshot meta is non-trivial.
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(4, 10), 42),
            payload: EntryPayload::Blank,
        };
        sm_src.apply(vec![entry]).await.unwrap();
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder
            .build_snapshot()
            .await
            .expect("build leader snapshot");
        let snapshot_bytes = snapshot.snapshot.clone().into_inner();

        // Follower: same namespace + `live`, PLUS a stale `stale` ConfigMap
        // that the leader has already deleted. This is the divergent member.
        let backend_dst: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_dst
            .create_namespace(
                "snap-ns",
                serde_json::json!({"metadata": {"name": "snap-ns"}}),
            )
            .await
            .expect("create follower namespace");
        backend_dst
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "live",
                serde_json::json!({
                    "metadata": {"name": "live", "namespace": "snap-ns", "uid": "uid-live"}
                }),
            )
            .await
            .expect("create follower live resource");
        backend_dst
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-ns"),
                "stale",
                serde_json::json!({
                    "metadata": {"name": "stale", "namespace": "snap-ns", "uid": "uid-stale"}
                }),
            )
            .await
            .expect("seed stale follower resource absent from leader snapshot");

        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install leader snapshot onto divergent follower");

        // The destination must now be key-identical to the leader: the stale
        // row must be GONE, not merged. Compare by identity + resourceVersion,
        // ignoring `creationTimestamp` (a server-set field that legitimately
        // differs microsecond-to-microsecond when the same object is created
        // independently on two in-memory backends during the test setup).
        let dst_fp = resource_identity_fingerprint(backend_dst.as_ref()).await;
        let leader_fp_id = resource_identity_fingerprint(backend_src.as_ref()).await;
        assert_eq!(
            dst_fp, leader_fp_id,
            "install_snapshot must replace (not merge) local state: the stale row must be removed so the follower's resource identity set matches the leader snapshot"
        );
        // Spot-check the stale row is truly gone.
        let stale = backend_dst
            .get_resource("v1", "ConfigMap", Some("snap-ns"), "stale")
            .await
            .expect("get stale resource");
        assert!(
            stale.is_none(),
            "stale row absent from the leader snapshot must be removed on install_snapshot; got {stale:?}"
        );
    }

    /// Identity + resourceVersion fingerprint, ignoring server-set volatile
    /// fields (`creationTimestamp`) that can legitimately differ when the same
    /// object is created independently on two backends during test setup.
    async fn resource_identity_fingerprint(
        backend: &dyn DatastoreBackend,
    ) -> Vec<(String, String, Option<String>, String, String, i64)> {
        let full = resource_fingerprint(backend).await;
        full.into_iter()
            .map(|(api_version, kind, namespace, name, uid, rv, mut data)| {
                // Drop creationTimestamp so the comparison is over stable
                // identity + version, not the moment a backend wrote the row.
                if let Some(obj) = data.as_object_mut() {
                    if let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                        meta.remove("creationTimestamp");
                    }
                }
                let _ = data; // creationTimestamp removed; remaining body ignored for identity
                (api_version, kind, namespace, name, uid, rv)
            })
            .collect()
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_resources_and_rv_counter() {
        let backend_src: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        backend_src
            .create_namespace(
                "snap-rich",
                serde_json::json!({
                    "metadata": {
                        "name": "snap-rich",
                        "uid": "uid-rich-ns",
                        "creationTimestamp": "2026-05-14T00:00:00Z"
                    },
                    "labels": {"purpose": "snapshot"}
                }),
            )
            .await
            .expect("create namespace");
        let cm = backend_src
            .create_resource(
                "v1",
                "ConfigMap",
                Some("snap-rich"),
                "settings",
                serde_json::json!({
                    "metadata": {
                        "name": "settings",
                        "namespace": "snap-rich",
                        "uid": "uid-settings"
                    },
                    "data": {"mode": "initial"}
                }),
            )
            .await
            .expect("create configmap");
        backend_src
            .update_resource(
                "v1",
                "ConfigMap",
                Some("snap-rich"),
                "settings",
                serde_json::json!({
                    "metadata": {
                        "name": "settings",
                        "namespace": "snap-rich",
                        "uid": cm.uid
                    },
                    "data": {"mode": "updated"}
                }),
                cm.resource_version,
            )
            .await
            .expect("update configmap");
        backend_src
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-snap",
                serde_json::json!({
                    "metadata": {"name": "worker-snap", "uid": "uid-worker-snap"},
                    "status": {"conditions": []}
                }),
            )
            .await
            .expect("create node");
        let current_rv = backend_src
            .advance_resource_version_after(64)
            .await
            .expect("advance rv past last object");
        let leader_fingerprint = resource_fingerprint(backend_src.as_ref()).await;

        let mut sm_src = build_sm_with_backend(backend_src.clone()).await;
        sm_src
            .apply(vec![Entry::<TypeConfig> {
                log_id: LogId::new(LeaderId::new(7, 10), current_rv as u64),
                payload: EntryPayload::Blank,
            }])
            .await
            .expect("advance last_applied");
        let mut builder = sm_src.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        let snapshot_bytes = snapshot.snapshot.into_inner();

        let backend_dst: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm_dst = build_sm_with_backend(backend_dst.clone()).await;
        sm_dst
            .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("install snapshot");

        assert_eq!(
            resource_fingerprint(backend_dst.as_ref()).await,
            leader_fingerprint,
            "snapshot install must preserve live Kubernetes resources exactly as read through the API boundary"
        );
        assert_eq!(
            backend_dst.get_current_resource_version().await.unwrap(),
            current_rv,
            "snapshot install must restore the leader RV counter even when it is ahead of object RVs"
        );
    }

    #[tokio::test]
    async fn get_current_snapshot_builds_fresh_snapshot_when_missing() {
        let mut sm = fresh_sm().await;
        let first = sm
            .get_current_snapshot()
            .await
            .expect("fresh bootstrap should return snapshot")
            .expect("snapshot should be present");
        assert!(
            first.meta.snapshot_id.starts_with("raft-snapshot"),
            "snapshot should carry a synthetic id, got {}",
            first.meta.snapshot_id
        );
        let first_payload = first.snapshot.get_ref().clone();
        assert!(
            !first_payload.is_empty(),
            "generated snapshot should be non-empty"
        );

        let cached = sm
            .node_local
            .raft_meta_get(META_KEY_CURRENT_SNAPSHOT)
            .await
            .expect("node-local read should work");
        assert!(
            cached.is_none(),
            "current snapshots are rebuildable and must not be cached as full blobs in node-local storage"
        );

        let second = sm
            .get_current_snapshot()
            .await
            .expect("cached snapshot lookup should succeed")
            .expect("snapshot should still be present");
        assert_eq!(
            second.snapshot.get_ref(),
            &first_payload,
            "subsequent reads should rebuild deterministic snapshot bytes"
        );
    }

    async fn resource_fingerprint(
        backend: &dyn DatastoreBackend,
    ) -> Vec<(
        String,
        String,
        Option<String>,
        String,
        String,
        i64,
        serde_json::Value,
    )> {
        let mut rows = Vec::new();
        let namespaces = backend
            .list_namespaces(None, None)
            .await
            .expect("list namespaces");
        for namespace in namespaces.items {
            rows.push(resource_fingerprint_row(namespace));
        }
        for namespace in backend
            .list_namespaces(None, None)
            .await
            .expect("list namespaces for namespaced resources")
            .items
        {
            for resource in backend
                .list_namespace_resources(&namespace.name)
                .await
                .expect("list namespace resources")
            {
                rows.push(resource_fingerprint_row(resource));
            }
        }
        for resource in backend
            .list_cluster_resources()
            .await
            .expect("list cluster resources")
        {
            rows.push(resource_fingerprint_row(resource));
        }
        rows.sort_by(|a, b| (&a.0, &a.1, &a.2, &a.3).cmp(&(&b.0, &b.1, &b.2, &b.3)));
        rows
    }

    fn resource_fingerprint_row(
        resource: crate::datastore::types::Resource,
    ) -> (
        String,
        String,
        Option<String>,
        String,
        String,
        i64,
        serde_json::Value,
    ) {
        (
            resource.api_version,
            resource.kind,
            resource.namespace,
            resource.name,
            resource.uid,
            resource.resource_version,
            crate::api::inject_resource_version(resource.data, resource.resource_version),
        )
    }

    #[tokio::test]
    async fn apply_normal_entry_decodes_log_apply_commit_and_mutates_backend() {
        // T1.3: state machine apply must decode EntryPayload::Normal as a
        // `LogApplyCommit` protobuf and run it through
        // `backend.apply_log_apply_commit`. After apply, the cluster.db
        // row produced by the PutResource mutation must be visible to a
        // `get_resource` read on the same backend.
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let mut sm = build_sm_with_backend(backend.clone()).await;

        let commit = crate::log_apply::LogApplyCommit::new(
            1,
            vec![crate::log_apply::LogApplyMutation::PutResource(
                crate::log_apply::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "from-raft".to_string(),
                    uid: "cm-uid-1".to_string(),
                    resource_version: 1,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "from-raft",
                            "namespace": "default",
                            "uid": "cm-uid-1",
                            "resourceVersion": "1"
                        },
                        "data": {"k": "v"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            )],
        );
        let payload_bytes =
            crate::log_apply::encode_commit_protobuf(&commit).expect("encode LogApplyCommit");

        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 1),
            payload: EntryPayload::Normal(
                crate::datastore::raft::types::StorageCommandPayload::from_bytes(payload_bytes),
            ),
        };
        let results = sm
            .apply(vec![entry])
            .await
            .expect("apply EntryPayload::Normal LogApplyCommit");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].applied_rv,
            Some(1),
            "state machine must return the commit's resource_version as applied_rv"
        );

        let row = backend
            .get_resource("v1", "ConfigMap", Some("default"), "from-raft")
            .await
            .expect("get_resource ok")
            .expect("PutResource mutation must materialize the row");
        assert_eq!(row.uid, "cm-uid-1");
    }

    #[tokio::test]
    async fn apply_normal_entry_stamps_provisional_rv_after_current_store_rv() {
        let backend: Arc<dyn DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let snapshot_rv = backend
            .advance_resource_version_after(100)
            .await
            .expect("establish a list snapshot rv above the raft log index");

        let commit = crate::log_apply::LogApplyCommit::new(
            0,
            vec![crate::log_apply::LogApplyMutation::PutResource(
                crate::log_apply::LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "provisional-rv".to_string(),
                    uid: "cm-uid-provisional".to_string(),
                    resource_version: 0,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "provisional-rv",
                            "namespace": "default",
                            "uid": "cm-uid-provisional"
                        },
                        "data": {"k": "v"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                },
            )],
        );
        let payload_bytes =
            crate::log_apply::encode_commit_protobuf(&commit).expect("encode LogApplyCommit");
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 42),
            payload: EntryPayload::Normal(
                crate::datastore::raft::types::StorageCommandPayload::from_bytes(payload_bytes),
            ),
        };

        let mut sm = build_sm_with_backend(backend.clone()).await;
        let results = sm
            .apply(vec![entry])
            .await
            .expect("apply provisional LogApplyCommit");
        assert_eq!(results[0].applied_rv, Some(snapshot_rv + 1));
        let row = backend
            .get_resource("v1", "ConfigMap", Some("default"), "provisional-rv")
            .await
            .expect("get_resource ok")
            .expect("PutResource mutation must materialize the row");
        assert_eq!(row.resource_version, snapshot_rv + 1);
        let expected_rv = (snapshot_rv + 1).to_string();
        assert_eq!(
            row.data
                .pointer("/metadata/resourceVersion")
                .and_then(|value| value.as_str()),
            Some(expected_rv.as_str())
        );
    }

    #[tokio::test]
    async fn apply_membership_entry_stores_membership() {
        let mut sm = fresh_sm().await;
        let voters: BTreeSet<NodeId> = [10u64, 20, 30].into_iter().collect();
        let m: Membership<NodeId, openraft::BasicNode> = Membership::new(vec![voters], None);
        let entry = Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(2, 10), 7),
            payload: EntryPayload::Membership(m),
        };
        sm.apply(vec![entry]).await.unwrap();
        let (last, stored_m) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 7);
        assert_eq!(stored_m.membership().voter_ids().count(), 3);
        assert_eq!(stored_m.log_id().unwrap().index, 7);
    }
}
