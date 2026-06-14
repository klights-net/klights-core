//! Leader election abstraction for HA control plane.
//!
//! Cluster-wide controllers (Deployment, ReplicaSet, Job, CronJob, GC) must run
//! on exactly one replica. This module provides an object-safe trait that
//! abstracts leader election, allowing single-node deployments to run without
//! coordination while HA deployments can use etcd or Raft-based election.
//!
//! ## Cancellation semantics
//!
//! The `LeaderLease::cancel` token MUST be a child of
//! `TaskSupervisor::root_cancellation_token()` so that supervisor shutdown
//! cancels every leader-held lease, and lease loss (drop or explicit cancel)
//! cancels every controller task spawned under that lease.

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub mod lease_loop;
pub use lease_loop::run_under_lease;

/// Scope for leader election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderScope {
    /// Cluster-wide leadership (controllers like GC, Deployment, etc.)
    Cluster,
    /// Namespace-scoped leadership (future: namespace-scoped controllers)
    Namespace(String),
}

/// Error type for leader election operations.
#[derive(Debug, Error)]
pub enum LeaderError {
    #[error("failed to acquire lease for scope {0:?}")]
    AcquireFailed(LeaderScope),
    #[error("lease lost for scope {0:?}")]
    LeaseLost(LeaderScope),
    #[error("leader election backend error: {0}")]
    Backend(String),
}

/// A held leader lease.
///
/// Dropping the lease or cancelling its token releases leadership.
#[derive(Debug)]
pub struct LeaderLease {
    /// The scope this lease covers.
    pub scope: LeaderScope,
    /// Cancellation token that stops every controller task acquired under this lease.
    ///
    /// This token MUST be a child of `TaskSupervisor::root_cancellation_token()`.
    /// Cancelling this token stops every controller task acquired under this lease;
    /// dropping the lease cancels the token.
    pub cancel: CancellationToken,
}

/// Object-safe leader election trait.
///
/// Implementations:
/// - `RaftLeaderLease`: Tracks leadership via Raft metrics watch.
///   Works for single-node (always leader) and multi-node (election).
#[async_trait]
pub trait LeaderElection: Send + Sync {
    /// Attempt to acquire a leader lease for the given scope.
    ///
    /// Returns immediately with a lease or an error. For backends that require
    /// waiting (etcd, Raft), the implementation should spawn a background task
    /// that retries acquisition and notifies via the lease's cancellation token
    /// when leadership is lost.
    async fn acquire(&self, scope: LeaderScope) -> Result<LeaderLease, LeaderError>;
}

/// Raft-based leader election.
///
/// Tracks the current node's leadership status via
/// `RaftNode::metrics_watch()`. When the node is the Raft leader the
/// lease remains active. When leadership is lost the cancellation
/// token fires, stopping all leader-only controller tasks. On
/// re-acquisition a new lease with a fresh token is issued.
///
/// T2 step 2: replaced `SingleNodeLeader` — a single-voter raft node
/// is always its own leader, so `acquire` succeeds immediately in
/// single-node deployments just like the old always-return-lease impl.
pub struct RaftLeaderLease {
    raft_node: Arc<crate::datastore::raft::node::RaftNode>,
    root_cancel: CancellationToken,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl RaftLeaderLease {
    pub fn new(
        raft_node: Arc<crate::datastore::raft::node::RaftNode>,
        root_cancel: CancellationToken,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            raft_node,
            root_cancel,
            supervisor,
        }
    }
}

#[async_trait]
impl LeaderElection for RaftLeaderLease {
    async fn acquire(&self, scope: LeaderScope) -> Result<LeaderLease, LeaderError> {
        let shape = self.raft_node.current_shape();
        if !shape.is_leader {
            return Err(LeaderError::AcquireFailed(scope));
        }
        let cancel = self.root_cancel.child_token();
        let raft = self.raft_node.clone();
        let cancel_clone = cancel.clone();
        let scope_clone = scope.clone();
        let supervisor = self.supervisor.clone();
        // Spawn a supervised background watcher that cancels the lease
        // token when this node loses Raft leadership.
        let _ = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "raft_leader_lease_watcher",
                async move {
                    // Deduped server-metrics: only wakes on real
                    // state/leadership/membership changes, so this watcher
                    // stays idle-silent (HR #1) instead of firing every tick.
                    let mut metrics = raft.server_metrics_watch();
                    loop {
                        if metrics.changed().await.is_err() {
                            cancel_clone.cancel();
                            return;
                        }
                        if !raft.current_shape().is_leader {
                            tracing::info!(?scope_clone, "raft leadership lost, cancelling lease");
                            cancel_clone.cancel();
                            return;
                        }
                    }
                },
            )
            .await;
        Ok(LeaderLease { scope, cancel })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // T2 step 3: SingleNodeLeader deleted. Trait-contract tests now
    // use MockLeaderElection (from lease_loop) or RaftLeaderLease.

    #[test]
    fn leader_election_trait_is_object_safe() {
        // Prove the trait is still object-safe by casting a concrete
        // impl to `Arc<dyn LeaderElection>`.
        use crate::leader_election::lease_loop::MockLeaderElection;
        let mock = MockLeaderElection::new(CancellationToken::new());
        let leader: Arc<dyn LeaderElection> = mock;
        let _leader: Arc<dyn LeaderElection> = leader;
    }

    // T2: RaftLeaderLease tests
    use crate::datastore::node_local::SqliteNodeLocalDb;
    use crate::datastore::raft::node::RaftNode;
    use crate::datastore::sqlite::{DbExecutor, opener};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    async fn fresh_raft_node(id: u64) -> RaftNode {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let exec = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            supervisor,
            "sqlite:raft-leader-test",
        )
        .await
        .expect("open node-local executor");
        let node_local =
            Arc::new(SqliteNodeLocalDb::from_executor(exec).expect("create node-local db"));
        let backend: Arc<dyn crate::datastore::DatastoreBackend> =
            Arc::new(crate::datastore::test_support::in_memory().await);
        RaftNode::start(id, format!("n{id}"), backend, node_local)
            .await
            .expect("start raft node")
    }

    #[tokio::test]
    async fn raft_leader_lease_acquires_when_leader() {
        let node = Arc::new(fresh_raft_node(80).await);
        node.bootstrap_single_voter("https://10.99.0.80:7679".into())
            .await
            .expect("bootstrap");
        // Wait for self-election.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if node.current_shape().is_leader {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("node did not become leader");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let sup = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let election = RaftLeaderLease::new(node.clone(), CancellationToken::new(), sup);
        let lease = election
            .acquire(LeaderScope::Cluster)
            .await
            .expect("should acquire lease when leader");
        assert!(!lease.cancel.is_cancelled());
        // T3: the lease holds a supervised background task that watches
        // for leadership loss. Clean up by cancelling the lease, which
        // drops the background task's Arc reference.
        drop(lease);
        drop(election);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn raft_leader_lease_fails_when_not_leader() {
        let node = fresh_raft_node(81).await;
        // Not bootstrapped, so not leader.
        let sup = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let election = RaftLeaderLease::new(Arc::new(node), CancellationToken::new(), sup);
        let err = election
            .acquire(LeaderScope::Cluster)
            .await
            .expect_err("should fail when not leader");
        assert!(matches!(
            err,
            LeaderError::AcquireFailed(LeaderScope::Cluster)
        ));
        drop(election);
    }
}
