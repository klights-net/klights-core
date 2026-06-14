//! T1.5: object-safe `MembershipClient` trait that abstracts the
//! membership-change surface of openraft (`add_learner`, `add_voter`,
//! `remove_voter`). Higher-level join orchestration depends on this
//! trait rather than reaching into a concrete `RaftNode`, so:
//!
//! - The production adapter (`RaftNodeMembershipClient`) delegates to
//!   `RaftNode::{add_learner_only, add_voter, remove_voter}`.
//! - Tests use `MockMembershipClient` to drive learner-join and
//!   leader-change-retry paths without a real openraft cluster or
//!   socket.

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::datastore::raft::types::NodeId;

/// Object-safe membership-change client. Implementations may live on a
/// raft leader (mutating the local engine) or on a follower that
/// transparently forwards through an RPC; callers don't need to know.
#[async_trait]
pub trait MembershipClient: Send + Sync {
    /// Add `node_id` as a learner — receives `AppendEntries` and applies
    /// committed entries but does not vote and does not count toward
    /// voter quorum. Idempotent.
    async fn add_learner(&self, node_id: NodeId, addr: String) -> Result<()>;

    /// Promote `node_id` to a voter (issuing `add_learner` first if
    /// needed). Idempotent.
    async fn add_voter(&self, node_id: NodeId, addr: String) -> Result<()>;

    /// Remove `node_id` from the voter set. Idempotent if the target is
    /// already absent.
    async fn remove_voter(&self, node_id: NodeId) -> Result<()>;
}

/// Production adapter: delegates every method to the local `RaftNode`.
/// Only useful on a node that is the current raft leader; non-leader
/// callers should use a forwarding client (added in a later sub-task).
pub struct RaftNodeMembershipClient {
    node: Arc<crate::datastore::raft::node::RaftNode>,
}

impl RaftNodeMembershipClient {
    pub fn new(node: Arc<crate::datastore::raft::node::RaftNode>) -> Self {
        Self { node }
    }
}

#[async_trait]
impl MembershipClient for RaftNodeMembershipClient {
    async fn add_learner(&self, node_id: NodeId, addr: String) -> Result<()> {
        self.node.add_learner_only(node_id, addr).await
    }

    async fn add_voter(&self, node_id: NodeId, addr: String) -> Result<()> {
        self.node.add_voter(node_id, addr).await
    }

    async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
        self.node.remove_voter(node_id).await
    }
}

#[cfg(test)]
pub(crate) mod mock {
    //! Mock `MembershipClient` used in unit tests. Records every call and
    //! supports scripted "fail-then-succeed" sequences so leader-change
    //! retry behavior can be exercised without a real cluster.

    use super::*;
    use std::sync::Mutex;

    #[derive(Clone, Debug, PartialEq)]
    pub enum MockCall {
        AddLearner { node_id: NodeId, addr: String },
        AddVoter { node_id: NodeId, addr: String },
        RemoveVoter { node_id: NodeId },
    }

    #[derive(Default)]
    struct MockState {
        calls: Vec<MockCall>,
        /// Per-method queue of scripted outcomes. Each call pops the
        /// next outcome; if the queue is empty the call defaults to Ok.
        add_learner_outcomes: std::collections::VecDeque<Result<()>>,
        add_voter_outcomes: std::collections::VecDeque<Result<()>>,
        remove_voter_outcomes: std::collections::VecDeque<Result<()>>,
    }

    #[derive(Clone, Default)]
    pub struct MockMembershipClient {
        state: Arc<Mutex<MockState>>,
    }

    impl MockMembershipClient {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn queue_add_learner(&self, outcome: Result<()>) {
            self.state
                .lock()
                .unwrap()
                .add_learner_outcomes
                .push_back(outcome);
        }

        pub fn queue_add_voter(&self, outcome: Result<()>) {
            self.state
                .lock()
                .unwrap()
                .add_voter_outcomes
                .push_back(outcome);
        }

        pub fn queue_remove_voter(&self, outcome: Result<()>) {
            self.state
                .lock()
                .unwrap()
                .remove_voter_outcomes
                .push_back(outcome);
        }

        pub fn calls(&self) -> Vec<MockCall> {
            self.state.lock().unwrap().calls.clone()
        }
    }

    #[async_trait]
    impl MembershipClient for MockMembershipClient {
        async fn add_learner(&self, node_id: NodeId, addr: String) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(MockCall::AddLearner {
                node_id,
                addr: addr.clone(),
            });
            state.add_learner_outcomes.pop_front().unwrap_or(Ok(()))
        }

        async fn add_voter(&self, node_id: NodeId, addr: String) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(MockCall::AddVoter {
                node_id,
                addr: addr.clone(),
            });
            state.add_voter_outcomes.pop_front().unwrap_or(Ok(()))
        }

        async fn remove_voter(&self, node_id: NodeId) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            state.calls.push(MockCall::RemoveVoter { node_id });
            state.remove_voter_outcomes.pop_front().unwrap_or(Ok(()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mock::{MockCall, MockMembershipClient};
    use super::*;

    #[tokio::test]
    async fn learner_join_records_call_against_mock() {
        // Callers depend on `Arc<dyn MembershipClient>` in production;
        // the test holds a parallel typed handle to inspect recorded
        // calls without needing runtime downcasting.
        let mock = MockMembershipClient::new();
        let client: Arc<dyn MembershipClient> = Arc::new(mock.clone());
        client
            .add_learner(42, "https://10.0.0.42:7679".into())
            .await
            .expect("add_learner succeeds");
        let calls = mock.calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            MockCall::AddLearner { node_id, addr } => {
                assert_eq!(*node_id, 42);
                assert_eq!(addr, "https://10.0.0.42:7679");
            }
            other => panic!("expected AddLearner, got {other:?}"),
        }
    }

    /// T1.5: caller retries on leader-change error and the second attempt
    /// succeeds — proves the leader-change retry path is mockable without
    /// standing up a real openraft cluster.
    #[tokio::test]
    async fn add_voter_retry_after_leader_change_eventually_succeeds() {
        let mock = MockMembershipClient::new();
        mock.queue_add_voter(Err(anyhow::anyhow!(
            "ForwardToLeader: leader changed mid-call"
        )));
        mock.queue_add_voter(Ok(()));
        let client: Arc<dyn MembershipClient> = Arc::new(mock.clone());

        let first = client.add_voter(7, "https://10.0.0.7:7679".into()).await;
        assert!(first.is_err(), "first attempt must surface leader-change");
        let second = client.add_voter(7, "https://10.0.0.7:7679".into()).await;
        assert!(second.is_ok(), "retry must succeed against the new leader");
        let calls = mock.calls();
        assert_eq!(calls.len(), 2, "both attempts must be recorded");
    }

    /// T1.5: production wiring stub — verify that the trait is
    /// object-safe and that an Arc<dyn MembershipClient> can be passed
    /// across functions accepting only the trait. The body intentionally
    /// uses a closure-style call so future refactors that want to take a
    /// `&dyn MembershipClient` instead of an `Arc<...>` keep compiling.
    #[tokio::test]
    async fn membership_client_trait_is_object_safe() {
        async fn drive(client: &dyn MembershipClient) -> Result<()> {
            client.remove_voter(99).await
        }
        let mock = MockMembershipClient::new();
        let client: Arc<dyn MembershipClient> = Arc::new(mock.clone());
        drive(client.as_ref()).await.unwrap();
        assert_eq!(mock.calls().len(), 1);
    }

    /// T1.5: learner-join retry. The first add_learner attempt fails
    /// (e.g. transient network blip mid-snapshot-install); the second
    /// attempt succeeds. Production code retries through
    /// `TaskSupervisor::spawn_delay` (no raw `tokio::sleep`) but this
    /// trait-level test just proves the surface accepts the retry shape.
    #[tokio::test]
    async fn add_learner_retry_after_transient_failure_succeeds() {
        let mock = MockMembershipClient::new();
        mock.queue_add_learner(Err(anyhow::anyhow!("snapshot install interrupted")));
        mock.queue_add_learner(Ok(()));
        let client: Arc<dyn MembershipClient> = Arc::new(mock.clone());
        assert!(
            client
                .add_learner(5, "https://10.0.0.5:7679".into())
                .await
                .is_err(),
            "first attempt must surface the transient failure"
        );
        assert!(
            client
                .add_learner(5, "https://10.0.0.5:7679".into())
                .await
                .is_ok(),
            "retry must succeed"
        );
        assert_eq!(mock.calls().len(), 2);
    }

    /// T1.5: remove_voter scripted failure — proves the trait surface
    /// exposes scriptable outcomes for the remove path too. Callers
    /// (T4 demote → learner) use this to test failover-mid-demote.
    #[tokio::test]
    async fn remove_voter_scripted_failure_surfaces() {
        let mock = MockMembershipClient::new();
        mock.queue_remove_voter(Err(anyhow::anyhow!("ForwardToLeader: stepped down")));
        let client: Arc<dyn MembershipClient> = Arc::new(mock.clone());
        assert!(client.remove_voter(2).await.is_err());
        assert_eq!(mock.calls().len(), 1);
    }
}
