//! Embedded controller-coordination adapter.
//!
//! Controller owners consume only the backend-neutral
//! [`klights_leader_api::ControllerCoordination`] contract. Root constructs
//! this adapter from the selected cluster engine's authority capability.

use std::sync::Arc;

use klights_leader_api::{
    ControllerAcquireFuture, ControllerCoordination, ControllerCoordinationError, ControllerLease,
    ControllerRevocationFuture, ControllerScope,
};

pub mod lease_loop;
pub use lease_loop::run_under_lease;

/// Embedded-Raft controller lease adapter.
///
/// The adapter deliberately owns no controller implementation and exports no
/// Raft value. Its opaque generation fence is validated by the injected
/// authority provider, including across demotion/promotion ABA transitions.
pub struct RaftLeaderLease {
    raft_node: Arc<crate::datastore::raft::node::RaftNode>,
}

struct RaftControllerFence {
    term: u64,
}

impl RaftLeaderLease {
    pub fn new(raft_node: Arc<crate::datastore::raft::node::RaftNode>) -> Self {
        Self { raft_node }
    }

    fn current_generation(&self) -> u64 {
        self.raft_node
            .server_metrics_watch()
            .borrow()
            .vote
            .leader_id()
            .get_term()
    }
}

impl ControllerCoordination for RaftLeaderLease {
    fn try_acquire(
        &self,
        scope: ControllerScope,
    ) -> Result<ControllerLease, ControllerCoordinationError> {
        if self.raft_node.current_shape().is_leader {
            Ok(ControllerLease::issue(
                scope,
                RaftControllerFence {
                    term: self.current_generation(),
                },
            ))
        } else {
            Err(ControllerCoordinationError::Unavailable)
        }
    }

    fn acquire(&self, scope: ControllerScope) -> ControllerAcquireFuture<'_> {
        Box::pin(async move {
            let mut metrics = self.raft_node.server_metrics_watch();
            loop {
                let generation = {
                    let current = metrics.borrow_and_update();
                    if current.current_leader == Some(self.raft_node.node_id) {
                        Some(current.vote.leader_id().get_term())
                    } else {
                        None
                    }
                };
                if let Some(generation) = generation {
                    return Ok(ControllerLease::issue(
                        scope,
                        RaftControllerFence { term: generation },
                    ));
                }
                metrics
                    .changed()
                    .await
                    .map_err(|_| ControllerCoordinationError::Closed)?;
            }
        })
    }

    fn validate(&self, lease: &ControllerLease) -> Result<(), ControllerCoordinationError> {
        if !self.raft_node.current_shape().is_leader {
            Err(ControllerCoordinationError::Unavailable)
        } else if lease
            .adapter_fence::<RaftControllerFence>()
            .is_none_or(|fence| fence.term != self.current_generation())
        {
            Err(ControllerCoordinationError::StalePermit)
        } else {
            Ok(())
        }
    }

    fn wait_for_revocation<'a>(
        &'a self,
        lease: &'a ControllerLease,
    ) -> ControllerRevocationFuture<'a> {
        let mut metrics = self.raft_node.server_metrics_watch();
        let Some(generation) = lease
            .adapter_fence::<RaftControllerFence>()
            .map(|fence| fence.term)
        else {
            return Box::pin(std::future::ready(()));
        };
        Box::pin(async move {
            loop {
                let revoked = {
                    let current = metrics.borrow_and_update();
                    current.current_leader != Some(self.raft_node.node_id)
                        || current.vote.leader_id().get_term() != generation
                };
                if revoked || metrics.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_object_safe(_: &dyn ControllerCoordination) {}

    #[test]
    fn raft_adapter_implements_neutral_coordination_contract() {
        fn assert_impl<T: ControllerCoordination>() {}
        assert_impl::<RaftLeaderLease>();
        let _object_safe: fn(&dyn ControllerCoordination) = assert_object_safe;
    }
}
