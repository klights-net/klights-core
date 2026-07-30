//! Embedded leader-authority and controller-coordination adapters.

use std::sync::Arc;

use klights_leader_api::{
    AuthorityAcquireFuture, AuthorityError, AuthorityPermit, AuthorityPermitIssuer,
    AuthorityRevocationFuture, AuthorityRoute, ControllerAcquireFuture, ControllerCoordination,
    ControllerCoordinationError, ControllerLease, ControllerRevocationFuture, ControllerScope,
    LeaderAuthority,
};
use openraft::Raft;

use crate::types::{NodeId, TypeConfig};

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityState {
    generation: u64,
    local: bool,
    endpoint: Option<String>,
}

pub struct AuthorityPublisher {
    sender: tokio::sync::watch::Sender<AuthorityState>,
}

impl AuthorityPublisher {
    pub fn publish(&self, local: bool, endpoint: Option<String>) {
        let generation = self.sender.borrow().generation.checked_add(1).unwrap_or(1);
        self.sender.send_replace(AuthorityState {
            generation,
            local,
            endpoint,
        });
    }
}

pub struct WatchLeaderAuthority {
    receiver: tokio::sync::watch::Receiver<AuthorityState>,
    issuer: AuthorityPermitIssuer,
}

impl WatchLeaderAuthority {
    pub fn channel(local: bool, endpoint: Option<String>) -> (Arc<Self>, AuthorityPublisher) {
        let (sender, receiver) = tokio::sync::watch::channel(AuthorityState {
            generation: 1,
            local,
            endpoint,
        });
        (
            Arc::new(Self {
                receiver,
                issuer: AuthorityPermitIssuer::new(),
            }),
            AuthorityPublisher { sender },
        )
    }
}

impl LeaderAuthority for WatchLeaderAuthority {
    fn route(&self) -> AuthorityRoute {
        let state = self.receiver.borrow();
        if state.local {
            AuthorityRoute::Local(self.issuer.issue(state.generation))
        } else if let Some(endpoint) = state.endpoint.clone() {
            AuthorityRoute::Forward { endpoint }
        } else {
            AuthorityRoute::Unavailable
        }
    }

    fn validate(&self, permit: &AuthorityPermit) -> Result<(), AuthorityError> {
        let state = self.receiver.borrow();
        if !state.local {
            Err(AuthorityError::NotAuthoritative)
        } else {
            self.issuer.validate(permit, state.generation)
        }
    }

    fn acquire(&self) -> AuthorityAcquireFuture<'_> {
        let mut receiver = self.receiver.clone();
        Box::pin(async move {
            loop {
                let state = receiver.borrow_and_update().clone();
                if state.local {
                    return Ok(self.issuer.issue(state.generation));
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| AuthorityError::Closed)?;
            }
        })
    }

    fn wait_for_revocation<'a>(
        &'a self,
        permit: &'a AuthorityPermit,
    ) -> AuthorityRevocationFuture<'a> {
        let mut receiver = self.receiver.clone();
        let permit = permit.clone();
        Box::pin(async move {
            loop {
                let revoked = {
                    let state = receiver.borrow();
                    !state.local || self.issuer.validate(&permit, state.generation).is_err()
                };
                if revoked || receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

pub struct RaftLeaderLease {
    raft: Raft<TypeConfig>,
    node_id: NodeId,
}

struct RaftControllerFence {
    term: u64,
}

impl RaftLeaderLease {
    pub fn new(raft: Raft<TypeConfig>, node_id: NodeId) -> Self {
        Self { raft, node_id }
    }

    fn current_generation(&self) -> u64 {
        self.raft.metrics().borrow().vote.leader_id().get_term()
    }

    fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.node_id)
    }
}

impl ControllerCoordination for RaftLeaderLease {
    fn try_acquire(
        &self,
        scope: ControllerScope,
    ) -> Result<ControllerLease, ControllerCoordinationError> {
        if self.is_leader() {
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
            let mut metrics = self.raft.metrics();
            loop {
                let generation = {
                    let current = metrics.borrow_and_update();
                    if current.current_leader == Some(self.node_id) {
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
        if !self.is_leader() {
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
        let mut metrics = self.raft.metrics();
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
                    current.current_leader != Some(self.node_id)
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

    fn assert_authority_object_safe(_: &dyn LeaderAuthority) {}
    fn assert_coordination_object_safe(_: &dyn ControllerCoordination) {}

    #[test]
    fn focused_authority_contracts_remain_object_safe() {
        let _authority: fn(&dyn LeaderAuthority) = assert_authority_object_safe;
        let _coordination: fn(&dyn ControllerCoordination) = assert_coordination_object_safe;
    }

    #[test]
    fn watch_authority_rejects_stale_permits() {
        let (authority, publisher) = WatchLeaderAuthority::channel(true, None);
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("expected local permit");
        };
        assert!(authority.validate(&permit).is_ok());
        publisher.publish(false, Some("https://leader.example".to_string()));
        assert_eq!(
            authority.validate(&permit),
            Err(AuthorityError::NotAuthoritative)
        );
    }
}
