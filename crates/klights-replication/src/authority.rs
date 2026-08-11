//! Embedded leader-authority and controller-coordination adapters.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use klights_leader_api::{
    AuthorityAcquireFuture, AuthorityError, AuthorityPermit, AuthorityPermitIssuer,
    AuthorityRevocationFuture, AuthorityRoute, ControllerAcquireFuture, ControllerCoordination,
    ControllerCoordinationError, ControllerLease, ControllerRevocationFuture, ControllerScope,
    LeaderAuthority,
};
use openraft::Raft;

use crate::types::TypeConfig;
use klights_cluster_core::NodeId;

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityState {
    generation: u64,
    local: bool,
    endpoint: Option<String>,
}

pub struct AuthorityPublisher {
    sender: tokio::sync::watch::Sender<AuthorityState>,
    transition_gate: Arc<tokio::sync::RwLock<()>>,
    pending_transitions: Arc<AtomicUsize>,
}

impl AuthorityPublisher {
    pub async fn publish(&self, local: bool, endpoint: Option<String>) {
        let _pending = PendingTransition::begin(&self.pending_transitions);
        let _transition = self.transition_gate.write().await;
        self.sender.send_if_modified(|state| {
            if state.local == local && state.endpoint == endpoint {
                return false;
            }
            state.generation = state.generation.checked_add(1).unwrap_or(1);
            state.local = local;
            state.endpoint = endpoint;
            true
        });
    }
}

/// Adapter-owned read side of the authority transition gate.
///
/// The concrete handle is intentionally separate from [`LeaderAuthority`].
/// Its blocking acquisition is used only in the supervised crypto worker,
/// never on a Tokio runtime thread.
#[derive(Clone)]
pub struct AuthoritySigningFence {
    gate: Arc<tokio::sync::RwLock<()>>,
}

impl AuthoritySigningFence {
    pub fn blocking_read(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.gate.blocking_read()
    }
}

pub struct WatchLeaderAuthority {
    receiver: tokio::sync::watch::Receiver<AuthorityState>,
    issuer: AuthorityPermitIssuer,
    transition_gate: Arc<tokio::sync::RwLock<()>>,
    pending_transitions: Arc<AtomicUsize>,
}

struct PendingTransition {
    pending_transitions: Arc<AtomicUsize>,
}

impl PendingTransition {
    fn begin(pending_transitions: &Arc<AtomicUsize>) -> Self {
        pending_transitions.fetch_add(1, Ordering::AcqRel);
        Self {
            pending_transitions: pending_transitions.clone(),
        }
    }
}

impl Drop for PendingTransition {
    fn drop(&mut self) {
        self.pending_transitions.fetch_sub(1, Ordering::AcqRel);
    }
}

impl WatchLeaderAuthority {
    pub fn channel(local: bool, endpoint: Option<String>) -> (Arc<Self>, AuthorityPublisher) {
        let transition_gate = Arc::new(tokio::sync::RwLock::new(()));
        let pending_transitions = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = tokio::sync::watch::channel(AuthorityState {
            generation: 1,
            local,
            endpoint,
        });
        (
            Arc::new(Self {
                receiver,
                issuer: AuthorityPermitIssuer::new(),
                transition_gate: transition_gate.clone(),
                pending_transitions: pending_transitions.clone(),
            }),
            AuthorityPublisher {
                sender,
                transition_gate,
                pending_transitions,
            },
        )
    }

    pub fn signing_fence(&self) -> AuthoritySigningFence {
        AuthoritySigningFence {
            gate: self.transition_gate.clone(),
        }
    }
}

impl LeaderAuthority for WatchLeaderAuthority {
    fn route(&self) -> AuthorityRoute {
        if self.pending_transitions.load(Ordering::Acquire) != 0 {
            return AuthorityRoute::Unavailable;
        }
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
        if self.pending_transitions.load(Ordering::Acquire) != 0 {
            return Err(AuthorityError::StalePermit);
        }
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

    #[tokio::test]
    async fn watch_authority_rejects_stale_permits() {
        let (authority, publisher) = WatchLeaderAuthority::channel(true, None);
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("expected local permit");
        };
        assert!(authority.validate(&permit).is_ok());
        publisher
            .publish(false, Some("https://leader.example".to_string()))
            .await;
        assert_eq!(
            authority.validate(&permit),
            Err(AuthorityError::NotAuthoritative)
        );
    }

    #[tokio::test]
    async fn identical_authority_publication_preserves_live_permit() {
        let endpoint = Some("https://leader.example".to_string());
        let (authority, publisher) = WatchLeaderAuthority::channel(true, endpoint.clone());
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("expected local permit");
        };
        let mut revocation = authority.wait_for_revocation(&permit);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());

        publisher.publish(true, endpoint).await;

        assert!(
            authority.validate(&permit).is_ok(),
            "an unchanged authority observation must not revoke active watch permits"
        );
        assert!(
            std::future::Future::poll(revocation.as_mut(), &mut context).is_pending(),
            "the long-lived watch permit must remain active after an identical observation"
        );

        publisher
            .publish(true, Some("https://new-leader.example".to_string()))
            .await;
        assert!(
            std::future::Future::poll(revocation.as_mut(), &mut context).is_ready(),
            "a real authority change must promptly revoke the old watch permit"
        );
    }

    #[tokio::test]
    async fn changed_authority_endpoint_revokes_live_permit() {
        let (authority, publisher) =
            WatchLeaderAuthority::channel(true, Some("https://leader-a.example".to_string()));
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("expected local permit");
        };

        publisher
            .publish(true, Some("https://leader-b.example".to_string()))
            .await;

        assert_eq!(
            authority.validate(&permit),
            Err(AuthorityError::StalePermit)
        );
    }
}
