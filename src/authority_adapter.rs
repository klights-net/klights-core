use std::sync::Arc;

use klights_leader_api::{
    AuthorityAcquireFuture, AuthorityError, AuthorityPermit, AuthorityRevocationFuture,
    AuthorityRoute, LeaderAuthority, NodeRoleProjection,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityState {
    generation: u64,
    local: bool,
    endpoint: Option<String>,
}

pub(crate) struct AuthorityPublisher {
    sender: tokio::sync::watch::Sender<AuthorityState>,
}

impl AuthorityPublisher {
    pub(crate) fn publish(&self, local: bool, endpoint: Option<String>) {
        let generation = self.sender.borrow().generation.checked_add(1).unwrap_or(1);
        self.sender.send_replace(AuthorityState {
            generation,
            local,
            endpoint,
        });
    }
}

pub(crate) struct WatchLeaderAuthority {
    receiver: tokio::sync::watch::Receiver<AuthorityState>,
}

impl WatchLeaderAuthority {
    pub(crate) fn channel(
        local: bool,
        endpoint: Option<String>,
    ) -> (Arc<Self>, AuthorityPublisher) {
        let (sender, receiver) = tokio::sync::watch::channel(AuthorityState {
            generation: 1,
            local,
            endpoint,
        });
        (Arc::new(Self { receiver }), AuthorityPublisher { sender })
    }
}

impl LeaderAuthority for WatchLeaderAuthority {
    fn route(&self) -> AuthorityRoute {
        let state = self.receiver.borrow();
        if state.local {
            AuthorityRoute::Local(AuthorityPermit::issue(state.generation))
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
        } else if state.generation != permit.generation() {
            Err(AuthorityError::StalePermit)
        } else {
            Ok(())
        }
    }

    fn acquire(&self) -> AuthorityAcquireFuture<'_> {
        let mut receiver = self.receiver.clone();
        Box::pin(async move {
            loop {
                let state = receiver.borrow_and_update().clone();
                if state.local {
                    return Ok(AuthorityPermit::issue(state.generation));
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
        let generation = permit.generation();
        Box::pin(async move {
            loop {
                let revoked = {
                    let state = receiver.borrow();
                    !state.local || state.generation != generation
                };
                if revoked || receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

#[cfg(test)]
pub(crate) struct TestBooleanWatchAuthority {
    receiver: std::sync::Mutex<tokio::sync::watch::Receiver<bool>>,
    generation: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl TestBooleanWatchAuthority {
    pub(crate) fn new(receiver: tokio::sync::watch::Receiver<bool>) -> Arc<Self> {
        Arc::new(Self {
            receiver: std::sync::Mutex::new(receiver),
            generation: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn issue(&self) -> (u64, bool) {
        use std::sync::atomic::Ordering;
        let current = *self.receiver.lock().unwrap().borrow_and_update();
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        (generation, current)
    }
}

#[cfg(test)]
impl LeaderAuthority for TestBooleanWatchAuthority {
    fn route(&self) -> AuthorityRoute {
        let (generation, local) = self.issue();
        if local {
            AuthorityRoute::Local(AuthorityPermit::issue(generation))
        } else {
            AuthorityRoute::Unavailable
        }
    }

    fn validate(&self, permit: &AuthorityPermit) -> Result<(), AuthorityError> {
        use std::sync::atomic::Ordering;
        let receiver = self.receiver.lock().unwrap();
        let local = *receiver.borrow();
        let generation = self.generation.load(Ordering::Acquire);
        if !local {
            Err(AuthorityError::NotAuthoritative)
        } else if receiver.has_changed().unwrap_or(true) || generation != permit.generation() {
            Err(AuthorityError::StalePermit)
        } else {
            Ok(())
        }
    }

    fn acquire(&self) -> AuthorityAcquireFuture<'_> {
        let mut receiver = self.receiver.lock().unwrap().clone();
        Box::pin(async move {
            loop {
                if *receiver.borrow_and_update() {
                    return match self.route() {
                        AuthorityRoute::Local(permit) => Ok(permit),
                        _ => Err(AuthorityError::NotAuthoritative),
                    };
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
        let mut receiver = self.receiver.lock().unwrap().clone();
        Box::pin(async move {
            loop {
                if self.validate(permit).is_err() || receiver.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

pub(crate) fn project_raft_shape(
    declared_role: &crate::kubelet::node_config::KubeletNodeRole,
    shape: &klights_cluster_core::RaftShape,
) -> NodeRoleProjection {
    use crate::kubelet::node_config::KubeletNodeRole;
    if shape.is_learner {
        return NodeRoleProjection::Replica;
    }
    match declared_role {
        KubeletNodeRole::Worker => NodeRoleProjection::Pending,
        KubeletNodeRole::Controlplane { .. } => match (shape.voter_count, shape.is_leader) {
            (0, _) => NodeRoleProjection::Pending,
            (_, true) => NodeRoleProjection::ControlPlaneLeader,
            (_, false) => NodeRoleProjection::ControlPlaneFollower,
        },
        KubeletNodeRole::Leader => match (shape.voter_count, shape.is_leader) {
            (0, _) | (1, false) => NodeRoleProjection::Pending,
            (1, true) => NodeRoleProjection::StandaloneLeader,
            (_, true) => NodeRoleProjection::ControlPlaneLeader,
            (_, false) => NodeRoleProjection::ControlPlaneFollower,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authority_generation_rejects_demotion_promotion_aba() {
        let (authority, publisher) = WatchLeaderAuthority::channel(true, None);
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("initial authority");
        };
        publisher.publish(false, Some("https://leader".to_string()));
        publisher.publish(true, None);
        assert_eq!(
            authority.validate(&permit),
            Err(AuthorityError::StalePermit)
        );
    }
}
