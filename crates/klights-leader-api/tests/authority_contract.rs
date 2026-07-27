use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use klights_leader_api::{
    AuthorityError, AuthorityPermit, AuthorityRoute, ControllerCoordination,
    ControllerCoordinationError, ControllerLease, ControllerScope, LeaderAuthority,
    NodeRoleProjection,
};

struct FakeAuthority {
    generation: AtomicU64,
}

impl FakeAuthority {
    fn new(generation: u64) -> Self {
        Self {
            generation: AtomicU64::new(generation),
        }
    }
}

impl LeaderAuthority for FakeAuthority {
    fn route(&self) -> AuthorityRoute {
        AuthorityRoute::Local(AuthorityPermit::issue(
            self.generation.load(Ordering::Acquire),
        ))
    }

    fn validate(&self, permit: &AuthorityPermit) -> Result<(), AuthorityError> {
        if permit.generation() == self.generation.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(AuthorityError::StalePermit)
        }
    }

    fn acquire(&self) -> klights_leader_api::AuthorityAcquireFuture<'_> {
        Box::pin(async move {
            match self.route() {
                AuthorityRoute::Local(permit) => Ok(permit),
                _ => Err(AuthorityError::NotAuthoritative),
            }
        })
    }

    fn wait_for_revocation<'a>(
        &'a self,
        _permit: &'a AuthorityPermit,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

#[test]
fn stale_authority_permit_is_rejected_after_generation_change() {
    let authority = FakeAuthority::new(7);
    let AuthorityRoute::Local(permit) = authority.route() else {
        panic!("fake must begin authoritative");
    };
    authority.validate(&permit).expect("fresh permit");

    authority.generation.store(8, Ordering::Release);
    assert_eq!(
        authority.validate(&permit),
        Err(AuthorityError::StalePermit)
    );
}

#[test]
fn node_role_projection_is_backend_neutral_and_exhaustive() {
    let projections = [
        NodeRoleProjection::Pending,
        NodeRoleProjection::StandaloneLeader,
        NodeRoleProjection::ControlPlaneLeader,
        NodeRoleProjection::ControlPlaneFollower,
        NodeRoleProjection::Replica,
    ];
    assert_eq!(projections.len(), 5);
}

fn assert_object_safe(_: &dyn LeaderAuthority) {}

#[test]
fn authority_capability_is_object_safe() {
    assert_object_safe(&FakeAuthority::new(1));
}

struct FakeCoordination {
    generation: AtomicU64,
}

struct FakeCoordinationFence(u64);

impl ControllerCoordination for FakeCoordination {
    fn try_acquire(
        &self,
        scope: ControllerScope,
    ) -> Result<ControllerLease, ControllerCoordinationError> {
        Ok(ControllerLease::issue(
            scope,
            FakeCoordinationFence(self.generation.load(Ordering::Acquire)),
        ))
    }

    fn acquire<'a>(
        &'a self,
        scope: ControllerScope,
    ) -> klights_leader_api::ControllerAcquireFuture<'a> {
        Box::pin(async move { self.try_acquire(scope) })
    }

    fn validate(&self, lease: &ControllerLease) -> Result<(), ControllerCoordinationError> {
        if lease
            .adapter_fence::<FakeCoordinationFence>()
            .is_some_and(|fence| fence.0 == self.generation.load(Ordering::Acquire))
        {
            Ok(())
        } else {
            Err(ControllerCoordinationError::StalePermit)
        }
    }

    fn wait_for_revocation<'a>(
        &'a self,
        _lease: &'a ControllerLease,
    ) -> klights_leader_api::ControllerRevocationFuture<'a> {
        Box::pin(std::future::pending())
    }
}

#[test]
fn controller_coordination_is_scoped_fenced_and_object_safe() {
    let coordination = FakeCoordination {
        generation: AtomicU64::new(11),
    };
    let capability: &dyn ControllerCoordination = &coordination;
    let lease = capability
        .try_acquire(ControllerScope::Cluster)
        .expect("current authority should acquire");
    assert_eq!(lease.scope(), &ControllerScope::Cluster);
    capability.validate(&lease).expect("fresh lease");

    coordination.generation.store(12, Ordering::Release);
    assert_eq!(
        capability.validate(&lease),
        Err(ControllerCoordinationError::StalePermit)
    );
}
