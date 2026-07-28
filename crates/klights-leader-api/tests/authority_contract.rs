use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use klights_leader_api::{
    AuthorityError, AuthorityPermit, AuthorityPermitIssuer, AuthorityRoute, ControllerCoordination,
    ControllerCoordinationError, ControllerLease, ControllerScope, LeaderAuthority,
    NodeRoleProjection, PostCommitAdvance, PostCommitWakeup, scope_authority,
    scope_controller_lease, validate_scoped_authority, validate_scoped_controller_lease,
};

struct FakeAuthority {
    generation: AtomicU64,
    issuer: AuthorityPermitIssuer,
}

impl FakeAuthority {
    fn new(generation: u64) -> Self {
        Self {
            generation: AtomicU64::new(generation),
            issuer: AuthorityPermitIssuer::new(),
        }
    }
}

impl LeaderAuthority for FakeAuthority {
    fn route(&self) -> AuthorityRoute {
        AuthorityRoute::Local(self.issuer.issue(self.generation.load(Ordering::Acquire)))
    }

    fn validate(&self, permit: &AuthorityPermit) -> Result<(), AuthorityError> {
        self.issuer
            .validate(permit, self.generation.load(Ordering::Acquire))
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
fn authority_permit_from_another_issuer_cannot_forge_current_generation() {
    let authority = FakeAuthority::new(7);
    let forged = AuthorityPermitIssuer::new().issue(7);
    assert_eq!(
        authority.validate(&forged),
        Err(AuthorityError::StalePermit)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scoped_authority_rejects_demote_promote_aba_at_effect_boundary() {
    let authority = std::sync::Arc::new(FakeAuthority::new(7));
    let AuthorityRoute::Local(permit) = authority.route() else {
        panic!("fake must begin authoritative");
    };
    let result = scope_authority(authority.clone(), permit, async {
        authority.generation.store(9, Ordering::Release);
        validate_scoped_authority()
    })
    .await;
    assert_eq!(result, Err(AuthorityError::StalePermit));
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

#[tokio::test(flavor = "current_thread")]
async fn scoped_controller_lease_rejects_demote_promote_aba_at_effect_boundary() {
    let coordination = std::sync::Arc::new(FakeCoordination {
        generation: AtomicU64::new(11),
    });
    let lease = coordination
        .try_acquire(ControllerScope::Cluster)
        .expect("current authority should acquire");
    let result = scope_controller_lease(coordination.clone(), lease, async {
        coordination.generation.store(13, Ordering::Release);
        validate_scoped_controller_lease()
    })
    .await;
    assert_eq!(result, Err(ControllerCoordinationError::StalePermit));
}

#[test]
fn post_commit_wakeup_contract_is_backend_neutral_and_object_safe() {
    struct RecordingWakeup(std::sync::Mutex<Vec<PostCommitAdvance>>);
    impl PostCommitWakeup for RecordingWakeup {
        fn wake(&self, advances: &[PostCommitAdvance]) {
            self.0.lock().unwrap().extend_from_slice(advances);
        }

        fn wake_namespace_contents(&self, _namespace: &str, _resource_version: i64) {}
    }
    let wakeup = RecordingWakeup(std::sync::Mutex::new(Vec::new()));
    let capability: &dyn PostCommitWakeup = &wakeup;
    capability.wake(&[PostCommitAdvance::new(
        "v1",
        "ConfigMap",
        Some("default".to_string()),
        41,
    )]);
    assert_eq!(wakeup.0.lock().unwrap()[0].resource_version(), 41);
}
