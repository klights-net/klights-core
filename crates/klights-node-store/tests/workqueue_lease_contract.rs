use klights_node_store::{
    PodWorkIdentity, PodWorkqueueClaimRequest, PodWorkqueueLeaseToken, PodWorkqueueMutationOutcome,
    PodWorkqueueRequeue, RuntimeWorkError, WorkItemId,
};
use klights_types::PodIdentity;

fn pod_identity() -> PodWorkIdentity {
    PodWorkIdentity::try_pod(PodIdentity::new("default", "web", "uid-web")).unwrap()
}

#[test]
fn workqueue_claim_and_token_values_are_checked_and_transport_neutral() {
    let claim = PodWorkqueueClaimRequest::try_new(10, 25).unwrap();
    assert_eq!(claim.now_ms().get(), 10);
    assert_eq!(claim.lease_duration_ms(), 25);

    let token = PodWorkqueueLeaseToken::try_new(7, pod_identity(), 35).unwrap();
    assert_eq!(token.id(), WorkItemId::try_new(7).unwrap());
    assert_eq!(token.leased_next_due_ms().get(), 35);
    assert_eq!(token.identity(), &pod_identity());
    assert_eq!(
        PodWorkqueueMutationOutcome::Applied,
        PodWorkqueueMutationOutcome::Applied
    );
    assert_eq!(
        PodWorkqueueMutationOutcome::Stale,
        PodWorkqueueMutationOutcome::Stale
    );
}

#[test]
fn workqueue_lease_inputs_reject_zero_lease_and_overflow() {
    assert!(matches!(
        PodWorkqueueClaimRequest::try_new(10, 0),
        Err(RuntimeWorkError::InvalidInput {
            field: "lease_duration_ms",
            ..
        })
    ));
    assert!(matches!(
        PodWorkqueueClaimRequest::try_new(i64::MAX, 1),
        Err(RuntimeWorkError::InvalidInput {
            field: "lease_deadline_ms",
            ..
        })
    ));

    let token = PodWorkqueueLeaseToken::try_new(7, pod_identity(), 35).unwrap();
    assert!(matches!(
        PodWorkqueueRequeue::try_new(token, vec![], 1, -1, None),
        Err(RuntimeWorkError::InvalidInput {
            field: "minimum_delay_ms",
            ..
        })
    ));
}
