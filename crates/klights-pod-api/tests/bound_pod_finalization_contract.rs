use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use klights_pod_api::{
    BoundPodFinalization, BoundPodFinalizationError, BoundPodFinalizationFuture,
    BoundPodFinalizationOutcome, BoundPodFinalizationRequest,
};
use klights_types::PodIdentity;

struct RecordingBoundPodFinalization {
    observed: Mutex<Vec<PodIdentity>>,
}

impl BoundPodFinalization for RecordingBoundPodFinalization {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_> {
        Box::pin(async move {
            self.observed
                .lock()
                .expect("recording lock")
                .push(request.into_identity());
            Ok(BoundPodFinalizationOutcome::Removed)
        })
    }
}

fn assert_object_safe(_: &dyn BoundPodFinalization) {}

#[test]
fn bound_finalization_is_object_safe_uid_qualified_and_transport_neutral() {
    let capability = RecordingBoundPodFinalization {
        observed: Mutex::new(Vec::new()),
    };
    assert_object_safe(&capability);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BoundPodFinalizationRequest>();
    assert_send_sync::<BoundPodFinalizationOutcome>();
    assert_send_sync::<BoundPodFinalizationError>();

    let request = BoundPodFinalizationRequest::try_new(PodIdentity::new(
        "default",
        "terminating",
        "uid-terminating",
    ))
    .expect("UID-qualified request");
    assert_eq!(request.identity().namespace, "default");
    assert_eq!(request.identity().name, "terminating");
    assert_eq!(request.identity().uid, "uid-terminating");
}

#[test]
fn bound_finalization_request_rejects_incomplete_identity() {
    for identity in [
        PodIdentity::new("", "pod", "uid-a"),
        PodIdentity::new("default", "", "uid-a"),
        PodIdentity::new("default", "pod", ""),
    ] {
        assert!(matches!(
            BoundPodFinalizationRequest::try_new(identity),
            Err(BoundPodFinalizationError::InvalidRequest { .. })
        ));
    }
}

#[test]
fn bound_finalization_delivers_only_the_validated_identity() {
    let capability = RecordingBoundPodFinalization {
        observed: Mutex::new(Vec::new()),
    };
    let request = BoundPodFinalizationRequest::try_new(PodIdentity::new(
        "default",
        "terminating",
        "uid-terminating",
    ))
    .expect("UID-qualified request");

    let outcome = {
        let mut future = capability.finalize_bound_pod(request);
        let mut context = Context::from_waker(Waker::noop());
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(result) => result.expect("fake finalization"),
            Poll::Pending => panic!("recording finalization must complete immediately"),
        }
    };

    assert_eq!(outcome, BoundPodFinalizationOutcome::Removed);
    assert_eq!(
        capability
            .observed
            .lock()
            .expect("recording lock")
            .as_slice(),
        &[PodIdentity::new(
            "default",
            "terminating",
            "uid-terminating"
        )]
    );
}

#[test]
fn bound_finalization_outcomes_keep_terminal_and_retry_dispositions_distinct() {
    let outcomes = [
        BoundPodFinalizationOutcome::Removed,
        BoundPodFinalizationOutcome::Accepted,
        BoundPodFinalizationOutcome::IdentityChanged,
        BoundPodFinalizationOutcome::FinalizersPending,
        BoundPodFinalizationOutcome::Retry,
    ];
    for (index, outcome) in outcomes.iter().enumerate() {
        assert!(
            outcomes[index + 1..].iter().all(|other| other != outcome),
            "bound finalization disposition must remain unambiguous: {outcome:?}"
        );
    }
}
