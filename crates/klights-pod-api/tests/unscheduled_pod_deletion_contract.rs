use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use klights_pod_api::{
    UnscheduledPodDeletion, UnscheduledPodDeletionError, UnscheduledPodDeletionFuture,
    UnscheduledPodDeletionOutcome, UnscheduledPodDeletionRequest,
};
use klights_types::PodIdentity;

struct RecordingUnscheduledPodDeletion {
    observed: Mutex<Vec<(PodIdentity, i64)>>,
}

impl UnscheduledPodDeletion for RecordingUnscheduledPodDeletion {
    fn delete_unscheduled_pod(
        &self,
        request: UnscheduledPodDeletionRequest,
    ) -> UnscheduledPodDeletionFuture<'_> {
        Box::pin(async move {
            self.observed.lock().expect("recording lock").push((
                request.identity().clone(),
                request.observed_resource_version(),
            ));
            Ok(UnscheduledPodDeletionOutcome::Removed)
        })
    }
}

fn assert_object_safe(_: &dyn UnscheduledPodDeletion) {}

#[test]
fn unscheduled_deletion_is_object_safe_uid_and_observed_rv_qualified() {
    let capability = RecordingUnscheduledPodDeletion {
        observed: Mutex::new(Vec::new()),
    };
    assert_object_safe(&capability);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<UnscheduledPodDeletionRequest>();
    assert_send_sync::<UnscheduledPodDeletionOutcome>();
    assert_send_sync::<UnscheduledPodDeletionError>();

    let identity = PodIdentity::new("default", "terminating", "uid-terminating");
    let request = UnscheduledPodDeletionRequest::try_new(identity.clone(), 41)
        .expect("UID- and observed-RV-qualified request");
    assert_eq!(request.identity(), &identity);
    assert_eq!(request.observed_resource_version(), 41);

    let outcome = {
        let mut future = capability.delete_unscheduled_pod(request);
        let mut context = Context::from_waker(Waker::noop());
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(result) => result.expect("fake unscheduled deletion"),
            Poll::Pending => panic!("recording deletion must complete immediately"),
        }
    };

    assert_eq!(outcome, UnscheduledPodDeletionOutcome::Removed);
    assert_eq!(
        capability
            .observed
            .lock()
            .expect("recording lock")
            .as_slice(),
        &[(identity, 41)]
    );
}

#[test]
fn unscheduled_deletion_request_rejects_incomplete_identity_or_rv() {
    for identity in [
        PodIdentity::new("", "pod", "uid-a"),
        PodIdentity::new("default", "", "uid-a"),
        PodIdentity::new("default", "pod", ""),
    ] {
        assert!(matches!(
            UnscheduledPodDeletionRequest::try_new(identity, 1),
            Err(UnscheduledPodDeletionError::InvalidRequest { .. })
        ));
    }

    assert!(matches!(
        UnscheduledPodDeletionRequest::try_new(PodIdentity::new("default", "pod", "uid-a"), 0,),
        Err(UnscheduledPodDeletionError::InvalidRequest {
            field: "pod.observed_resource_version",
            ..
        })
    ));
}

#[test]
fn unscheduled_deletion_outcomes_keep_actor_and_finalizer_deferrals_distinct() {
    assert_ne!(
        UnscheduledPodDeletionOutcome::DeferToActor,
        UnscheduledPodDeletionOutcome::FinalizersPending
    );
    assert_ne!(
        UnscheduledPodDeletionOutcome::Removed,
        UnscheduledPodDeletionOutcome::DeferToActor
    );
    assert_ne!(
        UnscheduledPodDeletionOutcome::Retry,
        UnscheduledPodDeletionOutcome::DeferToActor
    );
    assert_ne!(
        UnscheduledPodDeletionOutcome::Retry,
        UnscheduledPodDeletionOutcome::FinalizersPending
    );
}
