use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use klights_kubelet::unscheduled_deletion::{
    EligibleUnscheduledPodDeletion, UnscheduledPodDeleteCasOutcome,
    UnscheduledPodDeletionObservation, UnscheduledPodDeletionPort,
    UnscheduledPodDeletionPortFuture, UnscheduledPodDeletionService,
};
use klights_pod_api::{
    UnscheduledPodDeletion, UnscheduledPodDeletionOutcome, UnscheduledPodDeletionRequest,
};
use klights_types::PodIdentity;

fn resolve<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fake-port future unexpectedly pending"),
    }
}

fn identity(uid: &str) -> PodIdentity {
    PodIdentity::new("default", "pod", uid)
}

fn observation(
    uid: &str,
    resource_version: i64,
    node_name: Option<&str>,
    terminating: bool,
    has_finalizers: bool,
) -> UnscheduledPodDeletionObservation {
    UnscheduledPodDeletionObservation::try_new(
        identity(uid),
        resource_version,
        node_name.map(str::to_string),
        terminating,
        has_finalizers,
    )
    .unwrap()
}

#[derive(Default)]
struct FakePort {
    observation: Mutex<Option<UnscheduledPodDeletionObservation>>,
    cas_outcome: Mutex<Option<UnscheduledPodDeleteCasOutcome>>,
    observed: Mutex<Vec<PodIdentity>>,
    delete_attempts: Mutex<Vec<(PodIdentity, i64)>>,
}

impl FakePort {
    fn with_observation(observation: Option<UnscheduledPodDeletionObservation>) -> Self {
        Self {
            observation: Mutex::new(observation),
            ..Self::default()
        }
    }

    fn with_cas_outcome(
        observation: UnscheduledPodDeletionObservation,
        cas_outcome: UnscheduledPodDeleteCasOutcome,
    ) -> Self {
        Self {
            observation: Mutex::new(Some(observation)),
            cas_outcome: Mutex::new(Some(cas_outcome)),
            ..Self::default()
        }
    }

    fn delete_attempts(&self) -> Vec<(PodIdentity, i64)> {
        self.delete_attempts.lock().unwrap().clone()
    }
}

impl UnscheduledPodDeletionPort for FakePort {
    fn observe_pod<'a>(
        &'a self,
        identity: &'a PodIdentity,
    ) -> UnscheduledPodDeletionPortFuture<'a, Option<UnscheduledPodDeletionObservation>> {
        self.observed.lock().unwrap().push(identity.clone());
        let observation = self.observation.lock().unwrap().clone();
        Box::pin(async move { Ok(observation) })
    }

    fn compare_and_swap_delete(
        &self,
        eligible: EligibleUnscheduledPodDeletion,
    ) -> UnscheduledPodDeletionPortFuture<'_, UnscheduledPodDeleteCasOutcome> {
        self.delete_attempts.lock().unwrap().push((
            eligible.identity().clone(),
            eligible.observed_resource_version(),
        ));
        let outcome = self
            .cas_outcome
            .lock()
            .unwrap()
            .unwrap_or(UnscheduledPodDeleteCasOutcome::Removed);
        Box::pin(async move { Ok(outcome) })
    }
}

fn delete(
    port: Arc<FakePort>,
    uid: &str,
    observed_resource_version: i64,
) -> UnscheduledPodDeletionOutcome {
    let service = UnscheduledPodDeletionService::new(port);
    let request =
        UnscheduledPodDeletionRequest::try_new(identity(uid), observed_resource_version).unwrap();
    resolve(service.delete_unscheduled_pod(request)).unwrap()
}

#[test]
fn eligible_unscheduled_delete_uses_exact_observed_uid_and_resource_version() {
    let port = Arc::new(FakePort::with_cas_outcome(
        observation("uid-a", 41, None, true, false),
        UnscheduledPodDeleteCasOutcome::Removed,
    ));

    assert_eq!(
        delete(port.clone(), "uid-a", 41),
        UnscheduledPodDeletionOutcome::Removed
    );
    assert_eq!(port.delete_attempts(), vec![(identity("uid-a"), 41)]);
}

#[test]
fn bind_resource_version_and_same_name_races_never_authorize_delete() {
    struct Case {
        name: &'static str,
        observation: Option<UnscheduledPodDeletionObservation>,
        expected: UnscheduledPodDeletionOutcome,
    }

    let cases = [
        Case {
            name: "scheduler bind observed at same rv",
            observation: Some(observation("uid-a", 41, Some("node-a"), true, false)),
            expected: UnscheduledPodDeletionOutcome::DeferToActor,
        },
        Case {
            name: "scheduler bind advanced rv before policy observation",
            observation: Some(observation("uid-a", 42, Some("node-a"), true, false)),
            expected: UnscheduledPodDeletionOutcome::Retry,
        },
        Case {
            name: "metadata-only rv race",
            observation: Some(observation("uid-a", 42, None, true, false)),
            expected: UnscheduledPodDeletionOutcome::Retry,
        },
        Case {
            name: "same-name replacement owns slot",
            observation: Some(observation("uid-new", 42, None, false, false)),
            expected: UnscheduledPodDeletionOutcome::Removed,
        },
        Case {
            name: "row already absent",
            observation: None,
            expected: UnscheduledPodDeletionOutcome::Removed,
        },
    ];

    for case in cases {
        let port = Arc::new(FakePort::with_observation(case.observation));
        assert_eq!(
            delete(port.clone(), "uid-a", 41),
            case.expected,
            "{}",
            case.name
        );
        assert!(
            port.delete_attempts().is_empty(),
            "{} reached the CAS delete port",
            case.name
        );
    }
}

#[test]
fn eligibility_requires_terminating_finalizer_free_unscheduled_observation() {
    let cases = [
        (
            "live Pod",
            observation("uid-a", 41, None, false, false),
            UnscheduledPodDeletionOutcome::Retry,
        ),
        (
            "finalizers pending",
            observation("uid-a", 41, None, true, true),
            UnscheduledPodDeletionOutcome::FinalizersPending,
        ),
    ];

    for (name, observation, expected) in cases {
        let port = Arc::new(FakePort::with_observation(Some(observation)));
        assert_eq!(delete(port.clone(), "uid-a", 41), expected, "{name}");
        assert!(
            port.delete_attempts().is_empty(),
            "{name} reached the CAS delete port"
        );
    }
}

#[test]
fn bind_or_same_name_replacement_between_observation_and_cas_retries() {
    for (name, cas_outcome) in [
        (
            "scheduler bind won exact-rv CAS",
            UnscheduledPodDeleteCasOutcome::Conflict,
        ),
        (
            "same-name replacement won exact-uid-rv CAS",
            UnscheduledPodDeleteCasOutcome::Conflict,
        ),
    ] {
        let port = Arc::new(FakePort::with_cas_outcome(
            observation("uid-a", 41, None, true, false),
            cas_outcome,
        ));
        assert_eq!(
            delete(port.clone(), "uid-a", 41),
            UnscheduledPodDeletionOutcome::Retry,
            "{name}"
        );
        assert_eq!(
            port.delete_attempts(),
            vec![(identity("uid-a"), 41)],
            "{name}"
        );
    }
}

#[test]
fn concurrent_absence_is_idempotent_after_eligibility() {
    let port = Arc::new(FakePort::with_cas_outcome(
        observation("uid-a", 41, None, true, false),
        UnscheduledPodDeleteCasOutcome::Gone,
    ));

    assert_eq!(
        delete(port.clone(), "uid-a", 41),
        UnscheduledPodDeletionOutcome::Removed
    );
    assert_eq!(port.delete_attempts(), vec![(identity("uid-a"), 41)]);
}
