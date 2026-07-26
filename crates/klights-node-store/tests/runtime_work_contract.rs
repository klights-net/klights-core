use klights_node_store::{
    DueTimeMs, ObservedPodVersion, OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup,
    PodRuntimeRecord, PodRuntimeStore, PodSlotAdmissionEvent, PodSlotAdmissionEventSource,
    PodSlotAdmissionRequest, PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotAdmissionStore,
    PodSlotClearResult, PodSlotEventSubscription, PodSlotMutationResult, PodWorkIdentity,
    PodWorkqueueEnqueue, PodWorkqueueEntry, PodWorkqueueKind, PodWorkqueueStore, ProbeKey,
    ProbeResult, ProbeState, ProbeStateStore, RuntimeNamespace, RuntimePodUid, RuntimeWorkError,
    RuntimeWorkFuture, WorkItemId,
};
use klights_types::PodIdentity;
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

struct EmptyRuntimeWorkStore;

impl PodRuntimeStore for EmptyRuntimeWorkStore {
    fn admit_pod_runtime(&self, _admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_owned_sandbox(&self, _sandbox: OwnedPodSandbox) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_cgroup(&self, _cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete_pod_runtime_for_uid(&self, _pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_pod_runtime(
        &self,
        _pod_uid: RuntimePodUid,
    ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>> {
        Box::pin(async { Ok(None) })
    }

    fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_pod_runtime_by_namespace(
        &self,
        _namespace: RuntimeNamespace,
    ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl ProbeStateStore for EmptyRuntimeWorkStore {
    fn record_probe_result(&self, _result: ProbeResult) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get_probe_state(&self, _key: ProbeKey) -> RuntimeWorkFuture<'_, Option<ProbeState>> {
        Box::pin(async { Ok(None) })
    }
}

impl PodWorkqueueStore for EmptyRuntimeWorkStore {
    fn enqueue_work(&self, _entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>> {
        Box::pin(async { Ok(None) })
    }

    fn claim_due_work(
        &self,
        _now_ms: DueTimeMs,
    ) -> RuntimeWorkFuture<'_, Option<PodWorkqueueEntry>> {
        Box::pin(async { Ok(None) })
    }
}

impl PodSlotAdmissionStore for EmptyRuntimeWorkStore {
    fn try_admit(
        &self,
        _request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotAdmissionResult> {
        Box::pin(async {
            Ok(PodSlotAdmissionResult::Admitted {
                observed_pod_version: version(1),
            })
        })
    }

    fn mark_terminating(
        &self,
        _request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotMutationResult> {
        Box::pin(async {
            Ok(PodSlotMutationResult::Changed {
                observed_pod_version: version(2),
            })
        })
    }

    fn clear_if_uid(
        &self,
        _request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotClearResult> {
        Box::pin(async { Ok(PodSlotClearResult::NotFound) })
    }
}

struct ClosedSubscription;

impl PodSlotEventSubscription for ClosedSubscription {
    fn next_event(&mut self) -> RuntimeWorkFuture<'_, Option<PodSlotAdmissionEvent>> {
        Box::pin(async { Ok(None) })
    }
}

impl PodSlotAdmissionEventSource for EmptyRuntimeWorkStore {
    fn subscribe(&self) -> Box<dyn PodSlotEventSubscription> {
        Box::new(ClosedSubscription)
    }
}

fn pod() -> PodIdentity {
    PodIdentity::new("namespace/raw", "pod/raw", "uid/raw")
}

fn version(value: i64) -> ObservedPodVersion {
    ObservedPodVersion::try_new(value).unwrap()
}

fn assert_runtime_store_object_safe(_: &dyn PodRuntimeStore) {}
fn assert_probe_store_object_safe(_: &dyn ProbeStateStore) {}
fn assert_workqueue_store_object_safe(_: &dyn PodWorkqueueStore) {}
fn assert_slot_store_object_safe(_: &dyn PodSlotAdmissionStore) {}
fn assert_slot_events_object_safe(_: &dyn PodSlotAdmissionEventSource) {}
fn assert_slot_subscription_object_safe(_: &mut dyn PodSlotEventSubscription) {}

#[test]
fn runtime_work_capabilities_are_independently_object_safe() {
    let store = EmptyRuntimeWorkStore;
    assert_runtime_store_object_safe(&store);
    assert_probe_store_object_safe(&store);
    assert_workqueue_store_object_safe(&store);
    assert_slot_store_object_safe(&store);
    assert_slot_events_object_safe(&store);
    let mut subscription = store.subscribe();
    assert_slot_subscription_object_safe(subscription.as_mut());
}

#[test]
fn runtime_values_preserve_uid_identity_sandbox_cgroup_and_timestamps() {
    let admission = PodRuntimeAdmission::try_new(pod(), "node/raw").unwrap();
    assert_eq!(admission.pod(), &pod());
    assert_eq!(admission.node_name(), "node/raw");

    let sandbox = OwnedPodSandbox::try_new(pod(), "node/raw", "sandbox/raw", 41).unwrap();
    assert_eq!(sandbox.pod(), &pod());
    assert_eq!(sandbox.node_name(), "node/raw");
    assert_eq!(sandbox.sandbox_id(), "sandbox/raw");
    assert_eq!(sandbox.created_ms(), 41);

    let cgroup = PodRuntimeCgroup::try_new("uid/raw", "/cgroup/raw").unwrap();
    assert_eq!(cgroup.pod_uid(), "uid/raw");
    assert_eq!(cgroup.cgroup_path(), "/cgroup/raw");

    let record = PodRuntimeRecord::try_new(
        pod(),
        "node/raw",
        Some("sandbox/raw".to_string()),
        Some("/cgroup/raw".to_string()),
        101,
        Some(109),
    )
    .unwrap();
    assert_eq!(record.pod(), &pod());
    assert_eq!(record.node_name(), "node/raw");
    assert_eq!(record.sandbox_id(), Some("sandbox/raw"));
    assert_eq!(record.cgroup_path(), Some("/cgroup/raw"));
    assert_eq!(record.created_ms(), 101);
    assert_eq!(record.started_ms(), Some(109));
}

#[test]
fn probe_values_preserve_key_and_derived_failure_state() {
    let key = ProbeKey::try_new("uid/raw", "container/raw", "readiness/raw").unwrap();
    let result = ProbeResult::try_new(key.clone(), false, 107).unwrap();
    assert_eq!(result.key(), &key);
    assert!(!result.success());
    assert_eq!(result.result_ms(), 107);

    let state = ProbeState::try_new(key.clone(), Some(107), Some(false), 3, 107).unwrap();
    assert_eq!(state.key(), &key);
    assert_eq!(state.last_result_ms(), Some(107));
    assert_eq!(state.last_success(), Some(false));
    assert_eq!(state.consecutive_failures(), 3);
    assert_eq!(state.next_eligible_ms(), 107);
}

#[test]
fn workqueue_values_preserve_opaque_payload_retry_error_and_due_state() {
    let enqueue = PodWorkqueueEnqueue::try_new(
        PodWorkIdentity::try_pod(pod()).unwrap(),
        vec![0, 255, 1, 0],
        7,
        5_000,
        Some("error/raw".to_string()),
    )
    .unwrap();
    assert_eq!(enqueue.kind(), PodWorkqueueKind::Pod);
    assert_eq!(enqueue.identity().as_pod(), Some(&pod()));
    assert_eq!(enqueue.payload(), &[0, 255, 1, 0]);
    assert_eq!(enqueue.attempt_count(), 7);
    assert_eq!(enqueue.minimum_delay_ms(), 5_000);
    assert_eq!(enqueue.last_error(), Some("error/raw"));

    let row = PodWorkqueueEntry::try_new(
        41,
        PodWorkIdentity::try_namespace("namespace/raw", "uid/raw").unwrap(),
        vec![9, 8, 7],
        8,
        10_001,
    )
    .unwrap();
    assert_eq!(row.id().get(), 41);
    assert_eq!(row.kind(), PodWorkqueueKind::Namespace);
    assert_eq!(
        row.identity().namespace_parts(),
        Some(("namespace/raw", "uid/raw"))
    );
    assert_eq!(row.payload(), &[9, 8, 7]);
    assert_eq!(row.attempt_count(), 8);
    assert_eq!(row.next_due_ms().get(), 10_001);
}

#[test]
fn slot_values_preserve_uid_cas_observed_version_state_and_event_identity() {
    let request = PodSlotAdmissionRequest::try_new(pod(), "node/raw").unwrap();
    assert_eq!(request.pod(), &pod());
    assert_eq!(request.node_name(), "node/raw");

    let admitted = PodSlotAdmissionResult::Admitted {
        observed_pod_version: version(17),
    };
    assert!(matches!(
        admitted,
        PodSlotAdmissionResult::Admitted {
            observed_pod_version,
        } if observed_pod_version.get() == 17
    ));

    let blocked = PodSlotAdmissionResult::Blocked {
        blocking_uid: "uid/old".to_string(),
        blocking_node: "node/old".to_string(),
        state: PodSlotAdmissionState::Terminating,
        observed_pod_version: version(19),
    };
    assert!(matches!(
        blocked,
        PodSlotAdmissionResult::Blocked {
            blocking_uid,
            blocking_node,
            state: PodSlotAdmissionState::Terminating,
            observed_pod_version,
        } if blocking_uid == "uid/old"
            && blocking_node == "node/old"
            && observed_pod_version.get() == 19
    ));

    let not_cleared = PodSlotClearResult::UidMismatch {
        blocking_uid: "uid/new".to_string(),
        blocking_node: "node/new".to_string(),
        state: PodSlotAdmissionState::Admitted,
        observed_pod_version: version(23),
    };
    assert!(matches!(
        not_cleared,
        PodSlotClearResult::UidMismatch {
            blocking_uid,
            blocking_node,
            state: PodSlotAdmissionState::Admitted,
            observed_pod_version,
        } if blocking_uid == "uid/new"
            && blocking_node == "node/new"
            && observed_pod_version.get() == 23
    ));

    for transition in [
        PodSlotMutationResult::Changed {
            observed_pod_version: version(24),
        },
        PodSlotMutationResult::Unchanged {
            observed_pod_version: version(25),
        },
    ] {
        match transition {
            PodSlotMutationResult::Changed {
                observed_pod_version,
            } => assert_eq!(observed_pod_version.get(), 24),
            PodSlotMutationResult::Unchanged {
                observed_pod_version,
            } => assert_eq!(observed_pod_version.get(), 25),
        }
    }

    assert!(matches!(
        PodSlotClearResult::Cleared {
            observed_pod_version: version(26),
        },
        PodSlotClearResult::Cleared {
            observed_pod_version,
        } if observed_pod_version.get() == 26
    ));
    assert!(matches!(
        PodSlotClearResult::NotFound,
        PodSlotClearResult::NotFound
    ));

    let event = PodSlotAdmissionEvent::Changed {
        pod: pod(),
        state: PodSlotAdmissionState::Admitted,
        observed_pod_version: version(29),
    };
    assert!(matches!(
        event,
        PodSlotAdmissionEvent::Changed {
            pod,
            state: PodSlotAdmissionState::Admitted,
            observed_pod_version,
        } if pod == self::pod() && observed_pod_version.get() == 29
    ));
    assert!(matches!(
        PodSlotAdmissionEvent::Cleared {
            pod: pod(),
            observed_pod_version: version(31),
        },
        PodSlotAdmissionEvent::Cleared {
            pod,
            observed_pod_version,
        } if pod == self::pod() && observed_pod_version.get() == 31
    ));
}

#[test]
fn invalid_and_operational_errors_remain_distinct() {
    assert!(matches!(
        OwnedPodSandbox::try_new(
            PodIdentity::new("default", "pod", ""),
            "node",
            "sandbox",
            41,
        ),
        Err(RuntimeWorkError::InvalidInput {
            field: "pod.uid",
            ..
        })
    ));
    assert!(matches!(
        OwnedPodSandbox::try_new(pod(), "node", "sandbox", -1),
        Err(RuntimeWorkError::InvalidInput {
            field: "created_ms",
            ..
        })
    ));
    assert!(matches!(
        ObservedPodVersion::try_new(0),
        Err(RuntimeWorkError::InvalidInput {
            field: "observed_pod_version",
            ..
        })
    ));
    assert!(matches!(
        PodRuntimeRecord::try_new(pod(), "node", None, None, -1, None),
        Err(RuntimeWorkError::InvalidInput {
            field: "created_ms",
            ..
        })
    ));
    assert!(matches!(
        ProbeResult::try_new(
            ProbeKey::try_new("uid", "container", "readiness").unwrap(),
            false,
            -1,
        ),
        Err(RuntimeWorkError::InvalidInput {
            field: "result_ms",
            ..
        })
    ));
    assert!(matches!(
        ProbeState::try_new(
            ProbeKey::try_new("uid", "container", "readiness").unwrap(),
            Some(1),
            Some(true),
            1,
            2,
        ),
        Err(RuntimeWorkError::InvalidInput {
            field: "consecutive_failures",
            ..
        })
    ));
    assert!(matches!(
        PodWorkqueueEnqueue::try_new(
            PodWorkIdentity::try_pod(pod()).unwrap(),
            Vec::new(),
            -1,
            0,
            None,
        ),
        Err(RuntimeWorkError::InvalidInput {
            field: "attempt_count",
            ..
        })
    ));
    assert!(matches!(
        PodWorkqueueEntry::try_new(
            0,
            PodWorkIdentity::try_pod(pod()).unwrap(),
            Vec::new(),
            0,
            0,
        ),
        Err(RuntimeWorkError::InvalidInput { field: "id", .. })
    ));

    let errors = [
        RuntimeWorkError::persistence_failed("disk/full"),
        RuntimeWorkError::corrupt_data("bad/row"),
        RuntimeWorkError::retryable("busy"),
        RuntimeWorkError::uid_conflict("uid/expected", "uid/actual"),
        RuntimeWorkError::Timeout,
        RuntimeWorkError::Cancelled,
    ];
    assert!(matches!(
        errors[0],
        RuntimeWorkError::PersistenceFailed { .. }
    ));
    assert!(matches!(errors[1], RuntimeWorkError::CorruptData { .. }));
    assert!(matches!(errors[2], RuntimeWorkError::Retryable { .. }));
    assert!(matches!(errors[3], RuntimeWorkError::UidConflict { .. }));
    assert_eq!(
        errors[4].to_string(),
        "node runtime/work persistence timed out"
    );
    assert_eq!(
        errors[5].to_string(),
        "node runtime/work persistence was cancelled"
    );
}

#[test]
fn pod_and_namespace_work_identities_round_trip_the_exact_persisted_shapes() {
    let pod_identity = PodWorkIdentity::try_pod(pod()).unwrap();
    let (kind, persisted) = pod_identity.clone().into_persisted();
    assert_eq!(kind, PodWorkqueueKind::Pod);
    assert_eq!(
        PodWorkIdentity::try_from_persisted(kind, persisted).unwrap(),
        pod_identity
    );

    let namespace_identity =
        PodWorkIdentity::try_namespace("namespace/raw", "namespace-uid/raw").unwrap();
    let (kind, persisted) = namespace_identity.clone().into_persisted();
    assert_eq!(kind, PodWorkqueueKind::Namespace);
    assert_eq!(persisted.namespace, "");
    assert_eq!(persisted.name, "namespace/raw");
    assert_eq!(persisted.uid, "namespace-uid/raw");
    assert_eq!(
        PodWorkIdentity::try_from_persisted(kind, persisted).unwrap(),
        namespace_identity
    );
}

#[test]
fn work_payloads_preserve_json_lexical_forms_and_non_utf8_bytes() {
    for payload in [
        br#"{ "a": 1, "a": 2 }"#.to_vec(),
        b"{\n  \"z\": 1,\n  \"a\": 2\n}\n".to_vec(),
        vec![0xff, 0x00, 0xfe, 0x7f],
    ] {
        let enqueue = PodWorkqueueEnqueue::try_new(
            PodWorkIdentity::try_pod(pod()).unwrap(),
            payload.clone(),
            0,
            0,
            None,
        )
        .unwrap();
        assert_eq!(enqueue.payload(), payload);
        assert_eq!(enqueue.into_parts().1, payload);
    }
}

#[test]
fn probe_state_rejects_a_next_eligible_timestamp_not_derived_from_the_result() {
    let key = ProbeKey::try_new("uid", "container", "readiness").unwrap();
    assert!(matches!(
        ProbeState::try_new(key, Some(9), Some(false), 1, 10),
        Err(RuntimeWorkError::InvalidInput {
            field: "next_eligible_ms",
            ..
        })
    ));
}

fn ready<T>(mut future: RuntimeWorkFuture<'_, T>) -> Result<T, RuntimeWorkError> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("reference persistence future unexpectedly pending"),
    }
}

#[derive(Default)]
struct ModelWorkqueue {
    state: Mutex<Vec<PodWorkqueueEntry>>,
}

impl ModelWorkqueue {
    fn seed(&self, row: PodWorkqueueEntry) {
        self.state.lock().unwrap().push(row);
    }
}

impl PodWorkqueueStore for ModelWorkqueue {
    fn enqueue_work(&self, _entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .iter()
                .map(|row| row.next_due_ms().get())
                .min())
        })
    }

    fn claim_due_work(&self, now: DueTimeMs) -> RuntimeWorkFuture<'_, Option<PodWorkqueueEntry>> {
        Box::pin(async move {
            let mut rows = self.state.lock().unwrap();
            let selected = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.next_due_ms().get() <= now.get())
                .min_by_key(|(_, row)| (row.next_due_ms(), row.id()))
                .map(|(index, _)| index);
            Ok(selected.map(|index| rows.remove(index)))
        })
    }
}

#[test]
fn reference_workqueue_claim_is_destructive_and_orders_by_due_then_id() {
    let store = ModelWorkqueue::default();
    let identity = PodWorkIdentity::try_pod(pod()).unwrap();
    store.seed(PodWorkqueueEntry::try_new(3, identity.clone(), vec![3], 0, 8).unwrap());
    store.seed(PodWorkqueueEntry::try_new(2, identity.clone(), vec![2], 0, 8).unwrap());
    store.seed(PodWorkqueueEntry::try_new(1, identity, vec![1], 0, 9).unwrap());
    assert_eq!(ready(store.peek_next_due_ms()).unwrap(), Some(8));
    assert_eq!(
        ready(store.claim_due_work(DueTimeMs::try_new(8).unwrap()))
            .unwrap()
            .unwrap()
            .id()
            .get(),
        2
    );
    assert_eq!(
        ready(store.claim_due_work(DueTimeMs::try_new(8).unwrap()))
            .unwrap()
            .unwrap()
            .id()
            .get(),
        3
    );
    assert!(
        ready(store.claim_due_work(DueTimeMs::try_new(8).unwrap()))
            .unwrap()
            .is_none()
    );
}

#[test]
fn focused_runtime_keys_and_times_reject_invalid_values() {
    assert!(RuntimePodUid::try_new("").is_err());
    assert!(RuntimeNamespace::try_new("").is_err());
    assert!(WorkItemId::try_new(0).is_err());
    assert!(DueTimeMs::try_new(-1).is_err());
}
