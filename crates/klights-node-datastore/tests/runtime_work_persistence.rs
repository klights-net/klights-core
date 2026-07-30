use std::sync::Arc;
use std::time::{Duration, SystemTime};

use klights_node_datastore::{SqliteRuntimeWorkStore, open};
use klights_node_store::{
    DueTimeMs, OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup, PodRuntimeStore,
    PodSlotAdmissionEvent, PodSlotAdmissionEventSource, PodSlotAdmissionRequest,
    PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotAdmissionStore, PodSlotClearResult,
    PodSlotMutationResult, PodWorkIdentity, PodWorkqueueEnqueue, PodWorkqueueStore, ProbeKey,
    ProbeResult, ProbeStateStore, RuntimePodUid, RuntimeWorkError,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor, WallClock};
use klights_types::PodIdentity;

struct FixedClock(i64);

impl WallClock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.0 as u64)
    }
}

async fn fresh(now_ms: i64) -> SqliteRuntimeWorkStore {
    let executor = open::open_with_opts(
        open::in_memory_opts(),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:runtime-work-persistence-test",
    )
    .await
    .unwrap();
    SqliteRuntimeWorkStore::new(executor, Arc::new(FixedClock(now_ms)))
}

fn pod(namespace: &str, name: &str, uid: &str) -> PodIdentity {
    PodIdentity::new(namespace, name, uid)
}

#[tokio::test]
async fn runtime_probe_and_workqueue_preserve_existing_bytes_and_ordering() {
    let store = fresh(100).await;

    store
        .admit_pod_runtime(
            PodRuntimeAdmission::try_new(pod("default", "web", "uid-b"), "worker-a").unwrap(),
        )
        .await
        .unwrap();
    store
        .record_owned_sandbox(
            OwnedPodSandbox::try_new(pod("default", "web", "uid-b"), "worker-a", "sandbox-b", 40)
                .unwrap(),
        )
        .await
        .unwrap();
    store
        .record_cgroup(PodRuntimeCgroup::try_new("uid-b", "/pods/uid-b").unwrap())
        .await
        .unwrap();
    store
        .admit_pod_runtime(
            PodRuntimeAdmission::try_new(pod("default", "web", "uid-a"), "worker-a").unwrap(),
        )
        .await
        .unwrap();

    let runtime = store.list_pod_runtime().await.unwrap();
    assert_eq!(
        runtime
            .iter()
            .map(|record| record.pod().uid.as_str())
            .collect::<Vec<_>>(),
        ["uid-a", "uid-b"]
    );
    let owned = store
        .get_pod_runtime(RuntimePodUid::try_new("uid-b").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owned.sandbox_id(), Some("sandbox-b"));
    assert_eq!(owned.cgroup_path(), Some("/pods/uid-b"));
    assert_eq!(
        owned.created_ms(),
        100,
        "admission owns the original timestamp"
    );

    let conflict = store
        .record_owned_sandbox(
            OwnedPodSandbox::try_new(
                pod("other", "replacement", "uid-b"),
                "worker-a",
                "sandbox-other",
                90,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        RuntimeWorkError::OwnershipConflict { pod_uid, .. } if pod_uid == "uid-b"
    ));

    let probe_key = ProbeKey::try_new("uid-b", "main", "readiness").unwrap();
    for (success, result_ms) in [(false, 200), (false, 201), (true, 202)] {
        store
            .record_probe_result(
                ProbeResult::try_new(probe_key.clone(), success, result_ms).unwrap(),
            )
            .await
            .unwrap();
    }
    let probe = store.get_probe_state(probe_key).await.unwrap().unwrap();
    assert_eq!(probe.last_result_ms(), Some(202));
    assert_eq!(probe.last_success(), Some(true));
    assert_eq!(probe.consecutive_failures(), 0);
    assert_eq!(probe.next_eligible_ms(), 202);

    let first_payload = vec![0, 255, 1, 128];
    store
        .enqueue_work(
            PodWorkqueueEnqueue::try_new(
                PodWorkIdentity::try_pod(pod("default", "web", "uid-b")).unwrap(),
                first_payload.clone(),
                2,
                0,
                Some("retry".to_string()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .enqueue_work(
            PodWorkqueueEnqueue::try_new(
                PodWorkIdentity::try_namespace("default", "namespace-uid").unwrap(),
                vec![7, 6, 5],
                0,
                0,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(store.peek_next_due_ms().await.unwrap(), Some(100));
    let first = store
        .claim_due_work(DueTimeMs::try_new(101).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), first_payload);
    assert_eq!(first.attempt_count(), 2);
    assert_eq!(first.next_due_ms().get(), 100);
    let second = store
        .claim_due_work(DueTimeMs::try_new(101).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        second.identity(),
        PodWorkIdentity::Namespace { name, uid }
            if name == "default" && uid == "namespace-uid"
    ));
    assert_eq!(second.next_due_ms().get(), 101);
    assert!(store.peek_next_due_ms().await.unwrap().is_none());
}

#[tokio::test]
async fn slot_admission_is_uid_cas_versioned_and_emits_only_committed_changes() {
    let store = fresh(100).await;
    let mut events = store.subscribe();
    let old =
        PodSlotAdmissionRequest::try_new(pod("default", "web", "uid-old"), "worker-a").unwrap();
    let replacement =
        PodSlotAdmissionRequest::try_new(pod("default", "web", "uid-new"), "worker-a").unwrap();

    assert!(matches!(
        store.try_admit(old.clone()).await.unwrap(),
        PodSlotAdmissionResult::Admitted {
            observed_pod_version
        } if observed_pod_version.get() == 1
    ));
    assert!(matches!(
        events.next_event().await.unwrap(),
        Some(PodSlotAdmissionEvent::Changed {
            state: PodSlotAdmissionState::Admitted,
            observed_pod_version,
            ..
        }) if observed_pod_version.get() == 1
    ));
    assert!(matches!(
        store.try_admit(replacement.clone()).await.unwrap(),
        PodSlotAdmissionResult::Blocked {
            blocking_uid,
            observed_pod_version,
            ..
        } if blocking_uid == "uid-old" && observed_pod_version.get() == 1
    ));
    assert!(matches!(
        store.mark_terminating(old.clone()).await.unwrap(),
        PodSlotMutationResult::Changed {
            observed_pod_version
        } if observed_pod_version.get() == 2
    ));
    assert!(matches!(
        store.clear_if_uid(replacement).await.unwrap(),
        PodSlotClearResult::UidMismatch {
            blocking_uid,
            observed_pod_version,
            ..
        } if blocking_uid == "uid-old" && observed_pod_version.get() == 2
    ));
    assert!(matches!(
        store.clear_if_uid(old).await.unwrap(),
        PodSlotClearResult::Cleared {
            observed_pod_version
        } if observed_pod_version.get() == 3
    ));
}
