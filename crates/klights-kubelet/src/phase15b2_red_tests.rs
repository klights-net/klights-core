use std::sync::Arc;

use crate::pod_lifecycle_actor::actor::PodLifecycleActor;
use crate::pod_lifecycle_core::action::PodAction;
use crate::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey, PodLifecycleWorkKind};
use crate::pod_lifecycle_router::LifecycleReplyHandle;
use crate::pod_lifecycle_router::executor::{NoopExecutor, PodWorkExecutor};
use crate::pod_lifecycle_router::multiplex::MultiplexPodLifecycleBackend;

fn direct_test_actor() -> PodLifecycleActor {
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(16);
    let executor: Arc<dyn PodWorkExecutor> = Arc::new(NoopExecutor);
    PodLifecycleActor::new_with_event_sink_for_test(
        32,
        event_tx,
        Arc::new(std::sync::Mutex::new(executor)),
        LifecycleReplyHandle::new(Arc::new(MultiplexPodLifecycleBackend)),
    )
}

fn pod(namespace: &str, name: &str, uid: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid
        },
        "spec": {
            "nodeName": "worker-a",
            "containers": [{"name": "app", "image": "example.invalid/app"}]
        },
        "status": {"phase": "Pending"}
    })
}

#[test]
fn actor_clears_uid_and_slot_before_bound_uid_finalization() {
    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let old_key = PodLifecycleKey::new("default", "same-name", "uid-old");
    let replacement_key = PodLifecycleKey::new("default", "same-name", "uid-new");
    let old_pod = pod("default", "same-name", "uid-old");
    let replacement_pod = pod("default", "same-name", "uid-new");

    let admission = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: old_key.clone(),
        resource_version: Some(1),
        pod: old_pod.clone(),
    });
    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: old_key.clone(),
        operation_id: admission.operation_id().expect("admission operation"),
        pod: old_pod.clone(),
        resource_version: Some(1),
        start_after_admit: true,
    });
    let startup_finalization = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: start.operation_id().expect("start operation"),
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-old".to_string()),
    });
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: startup_finalization
            .operation_id()
            .expect("startup finalization operation"),
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-old".to_string()),
    });

    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: old_key.clone(),
        resource_version: Some(2),
        pod: old_pod,
    });
    assert!(matches!(stop, PodAction::StopPod { .. }));
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: replacement_key.clone(),
            resource_version: Some(3),
            pod: replacement_pod.clone(),
        }),
        PodAction::Noop
    ));

    let bound_finalization = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: stop.operation_id().expect("stop operation"),
        kind: PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });
    assert!(
        matches!(
            &bound_finalization,
            PodAction::FinalizePodDeletion { key, .. } if key == &old_key
        ),
        "runtime cleanup completion must route into UID-bound finalization"
    );
    assert_eq!(
        actor.active_uid_for_test(),
        None,
        "actor cache identity must be cleared before bound finalization"
    );
    assert_eq!(
        actor.admitted_slot_uid_for_test(),
        None,
        "same-name slot ownership must be cleared before bound finalization"
    );

    let replacement_admission = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key,
        operation_id: bound_finalization
            .operation_id()
            .expect("bound finalization operation"),
        kind: PodLifecycleWorkKind::FinalizePodDeletion,
        sandbox_id: None,
    });
    assert!(
        matches!(
            replacement_admission,
            PodAction::CheckSlotAdmission { key, pod, .. }
                if key == replacement_key && pod == replacement_pod
        ),
        "same-name replacement must wait for successful UID-bound finalization"
    );
}
