use crate::kubelet::pod_lifecycle_core::message::{
    LifecycleMessage, PodLifecycleKey, PodLifecycleWorkFailure, PodLifecycleWorkKind,
};
use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodSlotAdmission;
use tokio_util::sync::CancellationToken;

/// Request object for UID-qualified pod slot admission checks.
pub use klights_kubelet::runtime::PodSlotAdmissionRequest;

fn lifecycle_key_from_runtime_key(key: &PodRuntimeKey) -> PodLifecycleKey {
    PodLifecycleKey::new(&key.namespace, &key.name, &key.uid)
}

pub async fn check_slot_admission(
    slot_admission: &dyn PodSlotAdmission,
    node_name: &str,
    request: PodSlotAdmissionRequest,
    reply_to: LifecycleReplyHandle,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let PodSlotAdmissionRequest {
        key,
        pod,
        resource_version,
        start_after_admit,
        operation_id,
    } = request;
    let lifecycle_key = lifecycle_key_from_runtime_key(&key);
    match slot_admission.try_admit(&key, node_name).await {
        Ok(klights_node_store::PodSlotAdmissionResult::Admitted { .. }) => {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionGranted {
                    key: lifecycle_key,
                    operation_id,
                    pod,
                    resource_version,
                    start_after_admit,
                })
                .await;
        }
        Ok(klights_node_store::PodSlotAdmissionResult::Blocked {
            blocking_uid,
            blocking_node,
            state: _,
            ..
        }) => {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionBlocked {
                    key: lifecycle_key,
                    operation_id,
                    blocking_uid: blocking_uid.clone(),
                    blocking_node,
                })
                .await;
            wait_for_slot_admission_event(slot_admission, key, blocking_uid, reply_to, cancel)
                .await;
        }
        Err(err) => {
            let _ = reply_to
                .route(LifecycleMessage::PodWorkFailed {
                    key: lifecycle_key,
                    operation_id,
                    kind: PodLifecycleWorkKind::CheckSlotAdmission,
                    retryable: true,
                    failure: PodLifecycleWorkFailure::Startup(format!(
                        "pod_slot_try_admit: {err:#}"
                    )),
                })
                .await;
        }
    }
    Ok(())
}

async fn wait_for_slot_admission_event(
    slot_admission: &dyn PodSlotAdmission,
    key: PodRuntimeKey,
    blocking_uid: String,
    reply_to: LifecycleReplyHandle,
    cancel: CancellationToken,
) {
    let mut events = slot_admission.subscribe();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return;
            }
            event = events.next_event() => {
                match event {
                    Ok(Some(klights_node_store::PodSlotAdmissionEvent::Cleared {
                        pod,
                        ..
                    })) if pod.namespace == key.namespace
                        && pod.name == key.name
                        && pod.uid == blocking_uid =>
                    {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                    Ok(Some(klights_node_store::PodSlotAdmissionEvent::Changed {
                        pod,
                        ..
                    })) if pod.namespace == key.namespace && pod.name == key.name => {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => return,
                    Err(_) => {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                }
            }
        }
    }
}
