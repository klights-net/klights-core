use crate::kubelet::pod_lifecycle_core::message::{
    LifecycleMessage, PodLifecycleKey, PodLifecycleWorkFailure, PodLifecycleWorkKind,
};
use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodSlotAdmission;
use tokio_util::sync::CancellationToken;

/// Request object for UID-qualified pod slot admission checks.
#[derive(Clone, Debug)]
pub struct PodSlotAdmissionRequest {
    pub key: PodRuntimeKey,
    pub pod: serde_json::Value,
    pub resource_version: Option<i64>,
    pub start_after_admit: bool,
    pub operation_id: u64,
}

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
        Ok(crate::datastore::PodSlotAdmissionResult::Admitted { .. }) => {
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
        Ok(crate::datastore::PodSlotAdmissionResult::Blocked {
            blocking_uid,
            blocking_node,
            state,
            ..
        }) => {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionBlocked {
                    key: lifecycle_key,
                    operation_id,
                    blocking_uid: blocking_uid.clone(),
                    blocking_node,
                    state,
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
            event = events.recv() => {
                match event {
                    Ok(crate::datastore::PodSlotAdmissionEvent::Cleared {
                        namespace,
                        pod_name,
                        pod_uid,
                        ..
                    }) if namespace == key.namespace
                        && pod_name == key.name
                        && pod_uid == blocking_uid =>
                    {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                    Ok(crate::datastore::PodSlotAdmissionEvent::Changed {
                        namespace,
                        pod_name,
                        ..
                    }) if namespace == key.namespace && pod_name == key.name => {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = reply_to
                            .route(LifecycleMessage::SlotAdmissionWake {
                                key: lifecycle_key_from_runtime_key(&key),
                            })
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}
