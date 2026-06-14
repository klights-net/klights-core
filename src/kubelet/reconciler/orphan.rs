use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouteError, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::watch::{EventType, WatchEvent};

pub struct OrphanScanner;

impl OrphanScanner {
    pub fn key_for_deleted_pod(event: &WatchEvent) -> Option<PodLifecycleKey> {
        if event.event_type != EventType::Deleted {
            return None;
        }
        let namespace = event
            .object
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())?;
        let name = event
            .object
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())?;
        let uid = event
            .object
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str())?;
        Some(PodLifecycleKey::new(namespace, name, uid))
    }

    pub async fn scan_deleted_event(
        router: &PodLifecycleRouter,
        event: &WatchEvent,
    ) -> Result<bool, PodLifecycleRouteError> {
        let Some(key) = Self::key_for_deleted_pod(event) else {
            return Ok(false);
        };
        enqueue_orphan_finalize(router, key, OrphanReason::LeaderDeletedWhileDown).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deleted_pod_event() -> WatchEvent {
        WatchEvent {
            event_type: EventType::Deleted,
            object: std::sync::Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-web"}
            })),
            encoded_payload: None,
        }
    }

    #[test]
    fn deleted_event_triggers_finalize() {
        let key = OrphanScanner::key_for_deleted_pod(&deleted_pod_event())
            .expect("deleted pod event must produce lifecycle key");
        assert_eq!(key, PodLifecycleKey::new("default", "web", "uid-web"));
    }

    #[test]
    fn non_deleted_event_is_ignored() {
        let mut event = deleted_pod_event();
        event.event_type = EventType::Modified;
        assert!(OrphanScanner::key_for_deleted_pod(&event).is_none());
    }
}
