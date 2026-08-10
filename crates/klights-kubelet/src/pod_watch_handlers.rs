use crate::pod_watch_source::PodWatchEvent;

#[async_trait::async_trait]
pub trait PersistentVolumeEventHandler: Send + Sync {
    async fn handle_pvc_event(&self, event: &PodWatchEvent, event_name: &str);
    async fn handle_pv_event(&self, event: &PodWatchEvent, event_name: &str);
}

#[async_trait::async_trait]
impl PersistentVolumeEventHandler for () {
    async fn handle_pvc_event(&self, _event: &PodWatchEvent, _event_name: &str) {}
    async fn handle_pv_event(&self, _event: &PodWatchEvent, _event_name: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn zero_capability_handler_accepts_volume_events_without_side_effects() {
        let handler = ();
        let pvc = PodWatchEvent::added(serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"namespace": "default", "name": "claim", "uid": "claim-uid"}
        }));
        let pv = PodWatchEvent::added(serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "volume", "uid": "volume-uid"}
        }));

        handler.handle_pvc_event(&pvc, "claim").await;
        handler.handle_pv_event(&pv, "volume").await;
    }
}
