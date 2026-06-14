use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodNetworkKey {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
}

impl PodNetworkKey {
    pub fn new(sandbox_id: &str, namespace: &str, pod_name: &str, pod_uid: &str) -> Self {
        Self {
            sandbox_id: sandbox_id.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
        }
    }
}

#[derive(Clone, Default)]
pub struct PodNetworkEvents {
    inner: Arc<RwLock<HashMap<PodNetworkKey, Arc<Notify>>>>,
}

impl PodNetworkEvents {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe(&self, key: &PodNetworkKey) -> Arc<Notify> {
        let mut guard = self.inner.write().await;
        guard
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    pub async fn publish_assignment(&self, key: &PodNetworkKey) {
        let notify = {
            let guard = self.inner.read().await;
            guard.get(key).cloned()
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    pub async fn remove(&self, key: &PodNetworkKey) {
        self.inner.write().await.remove(key);
    }

    #[cfg(test)]
    pub async fn has_subscriber_for_test(&self, key: &PodNetworkKey) -> bool {
        self.inner.read().await.contains_key(key)
    }
}
