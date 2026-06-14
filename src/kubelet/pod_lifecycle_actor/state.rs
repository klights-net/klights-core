//! Re-exports from `pod_lifecycle_core::state` plus the actor side-table used
//! for keyed cleanup.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use super::message::PodLifecycleKey;

// Re-export the canonical types from core.
pub use crate::kubelet::pod_lifecycle_core::state::PodLifecycleState;

/// Side-table keyed by `PodLifecycleKey` for actor cleanup.
pub type PodLifecycleStateTracker = Arc<Mutex<HashMap<PodLifecycleKey, PodLifecycleState>>>;

pub fn new_pod_lifecycle_state_tracker() -> PodLifecycleStateTracker {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn remove_pod_state(tracker: &PodLifecycleStateTracker, key: &PodLifecycleKey) {
    tracker.lock().await.remove(key);
}
