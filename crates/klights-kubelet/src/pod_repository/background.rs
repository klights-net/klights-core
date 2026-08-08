//! Explicit startup boundary for the Pod repository workqueue.

use std::sync::Arc;

use super::workqueue::PodWorkqueue;

/// Services that must be started after repository construction.
pub struct PodRepositoryBackground {
    workqueue: Arc<PodWorkqueue>,
}

impl PodRepositoryBackground {
    pub fn new(workqueue: Arc<PodWorkqueue>) -> Self {
        Self { workqueue }
    }

    /// Start the deferred Pod workqueue reconciler.
    pub async fn start(&self) -> anyhow::Result<()> {
        self.workqueue.start().await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn workqueue_start_called(&self) -> bool {
        self.workqueue.start_called()
    }
}
