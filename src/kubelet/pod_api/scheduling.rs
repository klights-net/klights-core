//! `PodSchedulingService` — scheduling decision logic used by Pod create and
//! update flows. Delegates to `PodApiService::schedule_pending_pod` during
//! migration. Preserves preemption victim marking and unschedulable event
//! behaviour.

use std::sync::Arc;

use crate::api::AppError;
use crate::datastore::Resource;
use crate::kubelet::pod_repository::api::PodApiService;

pub struct PodSchedulingService {
    api: Arc<PodApiService>,
}

impl PodSchedulingService {
    pub fn new(api: Arc<PodApiService>) -> Self {
        Self { api }
    }

    /// Schedule a pending Pod onto a node. Delegates to the existing inline
    /// single-node / deferred multi-node scheduling decision logic.
    pub async fn schedule_pending_pod(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<Option<Resource>, AppError> {
        self.api.schedule_pending_pod(ns, name).await
    }
}
