//! `PodApiCreateService` — API-facing Pod create service.
//! Delegates to `PodApiService::api_create_pod` during migration.

use std::sync::Arc;

use crate::api::AppError;
use crate::kubelet::pod_repository::api::PodApiService;
use crate::kubelet::pod_repository::types::{PodApiCreateRequest, PodApiCreateResult};

pub struct PodApiCreateService {
    api: Arc<PodApiService>,
}

impl PodApiCreateService {
    pub fn new(api: Arc<PodApiService>) -> Self {
        Self { api }
    }

    pub async fn create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> Result<PodApiCreateResult, AppError> {
        self.api.api_create_pod(request).await
    }
}
