//! `PodApiDeleteService` — API-facing Pod delete service.
//! Delegates to `PodApiService::api_delete_pod` and `api_delete_collection_pods`
//! during migration. Marks Pods terminating and enqueues UID-bound actor work;
//! does not hard-delete Pod rows.

use std::sync::Arc;

use crate::api::{AppError, DeleteOptions};
use crate::kubelet::pod_repository::api::PodApiService;
use crate::kubelet::pod_repository::types::PodApiDeleteOutcome;

pub struct PodApiDeleteService {
    api: Arc<PodApiService>,
}

impl PodApiDeleteService {
    pub fn new(api: Arc<PodApiService>) -> Self {
        Self { api }
    }

    pub async fn delete_pod(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
    ) -> Result<PodApiDeleteOutcome, AppError> {
        self.api.api_delete_pod(ns, name, options, dry_run).await
    }

    pub async fn delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), AppError> {
        self.api
            .api_delete_collection_pods(ns, label_selector, field_selector, dry_run)
            .await
    }
}
