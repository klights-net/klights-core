//! `PodApiUpdateService` — API-facing Pod update and patch service.
//! Delegates to `PodApiService::api_update_pod` and `api_patch_pod` during migration.

use std::sync::Arc;

use crate::api::AppError;
use crate::datastore::Resource;
use crate::kubelet::pod_repository::PodStatusPatchType;
use crate::kubelet::pod_repository::api::PodApiService;
use crate::kubelet::pod_repository::types::PodApiUpdateOutcome;

pub struct PodApiUpdateService {
    api: Arc<PodApiService>,
}

impl PodApiUpdateService {
    pub fn new(api: Arc<PodApiService>) -> Self {
        Self { api }
    }

    pub async fn update_pod(
        &self,
        ns: &str,
        name: &str,
        body: serde_json::Value,
        current: Resource,
        dry_run: bool,
    ) -> Result<PodApiUpdateOutcome, AppError> {
        self.api
            .api_update_pod(ns, name, body, current, dry_run)
            .await
    }

    pub async fn patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> Result<PodApiUpdateOutcome, AppError> {
        self.api
            .api_patch_pod(ns, name, patch, patch_type, dry_run)
            .await
    }
}
