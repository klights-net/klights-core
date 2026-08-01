//! Root compatibility wrapper for ordinary SQLite patch transactions.

use super::ordinary;
use super::*;
use anyhow::{Result, anyhow};
use serde_json::Value;

impl Datastore {
    pub async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>> {
        self.patch_resource_latest_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePatchRequest::without_preconditions(patch_kind, patch),
        )
        .await
    }

    pub async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        #[cfg(any(test, feature = "test-support"))]
        self.pause_resource_mutation_if_requested(
            ResourceMutationPauseOperation::PatchLatest,
            api_version,
            kind,
            namespace,
            name,
        )
        .await;
        let transition_time =
            klights_cluster_core::k8s_time::format_legacy_timestamp(self.wall_clock.now_utc());
        let input = ordinary::PatchResourceInput::new(
            api_version,
            kind,
            namespace,
            name,
            request,
            transition_time,
        );
        self.db_call("db_query", move |conn| {
            ordinary::patch_resource_in_conn(conn, input)
        })
        .await
        .map_err(|error| anyhow!("Failed to patch resource: {error}"))
    }
}
