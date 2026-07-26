use anyhow::Result;

use crate::control_plane::client::{ListRequest, legacy_list_response};
use crate::datastore::{Resource, ResourceList, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessRequest, ResourceEvent, ResourceListRequest, ResourceQueryConsistency,
};
use klights_types::ResourceKey;

/// Temporary legacy-client facade over the canonical watch-leaf cache.
#[derive(Clone)]
pub(super) struct InformerCache {
    inner: klights_watch::WatchCache,
}

impl InformerCache {
    pub(super) fn new() -> Self {
        Self {
            inner: klights_watch::WatchCache::new(),
        }
    }

    pub(super) async fn get(&self, key: &ResourceKey) -> Option<Resource> {
        self.inner.get(key).await
    }

    pub(super) async fn insert(&self, resource: Resource) {
        self.inner.insert(resource).await;
    }

    pub(super) async fn list(&self, request: &ListRequest) -> Result<ResourceList> {
        let request = focused_list_request(request)?;
        self.inner
            .list(&request)
            .await
            .map(legacy_list_response)
            .map_err(Into::into)
    }

    pub(super) async fn replace_scope(
        &self,
        request: &ListRequest,
        list: ResourceList,
    ) -> Result<()> {
        let request = focused_list_request(request)?;
        let position = list
            .watch_replay_position
            .unwrap_or_else(|| WatchReplayPosition::from_resource_version(list.resource_version));
        self.inner
            .replace_scope(&request, list.items, position)
            .await
            .map_err(Into::into)
    }

    pub(super) async fn apply_event(&self, event: &ResourceEvent) -> Result<Option<Resource>> {
        Ok(self.inner.apply_event(event).await)
    }

    pub(super) async fn mark_primed(&self, scope: CacheReadinessRequest) -> Result<()> {
        self.inner.mark_ready(scope).await.map_err(Into::into)
    }

    #[cfg(test)]
    pub(super) async fn clear_scope_for_test(&self, scope: &CacheReadinessRequest) {
        self.inner.clear_ready(scope).await;
    }

    pub(super) async fn wait_ready(&self, scope: CacheReadinessRequest) -> Result<()> {
        self.inner.wait_ready(scope).await;
        Ok(())
    }

    pub(super) async fn is_ready(&self, scope: &CacheReadinessRequest) -> bool {
        self.inner.is_ready(scope).await
    }
}

pub(super) fn scope_for_request(request: &ListRequest) -> CacheReadinessRequest {
    CacheReadinessRequest::try_new(
        request.api_version.clone(),
        request.kind.clone(),
        request.namespace.clone(),
        request.label_selector.clone(),
        request.field_selector.clone(),
    )
    .expect("legacy LIST request identity was already validated")
}

fn focused_list_request(request: &ListRequest) -> Result<ResourceListRequest> {
    Ok(ResourceListRequest::try_new(
        request.api_version.clone(),
        request.kind.clone(),
        request.namespace.clone(),
        request.label_selector.clone(),
        request.field_selector.clone(),
        request.limit,
        request.continue_token.clone(),
        ResourceQueryConsistency::Cached,
    )?)
}
