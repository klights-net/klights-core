use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessRequest, ResourceEvent, ResourceListRequest, ResourceListResult,
};
use klights_types::ResourceKey;

use crate::control_plane::client::informer::RemoteInformerCache;
use crate::control_plane::client::informer::{
    PreparedWatchTransition, WatchTransitionProjector, WatchTransitionProjectorFactory,
};

#[derive(Clone, Default)]
pub(crate) struct WatchCacheAdapter {
    cache: klights_watch::WatchCache,
}

impl WatchCacheAdapter {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RemoteInformerCache for WatchCacheAdapter {
    async fn insert(&self, resource: Resource) {
        self.cache.insert(resource).await;
    }

    async fn get(&self, key: &ResourceKey) -> Option<Resource> {
        self.cache.get(key).await
    }

    async fn list(&self, request: &ResourceListRequest) -> Result<ResourceListResult> {
        Ok(self.cache.list(request).await?)
    }

    async fn replace_scope(
        &self,
        request: &ResourceListRequest,
        resources: Vec<Resource>,
        position: WatchReplayPosition,
    ) -> Result<()> {
        Ok(self
            .cache
            .replace_scope(request, resources, position)
            .await?)
    }

    async fn apply_event(&self, event: &ResourceEvent) -> Option<Resource> {
        self.cache.apply_event(event).await
    }

    async fn mark_ready(&self, scope: CacheReadinessRequest) -> Result<()> {
        Ok(self.cache.mark_ready(scope).await?)
    }

    async fn clear_ready(&self, scope: &CacheReadinessRequest) {
        self.cache.clear_ready(scope).await;
    }

    async fn is_ready(&self, scope: &CacheReadinessRequest) -> bool {
        self.cache.is_ready(scope).await
    }

    async fn wait_ready(&self, scope: CacheReadinessRequest) {
        self.cache.wait_ready(scope).await;
    }
}

struct RootWatchTransitionProjector {
    membership: klights_watch::WatchSelectorMembership,
}

impl WatchTransitionProjector for RootWatchTransitionProjector {
    fn replace(&mut self, resources: &[Resource]) {
        self.membership.replace(resources);
    }

    fn prepare(
        &self,
        event: ResourceEvent,
    ) -> Result<PreparedWatchTransition, klights_leader_api::LeaderWatchError> {
        let pending = self.membership.prepare(event)?;
        Ok(PreparedWatchTransition::new(
            pending.event().cloned(),
            pending,
        ))
    }

    fn commit(
        &mut self,
        prepared: PreparedWatchTransition,
    ) -> Result<(), klights_leader_api::LeaderWatchError> {
        self.membership
            .commit(prepared.into_token::<klights_watch::PendingWatchSelectorTransition>()?);
        Ok(())
    }
}

impl WatchTransitionProjectorFactory for WatchCacheAdapter {
    fn projector(
        &self,
        request: &klights_leader_api::WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, klights_leader_api::LeaderWatchError> {
        Ok(Box::new(RootWatchTransitionProjector {
            membership: klights_watch::WatchSelectorMembership::try_new(request)?,
        }))
    }
}
