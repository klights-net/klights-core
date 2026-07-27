use anyhow::Result;
use async_trait::async_trait;

use crate::control_plane::client::{ListRequest, legacy_list_response};
use crate::datastore::{ResourceList, WatchReplayPosition};
use klights_cluster_core::Resource;
use klights_leader_api::{
    CacheReadinessRequest, LeaderWatchError, ResourceEvent, ResourceListRequest,
    ResourceQueryConsistency, WatchRequest,
};
use klights_types::ResourceKey;

#[async_trait]
pub trait RemoteInformerCache: Send + Sync {
    async fn insert(&self, resource: Resource);
    async fn get(&self, key: &ResourceKey) -> Option<Resource>;
    async fn list(
        &self,
        request: &ResourceListRequest,
    ) -> Result<klights_leader_api::ResourceListResult>;
    async fn replace_scope(
        &self,
        request: &ResourceListRequest,
        resources: Vec<Resource>,
        position: WatchReplayPosition,
    ) -> Result<()>;
    async fn apply_event(&self, event: &ResourceEvent) -> Option<Resource>;
    async fn mark_ready(&self, scope: CacheReadinessRequest) -> Result<()>;
    async fn clear_ready(&self, scope: &CacheReadinessRequest);
    async fn is_ready(&self, scope: &CacheReadinessRequest) -> bool;
    async fn wait_ready(&self, scope: CacheReadinessRequest);
}

pub trait WatchTransitionProjectorFactory: Send + Sync {
    fn projector(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, LeaderWatchError>;
}

pub trait WatchTransitionProjector: Send {
    fn replace(&mut self, resources: &[Resource]);
    fn prepare(&self, event: ResourceEvent) -> Result<PreparedWatchTransition, LeaderWatchError>;
    fn commit(&mut self, prepared: PreparedWatchTransition) -> Result<(), LeaderWatchError>;
}

pub struct PreparedWatchTransition {
    event: Option<ResourceEvent>,
    token: Box<dyn std::any::Any + Send>,
}

impl PreparedWatchTransition {
    pub(crate) fn new<T: std::any::Any + Send>(event: Option<ResourceEvent>, token: T) -> Self {
        Self {
            event,
            token: Box::new(token),
        }
    }

    pub fn event(&self) -> Option<&ResourceEvent> {
        self.event.as_ref()
    }

    pub(crate) fn into_token<T: std::any::Any + Send>(self) -> Result<T, LeaderWatchError> {
        self.token.downcast::<T>().map(|token| *token).map_err(|_| {
            LeaderWatchError::malformed_event("watch transition projector token type mismatch")
        })
    }
}

pub(super) async fn list(
    cache: &dyn RemoteInformerCache,
    request: &ListRequest,
) -> Result<ResourceList> {
    cache
        .list(&focused_list_request(request)?)
        .await
        .map(legacy_list_response)
}

pub(super) async fn replace_scope(
    cache: &dyn RemoteInformerCache,
    request: &ListRequest,
    list: ResourceList,
) -> Result<()> {
    let position = list
        .watch_replay_position
        .unwrap_or_else(|| WatchReplayPosition::from_resource_version(list.resource_version));
    cache
        .replace_scope(&focused_list_request(request)?, list.items, position)
        .await
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
