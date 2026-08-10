use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessRequest, LeaderWatchError, ResourceEvent, ResourceListRequest,
    ResourceListResult, WatchRequest,
};
use klights_types::ResourceKey;

/// Cache behavior required by a remote LIST/WATCH reflector.
#[async_trait]
pub trait RemoteInformerCache: Send + Sync {
    async fn insert(&self, resource: Resource);
    async fn get(&self, key: &ResourceKey) -> Option<Resource>;
    async fn list(&self, request: &ResourceListRequest) -> Result<ResourceListResult>;
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

/// Creates selector-aware transition projectors for one remote watch.
pub trait WatchTransitionProjectorFactory: Send + Sync {
    fn projector(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, LeaderWatchError>;
}

/// Prepares and commits selector membership changes around event delivery.
pub trait WatchTransitionProjector: Send {
    fn replace(&mut self, resources: &[Resource]);
    fn prepare(&self, event: ResourceEvent) -> Result<PreparedWatchTransition, LeaderWatchError>;
    fn commit(&mut self, prepared: PreparedWatchTransition) -> Result<(), LeaderWatchError>;
}

/// An event projection paired with its opaque owner-specific commit token.
pub struct PreparedWatchTransition {
    event: Option<ResourceEvent>,
    token: Box<dyn std::any::Any + Send>,
}

impl PreparedWatchTransition {
    pub fn new<T: std::any::Any + Send>(event: Option<ResourceEvent>, token: T) -> Self {
        Self {
            event,
            token: Box::new(token),
        }
    }

    pub fn event(&self) -> Option<&ResourceEvent> {
        self.event.as_ref()
    }

    pub fn into_token<T: std::any::Any + Send>(self) -> Result<T, LeaderWatchError> {
        self.token.downcast::<T>().map(|token| *token).map_err(|_| {
            LeaderWatchError::malformed_event("watch transition projector token type mismatch")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::PreparedWatchTransition;

    #[test]
    fn prepared_transition_preserves_exact_token_type_and_rejects_mismatch() {
        assert_eq!(
            PreparedWatchTransition::new(None, 41_u64)
                .into_token::<u64>()
                .expect("matching projector token"),
            41
        );
        assert!(
            PreparedWatchTransition::new(None, 41_u64)
                .into_token::<String>()
                .is_err()
        );
    }
}
