use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessRequest, ResourceEvent, ResourceListRequest, ResourceListResult, WatchEventType,
};
use klights_types::ResourceKey;
use tokio::sync::{Notify, RwLock};

use crate::filter::ResourceFilter;

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchCacheError {
    InvalidSelector { message: String },
    CorruptState { message: String },
    NotReady { message: String },
}

impl WatchCacheError {
    pub(crate) fn invalid_selector(message: impl Into<String>) -> Self {
        Self::InvalidSelector {
            message: message.into(),
        }
    }
}

impl fmt::Display for WatchCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSelector { message }
            | Self::CorruptState { message }
            | Self::NotReady { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for WatchCacheError {}

#[derive(Clone)]
struct CachedResource {
    resource: Resource,
    position: Option<WatchReplayPosition>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl CacheKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }

    fn from_key(key: &ResourceKey) -> Self {
        Self {
            api_version: key.api_version.clone(),
            kind: key.kind.clone(),
            namespace: key.namespace.clone(),
            name: key.name.clone(),
        }
    }
}

#[derive(Default)]
struct WatchCacheState {
    resources: HashMap<CacheKey, CachedResource>,
    tombstones: HashMap<CacheKey, WatchReplayPosition>,
    scopes: HashMap<CacheReadinessRequest, CachedScope>,
    pinned_resources: HashSet<CacheKey>,
    ready_scopes: HashSet<CacheReadinessRequest>,
}

#[derive(Default)]
struct CachedScope {
    position: Option<WatchReplayPosition>,
    members: HashMap<CacheKey, CachedResource>,
}

#[derive(Clone, Default)]
pub struct WatchCache {
    state: Arc<RwLock<WatchCacheState>>,
    ready: Arc<Notify>,
}

impl WatchCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, key: &ResourceKey) -> Option<Resource> {
        self.state
            .read()
            .await
            .resources
            .get(&CacheKey::from_key(key))
            .map(|cached| cached.resource.clone())
    }

    pub async fn list(
        &self,
        request: &ResourceListRequest,
    ) -> Result<ResourceListResult, WatchCacheError> {
        let filter = ResourceFilter::for_list(request)?;
        let scope = cache_scope(request)?;
        let state = self.state.read().await;
        if !state.ready_scopes.contains(&scope) {
            return Err(WatchCacheError::NotReady {
                message: format!(
                    "watch cache scope {}/{} {:?} is not ready",
                    scope.api_version(),
                    scope.kind(),
                    scope.namespace(),
                ),
            });
        }
        let scope_state = state.scopes.get(&scope);
        let mut items = match scope_state {
            Some(scope_state) => scope_state
                .members
                .values()
                .filter(|cached| filter.matches(&cached.resource))
                .map(|cached| cached.resource.clone())
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        items.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.name.cmp(&right.name))
        });
        let position = scope_state.and_then(|scope| scope.position);
        ResourceListResult::try_new(
            items,
            position.map_or(0, |position| position.resource_version),
            position,
            None,
            None,
        )
        .map_err(|error| WatchCacheError::CorruptState {
            message: error.to_string(),
        })
    }

    pub async fn replace_scope(
        &self,
        request: &ResourceListRequest,
        resources: Vec<Resource>,
        position: WatchReplayPosition,
    ) -> Result<(), WatchCacheError> {
        let filter = ResourceFilter::for_list(request)?;
        let scope = cache_scope(request)?;
        position
            .validate()
            .map_err(|message| WatchCacheError::CorruptState { message })?;
        if resources.iter().any(|resource| !filter.matches(resource)) {
            return Err(WatchCacheError::CorruptState {
                message: "cache replacement contains a resource outside its LIST scope".to_string(),
            });
        }
        if resources
            .iter()
            .any(|resource| resource.resource_version > position.resource_version)
        {
            return Err(WatchCacheError::CorruptState {
                message: "cache replacement contains a body newer than its snapshot position"
                    .to_string(),
            });
        }
        let mut unique = HashSet::with_capacity(resources.len());
        if resources
            .iter()
            .map(CacheKey::from_resource)
            .any(|key| !unique.insert(key))
        {
            return Err(WatchCacheError::CorruptState {
                message: "cache replacement contains duplicate resource identities".to_string(),
            });
        }
        let mut state = self.state.write().await;
        if state
            .scopes
            .get(&scope)
            .and_then(|state| state.position)
            .is_some_and(|current| !current.permits_successor(position))
        {
            return Ok(());
        }
        let prior_members = state
            .scopes
            .remove(&scope)
            .map(|scope| scope.members.into_keys().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut members = HashMap::with_capacity(resources.len());
        for resource in resources {
            let key = CacheKey::from_resource(&resource);
            members.insert(
                key.clone(),
                CachedResource {
                    resource: resource.clone(),
                    position: Some(position),
                },
            );
            let keep_newer_tombstone = state.tombstones.get(&key).is_some_and(|tombstone| {
                *tombstone == position || !tombstone.permits_successor(position)
            });
            let keep_newer_resource = state.resources.get(&key).is_some_and(|cached| {
                cached
                    .position
                    .is_some_and(|current| !current.permits_successor(position))
            });
            let keep_newer = keep_newer_tombstone || keep_newer_resource;
            if !keep_newer {
                state.tombstones.remove(&key);
                state.resources.insert(
                    key,
                    CachedResource {
                        resource,
                        position: Some(position),
                    },
                );
            }
        }
        state.scopes.insert(
            scope,
            CachedScope {
                position: Some(position),
                members,
            },
        );
        for key in prior_members {
            let retained = state.pinned_resources.contains(&key)
                || state
                    .scopes
                    .values()
                    .any(|scope| scope.members.contains_key(&key));
            if !retained {
                state.resources.remove(&key);
            }
        }
        Ok(())
    }

    pub async fn insert(&self, resource: Resource) {
        let key = CacheKey::from_resource(&resource);
        let mut state = self.state.write().await;
        state.pinned_resources.insert(key.clone());
        state.resources.insert(
            key,
            CachedResource {
                resource,
                position: None,
            },
        );
    }

    pub async fn apply_event(&self, event: &ResourceEvent) -> Option<Resource> {
        if matches!(
            event.event_type(),
            WatchEventType::Bookmark | WatchEventType::Error
        ) {
            return None;
        }
        let resource = event.resource().clone();
        let key = CacheKey::from_resource(&resource);
        let mut state = self.state.write().await;
        let scopes = state.scopes.keys().cloned().collect::<Vec<_>>();
        let mut applied_to_scope = false;
        for scope in scopes {
            let Ok(filter) = ResourceFilter::for_cache_scope(&scope) else {
                continue;
            };
            if !filter.matches_identity(&resource) {
                continue;
            }
            let Some(scope_state) = state.scopes.get_mut(&scope) else {
                continue;
            };
            let successor = match (scope_state.position, event.resume_position()) {
                (Some(current), Some(delivered)) => {
                    delivered != current && current.permits_successor(delivered)
                }
                (None, Some(_)) => true,
                (_, None) => scope_state.members.get(&key).is_none_or(|current| {
                    resource.resource_version >= current.resource.resource_version
                }),
            };
            if !successor {
                continue;
            }
            match event.event_type() {
                WatchEventType::Deleted => {
                    scope_state.members.remove(&key);
                }
                WatchEventType::Added | WatchEventType::Modified if filter.matches(&resource) => {
                    scope_state.members.insert(
                        key.clone(),
                        CachedResource {
                            resource: resource.clone(),
                            position: event.resume_position(),
                        },
                    );
                }
                WatchEventType::Added | WatchEventType::Modified => {
                    scope_state.members.remove(&key);
                }
                WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
            }
            if let Some(delivered) = event.resume_position() {
                scope_state.position = Some(delivered);
            }
            applied_to_scope = true;
        }

        let global_successor = match event.resume_position() {
            Some(delivered) => {
                state.tombstones.get(&key).is_none_or(|tombstone| {
                    delivered != *tombstone && tombstone.permits_successor(delivered)
                }) && state.resources.get(&key).is_none_or(|current| {
                    current.position.is_none_or(|position| {
                        delivered != position && position.permits_successor(delivered)
                    })
                })
            }
            None => state.resources.get(&key).is_none_or(|current| {
                resource.resource_version >= current.resource.resource_version
            }),
        };
        if !global_successor && !applied_to_scope {
            return None;
        }
        match event.event_type() {
            WatchEventType::Deleted => {
                if global_successor {
                    state.pinned_resources.remove(&key);
                    if let Some(position) = event.resume_position() {
                        state.tombstones.insert(key.clone(), position);
                    }
                    let retained = state.pinned_resources.contains(&key)
                        || state
                            .scopes
                            .values()
                            .any(|scope| scope.members.contains_key(&key));
                    if !retained {
                        state.resources.remove(&key);
                    }
                }
            }
            WatchEventType::Added | WatchEventType::Modified => {
                if global_successor {
                    state.tombstones.remove(&key);
                    state.resources.insert(
                        key,
                        CachedResource {
                            resource: resource.clone(),
                            position: event.resume_position(),
                        },
                    );
                }
            }
            WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
        }
        Some(resource)
    }

    pub async fn mark_ready(&self, scope: CacheReadinessRequest) -> Result<(), WatchCacheError> {
        let mut state = self.state.write().await;
        if !state.scopes.contains_key(&scope) {
            return Err(WatchCacheError::NotReady {
                message: "watch cache scope cannot become ready before a baseline replacement"
                    .to_string(),
            });
        }
        state.ready_scopes.insert(scope);
        drop(state);
        self.ready.notify_waiters();
        Ok(())
    }

    pub async fn clear_ready(&self, scope: &CacheReadinessRequest) {
        self.state.write().await.ready_scopes.remove(scope);
    }

    pub async fn is_ready(&self, scope: &CacheReadinessRequest) -> bool {
        self.state.read().await.ready_scopes.contains(scope)
    }

    pub async fn wait_ready(&self, scope: CacheReadinessRequest) {
        loop {
            // Register before checking the predicate so a concurrent mark
            // cannot be lost between the read and the await.
            let notified = self.ready.notified();
            if self.is_ready(&scope).await {
                return;
            }
            notified.await;
        }
    }
}

fn cache_scope(request: &ResourceListRequest) -> Result<CacheReadinessRequest, WatchCacheError> {
    CacheReadinessRequest::try_new(
        request.api_version(),
        request.kind(),
        request.namespace().map(str::to_owned),
        request.label_selector().map(str::to_owned),
        request.field_selector().map(str::to_owned),
    )
    .map_err(|error| WatchCacheError::CorruptState {
        message: error.to_string(),
    })
}
