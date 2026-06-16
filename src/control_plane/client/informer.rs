use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{Notify, RwLock};

use crate::control_plane::client::{CacheScope, ListRequest, ResourceEvent, ResourceKey};
use crate::datastore::{Resource, ResourceList};
use crate::watch::EventType;

#[derive(Clone)]
pub(super) struct InformerCache {
    resources: Arc<RwLock<HashMap<String, Resource>>>,
    primed_scopes: Arc<RwLock<HashSet<CacheScope>>>,
    ready: Arc<Notify>,
}

impl InformerCache {
    pub(super) fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
            primed_scopes: Arc::new(RwLock::new(HashSet::new())),
            ready: Arc::new(Notify::new()),
        }
    }

    pub(super) async fn get(&self, key: &ResourceKey) -> Option<Resource> {
        let cache_key = resource_cache_key(
            &key.api_version,
            &key.kind,
            key.namespace.as_deref(),
            &key.name,
        );
        self.resources.read().await.get(&cache_key).cloned()
    }

    pub(super) async fn insert(&self, resource: Resource) {
        let key = resource_cache_key(
            &resource.api_version,
            &resource.kind,
            resource.namespace.as_deref(),
            &resource.name,
        );
        self.resources.write().await.insert(key, resource);
    }

    pub(super) async fn list(&self, req: &ListRequest) -> ResourceList {
        let guard = self.resources.read().await;
        let items = guard
            .values()
            .filter(|resource| resource_matches_request(resource, req))
            .cloned()
            .collect();
        ResourceList {
            items,
            resource_version: 0,
            continue_token: None,
            remaining_item_count: None,
        }
    }

    pub(super) async fn replace_scope(&self, req: &ListRequest, list: ResourceList) {
        let mut guard = self.resources.write().await;
        guard.retain(|_, resource| !resource_matches_request(resource, req));
        for resource in list.items {
            let key = resource_cache_key(
                &resource.api_version,
                &resource.kind,
                resource.namespace.as_deref(),
                &resource.name,
            );
            guard.insert(key, resource);
        }
    }

    pub(super) async fn apply_event(&self, event: &ResourceEvent) -> Result<Option<Resource>> {
        if event.event.event_type == EventType::Bookmark {
            return Ok(None);
        }
        let resource = Resource::try_from_watch_event(&event.event)?;
        let key = resource_cache_key(
            &resource.api_version,
            &resource.kind,
            resource.namespace.as_deref(),
            &resource.name,
        );
        let event_type = event.event.event_type;
        {
            let mut guard = self.resources.write().await;
            let should_apply = guard
                .get(&key)
                .map(|current| current.resource_version <= resource.resource_version)
                .unwrap_or(true);
            if !should_apply {
                return Ok(None);
            }
            match event_type {
                EventType::Deleted => {
                    guard.remove(&key);
                }
                EventType::Added | EventType::Modified => {
                    guard.insert(key, resource.clone());
                }
                // ERROR is a wire-only watch frame; never broadcast internally.
                EventType::Bookmark | EventType::Error => {}
            }
        }
        Ok(Some(resource))
    }

    pub(super) async fn mark_primed(&self, scope: CacheScope) {
        self.primed_scopes.write().await.insert(scope);
        self.ready.notify_waiters();
    }

    #[cfg(test)]
    pub(super) async fn clear_scope_for_test(&self, scope: &CacheScope) {
        self.primed_scopes.write().await.remove(scope);
    }

    pub(super) async fn wait_ready(&self, scope: CacheScope) -> Result<()> {
        loop {
            if self.primed_scopes.read().await.contains(&scope) {
                return Ok(());
            }
            self.ready.notified().await;
        }
    }

    pub(super) async fn is_ready(&self, scope: &CacheScope) -> bool {
        self.primed_scopes.read().await.contains(scope)
    }
}

pub(super) fn scope_for_request(req: &ListRequest) -> CacheScope {
    CacheScope::Resource {
        api_version: req.api_version.clone(),
        kind: req.kind.clone(),
        namespace: req.namespace.clone(),
    }
}

fn resource_cache_key(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> String {
    match namespace {
        Some(ns) => format!("{api_version}/{kind}/{ns}/{name}"),
        None => format!("{api_version}/{kind}/{name}"),
    }
}

fn resource_matches_request(resource: &Resource, req: &ListRequest) -> bool {
    resource.api_version == req.api_version
        && resource.kind == req.kind
        && req
            .namespace
            .as_deref()
            .is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
        && label_selector_matches(resource, req.label_selector.as_deref())
        && crate::watch::value_matches_field_selector(&resource.data, req.field_selector.as_deref())
}

fn label_selector_matches(resource: &Resource, selector: Option<&str>) -> bool {
    let Some(selector) = selector.filter(|selector| !selector.trim().is_empty()) else {
        return true;
    };
    crate::label_selector::LabelSelector::parse(selector)
        .map(|parsed| parsed.matches_resource(&resource.data))
        .unwrap_or(false)
}
