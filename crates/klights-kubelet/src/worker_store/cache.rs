//! Cached worker resource queries and list pagination.

use anyhow::Result;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    ResourceGetRequest, ResourceListRequest, ResourceListScope, ResourceQueryConsistency,
};
use klights_types::ResourceKey;
use serde_json::Value;

use super::WorkerStoreAdapter;

/// Focused list result returned by the worker cache.  It mirrors Kubernetes
/// list metadata without exposing the root datastore `ResourceList` type.
#[derive(Clone, Debug)]
pub struct WorkerResourceList {
    pub items: Vec<Resource>,
    pub resource_version: i64,
    pub watch_replay_position: Option<WatchReplayPosition>,
    pub continue_token: Option<String>,
    pub remaining_item_count: Option<i64>,
}

/// Kubernetes list pagination input for worker-local cached reads.
#[derive(Clone, Debug, Default)]
pub struct WorkerListPage {
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
}

impl WorkerListPage {
    pub const fn unbounded() -> Self {
        Self {
            limit: None,
            continue_token: None,
        }
    }
}

impl WorkerStoreAdapter {
    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let key = ResourceKey {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        };
        let resource = self
            .resource_query
            .get_resource(ResourceGetRequest::try_new(
                key,
                ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        if is_pod_resource(api_version, kind)
            && resource
                .as_ref()
                .is_some_and(|resource| !pod_belongs_to_local_node(resource, &self.node_name))
        {
            return Ok(None);
        }
        if let Some(resource) = &resource {
            self.observe_rv(resource.resource_version);
        }
        Ok(resource)
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        scope: ResourceListScope,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: WorkerListPage,
    ) -> Result<WorkerResourceList> {
        let field_selector = if is_pod_resource(api_version, kind) {
            Some(local_pod_field_selector(field_selector, &self.node_name))
        } else {
            field_selector.map(str::to_string)
        };
        let result = self
            .resource_query
            .list_resources(ResourceListRequest::try_new(
                api_version,
                kind,
                scope,
                label_selector.map(str::to_string),
                field_selector,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        let (
            mut items,
            resource_version,
            watch_replay_position,
            continue_token,
            remaining_item_count,
        ) = result.into_parts();
        self.observe_rv(resource_version);
        if page.limit.is_some() || page.continue_token.is_some() {
            items.sort_by(|left, right| left.name.cmp(&right.name));
            let (items, continue_token, remaining_item_count) = apply_page(
                items,
                page.limit,
                page.continue_token,
                continue_token,
                remaining_item_count,
            )?;
            return Ok(WorkerResourceList {
                items,
                resource_version,
                watch_replay_position,
                continue_token,
                remaining_item_count,
            });
        }
        Ok(WorkerResourceList {
            items,
            resource_version,
            watch_replay_position,
            continue_token,
            remaining_item_count,
        })
    }

    pub async fn list_resource_keys_for_scope(
        &self,
        api_version: &str,
        kind: &str,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        Ok(self
            .list_resources(
                api_version,
                kind,
                if namespaced {
                    ResourceListScope::AllNamespaces
                } else {
                    ResourceListScope::Cluster
                },
                None,
                None,
                WorkerListPage::unbounded(),
            )
            .await?
            .items
            .into_iter()
            .map(|resource| {
                (
                    namespaced.then_some(resource.namespace).flatten(),
                    resource.name,
                )
            })
            .collect())
    }

    pub async fn current_resource_version(&self) -> i64 {
        self.current_rv.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn observe_rv(&self, rv: i64) {
        use std::sync::atomic::Ordering;
        let mut current = self.current_rv.load(Ordering::Relaxed);
        while rv > current {
            match self.current_rv.compare_exchange_weak(
                current,
                rv,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

fn apply_page(
    items: Vec<Resource>,
    limit: Option<i64>,
    continue_token: Option<String>,
    upstream_continue_token: Option<String>,
    upstream_remaining_item_count: Option<i64>,
) -> Result<(Vec<Resource>, Option<String>, Option<i64>)> {
    let mut items = items;
    if let Some(token) = continue_token.as_deref().filter(|token| !token.is_empty()) {
        items.retain(|item| item.name.as_str() > token);
    }
    let end = limit
        .filter(|limit| *limit > 0)
        .map(|limit| (limit as usize).min(items.len()))
        .unwrap_or(items.len());
    let remaining = items.len().saturating_sub(end) as i64;
    let next = (end < items.len())
        .then(|| {
            items
                .get(end.saturating_sub(1))
                .map(|item| item.name.clone())
        })
        .flatten()
        .or(upstream_continue_token);
    let remaining = if end < items.len() {
        Some(remaining)
    } else {
        upstream_remaining_item_count
    };
    Ok((items[..end].to_vec(), next, remaining))
}

fn is_pod_resource(api_version: &str, kind: &str) -> bool {
    api_version == "v1" && kind == "Pod"
}

fn pod_belongs_to_local_node(resource: &Resource, node_name: &str) -> bool {
    resource
        .data
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .is_some_and(|node| node == node_name)
}

fn local_pod_field_selector(field_selector: Option<&str>, node_name: &str) -> String {
    let local_selector = format!("spec.nodeName={node_name}");
    match field_selector
        .map(str::trim)
        .filter(|selector| !selector.is_empty())
    {
        Some(selector)
            if selector
                .split(',')
                .any(|part| part.trim() == local_selector) =>
        {
            selector.to_string()
        }
        Some(selector) => format!("{selector},{local_selector}"),
        None => local_selector,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn resource(name: &str) -> Resource {
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            resource_version: 1,
            data: json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": name}
            })
            .into(),
        }
    }

    #[test]
    fn page_continuation_is_the_last_resource_name() {
        let items = vec![resource("cm-a"), resource("cm-b"), resource("cm-c")];
        let first = apply_page(items.clone(), Some(2), None, None, None).expect("first page");
        assert_eq!(
            first
                .0
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            ["cm-a", "cm-b"]
        );
        assert_eq!(first.1.as_deref(), Some("cm-b"));
        assert_eq!(first.2, Some(1));

        let second = apply_page(items, Some(2), first.1, None, None).expect("second page");
        assert_eq!(
            second
                .0
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            ["cm-c"]
        );
        assert_eq!(second.1, None);
        assert_eq!(second.2, None);
    }
}
