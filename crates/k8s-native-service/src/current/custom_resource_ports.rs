use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_leader_api::ResourceListResult;

use crate::current::AppError;

#[derive(Clone, Debug)]
pub enum CustomResourceWatchTarget {
    Cluster {
        api_version: String,
        kind: String,
    },
    Namespaced {
        api_version: String,
        kind: String,
        namespace: Option<String>,
    },
}

impl CustomResourceWatchTarget {
    pub fn cluster(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self::Cluster {
            api_version: api_version.into(),
            kind: kind.into(),
        }
    }

    pub fn namespaced(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self::Namespaced {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: None,
        }
    }

    pub fn namespaced_in_namespace(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self::Namespaced {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: Some(namespace.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum CustomResourceListSnapshot {
    Current,
    Expired,
    List(ResourceListResult),
}

pub struct CustomResourceSnapshotRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
    pub resource_version: i64,
}

pub type CustomResourceReadFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AppError>> + Send + 'a>>;
pub type CustomResourceWaitFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait CustomResourceProjection: Send + Sync {
    fn project_resources(
        &self,
        resources: Vec<Resource>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<Resource>, klights_leader_api::LeaderWatchError>>;
}

/// Focused custom-resource snapshot/watch capability.
///
/// Ordinary live reads and writes use the existing leader query/command
/// contracts. This port contains only operations that require an atomic local
/// LIST-to-WATCH boundary or historical snapshot reconstruction.
pub trait CustomResourceReadPort: Send + Sync {
    fn snapshot_resources_at_rv(
        &self,
        request: CustomResourceSnapshotRequest,
    ) -> CustomResourceReadFuture<'_, CustomResourceListSnapshot>;

    fn list_resources_for_watch_targets(
        &self,
        targets: Vec<CustomResourceWatchTarget>,
        label_selector: Option<String>,
    ) -> CustomResourceReadFuture<'_, ResourceListResult>;

    fn watch_projected_resources(
        &self,
        request: klights_leader_api::WatchRequest,
        targets: Vec<CustomResourceWatchTarget>,
        projection: Arc<dyn CustomResourceProjection>,
    ) -> klights_leader_api::LeaderWatchFuture<'_>;

    fn wait_until_fresh(
        &self,
        target_rv: i64,
        api_version: String,
        kind: String,
    ) -> CustomResourceWaitFuture<'_>;

    fn current_collection_resource_version(
        &self,
        api_version: String,
        kind: String,
        namespace: Option<String>,
    ) -> CustomResourceReadFuture<'_, i64>;
}

pub fn resource_event_to_watch_event(
    event: &klights_leader_api::ResourceEvent,
) -> crate::current::watch_event::WatchEvent {
    let event_type = match event.event_type() {
        klights_leader_api::WatchEventType::Added => crate::current::watch_event::EventType::Added,
        klights_leader_api::WatchEventType::Modified => {
            crate::current::watch_event::EventType::Modified
        }
        klights_leader_api::WatchEventType::Deleted => {
            crate::current::watch_event::EventType::Deleted
        }
        klights_leader_api::WatchEventType::Bookmark => {
            crate::current::watch_event::EventType::Bookmark
        }
        klights_leader_api::WatchEventType::Error => crate::current::watch_event::EventType::Error,
    };
    crate::current::watch_event::WatchEvent {
        event_type,
        object: event.resource().data.clone(),
        encoded_payload: None,
    }
}

pub fn added_watch_event(mut resource: Resource) -> crate::current::watch_event::WatchEvent {
    if resource.resource_version > 0
        && let Some(metadata) = Arc::make_mut(&mut resource.data)
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::json!(resource.resource_version.to_string()),
        );
    }
    crate::current::watch_event::WatchEvent {
        event_type: crate::current::watch_event::EventType::Added,
        object: resource.data,
        encoded_payload: None,
    }
}
