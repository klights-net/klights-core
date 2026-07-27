use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use klights_leader_api::{LeaderWatchError, WatchEventType};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct PodWatchEvent {
    pub event_type: WatchEventType,
    pub object: Arc<Value>,
}

impl PodWatchEvent {
    pub fn from_resource_event(event: klights_leader_api::ResourceEvent) -> Self {
        let (event_type, resource, _) = event.into_parts();
        Self {
            event_type,
            object: resource.data,
        }
    }

    pub fn resource_version(&self) -> Option<i64> {
        self.object
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok())
    }

    #[cfg(test)]
    pub fn added(object: Value) -> Self {
        Self {
            event_type: WatchEventType::Added,
            object: Arc::new(object),
        }
    }

    #[cfg(test)]
    pub fn modified(object: Value) -> Self {
        Self {
            event_type: WatchEventType::Modified,
            object: Arc::new(object),
        }
    }

    #[cfg(test)]
    pub fn deleted(object: Value) -> Self {
        Self {
            event_type: WatchEventType::Deleted,
            object: Arc::new(object),
        }
    }
}

#[cfg(test)]
impl From<crate::watch::WatchEvent> for PodWatchEvent {
    fn from(event: crate::watch::WatchEvent) -> Self {
        let event_type = match event.event_type {
            crate::watch::EventType::Added => WatchEventType::Added,
            crate::watch::EventType::Modified => WatchEventType::Modified,
            crate::watch::EventType::Deleted => WatchEventType::Deleted,
            crate::watch::EventType::Bookmark => WatchEventType::Bookmark,
            crate::watch::EventType::Error => WatchEventType::Error,
        };
        Self {
            event_type,
            object: event.object,
        }
    }
}

pub type PodWatchStream =
    Pin<Box<dyn Stream<Item = Result<PodWatchEvent, LeaderWatchError>> + Send + 'static>>;

pub type PodWatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PodWatchStream, LeaderWatchError>> + Send + 'a>>;

pub trait PodWatchSource: Send + Sync {
    fn open_pod_manager_watch(&self, node_name: String) -> PodWatchFuture<'_>;
}
