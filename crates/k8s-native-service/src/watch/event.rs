use bytes::Bytes;
use klights_types::FieldSelector;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Added,
    Modified,
    Deleted,
    Bookmark,
    Error,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Added => "ADDED",
            Self::Modified => "MODIFIED",
            Self::Deleted => "DELETED",
            Self::Bookmark => "BOOKMARK",
            Self::Error => "ERROR",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchContentType {
    Json,
}

#[derive(Clone, Debug)]
pub struct EncodedWatchPayload {
    pub content_type: WatchContentType,
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WatchEvent {
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub object: Arc<Value>,
    #[serde(skip)]
    pub encoded_payload: Option<EncodedWatchPayload>,
}

impl WatchEvent {
    pub fn from_type(event_type: &str, object: Value) -> Self {
        let event_type = match event_type {
            "ADDED" => EventType::Added,
            "DELETED" => EventType::Deleted,
            "BOOKMARK" => EventType::Bookmark,
            "ERROR" => EventType::Error,
            _ => EventType::Modified,
        };
        Self {
            event_type,
            object: Arc::new(object),
            encoded_payload: None,
        }
    }

    pub fn added(object: Value) -> Self {
        Self::from_type("ADDED", object)
    }

    pub fn modified(object: Value) -> Self {
        Self::from_type("MODIFIED", object)
    }

    pub fn deleted(object: Value) -> Self {
        Self::from_type("DELETED", object)
    }

    pub fn bookmark_typed(resource_version: i64, api_version: &str, kind: &str) -> Self {
        Self {
            event_type: EventType::Bookmark,
            object: Arc::new(bookmark_object(resource_version, api_version, kind, false)),
            encoded_payload: None,
        }
    }

    pub fn bookmark_initial_events_end(
        resource_version: i64,
        api_version: &str,
        kind: &str,
    ) -> Self {
        Self {
            event_type: EventType::Bookmark,
            object: Arc::new(bookmark_object(resource_version, api_version, kind, true)),
            encoded_payload: None,
        }
    }
}

fn bookmark_object(
    resource_version: i64,
    api_version: &str,
    kind: &str,
    initial_events_end: bool,
) -> Value {
    let mut metadata = json!({"resourceVersion": resource_version.to_string()});
    if initial_events_end {
        metadata["annotations"] = json!({"k8s.io/initial-events-end": "true"});
    }
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": metadata
    })
}

pub(crate) fn value_matches_field_selector(object: &Value, selector: Option<&str>) -> bool {
    selector
        .filter(|selector| !selector.trim().is_empty())
        .is_none_or(|selector| {
            FieldSelector::parse(selector).is_ok_and(|selector| selector.matches_resource(object))
        })
}
