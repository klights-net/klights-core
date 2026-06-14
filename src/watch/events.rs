//! Watch event core types and filters.
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;

use super::bookmark;
use crate::label_selector::LabelSelector;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventType {
    Added,
    Modified,
    Deleted,
    Bookmark,
    /// `ERROR` — a mid-stream failure (e.g. 410 Gone). The event `object` is a
    /// `metav1.Status`. client-go's `StreamWatcher` decodes each frame as
    /// `{type, object}` and requires this wrapper rather than a bare Status.
    Error,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Added => f.write_str("ADDED"),
            EventType::Modified => f.write_str("MODIFIED"),
            EventType::Deleted => f.write_str("DELETED"),
            EventType::Bookmark => f.write_str("BOOKMARK"),
            EventType::Error => f.write_str("ERROR"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchContentType {
    Json,
    Protobuf,
    TableJson,
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
        match event_type {
            "ADDED" => Self::added(object),
            "MODIFIED" => Self::modified(object),
            "DELETED" => Self::deleted(object),
            "BOOKMARK" => Self {
                event_type: EventType::Bookmark,
                object: Arc::new(object),
                encoded_payload: None,
            },
            "ERROR" => Self {
                event_type: EventType::Error,
                object: Arc::new(object),
                encoded_payload: None,
            },
            _ => Self::modified(object),
        }
    }

    pub fn added(object: Value) -> Self {
        Self {
            event_type: EventType::Added,
            object: Arc::new(object),
            encoded_payload: None,
        }
    }

    pub fn modified(object: Value) -> Self {
        Self {
            event_type: EventType::Modified,
            object: Arc::new(object),
            encoded_payload: None,
        }
    }

    pub fn deleted(object: Value) -> Self {
        Self {
            event_type: EventType::Deleted,
            object: Arc::new(object),
            encoded_payload: None,
        }
    }

    pub fn bookmark(resource_version: i64) -> Self {
        Self {
            event_type: EventType::Bookmark,
            object: Arc::new(bookmark::build_bookmark(resource_version)),
            encoded_payload: None,
        }
    }

    pub fn bookmark_typed(resource_version: i64, api_version: &str, kind: &str) -> Self {
        Self {
            event_type: EventType::Bookmark,
            object: Arc::new(bookmark::build_bookmark_typed(
                resource_version,
                api_version,
                kind,
            )),
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
            object: Arc::new(bookmark::build_bookmark_initial(
                resource_version,
                api_version,
                kind,
            )),
            encoded_payload: None,
        }
    }

    /// Returns true if the event's resourceVersion is at or before `threshold`.
    /// Used by watch streams to skip broadcast events already covered by the
    /// initial list phase (prevents duplicate ADDED events in rv=0 list+watch).
    pub fn resource_version(&self) -> Option<i64> {
        self.object
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .and_then(|rv| rv.as_str())
            .and_then(|s| s.parse::<i64>().ok())
    }

    #[cfg(test)]
    pub fn event_rv_at_or_before(&self, threshold: i64) -> bool {
        if let Some(rv_str) = self
            .object
            .get("metadata")
            .and_then(|m| m.get("resourceVersion"))
            .and_then(|rv| rv.as_str())
            && let Ok(rv) = rv_str.parse::<i64>()
        {
            return rv <= threshold;
        }
        false
    }

    pub fn matches_filter(
        &self,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> bool {
        let parsed_selector = match label_selector {
            Some(selector) if !selector.trim().is_empty() => match LabelSelector::parse(selector) {
                Ok(parsed) => Some(parsed),
                Err(_) => return false,
            },
            _ => None,
        };
        self.matches_filter_parsed(kind, namespace, parsed_selector.as_ref())
    }

    pub fn matches_filter_parsed(
        &self,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&LabelSelector>,
    ) -> bool {
        // Don't filter BOOKMARK events
        if self.event_type == EventType::Bookmark {
            return true;
        }

        // Check kind
        if let Some(obj_kind) = self.object.get("kind").and_then(|k| k.as_str()) {
            if obj_kind != kind {
                return false;
            }
        } else {
            return false;
        }

        // Check namespace for namespaced resources
        if let Some(expected_ns) = namespace {
            if let Some(obj_ns) = self
                .object
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str())
            {
                if obj_ns != expected_ns {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check label selector if provided
        if let Some(selector) = label_selector
            && !selector.matches_resource(&self.object)
        {
            return false;
        }

        true
    }

    /// Field-selector filter used by watch handlers.
    /// Supports comma-separated `key=value` and `key!=value` with dotted field paths.
    pub fn matches_field_selector(&self, field_selector: Option<&str>) -> bool {
        // BOOKMARK events must always pass through.
        if self.event_type == EventType::Bookmark {
            return true;
        }
        value_matches_field_selector(&self.object, field_selector)
    }
}

/// Pre-encode a watch event in the given content type.
pub fn encode_watch_payload(
    event: &WatchEvent,
    mode: WatchContentType,
) -> Result<EncodedWatchPayload, serde_json::Error> {
    match mode {
        WatchContentType::Json => {
            let bytes = Bytes::from(serde_json::to_vec(event)?);
            Ok(EncodedWatchPayload {
                content_type: mode,
                bytes,
            })
        }
        WatchContentType::Protobuf | WatchContentType::TableJson => {
            unreachable!("pre-encoding only supported for standard JSON at broadcast time")
        }
    }
}

/// Field-selector match against a borrowed Value. Lifted out of
/// `WatchEvent::matches_field_selector` so call sites that have a raw
/// resource Value (initial-list filtering, watch-stream selector tests)
/// can ask "does this object match?" without having to wrap the value
/// in a WatchEvent — which used to cost a deep `Value::clone()` per
/// event on the hot path.
pub fn value_matches_field_selector(object: &Value, field_selector: Option<&str>) -> bool {
    let Some(selector) = field_selector else {
        return true;
    };
    if selector.trim().is_empty() {
        return true;
    }

    for part in selector.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (path, expected, is_eq) = if let Some(idx) = part.find("!=") {
            (&part[..idx], &part[idx + 2..], false)
        } else if let Some(idx) = part.find('=') {
            (&part[..idx], &part[idx + 1..], true)
        } else {
            // Ignore invalid fragments (same behavior as list filter parser).
            continue;
        };

        let path = path.trim();
        let expected = expected.trim();
        let actual = resolve_field_path(object, path);
        // Match DB field-selector semantics: absent boolean field defaults to false.
        let effective = actual.as_deref().or(if expected == "false" {
            Some("false")
        } else {
            None
        });
        let matches = effective == Some(expected);
        if is_eq != matches {
            return false;
        }
    }

    true
}

fn resolve_field_path<'a>(object: &'a Value, path: &str) -> Option<Cow<'a, str>> {
    fn non_empty_str(value: Option<&Value>) -> Option<&str> {
        value.and_then(|v| v.as_str()).filter(|s| !s.is_empty())
    }

    // Event selector compatibility:
    // `source=<component>` matches core/v1 (`source.component`) and
    // events.k8s.io/v1 (`deprecatedSource.component` / `reportingController`).
    if path == "source" {
        if let Some(component) =
            non_empty_str(object.get("source").and_then(|s| s.get("component")))
        {
            return Some(Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(
            object
                .get("deprecatedSource")
                .and_then(|s| s.get("component")),
        ) {
            return Some(Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(object.get("reportingController")) {
            return Some(Cow::Borrowed(component));
        }
        if let Some(component) = non_empty_str(object.get("reportingComponent")) {
            return Some(Cow::Borrowed(component));
        }
    }

    let mut current: &Value = object;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    match current {
        Value::String(s) => Some(Cow::Borrowed(s.as_str())),
        Value::Bool(b) => Some(Cow::Owned(b.to_string())),
        Value::Number(n) => Some(Cow::Owned(n.to_string())),
        _ => None,
    }
}
