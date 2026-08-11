//! Worker informer reflector state.
//!
//! This module owns only the pure snapshot/event state machine.  Transport,
//! cache policy, and node-local/leader effects are kept in the sibling worker
//! store modules so the reflector remains usable without a cluster datastore.

use std::collections::HashMap;
use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_watch::{EventType, WatchEvent};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ReflectedResourceKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

#[derive(Clone, Debug)]
struct ReflectedResource {
    uid: String,
    object: Arc<Value>,
}

/// In-memory state for one worker informer scope.
#[derive(Default)]
pub struct ReflectorState {
    resources: HashMap<ReflectedResourceKey, ReflectedResource>,
}

/// A prepared snapshot diff.  Callers publish `events()` first and commit the
/// replacement only after every side effect succeeds.
pub struct PendingReflectorSnapshot {
    replacement: HashMap<ReflectedResourceKey, ReflectedResource>,
    events: Vec<WatchEvent>,
}

impl PendingReflectorSnapshot {
    pub fn events(&self) -> &[WatchEvent] {
        &self.events
    }

    pub fn commit_into(self, state: &mut ReflectorState) {
        state.resources = self.replacement;
    }
}

impl ReflectorState {
    pub fn prepare_snapshot(
        &self,
        resources: Vec<Resource>,
        snapshot_rv: i64,
    ) -> PendingReflectorSnapshot {
        let mut replacement = HashMap::with_capacity(resources.len());
        for resource in resources {
            replacement.insert(
                ReflectedResourceKey {
                    api_version: resource.api_version,
                    kind: resource.kind,
                    namespace: resource.namespace,
                    name: resource.name,
                },
                ReflectedResource {
                    uid: resource.uid,
                    object: resource.data,
                },
            );
        }

        let mut events = Vec::new();
        for (key, previous) in &self.resources {
            match replacement.get(key) {
                None => events.push(WatchEvent {
                    event_type: EventType::Deleted,
                    object: object_at_resource_version(&previous.object, snapshot_rv),
                    encoded_payload: None,
                }),
                Some(current) if current.uid != previous.uid => {
                    events.push(WatchEvent {
                        event_type: EventType::Deleted,
                        object: object_at_resource_version(&previous.object, snapshot_rv),
                        encoded_payload: None,
                    });
                    events.push(WatchEvent {
                        event_type: EventType::Added,
                        object: current.object.clone(),
                        encoded_payload: None,
                    });
                }
                Some(current) if current.object != previous.object => events.push(WatchEvent {
                    event_type: EventType::Modified,
                    object: current.object.clone(),
                    encoded_payload: None,
                }),
                Some(_) => {}
            }
        }
        for (key, current) in &replacement {
            if !self.resources.contains_key(key) {
                events.push(WatchEvent {
                    event_type: EventType::Added,
                    object: current.object.clone(),
                    encoded_payload: None,
                });
            }
        }
        events.sort_unstable_by(|left, right| {
            reflected_event_order_key(left).cmp(&reflected_event_order_key(right))
        });
        PendingReflectorSnapshot {
            replacement,
            events,
        }
    }

    pub fn replace_snapshot(
        &mut self,
        resources: Vec<Resource>,
        snapshot_rv: i64,
    ) -> Vec<WatchEvent> {
        let pending = self.prepare_snapshot(resources, snapshot_rv);
        let events = pending.events.clone();
        pending.commit_into(self);
        events
    }

    pub fn observe(&mut self, event: &WatchEvent) {
        let Some(key) = reflected_resource_key(&event.object) else {
            return;
        };
        match event.event_type {
            EventType::Added | EventType::Modified => {
                self.resources.insert(
                    key,
                    ReflectedResource {
                        uid: event
                            .object
                            .pointer("/metadata/uid")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        object: event.object.clone(),
                    },
                );
            }
            EventType::Deleted => {
                let event_uid = event
                    .object
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self
                    .resources
                    .get(&key)
                    .is_some_and(|current| current.uid == event_uid)
                {
                    self.resources.remove(&key);
                }
            }
            EventType::Bookmark | EventType::Error => {}
        }
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    pub fn len(&self) -> usize {
        self.resources.len()
    }
}

fn reflected_event_order_key(event: &WatchEvent) -> (&str, &str, Option<&str>, &str, u8) {
    let object = event.object.as_ref();
    let event_rank = match event.event_type {
        EventType::Deleted => 0,
        EventType::Added => 1,
        EventType::Modified => 2,
        EventType::Bookmark => 3,
        EventType::Error => 4,
    };
    (
        object
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        object
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        object
            .pointer("/metadata/namespace")
            .and_then(Value::as_str),
        object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        event_rank,
    )
}

fn reflected_resource_key(object: &Value) -> Option<ReflectedResourceKey> {
    Some(ReflectedResourceKey {
        api_version: object.get("apiVersion")?.as_str()?.to_string(),
        kind: object.get("kind")?.as_str()?.to_string(),
        namespace: object
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .map(str::to_string),
        name: object.pointer("/metadata/name")?.as_str()?.to_string(),
    })
}

fn object_at_resource_version(object: &Arc<Value>, resource_version: i64) -> Arc<Value> {
    let mut object = object.as_ref().clone();
    if let Some(metadata) = object.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.insert(
            "resourceVersion".to_string(),
            Value::String(resource_version.to_string()),
        );
    }
    Arc::new(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reflected_resource(name: &str, uid: &str, rv: i64) -> Resource {
        Resource {
            id: rv,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            uid: uid.to_string(),
            resource_version: rv,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": uid,
                    "resourceVersion": rv.to_string()
                }
            })),
        }
    }

    #[test]
    fn relist_diff_synthesizes_missed_delete_at_snapshot_rv() {
        let mut state = ReflectorState::default();
        let initial = state.replace_snapshot(vec![reflected_resource("removed", "uid-1", 41)], 41);
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].event_type, EventType::Added);

        let replacement = state.replace_snapshot(Vec::new(), 52);
        assert_eq!(replacement.len(), 1);
        assert_eq!(replacement[0].event_type, EventType::Deleted);
        assert_eq!(replacement[0].resource_version(), Some(52));
        assert_eq!(
            replacement[0]
                .object
                .pointer("/metadata/name")
                .and_then(Value::as_str),
            Some("removed")
        );
    }

    #[test]
    fn snapshot_keeps_distinct_objects_with_the_same_rv() {
        let mut state = ReflectorState::default();
        let events = state.replace_snapshot(
            vec![
                reflected_resource("first", "uid-first", 41),
                reflected_resource("second", "uid-second", 41),
            ],
            41,
        );

        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .map(|event| event
                    .object
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap())
                .collect::<std::collections::HashSet<_>>(),
            std::collections::HashSet::from(["first", "second"])
        );
    }

    #[test]
    fn snapshot_diff_order_is_stable_by_resource_key() {
        let mut state = ReflectorState::default();
        let names = [
            "hotel", "alpha", "golf", "bravo", "foxtrot", "charlie", "echo", "delta",
        ];
        let events = state.replace_snapshot(
            names
                .iter()
                .map(|name| reflected_resource(name, &format!("uid-{name}"), 41))
                .collect(),
            41,
        );

        assert_eq!(
            events
                .iter()
                .map(|event| event.object["metadata"]["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"
            ],
            "authoritative relist diffs must be deterministic for replay and tests"
        );
    }

    #[test]
    fn relist_replaces_same_name_uid_with_delete_then_add() {
        let mut state = ReflectorState::default();
        state.replace_snapshot(vec![reflected_resource("same-name", "uid-old", 41)], 41);

        let replacement =
            state.replace_snapshot(vec![reflected_resource("same-name", "uid-new", 52)], 52);

        assert_eq!(
            replacement
                .iter()
                .map(|event| event.event_type)
                .collect::<Vec<_>>(),
            vec![EventType::Deleted, EventType::Added]
        );
        assert_eq!(
            replacement[0]
                .object
                .pointer("/metadata/uid")
                .and_then(Value::as_str),
            Some("uid-old")
        );
        assert_eq!(
            replacement[1]
                .object
                .pointer("/metadata/uid")
                .and_then(Value::as_str),
            Some("uid-new")
        );
    }

    #[test]
    fn relist_marks_same_uid_changes_modified_and_ignores_unchanged_objects() {
        let mut state = ReflectorState::default();
        let initial = reflected_resource("updated", "uid-stable", 41);
        state.replace_snapshot(vec![initial.clone()], 41);

        assert!(state.replace_snapshot(vec![initial], 41).is_empty());

        let mut updated = reflected_resource("updated", "uid-stable", 52);
        Arc::make_mut(&mut updated.data)
            .as_object_mut()
            .unwrap()
            .insert("data".to_string(), serde_json::json!({"key": "new"}));
        let events = state.replace_snapshot(vec![updated], 52);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::Modified);
        assert_eq!(events[0].resource_version(), Some(52));
    }
}
