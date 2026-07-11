use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::datastore::Resource;

use super::{EventType, WatchEvent};

pub(crate) type SelectorObjectKey = (Option<String>, String);

/// Small state machine shared by HTTP watches and internal reflectors for
/// Kubernetes selector membership transitions.
#[derive(Debug, Default)]
pub(crate) struct SelectorMembership {
    matched_objects: HashMap<SelectorObjectKey, Arc<Value>>,
}

pub(crate) struct PendingSelectorTransition {
    event: Option<WatchEvent>,
    mutation: SelectorMembershipMutation,
}

pub(crate) enum SelectorMembershipMutation {
    None,
    Upsert(SelectorObjectKey, Arc<Value>),
    Remove(SelectorObjectKey),
}

impl PendingSelectorTransition {
    pub(crate) fn into_parts(self) -> (Option<WatchEvent>, SelectorMembershipMutation) {
        (self.event, self.mutation)
    }
}

impl SelectorMembership {
    pub(crate) fn replace_from_resources<'a>(
        &mut self,
        resources: impl IntoIterator<Item = &'a Resource>,
    ) {
        self.matched_objects.clear();
        self.matched_objects.extend(
            resources
                .into_iter()
                .map(|resource| (resource_key(resource), resource.data.clone())),
        );
    }

    pub(crate) fn record_event(&mut self, event: &WatchEvent) -> bool {
        let Some(key) = event_key(event) else {
            return false;
        };
        self.matched_objects.insert(key, event.object.clone());
        true
    }

    pub(crate) fn transition(
        &mut self,
        event: WatchEvent,
        matches_selector: bool,
    ) -> Option<WatchEvent> {
        let (event, mutation) = self
            .prepare_transition(event, matches_selector)
            .into_parts();
        self.commit(mutation);
        event
    }

    pub(crate) fn prepare_transition(
        &self,
        mut event: WatchEvent,
        matches_selector: bool,
    ) -> PendingSelectorTransition {
        if event.event_type == EventType::Bookmark {
            return PendingSelectorTransition {
                event: Some(event),
                mutation: SelectorMembershipMutation::None,
            };
        }
        let Some(key) = event_key(&event) else {
            return PendingSelectorTransition {
                event: None,
                mutation: SelectorMembershipMutation::None,
            };
        };
        let previous_object = self.matched_objects.get(&key);
        let was_member = previous_object.is_some();
        let rewrite = |event: &mut WatchEvent, new_type| {
            if event.event_type != new_type {
                event.event_type = new_type;
                event.encoded_payload = None;
            }
        };
        let (event, mutation) = match event.event_type {
            EventType::Deleted => {
                if was_member || matches_selector {
                    (
                        Some(event),
                        if was_member {
                            SelectorMembershipMutation::Remove(key)
                        } else {
                            SelectorMembershipMutation::None
                        },
                    )
                } else {
                    (None, SelectorMembershipMutation::None)
                }
            }
            EventType::Added | EventType::Modified => {
                if matches_selector {
                    if !was_member {
                        rewrite(&mut event, EventType::Added);
                    }
                    let object = event.object.clone();
                    (Some(event), SelectorMembershipMutation::Upsert(key, object))
                } else if was_member {
                    rewrite(&mut event, EventType::Deleted);
                    if let Some(previous_object) = previous_object {
                        event.object = previous_object.clone();
                    }
                    (Some(event), SelectorMembershipMutation::Remove(key))
                } else {
                    (None, SelectorMembershipMutation::None)
                }
            }
            _ if matches_selector => (Some(event), SelectorMembershipMutation::None),
            _ => (None, SelectorMembershipMutation::None),
        };
        PendingSelectorTransition { event, mutation }
    }

    pub(crate) fn commit(&mut self, mutation: SelectorMembershipMutation) {
        match mutation {
            SelectorMembershipMutation::None => {}
            SelectorMembershipMutation::Upsert(key, object) => {
                self.matched_objects.insert(key, object);
            }
            SelectorMembershipMutation::Remove(key) => {
                self.matched_objects.remove(&key);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.matched_objects.len()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &SelectorObjectKey) -> bool {
        self.matched_objects.contains_key(key)
    }
}

pub(crate) fn event_key(event: &WatchEvent) -> Option<SelectorObjectKey> {
    let name = event
        .object
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())?
        .to_string();
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    Some((namespace, name))
}

pub(crate) fn resource_key(resource: &Resource) -> SelectorObjectKey {
    let namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    (namespace, resource.name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(node: &str, rv: i64) -> WatchEvent {
        WatchEvent::modified(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "moving",
                "uid": "uid-moving",
                "resourceVersion": rv.to_string()
            },
            "spec": {"nodeName": node}
        }))
    }

    #[test]
    fn uncommitted_leave_transition_is_replayed_as_deleted() {
        let mut membership = SelectorMembership::default();
        assert_eq!(
            membership
                .transition(pod("node-a", 1), true)
                .unwrap()
                .event_type,
            EventType::Added
        );

        let (first, _uncommitted) = membership
            .prepare_transition(pod("node-b", 2), false)
            .into_parts();
        let first = first.unwrap();
        assert_eq!(first.event_type, EventType::Deleted);
        assert_eq!(first.object["spec"]["nodeName"], "node-a");
        assert_eq!(first.resource_version(), Some(1));

        let (replayed, mutation) = membership
            .prepare_transition(pod("node-b", 2), false)
            .into_parts();
        let replayed = replayed.unwrap();
        assert_eq!(replayed.event_type, EventType::Deleted);
        assert_eq!(replayed.object["spec"]["nodeName"], "node-a");
        assert_eq!(replayed.resource_version(), Some(1));
        membership.commit(mutation);
        assert!(!membership.contains(&(Some("default".into()), "moving".into())));
    }

    #[test]
    fn mutable_label_leave_transition_synthesizes_deleted() {
        let mut membership = SelectorMembership::default();
        let event = |label: &str, rv: i64| {
            WatchEvent::modified(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "selected",
                    "labels": {"app": label},
                    "resourceVersion": rv.to_string()
                }
            }))
        };
        let selection = |event: &WatchEvent| {
            crate::watch::WatchEventSelection::new("v1", "ConfigMap")
                .label_selector(Some("app=selected"))
                .matches(event)
        };
        let added = event("selected", 1);
        let added_matches = selection(&added);
        assert_eq!(
            membership
                .transition(added, added_matches)
                .unwrap()
                .event_type,
            EventType::Added
        );
        let latest_match = event("selected", 2);
        let latest_matching_object = latest_match.object.clone();
        assert_eq!(
            membership
                .transition(latest_match, true)
                .unwrap()
                .event_type,
            EventType::Modified
        );
        let left = event("other", 3);
        let left_matches = selection(&left);
        let deleted = membership.transition(left, left_matches).unwrap();
        assert_eq!(deleted.event_type, EventType::Deleted);
        assert_eq!(deleted.object["metadata"]["labels"]["app"], "selected");
        assert_eq!(deleted.resource_version(), Some(2));
        assert!(
            Arc::ptr_eq(&deleted.object, &latest_matching_object),
            "selector membership should retain and reuse the matching Arc<Value>"
        );
    }
}
