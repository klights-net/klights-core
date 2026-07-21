//! Shared reverse reconstruction for selector membership at a durable watch
//! position. Backend modules gather live rows and retained watch rows in one
//! read transaction; this module applies the cursor semantics without owning
//! any cache or backend state.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use super::{Resource, WatchReplayPosition, WatchTarget, WatchTargetScope};
use klights_types::LabelSelector;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct MembershipKey {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

impl MembershipKey {
    pub(crate) fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MembershipHistoryEvent {
    pub event_id: i64,
    pub event_type: String,
    pub resource: Resource,
}

impl MembershipHistoryEvent {
    fn key(&self) -> MembershipKey {
        MembershipKey::from_resource(&self.resource)
    }
}

pub(crate) enum ReconstructedMembership {
    Expired,
    Items(Vec<Resource>),
}

/// Whether an event's post-state is already represented by `position`.
///
/// A partially consumed composite cursor represents both the rows explicitly
/// consumed through `event_id` and the rows at/below its RV filter through the
/// establishment anchor. This is the exact complement of positioned replay.
pub(crate) fn position_represents(
    position: WatchReplayPosition,
    event_id: i64,
    resource_version: i64,
) -> bool {
    if event_id <= position.event_id {
        return true;
    }
    if position.resource_version_filter_through_event_id > 0 {
        return event_id <= position.resource_version_filter_through_event_id
            && resource_version <= position.resource_version;
    }
    position.event_id == 0 && resource_version <= position.resource_version
}

/// Reverse current materialized state to the state represented by `position`.
/// `history` must contain all retained rows for the requested targets in
/// ascending event-id order. A MODIFIED row requires its immediately preceding
/// object row; absence means retention made reconstruction unsafe.
#[cfg(test)]
pub(crate) fn reconstruct_membership(
    current: Vec<Resource>,
    history: Vec<MembershipHistoryEvent>,
    position: WatchReplayPosition,
) -> ReconstructedMembership {
    let mut reconstructor = MembershipReconstructor::new(current, position);
    for event in history.iter().rev() {
        reconstructor.observe(event);
    }
    reconstructor.finish()
}

/// Incremental reverse reconstructor. Backends can feed a descending database
/// iterator directly so memory stays proportional to the returned collection.
pub(crate) struct MembershipReconstructor {
    state: BTreeMap<MembershipKey, Resource>,
    needs_predecessor: HashSet<MembershipKey>,
    position: WatchReplayPosition,
    expired: bool,
}

impl MembershipReconstructor {
    pub(crate) fn new(current: Vec<Resource>, position: WatchReplayPosition) -> Self {
        Self {
            state: current
                .into_iter()
                .map(|resource| (MembershipKey::from_resource(&resource), resource))
                .collect(),
            needs_predecessor: HashSet::new(),
            position,
            expired: false,
        }
    }

    pub(crate) fn observe(&mut self, event: &MembershipHistoryEvent) {
        if self.expired {
            return;
        }
        let key = event.key();
        if self.needs_predecessor.remove(&key) {
            if event.event_type == "DELETED" {
                self.expired = true;
                return;
            }
            self.state.insert(key.clone(), event.resource.clone());
        }
        if position_represents(
            self.position,
            event.event_id,
            event.resource.resource_version,
        ) {
            return;
        }
        match event.event_type.as_str() {
            "ADDED" => {
                self.state.remove(&key);
            }
            "MODIFIED" => {
                self.needs_predecessor.insert(key);
            }
            // Kubernetes stores the pre-delete object on the delete row.
            "DELETED" => {
                self.state.insert(key, event.resource.clone());
            }
            _ => self.expired = true,
        }
    }

    pub(crate) fn can_stop_before(&self, event_id: i64) -> bool {
        self.position.event_id > 0
            && self.position.resource_version_filter_through_event_id == 0
            && event_id <= self.position.event_id
            && self.needs_predecessor.is_empty()
    }

    pub(crate) fn finish(self) -> ReconstructedMembership {
        if self.expired || !self.needs_predecessor.is_empty() {
            ReconstructedMembership::Expired
        } else {
            ReconstructedMembership::Items(self.state.into_values().collect())
        }
    }
}

pub(crate) fn sort_for_watch_targets(items: &mut [Resource], targets: &[WatchTarget]) {
    items.sort_unstable_by(|left, right| {
        let order = |resource: &Resource| {
            targets
                .iter()
                .position(|target| {
                    target.api_version == resource.api_version
                        && target.kind == resource.kind
                        && match &target.scope {
                            WatchTargetScope::Cluster => resource.namespace.is_none(),
                            WatchTargetScope::Namespaced(Some(namespace)) => {
                                resource.namespace.as_deref() == Some(namespace.as_str())
                            }
                            WatchTargetScope::Namespaced(None) => resource.namespace.is_some(),
                        }
                })
                .unwrap_or(usize::MAX)
        };
        (order(left), &left.namespace, &left.name).cmp(&(
            order(right),
            &right.namespace,
            &right.name,
        ))
    });
}

pub(crate) fn apply_membership_selectors(
    items: Vec<Resource>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<Vec<Resource>> {
    let labels = label_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(LabelSelector::parse)
        .transpose()?;
    let fields = field_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::FieldSelector::parse)
        .transpose()?;
    Ok(items
        .into_iter()
        .filter(|resource| {
            labels
                .as_ref()
                .is_none_or(|selector| selector.matches_resource(&resource.data))
                && fields.as_ref().is_none_or(|selector| {
                    selector.matches_resource_with_identity(
                        &resource.api_version,
                        &resource.kind,
                        &resource.data,
                    )
                })
        })
        .collect())
}

pub(crate) fn resource_from_history(
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
    resource_version: i64,
    data: Value,
) -> Resource {
    let data = Arc::new(data);
    Resource {
        id: 0,
        api_version,
        kind,
        namespace,
        name,
        uid: Resource::uid_from_data(&data),
        resource_version,
        data,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: i64, rv: i64, event_type: &str, selected: bool) -> MembershipHistoryEvent {
        MembershipHistoryEvent {
            event_id: id,
            event_type: event_type.to_string(),
            resource: resource_from_history(
                "v1".into(),
                "ConfigMap".into(),
                Some("default".into()),
                "sample".into(),
                rv,
                serde_json::json!({
                    "metadata": {
                        "name": "sample",
                        "namespace": "default",
                        "labels": {"selected": selected.to_string()}
                    }
                }),
            ),
        }
    }

    #[test]
    fn exact_event_position_excludes_later_lower_rv_change() {
        let selected = event(10, 50, "ADDED", true);
        let nonmatching = event(11, 40, "MODIFIED", false);
        let result = reconstruct_membership(
            vec![nonmatching.resource.clone()],
            vec![selected.clone(), nonmatching],
            WatchReplayPosition {
                resource_version: 50,
                event_id: 10,
                resource_version_filter_through_event_id: 0,
            },
        );
        let ReconstructedMembership::Items(items) = result else {
            panic!("retained prior object must reconstruct");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].resource_version,
            selected.resource.resource_version
        );
        assert_eq!(items[0].data, selected.resource.data);

        let mut membership = crate::watch::SelectorMembership::default();
        membership.replace_from_resources(&items);
        let transitioned = membership
            .transition(
                crate::watch::WatchEvent::modified((*nonmatching_resource()).clone()),
                false,
            )
            .expect("leaving an exact-position member must emit");
        assert_eq!(transitioned.event_type, crate::watch::EventType::Deleted);
        assert_eq!(
            transitioned.object.pointer("/metadata/labels/selected"),
            Some(&serde_json::json!("true")),
            "synthetic DELETED must carry the exact-position prior object"
        );
    }

    #[test]
    fn composite_position_excludes_lower_rv_after_anchor() {
        let selected = event(10, 50, "ADDED", true);
        let nonmatching = event(11, 40, "MODIFIED", false);
        let result = reconstruct_membership(
            vec![nonmatching.resource.clone()],
            vec![selected.clone(), nonmatching],
            WatchReplayPosition::from_resource_version_through_event_id(50, 10),
        );
        let ReconstructedMembership::Items(items) = result else {
            panic!("retained prior object must reconstruct");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].resource_version,
            selected.resource.resource_version
        );
        assert_eq!(items[0].data, selected.resource.data);
    }

    #[test]
    fn delete_row_is_the_pre_delete_object_when_older_history_is_gone() {
        let deleted = event(11, 51, "DELETED", true);
        let result = reconstruct_membership(
            Vec::new(),
            vec![deleted.clone()],
            WatchReplayPosition {
                resource_version: 50,
                event_id: 10,
                resource_version_filter_through_event_id: 0,
            },
        );
        let ReconstructedMembership::Items(items) = result else {
            panic!("delete row must restore its own pre-delete payload");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].data, deleted.resource.data);
    }

    #[test]
    fn modified_without_retained_predecessor_expires() {
        let modified = event(11, 51, "MODIFIED", false);
        assert!(matches!(
            reconstruct_membership(
                vec![modified.resource.clone()],
                vec![modified],
                WatchReplayPosition {
                    resource_version: 50,
                    event_id: 10,
                    resource_version_filter_through_event_id: 0,
                },
            ),
            ReconstructedMembership::Expired
        ));
    }

    #[test]
    fn equal_rv_served_versions_keep_storage_target_priority() {
        let make = |api_version: &str| {
            resource_from_history(
                api_version.into(),
                "Widget".into(),
                Some("default".into()),
                "same-logical-object".into(),
                50,
                serde_json::json!({
                    "apiVersion": api_version,
                    "kind": "Widget",
                    "metadata": {"name": "same-logical-object", "namespace": "default"}
                }),
            )
        };
        let mut items = vec![make("widgets.test/v1"), make("widgets.test/v2")];
        sort_for_watch_targets(
            &mut items,
            &[
                WatchTarget::namespaced("widgets.test/v2", "Widget"),
                WatchTarget::namespaced("widgets.test/v1", "Widget"),
            ],
        );
        assert_eq!(items[0].api_version, "widgets.test/v2");
    }

    fn nonmatching_resource() -> Arc<Value> {
        event(11, 40, "MODIFIED", false).resource.data
    }
}
