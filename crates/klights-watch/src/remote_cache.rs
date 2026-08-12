use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_leader_api::{
    CacheReadinessRequest, LeaderWatchError, ResourceEvent, ResourceListRequest,
    ResourceListResult, WatchEventType, WatchRequest,
};
use klights_types::{FieldSelector, LabelSelector, ResourceKey};
use std::collections::HashMap;

/// Cache behavior required by a remote LIST/WATCH reflector.
#[async_trait]
pub trait RemoteInformerCache: Send + Sync {
    async fn insert(&self, resource: Resource);
    async fn get(&self, key: &ResourceKey) -> Option<Resource>;
    async fn list(&self, request: &ResourceListRequest) -> Result<ResourceListResult>;
    async fn replace_scope(
        &self,
        request: &ResourceListRequest,
        resources: Vec<Resource>,
        position: WatchReplayPosition,
    ) -> Result<()>;
    async fn apply_event(&self, event: &ResourceEvent) -> Option<Resource>;
    async fn mark_ready(&self, scope: CacheReadinessRequest) -> Result<()>;
    async fn clear_ready(&self, scope: &CacheReadinessRequest);
    async fn is_ready(&self, scope: &CacheReadinessRequest) -> bool;
    async fn wait_ready(&self, scope: CacheReadinessRequest);
}

/// Creates selector-aware transition projectors for one remote watch.
pub trait WatchTransitionProjectorFactory: Send + Sync {
    fn projector(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, LeaderWatchError>;
}

/// Prepares and commits selector membership changes around event delivery.
pub trait WatchTransitionProjector: Send {
    fn replace(&mut self, resources: &[Resource]);
    fn prepare(&self, event: ResourceEvent) -> Result<PreparedWatchTransition, LeaderWatchError>;
    fn commit(&mut self, prepared: PreparedWatchTransition) -> Result<(), LeaderWatchError>;
}

/// An event projection paired with its opaque owner-specific commit token.
pub struct PreparedWatchTransition {
    event: Option<ResourceEvent>,
    token: Box<dyn std::any::Any + Send>,
}

impl PreparedWatchTransition {
    pub fn new<T: std::any::Any + Send>(event: Option<ResourceEvent>, token: T) -> Self {
        Self {
            event,
            token: Box::new(token),
        }
    }

    pub fn event(&self) -> Option<&ResourceEvent> {
        self.event.as_ref()
    }

    pub fn into_token<T: std::any::Any + Send>(self) -> Result<T, LeaderWatchError> {
        self.token.downcast::<T>().map(|token| *token).map_err(|_| {
            LeaderWatchError::malformed_event("watch transition projector token type mismatch")
        })
    }
}

/// Canonical selector-aware projector used by remote and worker reflectors.
///
/// The implementation deliberately has no durable-store/session dependency,
/// so worker builds keep their cluster-datastore-free dependency graph.
pub struct SelectorWatchTransitionProjector {
    filter: SelectorWatchFilter,
    membership: HashMap<SelectorKey, Resource>,
}

impl SelectorWatchTransitionProjector {
    pub fn try_new(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        Ok(Self {
            filter: SelectorWatchFilter::try_new(request)?,
            membership: HashMap::new(),
        })
    }
}

impl WatchTransitionProjector for SelectorWatchTransitionProjector {
    fn replace(&mut self, resources: &[Resource]) {
        self.membership.clear();
        self.membership.extend(
            resources
                .iter()
                .cloned()
                .map(|resource| (SelectorKey::from_resource(&resource), resource)),
        );
    }

    fn prepare(&self, event: ResourceEvent) -> Result<PreparedWatchTransition, LeaderWatchError> {
        if matches!(
            event.event_type(),
            WatchEventType::Bookmark | WatchEventType::Error
        ) {
            return Ok(PreparedWatchTransition::new(
                event.into(),
                SelectorMutation::None,
            ));
        }
        let key = SelectorKey::from_resource(event.resource());
        let prior = self.membership.get(&key).cloned();
        let was_member = prior.is_some();
        let matches = self.filter.matches(event.resource());
        let position = event.resume_position();
        let event_type = event.event_type();
        let current = event.resource().clone();
        let (event, mutation) = match event_type {
            WatchEventType::Deleted => {
                let mutation = if was_member {
                    SelectorMutation::Remove(key)
                } else {
                    SelectorMutation::None
                };
                ((was_member || matches).then_some(event), mutation)
            }
            WatchEventType::Added | WatchEventType::Modified if matches => {
                let event = if was_member || event_type == WatchEventType::Added {
                    Some(event)
                } else {
                    Some(ResourceEvent::try_new(
                        WatchEventType::Added,
                        current.clone(),
                        position,
                    )?)
                };
                (event, SelectorMutation::Upsert(key, current))
            }
            WatchEventType::Added | WatchEventType::Modified if was_member => (
                Some(ResourceEvent::try_new(
                    WatchEventType::Deleted,
                    prior.expect("membership was checked"),
                    position,
                )?),
                SelectorMutation::Remove(key),
            ),
            WatchEventType::Added | WatchEventType::Modified => (None, SelectorMutation::None),
            WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
        };
        Ok(PreparedWatchTransition::new(event, mutation))
    }

    fn commit(&mut self, prepared: PreparedWatchTransition) -> Result<(), LeaderWatchError> {
        match prepared.into_token::<SelectorMutation>()? {
            SelectorMutation::None => {}
            SelectorMutation::Upsert(key, resource) => {
                self.membership.insert(key, resource);
            }
            SelectorMutation::Remove(key) => {
                self.membership.remove(&key);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SelectorWatchTransitionProjectors;

impl WatchTransitionProjectorFactory for SelectorWatchTransitionProjectors {
    fn projector(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, LeaderWatchError> {
        Ok(Box::new(SelectorWatchTransitionProjector::try_new(
            request,
        )?))
    }
}

enum SelectorMutation {
    None,
    Upsert(SelectorKey, Resource),
    Remove(SelectorKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SelectorKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl SelectorKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

struct SelectorWatchFilter {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<LabelSelector>,
    field_selector: Option<FieldSelector>,
}

impl SelectorWatchFilter {
    fn try_new(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        let label_selector = request
            .label_selector()
            .filter(|selector| !selector.trim().is_empty())
            .map(LabelSelector::parse)
            .transpose()
            .map_err(|error| {
                LeaderWatchError::invalid_request("watch.label_selector", error.to_string())
            })?;
        let field_selector = request
            .field_selector()
            .filter(|selector| !selector.trim().is_empty())
            .map(FieldSelector::parse)
            .transpose()
            .map_err(|error| {
                LeaderWatchError::invalid_request("watch.field_selector", error.to_string())
            })?;
        Ok(Self {
            api_version: request.api_version().to_string(),
            kind: request.kind().to_string(),
            namespace: request.namespace().map(str::to_owned),
            label_selector,
            field_selector,
        })
    }

    fn matches(&self, resource: &Resource) -> bool {
        resource.api_version == self.api_version
            && resource.kind == self.kind
            && self
                .namespace
                .as_deref()
                .is_none_or(|namespace| resource.namespace.as_deref() == Some(namespace))
            && self
                .label_selector
                .as_ref()
                .is_none_or(|selector| selector.matches_resource(&resource.data))
            && self.field_selector.as_ref().is_none_or(|selector| {
                selector.matches_resource_with_identity(
                    &resource.api_version,
                    &resource.kind,
                    &resource.data,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn pod(name: &str, selected: bool, rv: i64) -> Resource {
        Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": format!("uid-{name}"),
                "resourceVersion": rv.to_string(),
                "labels": {"app": if selected { "selected" } else { "other" }}
            },
            "spec": {"nodeName": "worker-a"}
        })))
        .unwrap()
    }

    fn event(event_type: WatchEventType, resource: Resource, rv: i64) -> ResourceEvent {
        ResourceEvent::try_new(
            event_type,
            resource,
            Some(WatchReplayPosition::from_resource_version(rv)),
        )
        .unwrap()
    }

    fn projector() -> SelectorWatchTransitionProjector {
        SelectorWatchTransitionProjector::try_new(
            &WatchRequest::try_new(
                "v1",
                "Pod",
                Some("default".to_string()),
                Some("app=selected".to_string()),
                Some("spec.nodeName=worker-a".to_string()),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn prepared_transition_preserves_exact_token_type_and_rejects_mismatch() {
        assert_eq!(
            PreparedWatchTransition::new(None, 41_u64)
                .into_token::<u64>()
                .expect("matching projector token"),
            41
        );
        assert!(
            PreparedWatchTransition::new(None, 41_u64)
                .into_token::<String>()
                .is_err()
        );
    }

    #[test]
    fn selector_projector_state_machine_preserves_prepare_commit_and_membership_transitions() {
        let mut projector = projector();
        let selected = pod("web", true, 1);
        projector.replace(std::slice::from_ref(&selected));

        let leave = projector
            .prepare(event(WatchEventType::Modified, pod("web", false, 2), 2))
            .unwrap();
        assert_eq!(leave.event().unwrap().event_type(), WatchEventType::Deleted);
        let leave_again = projector
            .prepare(event(WatchEventType::Modified, pod("web", false, 2), 2))
            .unwrap();
        assert_eq!(
            leave_again.event().unwrap().event_type(),
            WatchEventType::Deleted,
            "prepare must not mutate membership before commit"
        );
        projector.commit(leave).unwrap();
        assert!(
            projector
                .prepare(event(WatchEventType::Modified, pod("web", false, 3), 3))
                .unwrap()
                .event()
                .is_none()
        );

        let enter = projector
            .prepare(event(WatchEventType::Modified, pod("web", true, 4), 4))
            .unwrap();
        assert_eq!(enter.event().unwrap().event_type(), WatchEventType::Added);
        projector.commit(enter).unwrap();
        let modify = projector
            .prepare(event(WatchEventType::Modified, pod("web", true, 5), 5))
            .unwrap();
        assert_eq!(
            modify.event().unwrap().event_type(),
            WatchEventType::Modified
        );
        projector.commit(modify).unwrap();

        let deleted = projector
            .prepare(event(WatchEventType::Deleted, pod("web", true, 6), 6))
            .unwrap();
        assert_eq!(
            deleted.event().unwrap().event_type(),
            WatchEventType::Deleted
        );
        projector.commit(deleted).unwrap();
    }

    #[test]
    fn selector_projector_replace_clears_prior_membership_and_wrong_token_fails_closed() {
        let mut projector = projector();
        projector.replace(&[pod("old", true, 1)]);
        projector.replace(&[pod("new", true, 2)]);
        assert!(
            projector
                .prepare(event(WatchEventType::Modified, pod("old", false, 3), 3))
                .unwrap()
                .event()
                .is_none(),
            "snapshot replacement clears membership absent from the new baseline"
        );
        assert!(
            projector
                .commit(PreparedWatchTransition::new(None, 7_u64))
                .is_err(),
            "owner-token mismatches fail closed"
        );
    }
}
