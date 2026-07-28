//! Read-only collection, key-scope, and positioned snapshot capabilities.

use klights_cluster_core::{Resource, WatchReplayPosition};

use crate::{
    DurableWatchTarget, ResourceCollectionKey, ResourceListSnapshot, ResourceReadError,
    ResourceReadFuture,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceWatchTargetsRequest {
    targets: Vec<DurableWatchTarget>,
    label_selector: Option<String>,
}

impl ResourceWatchTargetsRequest {
    pub fn try_new(
        targets: Vec<DurableWatchTarget>,
        label_selector: Option<String>,
    ) -> Result<Self, ResourceReadError> {
        validate_targets(&targets)?;
        Ok(Self {
            targets,
            label_selector,
        })
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceKeyScopeRequest {
    api_version: String,
    kind: String,
    namespaced: bool,
}

impl ResourceKeyScopeRequest {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespaced: bool,
    ) -> Result<Self, ResourceReadError> {
        let api_version = api_version.into();
        let kind = kind.into();
        crate::read_validation::validate_resource_identity(&api_version, &kind)
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self {
            api_version,
            kind,
            namespaced,
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub const fn namespaced(&self) -> bool {
        self.namespaced
    }
}

#[derive(Clone, Debug)]
pub struct ResourceScopeSnapshot {
    items: Vec<Resource>,
    snapshot: ResourceListSnapshot,
}

impl ResourceScopeSnapshot {
    pub fn try_new(
        items: Vec<Resource>,
        position: WatchReplayPosition,
    ) -> Result<Self, ResourceReadError> {
        Ok(Self {
            items,
            snapshot: ResourceListSnapshot::try_new(position)?,
        })
    }

    pub fn items(&self) -> &[Resource] {
        &self.items
    }

    pub fn into_items(self) -> Vec<Resource> {
        self.items
    }

    pub const fn snapshot(&self) -> ResourceListSnapshot {
        self.snapshot
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceSnapshotAtPositionRequest {
    targets: Vec<DurableWatchTarget>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    snapshot: ResourceListSnapshot,
}

impl ResourceSnapshotAtPositionRequest {
    pub fn try_new(
        targets: Vec<DurableWatchTarget>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        position: WatchReplayPosition,
    ) -> Result<Self, ResourceReadError> {
        validate_targets(&targets)?;
        Ok(Self {
            targets,
            label_selector,
            field_selector,
            snapshot: ResourceListSnapshot::try_new(position)?,
        })
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }

    pub const fn position(&self) -> WatchReplayPosition {
        self.snapshot.position()
    }
}

#[derive(Clone, Debug)]
pub enum ResourceSnapshotRead {
    Current,
    Historical(ResourceScopeSnapshot),
    Expired,
}

pub trait ClusterResourceScopeRead: Send + Sync {
    fn list_resources_for_watch_targets(
        &self,
        request: ResourceWatchTargetsRequest,
    ) -> ResourceReadFuture<'_, ResourceScopeSnapshot>;

    fn list_resource_keys_for_scope(
        &self,
        request: ResourceKeyScopeRequest,
    ) -> ResourceReadFuture<'_, Vec<ResourceCollectionKey>>;

    fn list_cluster_resources(&self) -> ResourceReadFuture<'_, Vec<Resource>>;

    fn snapshot_resources_at_position(
        &self,
        request: ResourceSnapshotAtPositionRequest,
    ) -> ResourceReadFuture<'_, ResourceSnapshotRead>;
}

fn validate_targets(targets: &[DurableWatchTarget]) -> Result<(), ResourceReadError> {
    for target in targets {
        crate::durable_recovery::validate_watch_target(target).map_err(|error| {
            ResourceReadError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
    }
    Ok(())
}
