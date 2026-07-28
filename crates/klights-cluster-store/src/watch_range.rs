//! Unpaged durable watch/resourceVersion range reads used by recovery paths.

use crate::{DurableWatchEvent, DurableWatchTarget, WatchHistoryError, WatchHistoryFuture};

pub type WatchRangeFuture<'a, T> = WatchHistoryFuture<'a, T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModifiedClusterResourcesRequest {
    api_version: String,
    kind: String,
    since_resource_version: i64,
}

impl ModifiedClusterResourcesRequest {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        since_resource_version: i64,
    ) -> Result<Self, WatchHistoryError> {
        let api_version = api_version.into();
        let kind = kind.into();
        crate::read_validation::validate_resource_identity(&api_version, &kind)
            .map_err(crate::read_validation::map_invalid_watch_request)?;
        validate_start(since_resource_version)?;
        Ok(Self {
            api_version,
            kind,
            since_resource_version,
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub const fn since_resource_version(&self) -> i64 {
        self.since_resource_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModifiedResourcesRequest {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    since_resource_version: i64,
}

impl ModifiedResourcesRequest {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        since_resource_version: i64,
    ) -> Result<Self, WatchHistoryError> {
        let api_version = api_version.into();
        let kind = kind.into();
        crate::read_validation::validate_resource_identity(&api_version, &kind)
            .map_err(crate::read_validation::map_invalid_watch_request)?;
        crate::read_validation::validate_optional_namespace(namespace.as_deref())
            .map_err(crate::read_validation::map_invalid_watch_request)?;
        validate_start(since_resource_version)?;
        Ok(Self {
            api_version,
            kind,
            namespace,
            since_resource_version,
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub const fn since_resource_version(&self) -> i64 {
        self.since_resource_version
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchEventsSinceRequest {
    targets: Vec<DurableWatchTarget>,
    since_resource_version: i64,
}

impl WatchEventsSinceRequest {
    pub fn try_new(
        targets: Vec<DurableWatchTarget>,
        since_resource_version: i64,
    ) -> Result<Self, WatchHistoryError> {
        validate_targets(&targets)?;
        validate_start(since_resource_version)?;
        Ok(Self {
            targets,
            since_resource_version,
        })
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub const fn since_resource_version(&self) -> i64 {
        self.since_resource_version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchRangeStart {
    since_resource_version: i64,
}

impl WatchRangeStart {
    pub fn try_new(since_resource_version: i64) -> Result<Self, WatchHistoryError> {
        validate_start(since_resource_version)?;
        Ok(Self {
            since_resource_version,
        })
    }

    pub const fn since_resource_version(self) -> i64 {
        self.since_resource_version
    }
}

pub trait DurableWatchRangeRead: Send + Sync {
    fn list_cluster_resources_modified_since(
        &self,
        request: ModifiedClusterResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>>;

    fn list_resources_modified_since(
        &self,
        request: ModifiedResourcesRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>>;

    fn list_watch_events_since(
        &self,
        request: WatchEventsSinceRequest,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>>;

    fn earliest_watch_event_rv(&self) -> WatchRangeFuture<'_, Option<i64>>;

    fn list_all_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>>;

    fn list_deleted_watch_events_since(
        &self,
        request: WatchRangeStart,
    ) -> WatchRangeFuture<'_, Vec<DurableWatchEvent>>;
}

pub(crate) fn validate_targets(targets: &[DurableWatchTarget]) -> Result<(), WatchHistoryError> {
    for target in targets {
        crate::durable_recovery::validate_watch_target(target)?;
    }
    Ok(())
}

fn validate_start(since_resource_version: i64) -> Result<(), WatchHistoryError> {
    crate::read_validation::validate_resource_version(since_resource_version)
        .map_err(|message| WatchHistoryError::InvalidPosition { message })
}
