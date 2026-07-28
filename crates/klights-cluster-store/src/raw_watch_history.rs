//! Byte-preserving durable watch-history reads.

use std::borrow::Cow;
use std::num::NonZeroUsize;

use bytes::Bytes;
use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};

use crate::{DurableWatchTarget, MAX_WATCH_HISTORY_PAGE, WatchHistoryError, WatchHistoryFuture};

pub type RawWatchHistoryFuture<'a, T> = WatchHistoryFuture<'a, T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRawWatchEvent {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub event_type: Cow<'static, str>,
    pub object_json: Bytes,
}

impl DurableRawWatchEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        resource_version: i64,
        event_type: impl Into<Cow<'static, str>>,
        object_json: Bytes,
    ) -> Result<Self, WatchHistoryError> {
        let api_version = api_version.into();
        let kind = kind.into();
        let name = name.into();
        let event_type = event_type.into();
        crate::read_validation::validate_resource_identity(&api_version, &kind)
            .map_err(crate::read_validation::map_invalid_watch_request)?;
        crate::read_validation::validate_optional_namespace(namespace.as_deref())
            .map_err(crate::read_validation::map_invalid_watch_request)?;
        crate::read_validation::validate_resource_version(resource_version)
            .map_err(|message| WatchHistoryError::InvalidPosition { message })?;
        Ok(Self {
            api_version,
            kind,
            namespace,
            name,
            resource_version,
            event_type,
            object_json,
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub const fn object_json(&self) -> &Bytes {
        &self.object_json
    }

    pub fn into_object_json(self) -> Bytes {
        self.object_json
    }

    pub fn key(&self) -> (Option<String>, String) {
        (self.namespace.clone(), self.name.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawWatchEventsSinceRequest {
    targets: Vec<DurableWatchTarget>,
    since_resource_version: i64,
    limit: NonZeroUsize,
}

impl RawWatchEventsSinceRequest {
    pub fn try_new(
        targets: Vec<DurableWatchTarget>,
        since_resource_version: i64,
        limit: usize,
    ) -> Result<Self, WatchHistoryError> {
        crate::watch_range::validate_targets(&targets)?;
        crate::read_validation::validate_resource_version(since_resource_version)
            .map_err(|message| WatchHistoryError::InvalidPosition { message })?;
        Ok(Self {
            targets,
            since_resource_version,
            limit: validate_limit(limit)?,
        })
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub const fn since_resource_version(&self) -> i64 {
        self.since_resource_version
    }

    pub const fn limit(&self) -> NonZeroUsize {
        self.limit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawWatchEventsAfterPositionRequest {
    targets: Vec<DurableWatchTarget>,
    position: WatchReplayPosition,
    limit: NonZeroUsize,
}

impl RawWatchEventsAfterPositionRequest {
    pub fn try_new(
        targets: Vec<DurableWatchTarget>,
        position: WatchReplayPosition,
        limit: usize,
    ) -> Result<Self, WatchHistoryError> {
        crate::watch_range::validate_targets(&targets)?;
        crate::durable_recovery::validate_replay_position(position, false)
            .map_err(|message| WatchHistoryError::InvalidPosition { message })?;
        Ok(Self {
            targets,
            position,
            limit: validate_limit(limit)?,
        })
    }

    pub fn targets(&self) -> &[DurableWatchTarget] {
        &self.targets
    }

    pub const fn position(&self) -> WatchReplayPosition {
        self.position
    }

    pub const fn limit(&self) -> NonZeroUsize {
        self.limit
    }
}

#[derive(Clone, Debug)]
pub struct RawWatchHistoryPage {
    events: Vec<DurableRawWatchEvent>,
}

impl RawWatchHistoryPage {
    pub fn try_new(events: Vec<DurableRawWatchEvent>) -> Result<Self, WatchHistoryError> {
        validate_event_count(events.len())?;
        Ok(Self { events })
    }

    pub const fn empty() -> Self {
        Self { events: Vec::new() }
    }

    pub fn events(&self) -> &[DurableRawWatchEvent] {
        &self.events
    }

    pub fn into_events(self) -> Vec<DurableRawWatchEvent> {
        self.events
    }
}

#[derive(Clone, Debug)]
pub enum RawWatchHistoryRead {
    Events(RawWatchHistoryPage),
    Expired,
}

#[derive(Clone, Debug)]
pub struct PositionedRawWatchHistoryPage {
    events: Vec<PositionedWatchEvent<DurableRawWatchEvent>>,
    next_position: WatchReplayPosition,
}

impl PositionedRawWatchHistoryPage {
    pub fn try_new(
        events: Vec<PositionedWatchEvent<DurableRawWatchEvent>>,
        next_position: WatchReplayPosition,
    ) -> Result<Self, WatchHistoryError> {
        validate_event_count(events.len())?;
        crate::durable_recovery::validate_replay_position(next_position, false)
            .map_err(|message| WatchHistoryError::CorruptData { message })?;
        let mut prior_event_id = None;
        for event in &events {
            crate::durable_recovery::validate_replay_position(event.position, false)
                .map_err(|message| WatchHistoryError::CorruptData { message })?;
            if prior_event_id.is_some_and(|prior| event.position.event_id <= prior) {
                return Err(WatchHistoryError::CorruptData {
                    message: "raw watch-history page contains duplicate or out-of-order event IDs"
                        .to_string(),
                });
            }
            if event.position.event_id > next_position.event_id {
                return Err(WatchHistoryError::CorruptData {
                    message: "raw watch-history event exceeds page position".to_string(),
                });
            }
            prior_event_id = Some(event.position.event_id);
        }
        Ok(Self {
            events,
            next_position,
        })
    }

    pub fn events(&self) -> &[PositionedWatchEvent<DurableRawWatchEvent>] {
        &self.events
    }

    pub fn into_events(self) -> Vec<PositionedWatchEvent<DurableRawWatchEvent>> {
        self.events
    }

    pub const fn next_position(&self) -> WatchReplayPosition {
        self.next_position
    }
}

#[derive(Clone, Debug)]
pub enum PositionedRawWatchHistoryRead {
    Events(PositionedRawWatchHistoryPage),
    Expired,
}

pub trait DurableRawWatchHistoryRead: Send + Sync {
    fn list_raw_watch_events_since_checked_bounded(
        &self,
        request: RawWatchEventsSinceRequest,
    ) -> RawWatchHistoryFuture<'_, RawWatchHistoryRead>;

    fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        request: RawWatchEventsAfterPositionRequest,
    ) -> RawWatchHistoryFuture<'_, PositionedRawWatchHistoryRead>;
}

fn validate_limit(limit: usize) -> Result<NonZeroUsize, WatchHistoryError> {
    let limit = NonZeroUsize::new(limit).ok_or(WatchHistoryError::InvalidLimit { limit })?;
    if limit.get() > MAX_WATCH_HISTORY_PAGE {
        return Err(WatchHistoryError::LimitTooLarge {
            limit: limit.get(),
            maximum: MAX_WATCH_HISTORY_PAGE,
        });
    }
    Ok(limit)
}

fn validate_event_count(len: usize) -> Result<(), WatchHistoryError> {
    if len > MAX_WATCH_HISTORY_PAGE {
        return Err(WatchHistoryError::LimitTooLarge {
            limit: len,
            maximum: MAX_WATCH_HISTORY_PAGE,
        });
    }
    Ok(())
}
