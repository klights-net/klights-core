//! Durable watch replay, allocator, snapshot restore, and cluster metadata ports.
//!
//! These capabilities describe persisted recovery state only. Live watch
//! broadcast, subscriber/session coordination, snapshot codecs, consensus
//! envelopes, and backend schemas remain adapter concerns outside this crate.

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;

use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation,
    OutboxStreamWatermark, PositionedWatchEvent, Resource, ResourceVersionAssignment,
    WatchReplayPosition,
};

pub const MAX_WATCH_HISTORY_PAGE: usize = 4096;
pub const MAX_SNAPSHOT_CAPTURE_PAGE: usize = 512;
pub const RAFT_VOTERS_META_KEY: &str = "voters";
pub const RAFT_TERM_META_KEY: &str = "term";
pub const RAFT_LEADER_HINT_META_KEY: &str = "leader_hint";

/// Persistence failure or invalid request at the durable watch-history boundary.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchHistoryError {
    InvalidLimit { limit: usize },
    LimitTooLarge { limit: usize, maximum: usize },
    InvalidPosition { message: String },
    InvalidTarget { message: String },
    InvalidReplayFloor { message: String },
    Expired { requested: WatchReplayPosition },
    CorruptData { message: String },
    UnsupportedMode { message: String },
    Retryable { message: String },
    Timeout,
    Cancelled,
    PersistenceFailed { message: String },
}

impl WatchHistoryError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }
}

impl fmt::Display for WatchHistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit { limit } => {
                write!(
                    formatter,
                    "invalid watch-history limit {limit}: limit must be non-zero"
                )
            }
            Self::LimitTooLarge { limit, maximum } => write!(
                formatter,
                "watch-history limit {limit} exceeds maximum {maximum}"
            ),
            Self::Expired { requested } => write!(
                formatter,
                "watch-history position is expired: {requested:?}"
            ),
            Self::InvalidPosition { message }
            | Self::InvalidTarget { message }
            | Self::InvalidReplayFloor { message }
            | Self::CorruptData { message }
            | Self::UnsupportedMode { message }
            | Self::Retryable { message }
            | Self::PersistenceFailed { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("watch-history read timed out"),
            Self::Cancelled => formatter.write_str("watch-history read was cancelled"),
        }
    }
}

impl std::error::Error for WatchHistoryError {}

/// Cluster or namespaced scope for one durable watch-history target.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DurableWatchScope {
    Cluster,
    Namespaced(Option<String>),
}

/// Adapter-neutral target for positioned durable watch replay.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DurableWatchTarget {
    api_version: String,
    kind: String,
    scope: DurableWatchScope,
}

impl DurableWatchTarget {
    pub fn cluster(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: DurableWatchScope::Cluster,
        }
    }

    pub fn namespaced(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: DurableWatchScope::Namespaced(None),
        }
    }

    pub fn namespaced_in_namespace(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: DurableWatchScope::Namespaced(Some(namespace.into())),
        }
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub const fn scope(&self) -> &DurableWatchScope {
        &self.scope
    }
}

/// One bounded positioned durable-history query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchHistoryRequest {
    targets: Vec<DurableWatchTarget>,
    position: WatchReplayPosition,
    limit: NonZeroUsize,
}

impl WatchHistoryRequest {
    pub fn new(
        targets: Vec<DurableWatchTarget>,
        position: WatchReplayPosition,
        limit: usize,
    ) -> Result<Self, WatchHistoryError> {
        let limit = NonZeroUsize::new(limit).ok_or(WatchHistoryError::InvalidLimit { limit })?;
        if limit.get() > MAX_WATCH_HISTORY_PAGE {
            return Err(WatchHistoryError::LimitTooLarge {
                limit: limit.get(),
                maximum: MAX_WATCH_HISTORY_PAGE,
            });
        }
        validate_replay_position(position, false)
            .map_err(|message| WatchHistoryError::InvalidPosition { message })?;
        if targets.is_empty() {
            return Err(WatchHistoryError::InvalidTarget {
                message: "watch-history request must contain at least one target".to_string(),
            });
        }
        for target in &targets {
            validate_watch_target(target)?;
        }
        let mut unique = HashSet::with_capacity(targets.len().min(MAX_WATCH_HISTORY_PAGE));
        if targets.iter().any(|target| !unique.insert(target.clone())) {
            return Err(WatchHistoryError::InvalidTarget {
                message: "watch-history request contains a duplicate target".to_string(),
            });
        }
        Ok(Self {
            targets,
            position,
            limit,
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

fn validate_watch_target(target: &DurableWatchTarget) -> Result<(), WatchHistoryError> {
    if !valid_api_version(&target.api_version) || !valid_kind(&target.kind) {
        return Err(WatchHistoryError::InvalidTarget {
            message: "watch-history target contains an empty, malformed, or reserved API identity"
                .to_string(),
        });
    }
    if let DurableWatchScope::Namespaced(Some(namespace)) = &target.scope
        && !valid_namespace(namespace)
    {
        return Err(WatchHistoryError::InvalidTarget {
            message: "watch-history target contains an empty, malformed, or reserved namespace"
                .to_string(),
        });
    }
    Ok(())
}

fn valid_api_version(value: &str) -> bool {
    if reserved_identity(value) {
        return false;
    }
    let mut parts = value.split('/');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() {
        return false;
    }
    match second {
        None => valid_dns_label(first),
        Some(version) => valid_dns_subdomain(first) && valid_dns_label(version),
    }
}

fn valid_kind(value: &str) -> bool {
    // Kubernetes applies DNS-1035 validation to a lower-cased CRD Kind, so
    // mixed case and interior hyphens are valid while path punctuation is not.
    !reserved_identity(value)
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
        && valid_dns_label(value)
}

fn valid_namespace(value: &str) -> bool {
    !reserved_identity(value) && value.len() <= 63 && valid_dns_label(value)
}

fn valid_dns_subdomain(value: &str) -> bool {
    !value.is_empty() && value.len() <= 253 && value.split('.').all(valid_dns_label)
}

fn valid_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
}

fn reserved_identity(value: &str) -> bool {
    value.is_empty() || matches!(value, "*" | "#cluster")
}

/// One retained watch row without a live broadcast/session representation.
#[derive(Clone, Debug)]
pub struct DurableWatchEvent {
    event_type: Cow<'static, str>,
    resource: Resource,
}

impl DurableWatchEvent {
    pub fn new(event_type: impl Into<Cow<'static, str>>, resource: Resource) -> Self {
        Self {
            event_type: event_type.into(),
            resource,
        }
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub const fn resource(&self) -> &Resource {
        &self.resource
    }

    pub fn into_resource(self) -> Resource {
        self.resource
    }
}

/// One bounded history page and the exact cursor covered by its read snapshot.
#[derive(Clone, Debug)]
pub struct WatchHistoryPage {
    events: Vec<PositionedWatchEvent<DurableWatchEvent>>,
    next_position: WatchReplayPosition,
}

impl WatchHistoryPage {
    pub fn try_new(
        events: Vec<PositionedWatchEvent<DurableWatchEvent>>,
        next_position: WatchReplayPosition,
    ) -> Result<Self, WatchHistoryError> {
        validate_replay_position(next_position, false)
            .map_err(|message| WatchHistoryError::CorruptData { message })?;
        let mut prior_event_id = None;
        for event in &events {
            validate_replay_position(event.position, false)
                .map_err(|message| WatchHistoryError::CorruptData { message })?;
            if prior_event_id.is_some_and(|prior| event.position.event_id <= prior) {
                return Err(WatchHistoryError::CorruptData {
                    message: "watch-history page contains duplicate or out-of-order event IDs"
                        .to_string(),
                });
            }
            if event.position.event_id > next_position.event_id {
                return Err(WatchHistoryError::CorruptData {
                    message: "watch-history event exceeds page position".to_string(),
                });
            }
            prior_event_id = Some(event.position.event_id);
        }
        Ok(Self {
            events,
            next_position,
        })
    }

    pub fn validate_after(&self, requested: WatchReplayPosition) -> Result<(), WatchHistoryError> {
        requested
            .validate()
            .map_err(|message| WatchHistoryError::CorruptData { message })?;
        let mut cursor = requested;
        for event in &self.events {
            cursor = cursor
                .advance_through_event(event.position)
                .map_err(|message| WatchHistoryError::CorruptData {
                    message: format!("invalid watch-history event continuation: {message}"),
                })?;
        }
        if !cursor.permits_successor(self.next_position) {
            return Err(WatchHistoryError::CorruptData {
                message: format!(
                    "watch-history page position {:?} is not a canonical continuation of {cursor:?}",
                    self.next_position
                ),
            });
        }
        Ok(())
    }

    pub fn events(&self) -> &[PositionedWatchEvent<DurableWatchEvent>] {
        &self.events
    }

    pub fn into_events(self) -> Vec<PositionedWatchEvent<DurableWatchEvent>> {
        self.events
    }

    pub const fn next_position(&self) -> WatchReplayPosition {
        self.next_position
    }
}

/// Result of checking a cursor against the retained durable history window.
#[derive(Clone, Debug)]
pub enum WatchHistoryRead {
    Events(WatchHistoryPage),
    Expired,
}

/// Semantic target for one durable replay floor.
///
/// Backend sentinel strings used for wildcard and cluster-scoped rows are
/// deliberately not part of this contract.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DurableReplayTarget {
    All,
    Cluster {
        api_version: String,
        kind: String,
    },
    Namespaced {
        api_version: String,
        kind: String,
        namespace: String,
    },
}

/// Per-target compaction boundary restored with authoritative history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableReplayFloor {
    target: DurableReplayTarget,
    boundary: DurableReplayBoundary,
}

/// Lossless persisted floor meaning. A legacy row remains legacy even if an
/// old writer happened to populate its ignored event-ID column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableReplayBoundary {
    LegacyResourceVersion {
        resource_version: i64,
        stored_event_id: i64,
    },
    Exact(WatchReplayPosition),
}

impl DurableReplayFloor {
    pub fn new(
        target: DurableReplayTarget,
        resource_version: i64,
        event_id: i64,
        position_is_exact: bool,
    ) -> Result<Self, WatchHistoryError> {
        validate_replay_target(&target)?;
        if resource_version < 0 || event_id < 0 {
            return Err(WatchHistoryError::InvalidReplayFloor {
                message: format!(
                    "watch replay floor must be non-negative, got resourceVersion {resource_version} and event ID {event_id}"
                ),
            });
        }
        let boundary = if position_is_exact {
            let position = WatchReplayPosition {
                resource_version,
                event_id,
                resource_version_filter_through_event_id: 0,
            };
            validate_replay_position(position, true)
                .map_err(|message| WatchHistoryError::InvalidReplayFloor { message })?;
            DurableReplayBoundary::Exact(position)
        } else {
            DurableReplayBoundary::LegacyResourceVersion {
                resource_version,
                stored_event_id: event_id,
            }
        };
        Ok(Self { target, boundary })
    }

    pub fn all(
        resource_version: i64,
        event_id: i64,
        position_is_exact: bool,
    ) -> Result<Self, WatchHistoryError> {
        Self::new(
            DurableReplayTarget::All,
            resource_version,
            event_id,
            position_is_exact,
        )
    }

    pub fn cluster(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        resource_version: i64,
        event_id: i64,
        position_is_exact: bool,
    ) -> Result<Self, WatchHistoryError> {
        Self::new(
            DurableReplayTarget::Cluster {
                api_version: api_version.into(),
                kind: kind.into(),
            },
            resource_version,
            event_id,
            position_is_exact,
        )
    }

    pub fn namespaced(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
        resource_version: i64,
        event_id: i64,
        position_is_exact: bool,
    ) -> Result<Self, WatchHistoryError> {
        Self::new(
            DurableReplayTarget::Namespaced {
                api_version: api_version.into(),
                kind: kind.into(),
                namespace: namespace.into(),
            },
            resource_version,
            event_id,
            position_is_exact,
        )
    }

    pub const fn target(&self) -> &DurableReplayTarget {
        &self.target
    }

    pub const fn resource_version(&self) -> i64 {
        match self.boundary {
            DurableReplayBoundary::LegacyResourceVersion {
                resource_version, ..
            } => resource_version,
            DurableReplayBoundary::Exact(position) => position.resource_version,
        }
    }

    pub const fn event_id(&self) -> i64 {
        match self.boundary {
            DurableReplayBoundary::LegacyResourceVersion {
                stored_event_id, ..
            } => stored_event_id,
            DurableReplayBoundary::Exact(position) => position.event_id,
        }
    }

    pub const fn position_is_exact(&self) -> bool {
        matches!(self.boundary, DurableReplayBoundary::Exact(_))
    }

    pub const fn boundary(&self) -> &DurableReplayBoundary {
        &self.boundary
    }

    pub fn into_parts(self) -> (DurableReplayTarget, i64, i64, bool) {
        let (resource_version, event_id, exact) = match self.boundary {
            DurableReplayBoundary::LegacyResourceVersion {
                resource_version,
                stored_event_id,
            } => (resource_version, stored_event_id, false),
            DurableReplayBoundary::Exact(position) => {
                (position.resource_version, position.event_id, true)
            }
        };
        (self.target, resource_version, event_id, exact)
    }
}

fn validate_replay_target(target: &DurableReplayTarget) -> Result<(), WatchHistoryError> {
    let fields = match target {
        DurableReplayTarget::All => return Ok(()),
        DurableReplayTarget::Cluster { api_version, kind } => {
            vec![api_version.as_str(), kind.as_str()]
        }
        DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => vec![api_version.as_str(), kind.as_str(), namespace.as_str()],
    };
    if fields
        .iter()
        .any(|value| value.is_empty() || matches!(*value, "*" | "#cluster"))
    {
        return Err(WatchHistoryError::InvalidTarget {
            message: "replay target contains an empty or reserved backend identity".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_replay_position(
    position: WatchReplayPosition,
    exact: bool,
) -> Result<(), String> {
    if exact {
        position.validate_exact()
    } else {
        position.validate()
    }
}

/// Heap-erased future used by the coarse durable-history boundary.
pub type WatchHistoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, WatchHistoryError>> + Send + 'a>>;

/// Read-only positioned history and retention floors.
///
/// No live receiver, subscription, broadcast, session, or filter-coordination
/// method is part of this persistence capability.
pub trait DurableWatchHistoryRead: Send + Sync {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead>;

    fn list_replay_floors(&self) -> WatchHistoryFuture<'_, Vec<DurableReplayFloor>>;
}

/// Persistence or range failure while reading durable allocators.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AllocatorStateError {
    InvalidPosition {
        message: String,
    },
    AllocatorExhausted {
        allocator: &'static str,
        current: i64,
    },
    PersistenceFailed {
        message: String,
    },
    CorruptData {
        message: String,
    },
    UnsupportedMode {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl AllocatorStateError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }
}

impl fmt::Display for AllocatorStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPosition { message }
            | Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::UnsupportedMode { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::AllocatorExhausted { allocator, current } => {
                write!(formatter, "{allocator} allocator exhausted at {current}")
            }
            Self::Timeout => formatter.write_str("allocator read timed out"),
            Self::Cancelled => formatter.write_str("allocator read was cancelled"),
        }
    }
}

impl std::error::Error for AllocatorStateError {}

/// Exact persisted public-RV and event-ID allocator state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableAllocatorState {
    resource_version_assignment: ResourceVersionAssignment,
    position: WatchReplayPosition,
    next_resource_version: i64,
    next_event_id: i64,
}

impl DurableAllocatorState {
    pub fn try_new(
        resource_version_assignment: ResourceVersionAssignment,
        position: WatchReplayPosition,
    ) -> Result<Self, AllocatorStateError> {
        if position.resource_version < 0
            || position.event_id < 0
            || position.resource_version_filter_through_event_id != 0
        {
            return Err(AllocatorStateError::InvalidPosition {
                message: format!(
                    "durable allocator position must be an exact non-negative boundary: {position:?}"
                ),
            });
        }
        let next_resource_version = position.resource_version.checked_add(1).ok_or(
            AllocatorStateError::AllocatorExhausted {
                allocator: "resourceVersion",
                current: position.resource_version,
            },
        )?;
        let next_event_id =
            position
                .event_id
                .checked_add(1)
                .ok_or(AllocatorStateError::AllocatorExhausted {
                    allocator: "event ID",
                    current: position.event_id,
                })?;
        Ok(Self {
            resource_version_assignment,
            position,
            next_resource_version,
            next_event_id,
        })
    }

    pub const fn resource_version_assignment(&self) -> ResourceVersionAssignment {
        self.resource_version_assignment
    }

    pub const fn position(&self) -> WatchReplayPosition {
        self.position
    }

    pub const fn next_resource_version(&self) -> i64 {
        self.next_resource_version
    }

    pub const fn next_event_id(&self) -> i64 {
        self.next_event_id
    }
}

/// Heap-erased future used by the allocator-state boundary.
pub type AllocatorStateFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AllocatorStateError>> + Send + 'a>>;

/// Read-only access to public-RV assignment mode and exact allocator position.
pub trait DurableAllocatorRead: Send + Sync {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState>;
}

/// Validation or persistence failure for authoritative snapshot restore.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotPersistenceError {
    InvalidSnapshot { message: String },
    PersistenceFailed { message: String },
    CorruptData { message: String },
    UnsupportedMode { message: String },
    ResourceExhausted { message: String },
    Retryable { message: String },
    Timeout,
    Cancelled,
}

impl SnapshotPersistenceError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidSnapshot {
            message: message.into(),
        }
    }
}

impl fmt::Display for SnapshotPersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSnapshot { message }
            | Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::UnsupportedMode { message }
            | Self::ResourceExhausted { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("snapshot persistence timed out"),
            Self::Cancelled => formatter.write_str("snapshot persistence was cancelled"),
        }
    }
}

impl std::error::Error for SnapshotPersistenceError {}

/// Adapter-neutral authoritative cluster-store snapshot.
///
/// Codec versioning, compression, OpenRaft log IDs/membership envelopes, and
/// backend table representations deliberately do not appear here.
#[derive(Clone, Debug)]
pub struct AuthoritativeSnapshot {
    commits: Vec<LogApplyCommit>,
    resource_version_assignment: Option<ResourceVersionAssignment>,
    position: Option<WatchReplayPosition>,
    replay_floors: Option<Vec<DurableReplayFloor>>,
    metadata: ClusterMetadata,
    membership: SnapshotMembership,
}

/// Snapshot membership presence has three meanings: an old envelope omitted
/// the field, a modern authoritative snapshot says no membership exists, or a
/// complete membership value is present.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotMembership {
    LegacyOmitted,
    AuthoritativeAbsent,
    Present(ClusterMembership),
}

/// Owned decomposition used by an adapter to persist one validated snapshot.
#[derive(Clone, Debug)]
pub struct AuthoritativeSnapshotParts {
    commits: Vec<LogApplyCommit>,
    resource_version_assignment: Option<ResourceVersionAssignment>,
    position: Option<WatchReplayPosition>,
    replay_floors: Option<Vec<DurableReplayFloor>>,
    metadata: ClusterMetadata,
    membership: SnapshotMembership,
}

impl AuthoritativeSnapshotParts {
    pub fn take_commits(&mut self) -> Vec<LogApplyCommit> {
        std::mem::take(&mut self.commits)
    }
    pub const fn resource_version_assignment(&self) -> Option<ResourceVersionAssignment> {
        self.resource_version_assignment
    }
    pub const fn position(&self) -> Option<WatchReplayPosition> {
        self.position
    }
    pub fn take_replay_floors(&mut self) -> Option<Vec<DurableReplayFloor>> {
        self.replay_floors.take()
    }
    pub const fn metadata(&self) -> &ClusterMetadata {
        &self.metadata
    }
    pub const fn membership(&self) -> &SnapshotMembership {
        &self.membership
    }
    pub fn into_metadata_and_membership(self) -> (ClusterMetadata, SnapshotMembership) {
        (self.metadata, self.membership)
    }
}

impl AuthoritativeSnapshot {
    pub fn try_new(
        commits: Vec<LogApplyCommit>,
        resource_version_assignment: Option<ResourceVersionAssignment>,
        position: Option<WatchReplayPosition>,
        replay_floors: Option<Vec<DurableReplayFloor>>,
        metadata: ClusterMetadata,
        membership: SnapshotMembership,
    ) -> Result<Self, SnapshotPersistenceError> {
        validate_snapshot(
            &commits,
            position,
            replay_floors.as_deref(),
            &metadata,
            &membership,
        )?;
        Ok(Self {
            commits,
            resource_version_assignment,
            position,
            replay_floors,
            metadata,
            membership,
        })
    }

    pub fn commits(&self) -> &[LogApplyCommit] {
        &self.commits
    }

    pub const fn resource_version_assignment(&self) -> Option<ResourceVersionAssignment> {
        self.resource_version_assignment
    }

    pub const fn position(&self) -> Option<WatchReplayPosition> {
        self.position
    }

    pub fn replay_floors(&self) -> Option<&[DurableReplayFloor]> {
        self.replay_floors.as_deref()
    }

    pub const fn metadata(&self) -> &ClusterMetadata {
        &self.metadata
    }

    pub const fn membership(&self) -> &SnapshotMembership {
        &self.membership
    }

    pub fn into_parts(self) -> AuthoritativeSnapshotParts {
        AuthoritativeSnapshotParts {
            commits: self.commits,
            resource_version_assignment: self.resource_version_assignment,
            position: self.position,
            replay_floors: self.replay_floors,
            metadata: self.metadata,
            membership: self.membership,
        }
    }
}

fn validate_snapshot(
    commits: &[LogApplyCommit],
    position: Option<WatchReplayPosition>,
    replay_floors: Option<&[DurableReplayFloor]>,
    metadata: &ClusterMetadata,
    membership: &SnapshotMembership,
) -> Result<(), SnapshotPersistenceError> {
    validate_snapshot_metadata(metadata, membership)?;
    if let Some(position) = position {
        if position.resource_version < 0
            || position.event_id < 0
            || position.resource_version_filter_through_event_id != 0
        {
            return Err(SnapshotPersistenceError::invalid(format!(
                "authoritative snapshot position must be an exact non-negative allocator boundary: {position:?}"
            )));
        }
        if metadata.current_rv != position.resource_version {
            return Err(SnapshotPersistenceError::invalid(format!(
                "snapshot metadata resourceVersion {} does not match allocator position {}",
                metadata.current_rv, position.resource_version
            )));
        }
    }
    let mut event_ids = HashSet::new();
    for commit in commits {
        if commit.resource_version <= 0 || commit.resource_version > metadata.current_rv {
            return Err(SnapshotPersistenceError::invalid(format!(
                "snapshot commit resourceVersion {} is outside 1..={}",
                commit.resource_version, metadata.current_rv
            )));
        }
        for mutation in &commit.mutations {
            if let LogApplyMutation::PutWatchEvent(row) = mutation {
                let event_id = match (row.event_id, position) {
                    (None, Some(_)) => {
                        return Err(SnapshotPersistenceError::invalid(
                            "positioned authoritative snapshot watch row is missing its durable event ID",
                        ));
                    }
                    (None, None) => continue,
                    (Some(event_id), _) => event_id,
                };
                if event_id <= 0 || position.is_some_and(|position| event_id > position.event_id) {
                    return Err(SnapshotPersistenceError::invalid(format!(
                        "snapshot watch event ID {event_id} is outside its allocator boundary"
                    )));
                }
                if !event_ids.insert(event_id) {
                    return Err(SnapshotPersistenceError::invalid(format!(
                        "snapshot contains duplicate watch event ID {event_id}"
                    )));
                }
            }
        }
    }
    let mut floor_targets = HashSet::new();
    for floor in replay_floors.unwrap_or_default() {
        if !floor_targets.insert(floor.target.clone()) {
            return Err(SnapshotPersistenceError::invalid(
                "snapshot contains duplicate replay-floor target",
            ));
        }
        if floor.resource_version() > metadata.current_rv {
            return Err(SnapshotPersistenceError::invalid(format!(
                "snapshot replay-floor resourceVersion {} exceeds current resourceVersion {}",
                floor.resource_version(),
                metadata.current_rv
            )));
        }
        if floor.position_is_exact() {
            let position = position.ok_or_else(|| {
                SnapshotPersistenceError::invalid(
                    "exact replay floor requires a positioned snapshot",
                )
            })?;
            if floor.event_id() > position.event_id {
                return Err(SnapshotPersistenceError::invalid(format!(
                    "snapshot replay-floor event ID {} exceeds allocator high-water {}",
                    floor.event_id(),
                    position.event_id
                )));
            }
        }
    }
    Ok(())
}

fn validate_snapshot_metadata(
    metadata: &ClusterMetadata,
    membership: &SnapshotMembership,
) -> Result<(), SnapshotPersistenceError> {
    if metadata.cluster_id.is_empty() {
        return Err(SnapshotPersistenceError::invalid(
            "authoritative snapshot cluster ID must not be empty",
        ));
    }
    if metadata.current_rv < 0 || metadata.leader_epoch < 0 {
        return Err(SnapshotPersistenceError::invalid(format!(
            "authoritative snapshot numeric metadata must be non-negative, got epoch {} and resourceVersion {}",
            metadata.leader_epoch, metadata.current_rv
        )));
    }
    if let SnapshotMembership::Present(membership) = membership {
        if membership.cluster_id != metadata.cluster_id {
            return Err(SnapshotPersistenceError::invalid(format!(
                "snapshot membership cluster ID {:?} does not match metadata cluster ID {:?}",
                membership.cluster_id, metadata.cluster_id
            )));
        }
        let mut voters = HashSet::with_capacity(membership.voters.len());
        if membership.term < 0
            || membership.voters.is_empty()
            || membership
                .voters
                .iter()
                .any(|voter| voter.is_empty() || !voters.insert(voter))
            || membership.leader_hint.as_deref().is_some_and(str::is_empty)
        {
            return Err(SnapshotPersistenceError::invalid(
                "snapshot membership has an invalid term, voter set, or leader hint",
            ));
        }
    }
    Ok(())
}

/// Heap-erased future used by authoritative snapshot persistence.
pub type SnapshotPersistenceFuture<'a, T = ()> =
    Pin<Box<dyn Future<Output = Result<T, SnapshotPersistenceError>> + Send + 'a>>;

/// Privileged right to atomically replace cluster-store state from a snapshot.
pub trait AuthoritativeSnapshotPersistence: Send + Sync {
    fn restore_authoritative_snapshot(
        &self,
        snapshot: AuthoritativeSnapshot,
    ) -> SnapshotPersistenceFuture<'_>;
}

/// Exclusive keyset continuation for ordered outbox-watermark snapshot pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotOutboxWatermarkCursor {
    client_id: String,
    stream_id: i64,
}

impl SnapshotOutboxWatermarkCursor {
    pub fn try_new(
        client_id: impl Into<String>,
        stream_id: i64,
    ) -> Result<Self, SnapshotPersistenceError> {
        let client_id = client_id.into();
        if client_id.is_empty() || client_id.contains('\0') || stream_id <= 0 {
            return Err(SnapshotPersistenceError::invalid(
                "outbox-watermark cursor requires a non-empty NUL-free client ID and positive stream ID",
            ));
        }
        Ok(Self {
            client_id,
            stream_id,
        })
    }

    pub fn from_watermark(
        watermark: &OutboxStreamWatermark,
    ) -> Result<Self, SnapshotPersistenceError> {
        Self::try_new(watermark.client_id.clone(), watermark.stream_id)
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub const fn stream_id(&self) -> i64 {
        self.stream_id
    }
}

/// Exclusive keyset continuation for ordered replay-floor snapshot pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotReplayFloorCursor {
    target: DurableReplayTarget,
}

impl SnapshotReplayFloorCursor {
    pub fn try_new(target: DurableReplayTarget) -> Result<Self, SnapshotPersistenceError> {
        validate_replay_target(&target)
            .map_err(|error| SnapshotPersistenceError::invalid(error.to_string()))?;
        Ok(Self { target })
    }

    pub fn from_floor(floor: &DurableReplayFloor) -> Self {
        Self {
            target: floor.target().clone(),
        }
    }

    pub const fn target(&self) -> &DurableReplayTarget {
        &self.target
    }
}

/// Metadata captured while the short exclusive fence pins the backend's
/// immutable read view. Bounded pages drain that pinned view after the fence
/// has been released.
#[derive(Clone, Debug)]
pub struct SnapshotCaptureHeader {
    resource_version_assignment: Option<ResourceVersionAssignment>,
    command_codec_activation_version: Option<u32>,
    position: WatchReplayPosition,
    metadata: ClusterMetadata,
    membership: SnapshotMembership,
}

impl SnapshotCaptureHeader {
    pub fn try_new(
        resource_version_assignment: Option<ResourceVersionAssignment>,
        command_codec_activation_version: Option<u32>,
        position: WatchReplayPosition,
        metadata: ClusterMetadata,
        membership: SnapshotMembership,
    ) -> Result<Self, SnapshotPersistenceError> {
        if command_codec_activation_version.is_some_and(|version| version != 3) {
            return Err(SnapshotPersistenceError::invalid(
                "snapshot command codec activation version must be exact v3",
            ));
        }
        validate_replay_position(position, true).map_err(SnapshotPersistenceError::invalid)?;
        validate_snapshot_metadata(&metadata, &membership)?;
        if metadata.current_rv != position.resource_version {
            return Err(SnapshotPersistenceError::invalid(
                "snapshot capture metadata does not match its allocator position",
            ));
        }
        Ok(Self {
            resource_version_assignment,
            command_codec_activation_version,
            position,
            metadata,
            membership,
        })
    }
    pub const fn resource_version_assignment(&self) -> Option<ResourceVersionAssignment> {
        self.resource_version_assignment
    }
    pub const fn command_codec_activation_version(&self) -> Option<u32> {
        self.command_codec_activation_version
    }
    pub const fn position(&self) -> WatchReplayPosition {
        self.position
    }
    pub const fn metadata(&self) -> &ClusterMetadata {
        &self.metadata
    }
    pub const fn membership(&self) -> &SnapshotMembership {
        &self.membership
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotCapturePageKind {
    Commits,
    AppliedOutbox,
    OutboxWatermarks,
    ReplayFloors,
}

#[derive(Clone, Debug)]
enum SnapshotCapturePageContents {
    Commits(Vec<LogApplyCommit>),
    AppliedOutbox(Vec<LogApplyAppliedOutboxRow>),
    OutboxWatermarks(Vec<OutboxStreamWatermark>),
    ReplayFloors(Vec<DurableReplayFloor>),
}

#[derive(Clone, Debug)]
pub struct SnapshotCapturePage {
    contents: SnapshotCapturePageContents,
}

impl SnapshotCapturePage {
    pub fn try_commits(rows: Vec<LogApplyCommit>) -> Result<Self, SnapshotPersistenceError> {
        validate_capture_len(rows.len())?;
        Ok(Self {
            contents: SnapshotCapturePageContents::Commits(rows),
        })
    }
    pub fn try_applied_outbox(
        rows: Vec<LogApplyAppliedOutboxRow>,
    ) -> Result<Self, SnapshotPersistenceError> {
        validate_capture_len(rows.len())?;
        Ok(Self {
            contents: SnapshotCapturePageContents::AppliedOutbox(rows),
        })
    }
    pub fn try_outbox_watermarks(
        rows: Vec<OutboxStreamWatermark>,
    ) -> Result<Self, SnapshotPersistenceError> {
        validate_capture_len(rows.len())?;
        Ok(Self {
            contents: SnapshotCapturePageContents::OutboxWatermarks(rows),
        })
    }
    pub fn try_replay_floors(
        rows: Vec<DurableReplayFloor>,
    ) -> Result<Self, SnapshotPersistenceError> {
        validate_capture_len(rows.len())?;
        Ok(Self {
            contents: SnapshotCapturePageContents::ReplayFloors(rows),
        })
    }
    pub const fn kind(&self) -> SnapshotCapturePageKind {
        match self.contents {
            SnapshotCapturePageContents::Commits(_) => SnapshotCapturePageKind::Commits,
            SnapshotCapturePageContents::AppliedOutbox(_) => SnapshotCapturePageKind::AppliedOutbox,
            SnapshotCapturePageContents::OutboxWatermarks(_) => {
                SnapshotCapturePageKind::OutboxWatermarks
            }
            SnapshotCapturePageContents::ReplayFloors(_) => SnapshotCapturePageKind::ReplayFloors,
        }
    }
    pub fn commits(&self) -> Option<&[LogApplyCommit]> {
        match &self.contents {
            SnapshotCapturePageContents::Commits(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn applied_outbox(&self) -> Option<&[LogApplyAppliedOutboxRow]> {
        match &self.contents {
            SnapshotCapturePageContents::AppliedOutbox(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn outbox_watermarks(&self) -> Option<&[OutboxStreamWatermark]> {
        match &self.contents {
            SnapshotCapturePageContents::OutboxWatermarks(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn replay_floors(&self) -> Option<&[DurableReplayFloor]> {
        match &self.contents {
            SnapshotCapturePageContents::ReplayFloors(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn into_commits(self) -> Option<Vec<LogApplyCommit>> {
        match self.contents {
            SnapshotCapturePageContents::Commits(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn into_applied_outbox(self) -> Option<Vec<LogApplyAppliedOutboxRow>> {
        match self.contents {
            SnapshotCapturePageContents::AppliedOutbox(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn into_outbox_watermarks(self) -> Option<Vec<OutboxStreamWatermark>> {
        match self.contents {
            SnapshotCapturePageContents::OutboxWatermarks(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn into_replay_floors(self) -> Option<Vec<DurableReplayFloor>> {
        match self.contents {
            SnapshotCapturePageContents::ReplayFloors(rows) => Some(rows),
            _ => None,
        }
    }
    pub fn len(&self) -> usize {
        match &self.contents {
            SnapshotCapturePageContents::Commits(rows) => rows.len(),
            SnapshotCapturePageContents::AppliedOutbox(rows) => rows.len(),
            SnapshotCapturePageContents::OutboxWatermarks(rows) => rows.len(),
            SnapshotCapturePageContents::ReplayFloors(rows) => rows.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn validate_capture_len(len: usize) -> Result<(), SnapshotPersistenceError> {
    if len == 0 || len > MAX_SNAPSHOT_CAPTURE_PAGE {
        return Err(SnapshotPersistenceError::invalid(format!(
            "snapshot capture page length {len} is outside 1..={MAX_SNAPSHOT_CAPTURE_PAGE}"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotPageLimit(usize);

impl SnapshotPageLimit {
    pub fn try_new(value: usize) -> Result<Self, SnapshotPersistenceError> {
        if value == 0 || value > MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(SnapshotPersistenceError::invalid(format!(
                "snapshot page limit {value} is outside 1..={MAX_SNAPSHOT_CAPTURE_PAGE}"
            )));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCaptureRequest {
    page_limit: SnapshotPageLimit,
    max_lifetime: std::time::Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotCommitCursor {
    Namespace(String),
    ClusterResource {
        api_version: String,
        kind: String,
        name: String,
    },
    NamespacedResource {
        api_version: String,
        kind: String,
        namespace: String,
        name: String,
    },
    WatchEvent(i64),
    NodeSubnet(String),
    NodeDataplane(String),
    PodCleanup {
        node_name: String,
        namespace: String,
        pod_name: String,
        pod_uid: String,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotCaptureCursor {
    Commit(SnapshotCommitCursor),
    AppliedOutbox(String),
    OutboxWatermark(SnapshotOutboxWatermarkCursor),
    ReplayFloor(SnapshotReplayFloorCursor),
    Complete,
}

impl SnapshotCaptureRequest {
    pub fn try_new(
        page_limit: SnapshotPageLimit,
        max_lifetime: std::time::Duration,
    ) -> Result<Self, SnapshotPersistenceError> {
        if max_lifetime.is_zero() {
            return Err(SnapshotPersistenceError::invalid(
                "snapshot capture max lifetime must be positive",
            ));
        }
        Ok(Self {
            page_limit,
            max_lifetime,
        })
    }

    pub const fn page_limit(self) -> SnapshotPageLimit {
        self.page_limit
    }

    pub const fn max_lifetime(self) -> std::time::Duration {
        self.max_lifetime
    }
}

pub trait SnapshotCaptureSession: Send {
    fn header(&self) -> &SnapshotCaptureHeader;
    fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>>;
    fn cancel(&mut self) -> SnapshotPersistenceFuture<'_>;
}

pub trait AuthoritativeSnapshotCapture: Send + Sync {
    fn begin_capture(
        &self,
        request: SnapshotCaptureRequest,
    ) -> SnapshotPersistenceFuture<'_, Box<dyn SnapshotCaptureSession>>;
}

/// Persistence failure while reading canonical cluster metadata.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterMetadataStoreError {
    PersistenceFailed { message: String },
    Incomplete { message: String },
    CorruptData { message: String },
    Retryable { message: String },
    Timeout,
    Cancelled,
}

impl ClusterMetadataStoreError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }
}

impl fmt::Display for ClusterMetadataStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PersistenceFailed { message }
            | Self::Incomplete { message }
            | Self::CorruptData { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("cluster metadata read timed out"),
            Self::Cancelled => formatter.write_str("cluster metadata read was cancelled"),
        }
    }
}

impl std::error::Error for ClusterMetadataStoreError {}

/// Heap-erased future used by canonical cluster-metadata reads.
pub type ClusterMetadataFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ClusterMetadataStoreError>> + Send + 'a>>;

/// Typed canonical cluster identity and membership metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedClusterMetadata {
    metadata: ClusterMetadata,
    membership: SnapshotMembership,
}

impl PersistedClusterMetadata {
    pub fn new(metadata: ClusterMetadata, membership: SnapshotMembership) -> Self {
        Self {
            metadata,
            membership,
        }
    }

    pub const fn metadata(&self) -> &ClusterMetadata {
        &self.metadata
    }

    pub const fn membership(&self) -> &SnapshotMembership {
        &self.membership
    }

    pub fn into_parts(self) -> (ClusterMetadata, SnapshotMembership) {
        (self.metadata, self.membership)
    }
}

/// Read-only access to one typed canonical cluster-metadata observation.
pub trait ClusterMetadataRead: Send + Sync {
    fn read_cluster_metadata(&self) -> ClusterMetadataFuture<'_, PersistedClusterMetadata>;
}
