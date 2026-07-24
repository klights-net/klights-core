//! Transport-neutral leader-owned API contracts for klights.

use std::fmt;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::Arc;

use futures_core::Stream;
use klights_cluster_core::{Resource, StorageCommand, StorageResponse, WatchReplayPosition};
use klights_types::ResourceKey;

/// Whether a query may use the worker's coherent informer cache or must read
/// from the current leader.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResourceQueryConsistency {
    Cached,
    LeaderFresh,
}

/// Failure returned by the focused leader resource-query capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceQueryError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotFound {
        key: ResourceKey,
    },
    QueryFailed {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Unsupported {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl ResourceQueryError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn query_failed(message: impl Into<String>) -> Self {
        Self::QueryFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for ResourceQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotFound { key } => write!(
                formatter,
                "{}/{}/{}{} not found",
                key.api_version,
                key.kind,
                key.namespace
                    .as_deref()
                    .map(|namespace| format!("{namespace}/"))
                    .unwrap_or_default(),
                key.name
            ),
            Self::QueryFailed { message }
            | Self::CorruptResponse { message }
            | Self::Unsupported { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("leader resource query timed out"),
            Self::Cancelled => formatter.write_str("leader resource query was cancelled"),
        }
    }
}

impl std::error::Error for ResourceQueryError {}

fn require_nonempty(value: &str, field: &'static str) -> Result<(), ResourceQueryError> {
    if value.is_empty() {
        Err(ResourceQueryError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_key(key: &ResourceKey) -> Result<(), ResourceQueryError> {
    require_nonempty(&key.api_version, "resource.api_version")?;
    require_nonempty(&key.kind, "resource.kind")?;
    if let Some(namespace) = key.namespace.as_deref() {
        require_nonempty(namespace, "resource.namespace")?;
    }
    require_nonempty(&key.name, "resource.name")
}

/// Validated, owned request for one exact resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceGetRequest {
    key: ResourceKey,
    consistency: ResourceQueryConsistency,
}

impl ResourceGetRequest {
    pub fn try_new(
        key: ResourceKey,
        consistency: ResourceQueryConsistency,
    ) -> Result<Self, ResourceQueryError> {
        validate_key(&key)?;
        Ok(Self { key, consistency })
    }

    pub const fn key(&self) -> &ResourceKey {
        &self.key
    }

    pub const fn consistency(&self) -> ResourceQueryConsistency {
        self.consistency
    }

    pub fn into_key(self) -> ResourceKey {
        self.key
    }
}

/// Validated, owned Kubernetes LIST request. Selector and continuation strings
/// remain opaque and byte-for-byte unchanged for the private transport adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceListRequest {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    limit: Option<i64>,
    continue_token: Option<String>,
    consistency: ResourceQueryConsistency,
}

impl ResourceListRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        limit: Option<i64>,
        continue_token: Option<String>,
        consistency: ResourceQueryConsistency,
    ) -> Result<Self, ResourceQueryError> {
        let api_version = api_version.into();
        let kind = kind.into();
        require_nonempty(&api_version, "list.api_version")?;
        require_nonempty(&kind, "list.kind")?;
        if let Some(namespace) = namespace.as_deref() {
            require_nonempty(namespace, "list.namespace")?;
        }
        if limit.is_some_and(|limit| limit < 0) {
            return Err(ResourceQueryError::invalid(
                "list.limit",
                "must be non-negative",
            ));
        }
        if let Some(selector) = field_selector
            .as_deref()
            .filter(|selector| !selector.trim().is_empty())
        {
            klights_types::FieldSelector::parse(selector).map_err(|error| {
                ResourceQueryError::invalid("list.field_selector", error.to_string())
            })?;
        }
        Ok(Self {
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
            consistency,
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

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }

    pub const fn limit(&self) -> Option<i64> {
        self.limit
    }

    pub fn continue_token(&self) -> Option<&str> {
        self.continue_token.as_deref()
    }

    pub const fn consistency(&self) -> ResourceQueryConsistency {
        self.consistency
    }
}

/// Exact LIST response, including the public resourceVersion and atomic
/// LIST-to-WATCH handoff captured by the leader.
#[derive(Clone, Debug)]
pub struct ResourceListResult {
    items: Vec<Resource>,
    resource_version: i64,
    watch_replay_position: Option<WatchReplayPosition>,
    continue_token: Option<String>,
    remaining_item_count: Option<i64>,
}

impl ResourceListResult {
    pub fn try_new(
        items: Vec<Resource>,
        resource_version: i64,
        watch_replay_position: Option<WatchReplayPosition>,
        continue_token: Option<String>,
        remaining_item_count: Option<i64>,
    ) -> Result<Self, ResourceQueryError> {
        if resource_version < 0 {
            return Err(ResourceQueryError::corrupt_response(
                "LIST resourceVersion is negative",
            ));
        }
        if let Some(position) = watch_replay_position
            && (position.validate().is_err() || position.resource_version != resource_version)
        {
            return Err(ResourceQueryError::corrupt_response(
                "LIST replay position is invalid or does not match its public resourceVersion",
            ));
        }
        if remaining_item_count.is_some_and(|remaining| remaining < 0) {
            return Err(ResourceQueryError::corrupt_response(
                "LIST remaining item count is negative",
            ));
        }
        Ok(Self {
            items,
            resource_version,
            watch_replay_position,
            continue_token,
            remaining_item_count,
        })
    }

    pub fn items(&self) -> &[Resource] {
        &self.items
    }

    pub fn into_items(self) -> Vec<Resource> {
        self.items
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub const fn watch_replay_position(&self) -> Option<WatchReplayPosition> {
        self.watch_replay_position
    }

    pub fn continue_token(&self) -> Option<&str> {
        self.continue_token.as_deref()
    }

    pub const fn remaining_item_count(&self) -> Option<i64> {
        self.remaining_item_count
    }

    pub fn into_parts(
        self,
    ) -> (
        Vec<Resource>,
        i64,
        Option<WatchReplayPosition>,
        Option<String>,
        Option<i64>,
    ) {
        (
            self.items,
            self.resource_version,
            self.watch_replay_position,
            self.continue_token,
            self.remaining_item_count,
        )
    }
}

/// Heap-erased future used at the leader-query boundary.
pub type ResourceQueryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ResourceQueryError>> + Send + 'a>>;

/// Focused resource-query port. Consistency is explicit on each owned request;
/// transports remain private adapters.
pub trait LeaderResourceQuery: Send + Sync {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>>;

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult>;
}

/// Failure returned by the focused leader resource-command capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceCommandError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    UnsupportedCommand {
        command: &'static str,
    },
    PodDeletionForbidden,
    NotLeader,
    Unauthorized,
    Conflict {
        message: String,
    },
    NotFound {
        message: String,
    },
    SubmissionFailed {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl ResourceCommandError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::invalid(field, message)
    }

    pub fn submission_failed(message: impl Into<String>) -> Self {
        Self::SubmissionFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for ResourceCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::UnsupportedCommand { command } => {
                write!(formatter, "unsupported leader resource command: {command}")
            }
            Self::PodDeletionForbidden => formatter.write_str(
                "generic Pod deletion is forbidden; use the UID-bound Pod lifecycle actor path",
            ),
            Self::NotLeader => formatter.write_str("resource command target is not raft leader"),
            Self::Unauthorized => formatter
                .write_str("resource command submission requires a control-plane node identity"),
            Self::Conflict { message }
            | Self::NotFound { message }
            | Self::SubmissionFailed { message }
            | Self::CorruptResponse { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("leader resource command timed out"),
            Self::Cancelled => formatter.write_str("leader resource command was cancelled"),
        }
    }
}

impl std::error::Error for ResourceCommandError {}

fn validate_command_identity(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<(), ResourceCommandError> {
    let require = |value: &str, field| {
        if value.is_empty() {
            Err(ResourceCommandError::invalid(field, "must not be empty"))
        } else {
            Ok(())
        }
    };
    require(api_version, "resource.api_version")?;
    require(kind, "resource.kind")?;
    if let Some(namespace) = namespace {
        require(namespace, "resource.namespace")?;
    }
    require(name, "resource.name")
}

/// Validated, owned request for one canonical generic resource mutation.
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceCommandRequest {
    command: StorageCommand,
}

impl ResourceCommandRequest {
    pub fn try_new(command: StorageCommand) -> Result<Self, ResourceCommandError> {
        let (api_version, kind, namespace, name) = match &command {
            StorageCommand::CreateResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            }
            | StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            }
            | StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            }
            | StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            }
            | StorageCommand::DeleteResourceWithTombstone {
                api_version,
                kind,
                namespace,
                name,
                ..
            } => (api_version, kind, namespace, name),
            unsupported => {
                return Err(ResourceCommandError::UnsupportedCommand {
                    command: unsupported.variant_name(),
                });
            }
        };
        validate_command_identity(api_version, kind, namespace.as_deref(), name)?;
        if kind == "Pod"
            && matches!(
                command,
                StorageCommand::DeleteResource { .. }
                    | StorageCommand::DeleteResourceWithTombstone { .. }
            )
        {
            return Err(ResourceCommandError::PodDeletionForbidden);
        }
        if let StorageCommand::DeleteResourceWithTombstone { grace_seconds, .. } = &command
            && *grace_seconds < 0
        {
            return Err(ResourceCommandError::invalid(
                "delete.grace_seconds",
                "must be non-negative",
            ));
        }
        if let StorageCommand::UpdateResource {
            expected_rv,
            preconditions,
            ..
        } = &command
            && preconditions.resource_version.unwrap_or(0) != *expected_rv
        {
            return Err(ResourceCommandError::invalid(
                "update.expected_rv",
                "must match the resourceVersion precondition",
            ));
        }
        Ok(Self { command })
    }

    pub const fn command(&self) -> &StorageCommand {
        &self.command
    }

    pub fn into_command(self) -> StorageCommand {
        self.command
    }
}

/// Successful result of a generic resource command. Internal command response
/// variants never cross this focused port.
#[derive(Clone, Debug)]
pub enum ResourceCommandResult {
    Resource(Resource),
    Ack { resource_version: i64 },
}

impl PartialEq for ResourceCommandResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Resource(left), Self::Resource(right)) => {
                left.id == right.id
                    && left.api_version == right.api_version
                    && left.kind == right.kind
                    && left.namespace == right.namespace
                    && left.name == right.name
                    && left.uid == right.uid
                    && left.resource_version == right.resource_version
                    && left.data == right.data
            }
            (
                Self::Ack {
                    resource_version: left,
                },
                Self::Ack {
                    resource_version: right,
                },
            ) => left == right,
            _ => false,
        }
    }
}

impl ResourceCommandResult {
    pub fn try_from_response(response: StorageResponse) -> Result<Self, ResourceCommandError> {
        match response {
            StorageResponse::Resource {
                resource_version,
                data,
            } => {
                if resource_version < 0 {
                    return Err(ResourceCommandError::corrupt_response(
                        "resource command response has a negative resourceVersion",
                    ));
                }
                let resource =
                    Resource::try_from_data(std::sync::Arc::new(data)).map_err(|err| {
                        ResourceCommandError::corrupt_response(format!(
                            "resource command response has invalid identity: {err}"
                        ))
                    })?;
                if resource.resource_version != resource_version {
                    return Err(ResourceCommandError::corrupt_response(
                        "resource command response resourceVersion does not match its object",
                    ));
                }
                Ok(Self::Resource(resource))
            }
            StorageResponse::Ack { resource_version } if resource_version >= 0 => {
                Ok(Self::Ack { resource_version })
            }
            StorageResponse::Ack { .. } => Err(ResourceCommandError::corrupt_response(
                "resource command acknowledgement has a negative resourceVersion",
            )),
            StorageResponse::Error { message } => {
                Err(ResourceCommandError::submission_failed(message))
            }
            StorageResponse::NodeSubnet { .. } => Err(ResourceCommandError::corrupt_response(
                "resource command returned a non-resource result",
            )),
            _ => Err(ResourceCommandError::corrupt_response(
                "resource command returned an unknown result variant",
            )),
        }
    }
}

/// Heap-erased future used at the leader resource-command boundary.
pub type ResourceCommandFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ResourceCommandError>> + Send + 'a>>;

/// One-method, object-safe capability for canonical generic resource commands.
pub trait LeaderResourceCommand: Send + Sync {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult>;
}

/// Stable Kubernetes watch transition carried across the leader boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
    Bookmark,
    Error,
}

impl WatchEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "ADDED",
            Self::Modified => "MODIFIED",
            Self::Deleted => "DELETED",
            Self::Bookmark => "BOOKMARK",
            Self::Error => "ERROR",
        }
    }
}

/// Failure returned while establishing or pulling a positioned watch.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaderWatchError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    ReplayExpired {
        accepted_resource_version: i64,
    },
    MalformedEvent {
        message: String,
    },
    MismatchedEvent {
        message: String,
    },
    UnknownEventType {
        event_type: String,
    },
    OutOfOrderEvent {
        current_event_id: i64,
        delivered_event_id: i64,
    },
    Unavailable {
        message: String,
    },
    Transport {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl LeaderWatchError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::invalid(field, message)
    }

    pub fn malformed_event(message: impl Into<String>) -> Self {
        Self::MalformedEvent {
            message: message.into(),
        }
    }

    pub fn mismatched_event(message: impl Into<String>) -> Self {
        Self::MismatchedEvent {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }
}

impl fmt::Display for LeaderWatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::ReplayExpired {
                accepted_resource_version,
            } => write!(
                formatter,
                "watch replay expired at accepted resourceVersion {accepted_resource_version}"
            ),
            Self::MalformedEvent { message }
            | Self::MismatchedEvent { message }
            | Self::Unavailable { message }
            | Self::Transport { message } => formatter.write_str(message),
            Self::UnknownEventType { event_type } => {
                write!(formatter, "unknown watch event type {event_type:?}")
            }
            Self::OutOfOrderEvent {
                current_event_id,
                delivered_event_id,
            } => write!(
                formatter,
                "watch event ID regressed from {current_event_id} to {delivered_event_id}"
            ),
            Self::Timeout => formatter.write_str("leader watch timed out"),
            Self::Cancelled => formatter.write_str("leader watch was cancelled"),
        }
    }
}

impl std::error::Error for LeaderWatchError {}

fn validate_watch_identity(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> Result<(), LeaderWatchError> {
    if api_version.is_empty() {
        return Err(LeaderWatchError::invalid(
            "watch.api_version",
            "must not be empty",
        ));
    }
    if kind.is_empty() {
        return Err(LeaderWatchError::invalid("watch.kind", "must not be empty"));
    }
    if namespace.is_some_and(str::is_empty) {
        return Err(LeaderWatchError::invalid(
            "watch.namespace",
            "must not be empty",
        ));
    }
    Ok(())
}

fn validate_replay_position(position: WatchReplayPosition) -> Result<(), LeaderWatchError> {
    position
        .validate()
        .map_err(|message| LeaderWatchError::invalid("watch.start_watch_replay_position", message))
}

/// Validated, transport-neutral positioned-watch request. Selector strings are
/// opaque and preserved exactly. An exact replay position is authoritative;
/// the scalar resourceVersion remains populated for rolling-upgrade peers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchRequest {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    start_resource_version: Option<i64>,
    start_watch_replay_position: Option<WatchReplayPosition>,
}

impl WatchRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        start_resource_version: Option<i64>,
        start_watch_replay_position: Option<WatchReplayPosition>,
    ) -> Result<Self, LeaderWatchError> {
        let api_version = api_version.into();
        let kind = kind.into();
        validate_watch_identity(&api_version, &kind, namespace.as_deref())?;
        if start_resource_version.is_some_and(|resource_version| resource_version < 0) {
            return Err(LeaderWatchError::invalid(
                "watch.start_resource_version",
                "must be non-negative",
            ));
        }
        if let Some(position) = start_watch_replay_position {
            validate_replay_position(position)?;
        }
        if let Some(selector) = field_selector
            .as_deref()
            .filter(|selector| !selector.trim().is_empty())
        {
            klights_types::FieldSelector::parse(selector).map_err(|error| {
                LeaderWatchError::invalid("watch.field_selector", error.to_string())
            })?;
        }
        Ok(Self {
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            start_resource_version,
            start_watch_replay_position,
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

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }

    pub const fn start_resource_version(&self) -> Option<i64> {
        self.start_resource_version
    }

    pub const fn start_watch_replay_position(&self) -> Option<WatchReplayPosition> {
        self.start_watch_replay_position
    }

    pub const fn preferred_replay_position(&self) -> Option<WatchReplayPosition> {
        self.start_watch_replay_position
    }

    pub fn with_resume_cursor(
        mut self,
        cursor: WatchResumeCursor,
    ) -> Result<Self, LeaderWatchError> {
        if let Some(resource_version) = cursor.resource_version {
            if resource_version < 0 {
                return Err(LeaderWatchError::invalid(
                    "watch.start_resource_version",
                    "must be non-negative",
                ));
            }
            self.start_resource_version = Some(resource_version);
        }
        if let Some(position) = cursor.replay_position {
            validate_replay_position(position)?;
        }
        self.start_watch_replay_position = cursor.replay_position;
        Ok(self)
    }
}

/// Transport-neutral event. The canonical resource owns a shared `Arc` JSON
/// body, so adapters and consumers do not deep-clone payloads.
#[derive(Clone, Debug)]
pub struct ResourceEvent {
    event_type: WatchEventType,
    resource: Resource,
    resume_position: Option<WatchReplayPosition>,
}

impl ResourceEvent {
    pub fn try_new(
        event_type: WatchEventType,
        resource: Resource,
        resume_position: Option<WatchReplayPosition>,
    ) -> Result<Self, LeaderWatchError> {
        if resource.api_version.is_empty() || resource.kind.is_empty() {
            return Err(LeaderWatchError::malformed_event(
                "watch event is missing apiVersion or kind",
            ));
        }
        if matches!(
            event_type,
            WatchEventType::Added | WatchEventType::Modified | WatchEventType::Deleted
        ) && resource.name.is_empty()
        {
            return Err(LeaderWatchError::malformed_event(
                "resource watch event is missing metadata.name",
            ));
        }
        if resource.resource_version < 0 {
            return Err(LeaderWatchError::malformed_event(
                "watch event has a negative resourceVersion",
            ));
        }
        if let Some(position) = resume_position {
            validate_replay_position(position).map_err(|error| {
                LeaderWatchError::malformed_event(format!(
                    "watch event resume position is invalid: {error}"
                ))
            })?;
        }
        Ok(Self {
            event_type,
            resource,
            resume_position,
        })
    }

    pub fn try_from_wire_type(
        event_type: &str,
        resource: Resource,
        resume_position: Option<WatchReplayPosition>,
    ) -> Result<Self, LeaderWatchError> {
        let event_type = match event_type {
            "ADDED" => WatchEventType::Added,
            "MODIFIED" => WatchEventType::Modified,
            "DELETED" => WatchEventType::Deleted,
            "BOOKMARK" => WatchEventType::Bookmark,
            "ERROR" => WatchEventType::Error,
            unknown => {
                return Err(LeaderWatchError::UnknownEventType {
                    event_type: unknown.to_string(),
                });
            }
        };
        Self::try_new(event_type, resource, resume_position)
    }

    pub const fn event_type(&self) -> WatchEventType {
        self.event_type
    }

    pub const fn resource(&self) -> &Resource {
        &self.resource
    }

    pub const fn resume_position(&self) -> Option<WatchReplayPosition> {
        self.resume_position
    }

    pub fn into_parts(self) -> (WatchEventType, Resource, Option<WatchReplayPosition>) {
        (self.event_type, self.resource, self.resume_position)
    }

    pub fn validate_for(&self, request: &WatchRequest) -> Result<(), LeaderWatchError> {
        if self.event_type == WatchEventType::Error {
            return Ok(());
        }
        if self.resource.api_version != request.api_version
            || self.resource.kind != request.kind
            || request
                .namespace
                .as_deref()
                .is_some_and(|namespace| self.resource.namespace.as_deref() != Some(namespace))
        {
            return Err(LeaderWatchError::mismatched_event(format!(
                "watch event {}/{} {:?} does not match request {}/{} {:?}",
                self.resource.api_version,
                self.resource.kind,
                self.resource.namespace,
                request.api_version,
                request.kind,
                request.namespace,
            )));
        }
        Ok(())
    }
}

/// Reconnect cursor advanced explicitly only after a consumer has safely
/// applied a delivered event. Durable event ID ordering remains authoritative
/// even when a later-applied event carries a lower public resourceVersion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchResumeCursor {
    resource_version: Option<i64>,
    replay_position: Option<WatchReplayPosition>,
}

impl WatchResumeCursor {
    pub fn try_new(
        resource_version: Option<i64>,
        replay_position: Option<WatchReplayPosition>,
    ) -> Result<Self, LeaderWatchError> {
        if resource_version.is_some_and(|resource_version| resource_version < 0) {
            return Err(LeaderWatchError::invalid(
                "watch.cursor.resource_version",
                "must be non-negative",
            ));
        }
        if let Some(position) = replay_position {
            validate_replay_position(position)?;
        }
        Ok(Self {
            resource_version,
            replay_position,
        })
    }

    pub const fn resource_version(self) -> Option<i64> {
        self.resource_version
    }

    pub const fn replay_position(self) -> Option<WatchReplayPosition> {
        self.replay_position
    }

    pub fn advance_after_apply(&mut self, event: &ResourceEvent) -> Result<(), LeaderWatchError> {
        if let (Some(current), Some(delivered)) = (self.replay_position, event.resume_position)
            && !current.permits_successor(delivered)
        {
            return Err(LeaderWatchError::OutOfOrderEvent {
                current_event_id: current.event_id,
                delivered_event_id: delivered.event_id,
            });
        }
        let delivered_resource_version = event.resource.resource_version;
        let resource_version = (delivered_resource_version > 0)
            .then(|| {
                self.resource_version
                    .unwrap_or_default()
                    .max(delivered_resource_version)
            })
            .or(self.resource_version);
        self.resource_version = resource_version;
        self.replay_position = event.resume_position;
        Ok(())
    }
}

/// Pull-based stream. The caller controls demand and dropping the stream is
/// the cancellation mechanism; implementations must not hide unbounded queues.
pub type WatchStream =
    Pin<Box<dyn Stream<Item = Result<ResourceEvent, LeaderWatchError>> + Send + 'static>>;

/// Heap-erased future used to establish one positioned watch.
pub type LeaderWatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WatchStream, LeaderWatchError>> + Send + 'a>>;

/// One-method, object-safe transport-neutral positioned-watch capability.
pub trait LeaderWatch: Send + Sync {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_>;
}

/// Scope whose informer readiness is required before a consumer reads cached
/// state. Selectors remain part of the scope so readiness cannot be widened
/// accidentally to an unrelated unfiltered informer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheReadinessRequest {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
}

impl CacheReadinessRequest {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        label_selector: Option<String>,
        field_selector: Option<String>,
    ) -> Result<Self, CacheReadinessError> {
        let api_version = api_version.into();
        let kind = kind.into();
        validate_watch_identity(&api_version, &kind, namespace.as_deref()).map_err(|error| {
            CacheReadinessError::InvalidRequest {
                message: error.to_string(),
            }
        })?;
        Ok(Self {
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
        })
    }

    pub fn from_watch_request(request: &WatchRequest) -> Self {
        Self {
            api_version: request.api_version.clone(),
            kind: request.kind.clone(),
            namespace: request.namespace.clone(),
            label_selector: request.label_selector.clone(),
            field_selector: request.field_selector.clone(),
        }
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

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }
}

/// Failure returned while waiting for a particular informer scope.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheReadinessError {
    InvalidRequest { message: String },
    Unavailable { message: String },
    Timeout,
    Cancelled,
}

impl CacheReadinessError {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

impl fmt::Display for CacheReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message } | Self::Unavailable { message } => {
                formatter.write_str(message)
            }
            Self::Timeout => formatter.write_str("leader cache readiness timed out"),
            Self::Cancelled => formatter.write_str("leader cache readiness was cancelled"),
        }
    }
}

impl std::error::Error for CacheReadinessError {}

/// Heap-erased future used by the cache-readiness boundary.
pub type CacheReadinessFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), CacheReadinessError>> + Send + 'a>>;

/// One-method cache-readiness capability, deliberately separate from watch.
pub trait LeaderCacheReadiness: Send + Sync {
    fn wait_cache_ready(&self, request: CacheReadinessRequest) -> CacheReadinessFuture<'_>;
}

/// Failure returned by projected ServiceAccount token issuance. Transport,
/// datastore, signing, and authentication implementations remain private to
/// their adapters; callers receive only stable operation semantics.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedServiceAccountTokenError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Unauthorized,
    ServiceAccountNotFound,
    BoundPodNotFound,
    BoundNodeNotFound,
    BindingMismatch {
        message: String,
    },
    CorruptResource {
        message: String,
    },
    SigningFailed {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Unavailable {
        message: String,
    },
    Transport {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl ProjectedServiceAccountTokenError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn binding_mismatch(message: impl Into<String>) -> Self {
        Self::BindingMismatch {
            message: message.into(),
        }
    }

    pub fn corrupt_resource(message: impl Into<String>) -> Self {
        Self::CorruptResource {
            message: message.into(),
        }
    }

    pub fn signing_failed(message: impl Into<String>) -> Self {
        Self::SigningFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }
}

impl fmt::Display for ProjectedServiceAccountTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => formatter.write_str("projected token target is not raft leader"),
            Self::Unauthorized => formatter
                .write_str("projected token issuance requires the node identity bound to the Pod"),
            Self::ServiceAccountNotFound => {
                formatter.write_str("bound ServiceAccount was not found")
            }
            Self::BoundPodNotFound => formatter.write_str("bound Pod was not found"),
            Self::BoundNodeNotFound => formatter.write_str("bound node was not found"),
            Self::BindingMismatch { message }
            | Self::CorruptResource { message }
            | Self::SigningFailed { message }
            | Self::CorruptResponse { message }
            | Self::Unavailable { message }
            | Self::Transport { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("projected token issuance timed out"),
            Self::Cancelled => formatter.write_str("projected token issuance was cancelled"),
        }
    }
}

impl std::error::Error for ProjectedServiceAccountTokenError {}

fn require_token_field(
    value: &str,
    field: &'static str,
) -> Result<(), ProjectedServiceAccountTokenError> {
    if value.trim().is_empty() {
        Err(ProjectedServiceAccountTokenError::invalid(
            field,
            "must not be empty",
        ))
    } else {
        Ok(())
    }
}

/// Validated, owned request for a kubelet-originated projected token. The Pod
/// name, Pod UID, and node name are mandatory so issuance cannot widen into an
/// unbound northbound TokenRequest operation. The node UID remains optional
/// because the leader resolves and validates the authoritative Node object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedServiceAccountTokenRequest {
    namespace: String,
    service_account_name: String,
    audiences: Vec<String>,
    expiration_seconds: i64,
    bound_pod_name: String,
    bound_pod_uid: String,
    bound_node_name: String,
    bound_node_uid: Option<String>,
}

impl ProjectedServiceAccountTokenRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        namespace: impl Into<String>,
        service_account_name: impl Into<String>,
        audiences: Vec<String>,
        expiration_seconds: i64,
        bound_pod_name: impl Into<String>,
        bound_pod_uid: impl Into<String>,
        bound_node_name: impl Into<String>,
        bound_node_uid: Option<String>,
    ) -> Result<Self, ProjectedServiceAccountTokenError> {
        let namespace = namespace.into();
        let service_account_name = service_account_name.into();
        let bound_pod_name = bound_pod_name.into();
        let bound_pod_uid = bound_pod_uid.into();
        let bound_node_name = bound_node_name.into();
        require_token_field(&namespace, "token.namespace")?;
        require_token_field(&service_account_name, "token.service_account_name")?;
        if audiences.is_empty() {
            return Err(ProjectedServiceAccountTokenError::invalid(
                "token.audiences",
                "must contain at least one audience",
            ));
        }
        for audience in &audiences {
            require_token_field(audience, "token.audiences")?;
        }
        if expiration_seconds <= 0 {
            return Err(ProjectedServiceAccountTokenError::invalid(
                "token.expiration_seconds",
                "must be positive",
            ));
        }
        require_token_field(&bound_pod_name, "token.bound_pod_name")?;
        require_token_field(&bound_pod_uid, "token.bound_pod_uid")?;
        require_token_field(&bound_node_name, "token.bound_node_name")?;
        if let Some(node_uid) = bound_node_uid.as_deref() {
            require_token_field(node_uid, "token.bound_node_uid")?;
        }
        Ok(Self {
            namespace,
            service_account_name,
            audiences,
            expiration_seconds,
            bound_pod_name,
            bound_pod_uid,
            bound_node_name,
            bound_node_uid,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn service_account_name(&self) -> &str {
        &self.service_account_name
    }

    pub fn audiences(&self) -> &[String] {
        &self.audiences
    }

    pub const fn expiration_seconds(&self) -> i64 {
        self.expiration_seconds
    }

    pub fn bound_pod_name(&self) -> &str {
        &self.bound_pod_name
    }

    pub fn bound_pod_uid(&self) -> &str {
        &self.bound_pod_uid
    }

    pub fn bound_node_name(&self) -> &str {
        &self.bound_node_name
    }

    pub fn bound_node_uid(&self) -> Option<&str> {
        self.bound_node_uid.as_deref()
    }

    pub fn into_parts(
        self,
    ) -> (
        String,
        String,
        Vec<String>,
        i64,
        String,
        String,
        String,
        Option<String>,
    ) {
        (
            self.namespace,
            self.service_account_name,
            self.audiences,
            self.expiration_seconds,
            self.bound_pod_name,
            self.bound_pod_uid,
            self.bound_node_name,
            self.bound_node_uid,
        )
    }
}

/// Validated projected token returned to the kubelet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedServiceAccountToken {
    token: String,
}

impl ProjectedServiceAccountToken {
    pub fn try_new(token: impl Into<String>) -> Result<Self, ProjectedServiceAccountTokenError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(ProjectedServiceAccountTokenError::corrupt_response(
                "projected token response is empty",
            ));
        }
        Ok(Self { token })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn into_token(self) -> String {
        self.token
    }
}

/// Heap-erased future used by projected token issuance.
pub type ProjectedServiceAccountTokenFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ProjectedServiceAccountToken, ProjectedServiceAccountTokenError>>
            + Send
            + 'a,
    >,
>;

/// One-method object-safe capability for kubelet projected token issuance.
pub trait LeaderProjectedServiceAccountToken: Send + Sync {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_>;
}

/// Leader-local issuer entered only after a transport adapter has authenticated
/// the caller and constrained it to `request.bound_node_name()`.
///
/// This capability deliberately differs from
/// [`LeaderProjectedServiceAccountToken`], whose callers are kubelets and must
/// still enforce their own node identity. Implementations must resolve the
/// bound Pod and Node from authoritative leader state before signing.
pub trait LeaderAuthenticatedProjectedServiceAccountToken: Send + Sync {
    fn issue_authenticated_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_>;
}

/// Failure returned by the exact cleanup-intent list/ack capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodCleanupIntentError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Unauthorized,
    CorruptIntent {
        message: String,
    },
    Unavailable {
        message: String,
    },
    Transport {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl PodCleanupIntentError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn corrupt_intent(message: impl Into<String>) -> Self {
        Self::CorruptIntent {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }
}

impl fmt::Display for PodCleanupIntentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => formatter.write_str("cleanup-intent target is not raft leader"),
            Self::Unauthorized => {
                formatter.write_str("a node may only access its own Pod cleanup intents")
            }
            Self::CorruptIntent { message }
            | Self::Unavailable { message }
            | Self::Transport { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("cleanup-intent operation timed out"),
            Self::Cancelled => formatter.write_str("cleanup-intent operation was cancelled"),
        }
    }
}

impl std::error::Error for PodCleanupIntentError {}

fn require_cleanup_field(value: &str, field: &'static str) -> Result<(), PodCleanupIntentError> {
    if value.trim().is_empty() {
        Err(PodCleanupIntentError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

/// Validated request for the cleanup intents owned by one exact node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodCleanupIntentListRequest {
    node_name: String,
}

impl PodCleanupIntentListRequest {
    pub fn try_new(node_name: impl Into<String>) -> Result<Self, PodCleanupIntentError> {
        let node_name = node_name.into();
        require_cleanup_field(&node_name, "cleanup.node_name")?;
        Ok(Self { node_name })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn into_node_name(self) -> String {
        self.node_name
    }
}

/// Exact five-part acknowledgement key. No namespace/name-only or node-wide
/// acknowledgement can be represented by this contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodCleanupIntentAckRequest {
    node_name: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
    reason: String,
}

impl PodCleanupIntentAckRequest {
    pub fn try_new(
        node_name: impl Into<String>,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        pod_uid: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Self, PodCleanupIntentError> {
        let node_name = node_name.into();
        let namespace = namespace.into();
        let pod_name = pod_name.into();
        let pod_uid = pod_uid.into();
        let reason = reason.into();
        require_cleanup_field(&node_name, "cleanup.node_name")?;
        require_cleanup_field(&namespace, "cleanup.namespace")?;
        require_cleanup_field(&pod_name, "cleanup.pod_name")?;
        require_cleanup_field(&pod_uid, "cleanup.pod_uid")?;
        require_cleanup_field(&reason, "cleanup.reason")?;
        Ok(Self {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn into_parts(self) -> (String, String, String, String, String) {
        (
            self.node_name,
            self.namespace,
            self.pod_name,
            self.pod_uid,
            self.reason,
        )
    }
}

/// Transport-neutral cleanup intent with a canonical, shared Pod snapshot.
#[derive(Clone, Debug)]
pub struct PodCleanupIntent {
    node_name: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
    reason: String,
    resource_version: i64,
    created_at_ms: i64,
    pod_snapshot: Resource,
}

impl PodCleanupIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        node_name: impl Into<String>,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        pod_uid: impl Into<String>,
        reason: impl Into<String>,
        resource_version: i64,
        created_at_ms: i64,
        pod_snapshot: Resource,
    ) -> Result<Self, PodCleanupIntentError> {
        let node_name = node_name.into();
        let namespace = namespace.into();
        let pod_name = pod_name.into();
        let pod_uid = pod_uid.into();
        let reason = reason.into();
        require_cleanup_field(&node_name, "cleanup.node_name")?;
        require_cleanup_field(&namespace, "cleanup.namespace")?;
        require_cleanup_field(&pod_name, "cleanup.pod_name")?;
        require_cleanup_field(&pod_uid, "cleanup.pod_uid")?;
        require_cleanup_field(&reason, "cleanup.reason")?;
        if resource_version <= 0 {
            return Err(PodCleanupIntentError::corrupt_intent(
                "cleanup intent resourceVersion must be positive",
            ));
        }
        if created_at_ms <= 0 {
            return Err(PodCleanupIntentError::corrupt_intent(
                "cleanup intent creation time must be positive",
            ));
        }

        let canonical = Resource::try_from_data(pod_snapshot.data.clone()).map_err(|error| {
            PodCleanupIntentError::corrupt_intent(format!(
                "cleanup intent Pod snapshot has invalid identity: {error}"
            ))
        })?;
        if canonical.api_version != pod_snapshot.api_version
            || canonical.kind != pod_snapshot.kind
            || canonical.namespace != pod_snapshot.namespace
            || canonical.name != pod_snapshot.name
            || canonical.uid != pod_snapshot.uid
            || canonical.resource_version != pod_snapshot.resource_version
        {
            return Err(PodCleanupIntentError::corrupt_intent(
                "cleanup intent Pod snapshot fields do not match its canonical body",
            ));
        }
        if canonical.api_version != "v1"
            || canonical.kind != "Pod"
            || canonical.namespace.as_deref() != Some(namespace.as_str())
            || canonical.name != pod_name
            || canonical.uid != pod_uid
            || canonical.resource_version <= 0
        {
            return Err(PodCleanupIntentError::corrupt_intent(
                "cleanup intent Pod snapshot identity does not match its row",
            ));
        }
        if canonical
            .data
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str())
            != Some(node_name.as_str())
        {
            return Err(PodCleanupIntentError::corrupt_intent(
                "cleanup intent Pod snapshot is not bound to its owning node",
            ));
        }

        Ok(Self {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
            resource_version,
            created_at_ms,
            pod_snapshot,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub const fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    pub const fn pod_snapshot(&self) -> &Resource {
        &self.pod_snapshot
    }

    pub fn ack_request(&self) -> Result<PodCleanupIntentAckRequest, PodCleanupIntentError> {
        PodCleanupIntentAckRequest::try_new(
            self.node_name.clone(),
            self.namespace.clone(),
            self.pod_name.clone(),
            self.pod_uid.clone(),
            self.reason.clone(),
        )
    }

    pub fn into_parts(self) -> (String, String, String, String, String, i64, i64, Resource) {
        (
            self.node_name,
            self.namespace,
            self.pod_name,
            self.pod_uid,
            self.reason,
            self.resource_version,
            self.created_at_ms,
            self.pod_snapshot,
        )
    }
}

/// Heap-erased future used by cleanup-intent list and acknowledgement.
pub type PodCleanupIntentFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, PodCleanupIntentError>> + Send + 'a>>;

/// Exact UID-bound cleanup-intent capability. Acknowledgement only removes the
/// intent row; it does not expose Pod deletion, slot release, or finalization.
pub trait LeaderPodCleanupIntents: Send + Sync {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>>;

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()>;
}

/// Failure returned by the memory-only Node lease-renewal capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeLeaseRenewalError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Unauthorized {
        message: String,
    },
    Unavailable {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NodeLeaseRenewalError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeLeaseRenewalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => {
                formatter.write_str("node lease renewal requires the current leader")
            }
            Self::Unauthorized { message }
            | Self::Unavailable { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("node lease renewal timed out"),
            Self::Cancelled => formatter.write_str("node lease renewal was cancelled"),
        }
    }
}

impl std::error::Error for NodeLeaseRenewalError {}

/// Validated, owned request for one node to renew only its in-memory lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLeaseRenewalRequest {
    node_name: String,
    renew_time: String,
    lease_duration_seconds: i64,
}

impl NodeLeaseRenewalRequest {
    pub fn try_new(
        node_name: impl Into<String>,
        renew_time: impl Into<String>,
        lease_duration_seconds: i64,
    ) -> Result<Self, NodeLeaseRenewalError> {
        let node_name = node_name.into();
        let renew_time = renew_time.into();
        if node_name.is_empty() {
            return Err(NodeLeaseRenewalError::invalid(
                "lease.node_name",
                "must not be empty",
            ));
        }
        if renew_time.is_empty() {
            return Err(NodeLeaseRenewalError::invalid(
                "lease.renew_time",
                "must not be empty",
            ));
        }
        if lease_duration_seconds <= 0 {
            return Err(NodeLeaseRenewalError::invalid(
                "lease.lease_duration_seconds",
                "must be positive",
            ));
        }
        Ok(Self {
            node_name,
            renew_time,
            lease_duration_seconds,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn renew_time(&self) -> &str {
        &self.renew_time
    }

    pub const fn lease_duration_seconds(&self) -> i64 {
        self.lease_duration_seconds
    }

    pub fn into_parts(self) -> (String, String, i64) {
        (self.node_name, self.renew_time, self.lease_duration_seconds)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeLeaseRenewalResult {
    Renewed,
}

pub type NodeLeaseRenewalFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeLeaseRenewalError>> + Send + 'a>>;

pub trait LeaderNodeLeaseRenewal: Send + Sync {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult>;
}

/// Failure returned while publishing a worker-owned Node status snapshot.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeSelfStatusError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    UnsupportedCommand {
        command: &'static str,
    },
    NotFound,
    UidMismatch,
    Unauthorized {
        message: String,
    },
    EnqueueFailed {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NodeSelfStatusError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized {
            message: message.into(),
        }
    }

    pub fn enqueue_failed(message: impl Into<String>) -> Self {
        Self::EnqueueFailed {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeSelfStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::UnsupportedCommand { command } => {
                write!(
                    formatter,
                    "unsupported worker Node status command: {command}"
                )
            }
            Self::NotFound => formatter.write_str("worker Node was not found"),
            Self::UidMismatch => formatter.write_str("worker Node UID does not match"),
            Self::Unauthorized { message }
            | Self::EnqueueFailed { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("worker Node status submission timed out"),
            Self::Cancelled => formatter.write_str("worker Node status submission was cancelled"),
        }
    }
}

impl std::error::Error for NodeSelfStatusError {}

/// Worker-owned status-only command. The exact Node UID is mandatory while
/// resourceVersion authority is deliberately absent; durable sequencing and
/// retry identity remain owned by the node-local outbox.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeSelfStatusRequest {
    command: StorageCommand,
}

impl NodeSelfStatusRequest {
    pub fn validate_command(command: &StorageCommand) -> Result<(), NodeSelfStatusError> {
        match command {
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => {
                if api_version != "v1" || kind != "Node" || namespace.is_some() {
                    return Err(NodeSelfStatusError::invalid(
                        "status.resource",
                        "must be the cluster-scoped v1/Node status subresource",
                    ));
                }
                if name.is_empty() {
                    return Err(NodeSelfStatusError::invalid(
                        "status.node_name",
                        "must not be empty",
                    ));
                }
                if !status.is_object() {
                    return Err(NodeSelfStatusError::invalid(
                        "status.value",
                        "must be an object",
                    ));
                }
                if expected_rv.is_some() || preconditions.resource_version.is_some() {
                    return Err(NodeSelfStatusError::invalid(
                        "status.resource_version",
                        "worker self-status must not carry resourceVersion authority",
                    ));
                }
                if preconditions.uid.as_deref().is_none_or(str::is_empty) {
                    return Err(NodeSelfStatusError::invalid(
                        "status.node_uid",
                        "an exact Node UID is required",
                    ));
                }
                if observed_status_stamp.is_some() {
                    return Err(NodeSelfStatusError::invalid(
                        "status.observed_status_stamp",
                        "is reserved for Pod status snapshots",
                    ));
                }
            }
            other => {
                return Err(NodeSelfStatusError::UnsupportedCommand {
                    command: other.variant_name(),
                });
            }
        }
        Ok(())
    }

    pub fn try_new(command: StorageCommand) -> Result<Self, NodeSelfStatusError> {
        Self::validate_command(&command)?;
        Ok(Self { command })
    }

    pub fn node_name(&self) -> &str {
        match &self.command {
            StorageCommand::UpdateStatus { name, .. } => name,
            _ => unreachable!("NodeSelfStatusRequest constructor enforces UpdateStatus"),
        }
    }

    pub fn node_uid(&self) -> &str {
        match &self.command {
            StorageCommand::UpdateStatus { preconditions, .. } => preconditions
                .uid
                .as_deref()
                .expect("NodeSelfStatusRequest constructor enforces UID"),
            _ => unreachable!("NodeSelfStatusRequest constructor enforces UpdateStatus"),
        }
    }

    pub const fn command(&self) -> &StorageCommand {
        &self.command
    }

    pub fn into_command(self) -> StorageCommand {
        self.command
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeSelfStatusResult {
    Enqueued,
}

pub type NodeSelfStatusFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeSelfStatusError>> + Send + 'a>>;

pub trait LeaderNodeSelfStatus: Send + Sync {
    fn submit_node_self_status(
        &self,
        request: NodeSelfStatusRequest,
    ) -> NodeSelfStatusFuture<'_, NodeSelfStatusResult>;
}

/// Failure returned by the leader-local Node lifecycle status CAS.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeLifecycleStatusError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    UnsupportedCommand {
        command: &'static str,
    },
    NotLeader,
    NotFound,
    UidMismatch,
    Conflict {
        message: String,
    },
    ApplyFailed {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NodeLifecycleStatusError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn apply_failed(message: impl Into<String>) -> Self {
        Self::ApplyFailed {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeLifecycleStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::UnsupportedCommand { command } => {
                write!(formatter, "unsupported Node lifecycle command: {command}")
            }
            Self::NotLeader => {
                formatter.write_str("Node lifecycle status requires the current leader")
            }
            Self::NotFound => formatter.write_str("Node was not found"),
            Self::UidMismatch => formatter.write_str("Node UID does not match"),
            Self::Conflict { message }
            | Self::ApplyFailed { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("Node lifecycle status update timed out"),
            Self::Cancelled => formatter.write_str("Node lifecycle status update was cancelled"),
        }
    }
}

impl std::error::Error for NodeLifecycleStatusError {}

/// Leader-local status-only CAS for the Node lifecycle controller. Unlike a
/// worker self-status request, both the exact UID and a positive current
/// resourceVersion are mandatory and must identify the same observed object.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeLifecycleStatusRequest {
    command: StorageCommand,
}

impl NodeLifecycleStatusRequest {
    pub fn try_new(command: StorageCommand) -> Result<Self, NodeLifecycleStatusError> {
        match &command {
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                expected_rv,
                preconditions,
                observed_status_stamp,
            } => {
                if api_version != "v1" || kind != "Node" || namespace.is_some() {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.resource",
                        "must be the cluster-scoped v1/Node status subresource",
                    ));
                }
                if name.is_empty() {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.node_name",
                        "must not be empty",
                    ));
                }
                if !status.is_object() {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.value",
                        "must be an object",
                    ));
                }
                if preconditions.uid.as_deref().is_none_or(str::is_empty) {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.node_uid",
                        "an exact Node UID is required",
                    ));
                }
                let Some(expected_rv) = *expected_rv else {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.resource_version",
                        "a positive resourceVersion CAS is required",
                    ));
                };
                if expected_rv <= 0 || preconditions.resource_version != Some(expected_rv) {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.resource_version",
                        "expected and precondition resourceVersions must match and be positive",
                    ));
                }
                if observed_status_stamp.is_some() {
                    return Err(NodeLifecycleStatusError::invalid(
                        "status.observed_status_stamp",
                        "is reserved for Pod status snapshots",
                    ));
                }
            }
            other => {
                return Err(NodeLifecycleStatusError::UnsupportedCommand {
                    command: other.variant_name(),
                });
            }
        }
        Ok(Self { command })
    }

    pub fn node_name(&self) -> &str {
        match &self.command {
            StorageCommand::UpdateStatus { name, .. } => name,
            _ => unreachable!("NodeLifecycleStatusRequest constructor enforces UpdateStatus"),
        }
    }

    pub fn node_uid(&self) -> &str {
        match &self.command {
            StorageCommand::UpdateStatus { preconditions, .. } => preconditions
                .uid
                .as_deref()
                .expect("NodeLifecycleStatusRequest constructor enforces UID"),
            _ => unreachable!("NodeLifecycleStatusRequest constructor enforces UpdateStatus"),
        }
    }

    pub fn resource_version(&self) -> i64 {
        match &self.command {
            StorageCommand::UpdateStatus {
                expected_rv: Some(resource_version),
                ..
            } => *resource_version,
            _ => unreachable!("NodeLifecycleStatusRequest constructor enforces resourceVersion"),
        }
    }

    pub const fn command(&self) -> &StorageCommand {
        &self.command
    }

    pub fn into_command(self) -> StorageCommand {
        self.command
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeLifecycleStatusResult {
    Updated { resource_version: i64 },
}

pub type NodeLifecycleStatusFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeLifecycleStatusError>> + Send + 'a>>;

pub trait LeaderNodeLifecycleStatus: Send + Sync {
    fn submit_node_lifecycle_status(
        &self,
        request: NodeLifecycleStatusRequest,
    ) -> NodeLifecycleStatusFuture<'_, NodeLifecycleStatusResult>;
}

/// Failure returned by the self-authorized node-subnet allocation capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeSubnetAllocationError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Unauthorized {
        message: String,
    },
    Conflict {
        message: String,
    },
    Exhausted {
        cluster_cidr: String,
    },
    AllocationFailed {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NodeSubnetAllocationError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::invalid(field, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized {
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn exhausted(cluster_cidr: impl Into<String>) -> Self {
        Self::Exhausted {
            cluster_cidr: cluster_cidr.into(),
        }
    }

    pub fn allocation_failed(message: impl Into<String>) -> Self {
        Self::AllocationFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeSubnetAllocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => {
                formatter.write_str("node subnet allocation requires the current leader")
            }
            Self::Unauthorized { message }
            | Self::Conflict { message }
            | Self::AllocationFailed { message }
            | Self::CorruptResponse { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Exhausted { cluster_cidr } => {
                write!(formatter, "node subnet allocation exhausted {cluster_cidr}")
            }
            Self::Timeout => formatter.write_str("node subnet allocation timed out"),
            Self::Cancelled => formatter.write_str("node subnet allocation was cancelled"),
        }
    }
}

impl std::error::Error for NodeSubnetAllocationError {}

/// Failure returned by a leader-fresh network-topology query.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkTopologyError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Unauthorized {
        message: String,
    },
    QueryFailed {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NetworkTopologyError {
    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::invalid(field, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized {
            message: message.into(),
        }
    }

    pub fn query_failed(message: impl Into<String>) -> Self {
        Self::QueryFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }
}

impl fmt::Display for NetworkTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => {
                formatter.write_str("network topology query requires the current leader")
            }
            Self::Unauthorized { message }
            | Self::QueryFailed { message }
            | Self::CorruptResponse { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("network topology query timed out"),
            Self::Cancelled => formatter.write_str("network topology query was cancelled"),
        }
    }
}

impl std::error::Error for NetworkTopologyError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetworkNodeMode {
    Root,
    Rootless,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DataplaneEncryption {
    WireGuard,
    Direct,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HostPortRange {
    start: u16,
    end: u16,
}

impl HostPortRange {
    pub fn try_new(start: u16, end: u16) -> Result<Self, NetworkTopologyError> {
        if start == 0 || end == 0 || start > end {
            return Err(NetworkTopologyError::corrupt_response(
                "host-port range must be non-zero and ordered",
            ));
        }
        Ok(Self { start, end })
    }

    pub const fn start(self) -> u16 {
        self.start
    }

    pub const fn end(self) -> u16 {
        self.end
    }
}

impl fmt::Display for HostPortRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.start, self.end)
    }
}

/// Canonical leader-owned projection of one allocated `/24` pod subnet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSubnet {
    node_name: String,
    subnet: String,
    subnet_base_int: u32,
    gateway_ip: Ipv4Addr,
    node_ip: Ipv4Addr,
    mode: NetworkNodeMode,
    hostport_range: Option<HostPortRange>,
}

impl NodeSubnet {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        node_name: impl Into<String>,
        subnet: impl Into<String>,
        subnet_base_int: u32,
        gateway_ip: Ipv4Addr,
        node_ip: Ipv4Addr,
        mode: NetworkNodeMode,
        hostport_range: Option<HostPortRange>,
    ) -> Result<Self, NetworkTopologyError> {
        let node_name = node_name.into();
        validate_network_node_name(&node_name).map_err(|message| {
            NetworkTopologyError::corrupt_response(format!("invalid node identity: {message}"))
        })?;
        let subnet = subnet.into();
        let (base, prefix) = parse_canonical_ipv4_cidr(&subnet).map_err(|message| {
            NetworkTopologyError::corrupt_response(format!("invalid node subnet: {message}"))
        })?;
        if prefix != 24 {
            return Err(NetworkTopologyError::corrupt_response(
                "node subnet must use a /24 prefix",
            ));
        }
        if subnet_base_int != u32::from(base) {
            return Err(NetworkTopologyError::corrupt_response(
                "node subnet base integer does not match its CIDR",
            ));
        }
        if gateway_ip != base {
            return Err(NetworkTopologyError::corrupt_response(
                "node subnet gateway compatibility field must match the network base",
            ));
        }
        match (mode, hostport_range) {
            (NetworkNodeMode::Root, Some(_)) => {
                return Err(NetworkTopologyError::corrupt_response(
                    "root node subnet must not carry a host-port range",
                ));
            }
            (NetworkNodeMode::Rootless, None) => {
                return Err(NetworkTopologyError::corrupt_response(
                    "rootless node subnet requires a host-port range",
                ));
            }
            _ => {}
        }
        Ok(Self {
            node_name,
            subnet,
            subnet_base_int,
            gateway_ip,
            node_ip,
            mode,
            hostport_range,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn subnet(&self) -> &str {
        &self.subnet
    }

    pub const fn subnet_base_int(&self) -> u32 {
        self.subnet_base_int
    }

    pub const fn gateway_ip(&self) -> Ipv4Addr {
        self.gateway_ip
    }

    pub const fn node_ip(&self) -> Ipv4Addr {
        self.node_ip
    }

    pub const fn mode(&self) -> NetworkNodeMode {
        self.mode
    }

    pub const fn hostport_range(&self) -> Option<HostPortRange> {
        self.hostport_range
    }
}

/// Canonical leader-owned projection of one node's route-selection metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkDataplane {
    node_name: String,
    mode: NetworkNodeMode,
    encryption: DataplaneEncryption,
    public_key: Option<String>,
    endpoint: IpAddr,
    port: Option<u16>,
}

impl NetworkDataplane {
    pub fn try_new(
        node_name: impl Into<String>,
        mode: NetworkNodeMode,
        encryption: DataplaneEncryption,
        public_key: Option<&str>,
        endpoint: IpAddr,
        port: Option<u16>,
    ) -> Result<Self, NetworkTopologyError> {
        let node_name = node_name.into();
        validate_network_node_name(&node_name).map_err(|message| {
            NetworkTopologyError::corrupt_response(format!(
                "invalid dataplane node identity: {message}"
            ))
        })?;
        let public_key = public_key.map(str::to_owned);
        match encryption {
            DataplaneEncryption::WireGuard => {
                let key = public_key.as_deref().ok_or_else(|| {
                    NetworkTopologyError::corrupt_response(
                        "encrypted dataplane metadata requires a WireGuard public key",
                    )
                })?;
                validate_wireguard_public_key(key)?;
                if port.is_none_or(|value| value == 0) {
                    return Err(NetworkTopologyError::corrupt_response(
                        "encrypted dataplane metadata requires a non-zero WireGuard port",
                    ));
                }
            }
            DataplaneEncryption::Direct => {
                if public_key.is_some() || port.is_some() {
                    return Err(NetworkTopologyError::corrupt_response(
                        "direct-route metadata must not carry WireGuard key or port fields",
                    ));
                }
            }
        }
        Ok(Self {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub const fn mode(&self) -> NetworkNodeMode {
        self.mode
    }

    pub const fn encryption(&self) -> DataplaneEncryption {
        self.encryption
    }

    pub fn public_key(&self) -> Option<&str> {
        self.public_key.as_deref()
    }

    pub const fn endpoint(&self) -> IpAddr {
        self.endpoint
    }

    pub const fn port(&self) -> Option<u16> {
        self.port
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSubnetAllocationRequest {
    node_name: String,
    cluster_cidr: String,
    node_ip: Ipv4Addr,
}

impl NodeSubnetAllocationRequest {
    pub fn try_new(
        node_name: impl Into<String>,
        cluster_cidr: impl Into<String>,
        node_ip: &str,
    ) -> Result<Self, NodeSubnetAllocationError> {
        let node_name = node_name.into();
        validate_network_node_name(&node_name)
            .map_err(|message| NodeSubnetAllocationError::invalid("node_name", message))?;
        let cluster_cidr = cluster_cidr.into();
        let (_, prefix) = parse_canonical_ipv4_cidr(&cluster_cidr)
            .map_err(|message| NodeSubnetAllocationError::invalid("cluster_cidr", message))?;
        if prefix > 24 {
            return Err(NodeSubnetAllocationError::invalid(
                "cluster_cidr",
                "must contain at least one allocatable /24",
            ));
        }
        let raw_node_ip = node_ip;
        let node_ip = raw_node_ip.parse::<Ipv4Addr>().map_err(|error| {
            NodeSubnetAllocationError::invalid(
                "node_ip",
                format!("must be canonical IPv4: {error}"),
            )
        })?;
        if raw_node_ip != node_ip.to_string() {
            return Err(NodeSubnetAllocationError::invalid(
                "node_ip",
                "must use canonical IPv4 text",
            ));
        }
        Ok(Self {
            node_name,
            cluster_cidr,
            node_ip,
        })
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn cluster_cidr(&self) -> &str {
        &self.cluster_cidr
    }

    pub const fn node_ip(&self) -> Ipv4Addr {
        self.node_ip
    }

    pub fn into_parts(self) -> (String, String, Ipv4Addr) {
        (self.node_name, self.cluster_cidr, self.node_ip)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeSubnetAllocationResult {
    Allocated(NodeSubnet),
}

impl NodeSubnetAllocationResult {
    pub fn try_from_wire(
        expected_node_name: &str,
        subnet: Option<NodeSubnet>,
    ) -> Result<Self, NodeSubnetAllocationError> {
        let subnet = subnet.ok_or_else(|| {
            NodeSubnetAllocationError::corrupt_response(
                "node subnet allocation response is missing its subnet payload",
            )
        })?;
        if subnet.node_name() != expected_node_name {
            return Err(NodeSubnetAllocationError::corrupt_response(format!(
                "node subnet allocation returned node {} for requested node {expected_node_name}",
                subnet.node_name()
            )));
        }
        Ok(Self::Allocated(subnet))
    }

    pub fn into_subnet(self) -> NodeSubnet {
        match self {
            Self::Allocated(subnet) => subnet,
        }
    }
}

macro_rules! node_query {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            node_name: String,
        }

        impl $name {
            pub fn try_new(node_name: impl Into<String>) -> Result<Self, NetworkTopologyError> {
                let node_name = node_name.into();
                validate_network_node_name(&node_name)
                    .map_err(|message| NetworkTopologyError::invalid("node_name", message))?;
                Ok(Self { node_name })
            }

            pub fn node_name(&self) -> &str {
                &self.node_name
            }

            pub fn into_node_name(self) -> String {
                self.node_name
            }
        }
    };
}

node_query!(NodeSubnetQuery);
node_query!(PeerSubnetsQuery);
node_query!(NodeDataplaneQuery);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSubnetResult(Option<NodeSubnet>);

impl NodeSubnetResult {
    pub fn try_from_wire(
        expected_node_name: &str,
        found: bool,
        subnet: Option<NodeSubnet>,
    ) -> Result<Self, NetworkTopologyError> {
        validate_found_payload("node subnet", found, subnet.is_some())?;
        if let Some(subnet) = subnet.as_ref()
            && subnet.node_name() != expected_node_name
        {
            return Err(NetworkTopologyError::corrupt_response(format!(
                "node subnet response returned node {} for requested node {expected_node_name}",
                subnet.node_name()
            )));
        }
        Ok(Self(subnet))
    }

    pub fn as_ref(&self) -> Option<&NodeSubnet> {
        self.0.as_ref()
    }

    pub fn into_option(self) -> Option<NodeSubnet> {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSubnetsResult(Vec<NodeSubnet>);

impl PeerSubnetsResult {
    pub fn try_new(
        requesting_node_name: &str,
        subnets: Vec<NodeSubnet>,
    ) -> Result<Self, NetworkTopologyError> {
        let mut node_names = std::collections::HashSet::with_capacity(subnets.len());
        let mut bases = std::collections::HashSet::with_capacity(subnets.len());
        for subnet in &subnets {
            if subnet.node_name() == requesting_node_name {
                return Err(NetworkTopologyError::corrupt_response(
                    "peer subnet response includes the requesting node",
                ));
            }
            if !node_names.insert(subnet.node_name()) {
                return Err(NetworkTopologyError::corrupt_response(format!(
                    "peer subnet response repeats node {}",
                    subnet.node_name()
                )));
            }
            if !bases.insert(subnet.subnet_base_int()) {
                return Err(NetworkTopologyError::corrupt_response(format!(
                    "peer subnet response contains overlapping subnet {}",
                    subnet.subnet()
                )));
            }
        }
        Ok(Self(subnets))
    }

    pub fn as_slice(&self) -> &[NodeSubnet] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<NodeSubnet> {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeDataplaneResult(Option<NetworkDataplane>);

impl NodeDataplaneResult {
    pub fn try_from_wire(
        expected_node_name: &str,
        found: bool,
        metadata: Option<NetworkDataplane>,
    ) -> Result<Self, NetworkTopologyError> {
        validate_found_payload("node dataplane", found, metadata.is_some())?;
        if let Some(metadata) = metadata.as_ref()
            && metadata.node_name() != expected_node_name
        {
            return Err(NetworkTopologyError::corrupt_response(format!(
                "node dataplane response returned node {} for requested node {expected_node_name}",
                metadata.node_name()
            )));
        }
        Ok(Self(metadata))
    }

    pub fn as_ref(&self) -> Option<&NetworkDataplane> {
        self.0.as_ref()
    }

    pub fn into_option(self) -> Option<NetworkDataplane> {
        self.0
    }
}

pub type NodeSubnetAllocationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeSubnetAllocationError>> + Send + 'a>>;

pub trait LeaderNodeSubnetAllocation: Send + Sync {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult>;
}

pub type NetworkTopologyFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkTopologyError>> + Send + 'a>>;

pub trait LeaderNetworkTopologyQuery: Send + Sync {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult>;

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult>;

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult>;
}

/// Focused leader-owned mutation for a node's validated dataplane registration.
/// The adapter is responsible for persisting the record and projecting any
/// derived routing metadata onto the Node object atomically with normal leader
/// command semantics.
pub trait LeaderNetworkTopologyCommand: Send + Sync {
    fn register_node_dataplane(&self, metadata: NetworkDataplane) -> NetworkTopologyFuture<'_, ()>;
}

fn validate_found_payload(
    kind: &'static str,
    found: bool,
    payload_present: bool,
) -> Result<(), NetworkTopologyError> {
    if found != payload_present {
        return Err(NetworkTopologyError::corrupt_response(format!(
            "{kind} found flag and payload presence disagree"
        )));
    }
    Ok(())
}

fn validate_network_node_name(node_name: &str) -> Result<(), String> {
    if node_name.is_empty() || node_name.len() > 253 || node_name.trim() != node_name {
        return Err("must be a non-empty canonical DNS-1123 name of at most 253 bytes".to_string());
    }
    for label in node_name.split('.') {
        if label.is_empty()
            || label.len() > 63
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || !label
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            || !label
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err("must be a canonical lowercase DNS-1123 subdomain".to_string());
        }
    }
    Ok(())
}

fn parse_canonical_ipv4_cidr(raw: &str) -> Result<(Ipv4Addr, u8), String> {
    let (address, prefix) = raw
        .split_once('/')
        .ok_or_else(|| "must be an IPv4 CIDR".to_string())?;
    if raw.trim() != raw || prefix.contains('/') {
        return Err("must be a canonical IPv4 CIDR".to_string());
    }
    let address = address
        .parse::<Ipv4Addr>()
        .map_err(|error| format!("invalid IPv4 address: {error}"))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|error| format!("invalid IPv4 prefix: {error}"))?;
    if prefix > 32 {
        return Err("IPv4 prefix must be at most 32".to_string());
    }
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    };
    if u32::from(address) & mask != u32::from(address) || raw != format!("{address}/{prefix}") {
        return Err("must use the canonical network address and prefix".to_string());
    }
    Ok((address, prefix))
}

fn validate_wireguard_public_key(key: &str) -> Result<(), NetworkTopologyError> {
    let bytes = key.as_bytes();
    let value = |byte: u8| match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    if bytes.len() != 44
        || bytes[43] != b'='
        || bytes[..43].iter().any(|byte| value(*byte).is_none())
        || value(bytes[42]).is_none_or(|last| last % 4 != 0)
    {
        return Err(NetworkTopologyError::corrupt_response(
            "WireGuard public key must be canonical base64 encoding exactly 32 bytes",
        ));
    }
    Ok(())
}

fn typed_get_request(
    kind: &'static str,
    namespace: Option<&str>,
    name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceGetRequest, ResourceQueryError> {
    ResourceGetRequest::try_new(
        ResourceKey::new("v1", kind, namespace.map(str::to_owned), name),
        consistency,
    )
}

pub fn pod_get_request(
    namespace: &str,
    name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceGetRequest, ResourceQueryError> {
    typed_get_request("Pod", Some(namespace), name, consistency)
}

pub fn config_map_get_request(
    namespace: &str,
    name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceGetRequest, ResourceQueryError> {
    typed_get_request("ConfigMap", Some(namespace), name, consistency)
}

pub fn secret_get_request(
    namespace: &str,
    name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceGetRequest, ResourceQueryError> {
    typed_get_request("Secret", Some(namespace), name, consistency)
}

pub fn node_get_request(
    name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceGetRequest, ResourceQueryError> {
    typed_get_request("Node", None, name, consistency)
}

pub fn pods_on_node_list_request(
    node_name: &str,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceListRequest, ResourceQueryError> {
    require_nonempty(node_name, "node_name")?;
    ResourceListRequest::try_new(
        "v1",
        "Pod",
        None,
        None,
        Some(format!("spec.nodeName={node_name}")),
        None,
        None,
        consistency,
    )
}

/// A worker-originated operation that may be delivered through the durable
/// leader outbox boundary.
///
/// This is intentionally narrower than the node-local queue's operation
/// classification. In particular, lease renewal has its own authenticated
/// capability and cannot be smuggled through durable command delivery.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxDeliveryOperation {
    PodStatus,
    RuntimeReconcile,
    ProbeReadiness,
    DeadlineExceeded,
    ContainerStatusSnapshot,
    EphemeralContainerStatuses,
    PodMetadata,
    NodeRegistration,
    NodeDataplane,
    NodeStatus,
    EventCreate,
}

impl OutboxDeliveryOperation {
    pub const ALL: [Self; 11] = [
        Self::PodStatus,
        Self::RuntimeReconcile,
        Self::ProbeReadiness,
        Self::DeadlineExceeded,
        Self::ContainerStatusSnapshot,
        Self::EphemeralContainerStatuses,
        Self::PodMetadata,
        Self::NodeRegistration,
        Self::NodeDataplane,
        Self::NodeStatus,
        Self::EventCreate,
    ];

    pub const fn as_wire_name(self) -> &'static str {
        match self {
            Self::PodStatus => "PodStatus",
            Self::RuntimeReconcile => "RuntimeReconcile",
            Self::ProbeReadiness => "ProbeReadiness",
            Self::DeadlineExceeded => "DeadlineExceeded",
            Self::ContainerStatusSnapshot => "ContainerStatusSnapshot",
            Self::EphemeralContainerStatuses => "EphemeralContainerStatuses",
            Self::PodMetadata => "PodMetadata",
            Self::NodeRegistration => "NodeRegistration",
            Self::NodeDataplane => "NodeDataplane",
            Self::NodeStatus => "NodeStatus",
            Self::EventCreate => "EventCreate",
        }
    }

    pub fn try_from_wire_name(value: &str) -> Result<Self, OutboxDeliveryError> {
        match value {
            "PodStatus" => Ok(Self::PodStatus),
            "RuntimeReconcile" => Ok(Self::RuntimeReconcile),
            "ProbeReadiness" => Ok(Self::ProbeReadiness),
            "DeadlineExceeded" => Ok(Self::DeadlineExceeded),
            "ContainerStatusSnapshot" => Ok(Self::ContainerStatusSnapshot),
            "EphemeralContainerStatuses" => Ok(Self::EphemeralContainerStatuses),
            "PodMetadata" => Ok(Self::PodMetadata),
            "NodeRegistration" => Ok(Self::NodeRegistration),
            "NodeDataplane" => Ok(Self::NodeDataplane),
            "NodeStatus" => Ok(Self::NodeStatus),
            "EventCreate" => Ok(Self::EventCreate),
            _ => Err(OutboxDeliveryError::invalid(
                "delivery.operation",
                format!("unsupported durable delivery operation {value:?}"),
            )),
        }
    }
}

/// Validated transport-neutral durable-delivery request.
///
/// The authenticated author is deliberately absent. Local and remote adapters
/// bind it from their configured or authenticated node identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxDeliveryRequest {
    idempotency_key: String,
    operation: OutboxDeliveryOperation,
    payload: Arc<[u8]>,
    client_id: String,
    stream_id: i64,
    stream_sequence: i64,
}

impl OutboxDeliveryRequest {
    pub fn try_new(
        idempotency_key: impl Into<String>,
        operation: OutboxDeliveryOperation,
        payload: Arc<[u8]>,
        client_id: impl Into<String>,
        stream_id: i64,
        stream_sequence: i64,
    ) -> Result<Self, OutboxDeliveryError> {
        let idempotency_key = idempotency_key.into();
        require_delivery_nonempty(&idempotency_key, "delivery.idempotency_key")?;
        if payload.is_empty() {
            return Err(OutboxDeliveryError::invalid(
                "delivery.payload",
                "must not be empty",
            ));
        }
        let client_id = client_id.into();
        require_delivery_nonempty(&client_id, "delivery.client_id")?;
        if stream_id <= 0 {
            return Err(OutboxDeliveryError::invalid(
                "delivery.stream_id",
                "must be positive",
            ));
        }
        if stream_sequence <= 0 {
            return Err(OutboxDeliveryError::invalid(
                "delivery.stream_sequence",
                "must be positive",
            ));
        }
        Ok(Self {
            idempotency_key,
            operation,
            payload,
            client_id,
            stream_id,
            stream_sequence,
        })
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub const fn operation(&self) -> OutboxDeliveryOperation {
        self.operation
    }

    pub const fn payload(&self) -> &Arc<[u8]> {
        &self.payload
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub const fn stream_id(&self) -> i64 {
        self.stream_id
    }

    pub const fn stream_sequence(&self) -> i64 {
        self.stream_sequence
    }

    pub fn into_parts(self) -> (String, OutboxDeliveryOperation, Arc<[u8]>, String, i64, i64) {
        (
            self.idempotency_key,
            self.operation,
            self.payload,
            self.client_id,
            self.stream_id,
            self.stream_sequence,
        )
    }
}

/// Result of one durable leader-delivery decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxDeliveryResult {
    Applied { applied_rv: i64 },
    AlreadyApplied { applied_rv: Option<i64> },
}

impl OutboxDeliveryResult {
    pub fn try_applied(resource_version: i64) -> Result<Self, OutboxDeliveryError> {
        validate_delivery_resource_version(resource_version)?;
        Ok(Self::Applied {
            applied_rv: resource_version,
        })
    }

    pub fn try_already_applied(resource_version: Option<i64>) -> Result<Self, OutboxDeliveryError> {
        if let Some(resource_version) = resource_version {
            validate_delivery_resource_version(resource_version)?;
        }
        Ok(Self::AlreadyApplied {
            applied_rv: resource_version,
        })
    }

    pub const fn already_applied(&self) -> bool {
        matches!(self, Self::AlreadyApplied { .. })
    }

    pub const fn resource_version(&self) -> Option<i64> {
        match self {
            Self::Applied { applied_rv } => Some(*applied_rv),
            Self::AlreadyApplied { applied_rv } => *applied_rv,
        }
    }
}

/// Failure returned by durable leader delivery.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxDeliveryError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotLeader,
    Retryable(String),
    Timeout,
    Cancelled,
    NotFound(String),
    UidMismatch {
        expected: String,
        actual: String,
    },
    ConflictTerminal(String),
    CorruptResponse {
        message: String,
    },
}

impl OutboxDeliveryError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub const fn not_leader() -> Self {
        Self::NotLeader
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Retryable(message.into())
    }

    pub const fn timeout() -> Self {
        Self::Timeout
    }

    pub const fn cancelled() -> Self {
        Self::Cancelled
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn uid_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::UidMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::ConflictTerminal(message.into())
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NotLeader
                | Self::Retryable(_)
                | Self::Timeout
                | Self::Cancelled
                | Self::CorruptResponse { .. }
        )
    }

    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::InvalidRequest { .. }
                | Self::NotFound(_)
                | Self::UidMismatch { .. }
                | Self::ConflictTerminal(_)
        )
    }
}

impl fmt::Display for OutboxDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotLeader => formatter.write_str("durable delivery requires the current leader"),
            Self::Retryable(message)
            | Self::NotFound(message)
            | Self::ConflictTerminal(message)
            | Self::CorruptResponse { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("durable delivery timed out"),
            Self::Cancelled => formatter.write_str("durable delivery was cancelled"),
            Self::UidMismatch { expected, actual } => {
                write!(
                    formatter,
                    "delivery UID mismatch: expected {expected}, actual {actual}"
                )
            }
        }
    }
}

impl std::error::Error for OutboxDeliveryError {}

pub type OutboxDeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<OutboxDeliveryResult, OutboxDeliveryError>> + Send + 'a>>;

pub trait LeaderOutboxDelivery: Send + Sync {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedOutboxDeliveryRequest {
    authenticated_node: String,
    delivery: OutboxDeliveryRequest,
}

impl AuthenticatedOutboxDeliveryRequest {
    pub fn try_new(
        authenticated_node: impl Into<String>,
        delivery: OutboxDeliveryRequest,
    ) -> Result<Self, OutboxDeliveryError> {
        let authenticated_node = authenticated_node.into();
        require_delivery_nonempty(&authenticated_node, "delivery.authenticated_node")?;
        Ok(Self {
            authenticated_node,
            delivery,
        })
    }

    pub fn into_parts(self) -> (String, OutboxDeliveryRequest) {
        (self.authenticated_node, self.delivery)
    }
}

pub trait LeaderAuthenticatedOutboxDelivery: Send + Sync {
    fn deliver_authenticated_outbox(
        &self,
        request: AuthenticatedOutboxDeliveryRequest,
    ) -> OutboxDeliveryFuture<'_>;
}

/// Bootstrap identity class admitted by the leader.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BootstrapTokenScope {
    Worker,
    Controlplane,
}

/// Exact token and admission scope presented to the leader validator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapTokenValidationRequest {
    token: String,
    scope: BootstrapTokenScope,
}

impl BootstrapTokenValidationRequest {
    pub fn try_new(
        token: impl Into<String>,
        scope: BootstrapTokenScope,
    ) -> Result<Self, BootstrapTokenValidationError> {
        let token = token.into();
        if token.is_empty() {
            return Err(BootstrapTokenValidationError::invalid(
                "bootstrap_token.token",
                "must not be empty",
            ));
        }
        Ok(Self { token, scope })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub const fn scope(&self) -> BootstrapTokenScope {
        self.scope
    }

    pub fn into_parts(self) -> (String, BootstrapTokenScope) {
        (self.token, self.scope)
    }
}

/// Validation failure returned without exposing datastore or Secret types.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapTokenValidationError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Rejected(String),
}

impl BootstrapTokenValidationError {
    pub fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }
}

impl fmt::Display for BootstrapTokenValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Rejected(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for BootstrapTokenValidationError {}

pub type BootstrapTokenValidationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), BootstrapTokenValidationError>> + Send + 'a>>;

pub trait BootstrapTokenValidation: Send + Sync {
    fn validate_bootstrap_token(
        &self,
        request: BootstrapTokenValidationRequest,
    ) -> BootstrapTokenValidationFuture<'_>;
}

fn require_delivery_nonempty(value: &str, field: &'static str) -> Result<(), OutboxDeliveryError> {
    if value.is_empty() {
        Err(OutboxDeliveryError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_delivery_resource_version(resource_version: i64) -> Result<(), OutboxDeliveryError> {
    if resource_version <= 0 {
        Err(OutboxDeliveryError::corrupt_response(
            "delivery resource version must be positive when present",
        ))
    } else {
        Ok(())
    }
}
