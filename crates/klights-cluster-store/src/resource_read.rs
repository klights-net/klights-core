//! Read-only persistent Kubernetes resource queries.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_types::ResourceKey;

/// Failure returned by the persistent resource read capability.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceReadError {
    InvalidLimit {
        limit: i64,
    },
    InvalidSelector {
        message: String,
    },
    InvalidContinuation {
        message: String,
    },
    Expired {
        requested: i64,
        oldest_available: i64,
    },
    Conflict {
        message: String,
    },
    UnsupportedMode {
        message: String,
    },
    CorruptData {
        message: String,
    },
    Retryable {
        message: String,
    },
    Timeout,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceReadStatus {
    pub code: u16,
    pub reason: &'static str,
    pub retryable: bool,
}

impl ResourceReadError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }

    fn corrupt(message: impl Into<String>) -> Self {
        Self::CorruptData {
            message: message.into(),
        }
    }

    pub const fn status(&self) -> ResourceReadStatus {
        match self {
            Self::InvalidLimit { .. }
            | Self::InvalidSelector { .. }
            | Self::InvalidContinuation { .. } => ResourceReadStatus {
                code: 400,
                reason: "BadRequest",
                retryable: false,
            },
            Self::Expired { .. } => ResourceReadStatus {
                code: 410,
                reason: "Expired",
                retryable: false,
            },
            Self::Conflict { .. } => ResourceReadStatus {
                code: 409,
                reason: "Conflict",
                retryable: false,
            },
            Self::UnsupportedMode { .. } => ResourceReadStatus {
                code: 501,
                reason: "NotImplemented",
                retryable: false,
            },
            Self::CorruptData { .. } => ResourceReadStatus {
                code: 500,
                reason: "InternalError",
                retryable: false,
            },
            Self::Retryable { .. } => ResourceReadStatus {
                code: 503,
                reason: "ServiceUnavailable",
                retryable: true,
            },
            Self::Timeout => ResourceReadStatus {
                code: 504,
                reason: "Timeout",
                retryable: true,
            },
            Self::Cancelled => ResourceReadStatus {
                code: 499,
                reason: "Cancelled",
                retryable: false,
            },
        }
    }
}

impl fmt::Display for ResourceReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit { limit } => write!(
                formatter,
                "invalid list limit {limit}: limit must be greater than or equal to 0"
            ),
            Self::Expired {
                requested,
                oldest_available,
            } => write!(
                formatter,
                "resourceVersion {requested} is expired; oldest available is {oldest_available}"
            ),
            Self::InvalidSelector { message }
            | Self::InvalidContinuation { message }
            | Self::Conflict { message }
            | Self::UnsupportedMode { message }
            | Self::CorruptData { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("resource read timed out"),
            Self::Cancelled => formatter.write_str("resource read was cancelled"),
        }
    }
}

impl std::error::Error for ResourceReadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceGetRequest {
    key: ResourceKey,
}

impl ResourceGetRequest {
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            key: ResourceKey::new(api_version, kind, namespace, name),
        }
    }
    pub fn from_key(key: ResourceKey) -> Self {
        Self { key }
    }
    pub fn key(&self) -> &ResourceKey {
        &self.key
    }
    pub fn into_key(self) -> ResourceKey {
        self.key
    }
}

/// Kubernetes collection scope; `AllNamespaces` is not confused with a
/// cluster-scoped kind or a namespace whose name happens to be empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceCollectionScope {
    Cluster,
    AllNamespaces,
    Namespace(String),
}

/// Kubernetes LIST resourceVersion policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResourceVersionMatch {
    #[default]
    Any,
    NotOlderThan(i64),
    Exact(i64),
    /// Exact durable LIST-to-WATCH boundary, including same-RV apply order.
    AtPosition(WatchReplayPosition),
}

/// Composite collection key used by keyset pagination. Namespace is required
/// to distinguish equal names in an all-namespaces collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ResourceCollectionKey {
    namespace: Option<String>,
    name: String,
}

impl ResourceCollectionKey {
    pub fn new(namespace: Option<impl Into<String>>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.map(Into::into),
            name: name.into(),
        }
    }
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One exact public-RV/event-ID read boundary. Keeping this as one value makes
/// a page with mutually inconsistent RV and replay position unconstructible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceListSnapshot {
    position: WatchReplayPosition,
}

impl ResourceListSnapshot {
    pub fn try_new(position: WatchReplayPosition) -> Result<Self, ResourceReadError> {
        crate::durable_recovery::validate_replay_position(position, false).map_err(|message| {
            ResourceReadError::corrupt(format!("invalid LIST snapshot position: {message}"))
        })?;
        Ok(Self { position })
    }
    pub const fn position(self) -> WatchReplayPosition {
        self.position
    }
    pub const fn resource_version(self) -> i64 {
        self.position.resource_version
    }
}

/// Typed continuation payload. Encoding/signing is an API-layer concern; the
/// persistence port receives the decoded key and pinned snapshot directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceContinuation {
    after: ResourceCollectionKey,
    snapshot: ResourceListSnapshot,
}

impl ResourceContinuation {
    pub fn new(after: ResourceCollectionKey, snapshot: ResourceListSnapshot) -> Self {
        Self { after, snapshot }
    }
    pub const fn after(&self) -> &ResourceCollectionKey {
        &self.after
    }
    pub const fn snapshot(&self) -> ResourceListSnapshot {
        self.snapshot
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceListQuery {
    label_selector: Option<String>,
    field_selector: Option<String>,
    limit: Option<i64>,
    continuation: Option<ResourceContinuation>,
    resource_version_match: ResourceVersionMatch,
}

impl ResourceListQuery {
    /// Owned hot-path constructor; callers that already decoded query fields
    /// transfer them without clone-on-construction.
    pub fn try_new(
        label_selector: Option<String>,
        field_selector: Option<String>,
        limit: Option<i64>,
        continuation: Option<ResourceContinuation>,
        resource_version_match: ResourceVersionMatch,
    ) -> Result<Self, ResourceReadError> {
        let limit = match limit {
            None | Some(0) => None,
            Some(v) if v > 0 => Some(v),
            Some(v) => return Err(ResourceReadError::InvalidLimit { limit: v }),
        };
        let rv = match resource_version_match {
            ResourceVersionMatch::Any => None,
            ResourceVersionMatch::NotOlderThan(rv) | ResourceVersionMatch::Exact(rv) => Some(rv),
            ResourceVersionMatch::AtPosition(position) => Some(position.resource_version),
        };
        if rv.is_some_and(|rv| rv < 0) {
            return Err(ResourceReadError::Conflict {
                message: "resourceVersion must be non-negative".to_string(),
            });
        }
        if let (Some(cursor), ResourceVersionMatch::Exact(rv)) =
            (&continuation, resource_version_match)
            && cursor.snapshot.resource_version() != rv
        {
            return Err(ResourceReadError::InvalidContinuation {
                message: "continuation snapshot does not match exact resourceVersion".to_string(),
            });
        }
        if let (Some(cursor), ResourceVersionMatch::NotOlderThan(rv)) =
            (&continuation, resource_version_match)
            && cursor.snapshot.resource_version() < rv
        {
            return Err(ResourceReadError::InvalidContinuation {
                message: "continuation snapshot is older than requested resourceVersion"
                    .to_string(),
            });
        }
        if let ResourceVersionMatch::AtPosition(position) = resource_version_match {
            crate::durable_recovery::validate_replay_position(position, false)
                .map_err(|message| ResourceReadError::Conflict { message })?;
        }
        if let (Some(cursor), ResourceVersionMatch::AtPosition(position)) =
            (&continuation, resource_version_match)
            && cursor.snapshot.position() != position
        {
            return Err(ResourceReadError::InvalidContinuation {
                message: "continuation snapshot does not match positioned LIST boundary"
                    .to_string(),
            });
        }
        Ok(Self {
            label_selector,
            field_selector,
            limit,
            continuation,
            resource_version_match,
        })
    }

    pub fn try_new_borrowed(
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continuation: Option<ResourceContinuation>,
        resource_version_match: ResourceVersionMatch,
    ) -> Result<Self, ResourceReadError> {
        Self::try_new(
            label_selector.map(str::to_owned),
            field_selector.map(str::to_owned),
            limit,
            continuation,
            resource_version_match,
        )
    }

    pub const fn all() -> Self {
        Self {
            label_selector: None,
            field_selector: None,
            limit: None,
            continuation: None,
            resource_version_match: ResourceVersionMatch::Any,
        }
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
    pub const fn continuation(&self) -> Option<&ResourceContinuation> {
        self.continuation.as_ref()
    }
    pub const fn resource_version_match(&self) -> ResourceVersionMatch {
        self.resource_version_match
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceListRequest {
    api_version: String,
    kind: String,
    scope: ResourceCollectionScope,
    query: ResourceListQuery,
}

impl ResourceListRequest {
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        scope: ResourceCollectionScope,
        query: ResourceListQuery,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope,
            query,
        }
    }
    pub fn api_version(&self) -> &str {
        &self.api_version
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub const fn scope(&self) -> &ResourceCollectionScope {
        &self.scope
    }
    pub const fn query(&self) -> &ResourceListQuery {
        &self.query
    }
}

#[derive(Clone, Debug)]
pub struct ResourceListPage {
    items: Vec<Resource>,
    snapshot: ResourceListSnapshot,
    continuation: Option<ResourceContinuation>,
    remaining_item_count: Option<i64>,
}

impl ResourceListPage {
    pub fn try_new(
        items: Vec<Resource>,
        snapshot: ResourceListSnapshot,
        continuation: Option<ResourceContinuation>,
        remaining_item_count: Option<i64>,
    ) -> Result<Self, ResourceReadError> {
        if continuation
            .as_ref()
            .is_some_and(|cursor| cursor.snapshot != snapshot)
        {
            return Err(ResourceReadError::InvalidContinuation {
                message: "next continuation is not pinned to the page snapshot".to_string(),
            });
        }
        if remaining_item_count.is_some_and(|remaining| remaining < 0) {
            return Err(ResourceReadError::CorruptData {
                message: "remaining item count is negative".to_string(),
            });
        }
        Ok(Self {
            items,
            snapshot,
            continuation,
            remaining_item_count,
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
    pub const fn continuation(&self) -> Option<&ResourceContinuation> {
        self.continuation.as_ref()
    }
    pub const fn remaining_item_count(&self) -> Option<i64> {
        self.remaining_item_count
    }
}

/// Current, historical, and compacted reads are distinct datastore results.
#[derive(Clone, Debug)]
pub enum ResourceListRead {
    Current(ResourceListPage),
    Historical(ResourceListPage),
    Expired {
        requested: i64,
        oldest_available: i64,
    },
}

impl ResourceListRead {
    pub const fn page(&self) -> Option<&ResourceListPage> {
        match self {
            Self::Current(page) | Self::Historical(page) => Some(page),
            Self::Expired { .. } => None,
        }
    }
    pub fn items(&self) -> &[Resource] {
        self.page().map_or(&[], ResourceListPage::items)
    }
    pub const fn snapshot(&self) -> Option<ResourceListSnapshot> {
        match self.page() {
            Some(page) => Some(page.snapshot()),
            None => None,
        }
    }
    pub const fn continuation(&self) -> Option<&ResourceContinuation> {
        match self.page() {
            Some(page) => page.continuation(),
            None => None,
        }
    }
    pub const fn remaining_item_count(&self) -> Option<i64> {
        match self.page() {
            Some(page) => page.remaining_item_count(),
            None => None,
        }
    }
}

pub type ResourceReadFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ResourceReadError>> + Send + 'a>>;

pub trait ClusterResourceRead: Send + Sync {
    fn get_resource(&self, request: ResourceGetRequest)
    -> ResourceReadFuture<'_, Option<Resource>>;
    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead>;
}
