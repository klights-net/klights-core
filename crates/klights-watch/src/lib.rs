//! Active Kubernetes LIST-to-WATCH delivery for klights.
//!
//! Durable history remains behind `klights-cluster-store`, while the request,
//! event, error, and stream values remain owned by `klights-leader-api`.

mod bookmark;
mod cache;
mod event;
mod event_bus;
mod filter;
#[cfg(feature = "session")]
mod freshness;
mod remote_cache;
mod replay;
#[cfg(feature = "session")]
mod resource_query;
mod selection;
#[cfg(feature = "session")]
mod session;
mod signal;

#[cfg(test)]
mod event_tests;

pub use cache::{WatchCache, WatchCacheError};
pub use event::{
    EncodedWatchPayload, EventType, WatchContentType, WatchEvent, encode_watch_payload,
    value_matches_field_selector, value_matches_field_selector_with_identity,
};
pub use event_bus::WatchBus;
#[cfg(any(test, feature = "integration-test-harness"))]
pub use event_bus::WatchReceiver;
pub use filter::WatchEventFilter;
#[cfg(feature = "session")]
pub use freshness::wait_until_resource_version_fresh;
pub use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};
pub use remote_cache::{
    PreparedWatchTransition, RemoteInformerCache, SelectorWatchTransitionProjector,
    SelectorWatchTransitionProjectors, WatchTransitionProjector, WatchTransitionProjectorFactory,
};
pub use replay::{
    PositionedWatchReplay, PositionedWatchReplayRead, WatchReplayRead, WatchTarget,
    WatchTargetScope,
};
#[cfg(feature = "session")]
pub use resource_query::{DatastoreResourceQueryAdapter, PRIVATE_PINNED_CONTINUATION_TTL};
pub use selection::WatchEventSelection;
#[cfg(feature = "session")]
pub use session::{
    PendingWatchSelectorTransition, PositionedWatchService, ProjectedWatchBaselineRead,
    ProjectedWatchBaselineRequest, ProjectedWatchPlan, SnapshotProjectedWatchBaseline,
    WatchResourceProjection, WatchSelectorMembership,
};
#[cfg(feature = "integration-test-harness")]
pub use signal::test_support;
pub use signal::{
    DEFAULT_WATCH_ADVANCE_GROUP_LIMIT, PostCommitWatchWakeup, WatchAdvance, WatchSignal,
    WatchSignalEvent, WatchSignalFuture, WatchSignalHub, WatchSignalPublish,
    WatchSignalReceiveError, WatchSignalReceiver, WatchSignalSubscribe, WatchSignalSubscription,
    WatchSignalTryReceiveError, WatchTopic,
};
