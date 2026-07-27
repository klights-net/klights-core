//! Active Kubernetes LIST-to-WATCH delivery for klights.
//!
//! Durable history remains behind `klights-cluster-store`, while the request,
//! event, error, and stream values remain owned by `klights-leader-api`.

mod cache;
mod filter;
mod replay;
mod session;
mod signal;

pub use cache::{WatchCache, WatchCacheError};
pub use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};
pub use replay::{
    PositionedWatchReplay, PositionedWatchReplayRead, WatchReplayRead, WatchTarget,
    WatchTargetScope,
};
pub use session::{
    PendingWatchSelectorTransition, PositionedWatchService, ProjectedWatchBaselineRead,
    ProjectedWatchBaselineRequest, ProjectedWatchPlan, WatchResourceProjection, WatchResourceScope,
    WatchScopeResolver, WatchSelectorMembership,
};
pub use signal::{
    DEFAULT_WATCH_ADVANCE_GROUP_LIMIT, WatchAdvance, WatchSignal, WatchSignalEvent,
    WatchSignalFuture, WatchSignalHub, WatchSignalReceiveError, WatchSignalReceiver,
    WatchSignalSubscribe, WatchSignalSubscription, WatchSignalTryReceiveError, WatchTopic,
};
