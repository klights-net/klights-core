use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchTargetScope {
    Cluster,
    Namespaced(Option<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchTarget {
    api_version: String,
    kind: String,
    scope: WatchTargetScope,
}

impl WatchTarget {
    pub fn cluster(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: WatchTargetScope::Cluster,
        }
    }

    pub fn namespaced(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: WatchTargetScope::Namespaced(None),
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
            scope: WatchTargetScope::Namespaced(Some(namespace.into())),
        }
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub const fn scope(&self) -> &WatchTargetScope {
        &self.scope
    }
}

#[derive(Clone, Debug)]
pub enum WatchReplayRead<T> {
    Events(Vec<T>),
    Expired,
}

#[derive(Clone, Debug)]
pub struct PositionedWatchReplay<T> {
    pub events: Vec<PositionedWatchEvent<T>>,
    pub next_position: WatchReplayPosition,
}

impl<T> PositionedWatchReplay<T> {
    pub fn new(events: Vec<PositionedWatchEvent<T>>, next_position: WatchReplayPosition) -> Self {
        Self {
            events,
            next_position,
        }
    }

    pub fn into_parts(self) -> (Vec<PositionedWatchEvent<T>>, WatchReplayPosition) {
        (self.events, self.next_position)
    }
}

#[derive(Clone, Debug)]
pub enum PositionedWatchReplayRead<T> {
    Events(PositionedWatchReplay<T>),
    Expired,
}
