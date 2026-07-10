use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use tokio::sync::broadcast::error::RecvError;

use crate::datastore::{
    PositionedWatchEvent, PositionedWatchReplayRead, RawWatchEvent, WatchReplayPosition,
};

use super::{
    WatchCursorError, WatchDeliveryScope, WatchEvent, WatchReplaySource, WatchSignal,
    WatchSignalReceiver, WatchTopic, WindowPolicy,
};

const RECENT_SIGNAL_SEEN_RV_CAPACITY: usize = 32_768;

pub trait ReplayCursorEvent: Clone + Send + Sync + 'static {
    fn resource_version(&self) -> Option<i64>;
    fn topic(&self) -> Option<WatchTopic>;
    fn namespace(&self) -> Option<&str>;
    fn key(&self) -> Option<(Option<String>, String)>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReplayEventMarker {
    rv: i64,
    topic: Option<WatchTopic>,
    key: Option<(Option<String>, String)>,
}

#[async_trait::async_trait]
pub trait SignalReplayCursorSource<E>: Send + Sync
where
    E: ReplayCursorEvent,
{
    async fn replay_after_checked(
        &self,
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<E>>;
}

#[async_trait::async_trait]
impl<S> SignalReplayCursorSource<WatchEvent> for S
where
    S: WatchReplaySource,
{
    async fn replay_after_checked(
        &self,
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<WatchEvent>> {
        WatchReplaySource::replay_after_checked(self, position, limit).await
    }
}

pub struct SignalReplayCursorCore<E, S>
where
    E: ReplayCursorEvent,
    S: SignalReplayCursorSource<E>,
{
    signal_rx: WatchSignalReceiver,
    replay_source: S,
    topics: HashSet<WatchTopic>,
    scope: WatchDeliveryScope,
    accepted_rv: i64,
    replay_position: WatchReplayPosition,
    pending: VecDeque<PositionedWatchEvent<E>>,
    window: WindowPolicy,
    replay_needed: bool,
    seen_events: HashSet<ReplayEventMarker>,
    seen_order: VecDeque<ReplayEventMarker>,
    // Baseline initial-state emission is RV-wide because its caller only has
    // the already emitted list item RV. Selector-filtered events are tracked
    // by object identity through `non_advancing_seen_events`.
    non_advancing_seen_rvs: HashSet<i64>,
    non_advancing_seen_events: HashSet<ReplayEventMarker>,
}

impl<E, S> SignalReplayCursorCore<E, S>
where
    E: ReplayCursorEvent,
    S: SignalReplayCursorSource<E>,
{
    pub fn new(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topics: Vec<WatchTopic>,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        window: WindowPolicy,
    ) -> Self {
        Self {
            signal_rx: signal_rx.into(),
            replay_source,
            topics: topics.into_iter().collect(),
            scope,
            accepted_rv,
            replay_position: WatchReplayPosition::from_resource_version(accepted_rv),
            pending: VecDeque::new(),
            window,
            replay_needed: false,
            seen_events: HashSet::new(),
            seen_order: VecDeque::new(),
            non_advancing_seen_rvs: HashSet::new(),
            non_advancing_seen_events: HashSet::new(),
        }
    }

    pub fn accepted_rv(&self) -> i64 {
        self.accepted_rv
    }

    pub fn accept_event(&mut self, rv: i64) {
        self.advance_processed_rv(rv);
    }

    pub fn mark_delivered(&mut self, rv: i64) {
        self.record_non_advancing_seen_rv(rv);
    }

    pub fn mark_filtered_for_key(&mut self, namespace: Option<String>, name: String, rv: i64) {
        if rv <= 0 {
            return;
        }
        self.record_non_advancing_seen_event(ReplayEventMarker {
            rv,
            topic: (self.topics.len() == 1)
                .then(|| self.topics.iter().next().cloned())
                .flatten(),
            key: Some((namespace, name)),
        });
    }

    pub fn allow_low_rv_for_key(
        &mut self,
        _namespace: Option<String>,
        _name: String,
        _after_rv: i64,
    ) {
        // Kept as a compatibility no-op while callers shed the old
        // resourceVersion exception. Durable insertion position now makes
        // later-applied lower-RV rows visible without per-key allowlists.
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.replay_once().await
    }

    pub async fn next_event(&mut self) -> Result<E, WatchCursorError> {
        loop {
            if let Some(event) = self.pop_pending_event() {
                return Ok(event);
            }

            if self.replay_needed {
                self.replay_needed = false;
                self.replay_once().await?;
                continue;
            }

            match self.signal_rx.recv().await {
                Ok(signal) => {
                    if self.signal_matches(&signal) {
                        self.replay_once().await?;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    self.replay_needed = true;
                }
                Err(RecvError::Closed) => return Err(WatchCursorError::Closed),
            }
        }
    }

    async fn replay_once(&mut self) -> Result<usize, WatchCursorError> {
        let limit = self.window.limit();
        let replay = self
            .replay_source
            .replay_after_checked(self.replay_position, limit)
            .await
            .map_err(WatchCursorError::Replay)?;
        match replay {
            PositionedWatchReplayRead::Events(replay) => {
                let event_count = replay.events.len();
                self.replay_position = replay.next_position;
                self.replay_needed = event_count == limit.get();
                self.pending.extend(replay.events);
                Ok(event_count)
            }
            PositionedWatchReplayRead::Expired => Err(WatchCursorError::Expired),
        }
    }

    fn pop_pending_event(&mut self) -> Option<E> {
        while let Some(positioned) = self.pending.pop_front() {
            let event = positioned.event;
            let Some(rv) = event.resource_version() else {
                continue;
            };
            let marker = Self::event_marker(&event, rv);
            if self.event_was_seen(&marker) {
                if !self.non_advancing_seen_events.contains(&marker) {
                    self.advance_processed_rv(rv);
                }
                continue;
            }
            if self.non_advancing_seen_rvs.contains(&rv) {
                continue;
            }
            if !self.event_matches(&event) {
                self.advance_processed_rv(rv);
                continue;
            }
            self.record_seen_event(marker);
            return Some(event);
        }
        None
    }

    fn signal_matches(&self, signal: &WatchSignal) -> bool {
        if !self.topics.contains(&signal.topic) {
            return false;
        }
        signal.advances.iter().any(|advance| {
            self.scope.matches_namespace(advance.namespace.as_deref()) && advance.high_rv > 0
        })
    }

    fn event_matches(&self, event: &E) -> bool {
        let Some(topic) = event.topic() else {
            return false;
        };
        self.topics.contains(&topic) && self.scope.matches_namespace(event.namespace())
    }

    fn advance_processed_rv(&mut self, rv: i64) {
        self.non_advancing_seen_rvs.remove(&rv);
        if rv > self.accepted_rv {
            self.accepted_rv = rv;
        }
    }

    fn record_non_advancing_seen_rv(&mut self, rv: i64) {
        if rv > 0 {
            self.non_advancing_seen_rvs.insert(rv);
        }
    }

    fn record_non_advancing_seen_event(&mut self, marker: ReplayEventMarker) {
        self.record_seen_event(marker.clone());
        self.non_advancing_seen_events.insert(marker);
    }

    fn record_seen_event(&mut self, marker: ReplayEventMarker) {
        if marker.rv <= 0 {
            return;
        }
        if self.seen_events.insert(marker.clone()) {
            self.seen_order.push_back(marker);
            while self.seen_order.len() > RECENT_SIGNAL_SEEN_RV_CAPACITY {
                if let Some(oldest) = self.seen_order.pop_front() {
                    self.seen_events.remove(&oldest);
                    self.non_advancing_seen_events.remove(&oldest);
                }
            }
        }
    }

    fn event_was_seen(&self, marker: &ReplayEventMarker) -> bool {
        self.seen_events.contains(marker)
    }

    fn event_marker(event: &E, rv: i64) -> ReplayEventMarker {
        ReplayEventMarker {
            rv,
            topic: event.topic(),
            key: event.key(),
        }
    }
}

impl ReplayCursorEvent for WatchEvent {
    fn resource_version(&self) -> Option<i64> {
        WatchEvent::resource_version(self)
    }

    fn topic(&self) -> Option<WatchTopic> {
        let api_version = self
            .object
            .get("apiVersion")
            .and_then(|value| value.as_str())?;
        let kind = self.object.get("kind").and_then(|value| value.as_str())?;
        Some(WatchTopic::new(api_version, kind))
    }

    fn namespace(&self) -> Option<&str> {
        self.object
            .get("metadata")
            .and_then(|metadata| metadata.get("namespace"))
            .and_then(|namespace| namespace.as_str())
    }

    fn key(&self) -> Option<(Option<String>, String)> {
        let name = self
            .object
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())
            .map(ToString::to_string)?;
        let namespace = self
            .object
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            .map(ToString::to_string);
        Some((namespace, name))
    }
}

impl ReplayCursorEvent for RawWatchEvent {
    fn resource_version(&self) -> Option<i64> {
        Some(self.resource_version)
    }

    fn topic(&self) -> Option<WatchTopic> {
        Some(RawWatchEvent::topic(self))
    }

    fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    fn key(&self) -> Option<(Option<String>, String)> {
        Some(RawWatchEvent::key(self))
    }
}
