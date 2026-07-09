use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::Result;
use tokio::sync::broadcast::error::RecvError;

use crate::datastore::{RawWatchEvent, WatchReplayRead};

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
    key: Option<(Option<String>, String)>,
}

#[async_trait::async_trait]
pub trait SignalReplayCursorSource<E>: Send + Sync
where
    E: ReplayCursorEvent,
{
    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<E>>;
}

#[async_trait::async_trait]
impl<S> SignalReplayCursorSource<WatchEvent> for S
where
    S: WatchReplaySource,
{
    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<WatchEvent>> {
        WatchReplaySource::replay_since_checked(self, since_rv, limit).await
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
    pending: VecDeque<E>,
    window: WindowPolicy,
    replay_needed: bool,
    replay_resume_rv: Option<i64>,
    seen_events: HashSet<ReplayEventMarker>,
    seen_order: VecDeque<ReplayEventMarker>,
    // Baseline initial-state emission is RV-wide because its caller only has
    // the already emitted list item RV. Selector-filtered events are tracked
    // by object identity through `non_advancing_seen_events`.
    non_advancing_seen_rvs: HashSet<i64>,
    non_advancing_seen_events: HashSet<ReplayEventMarker>,
    low_rv_allowlist: HashMap<(Option<String>, String), i64>,
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
            pending: VecDeque::new(),
            window,
            replay_needed: false,
            replay_resume_rv: None,
            seen_events: HashSet::new(),
            seen_order: VecDeque::new(),
            non_advancing_seen_rvs: HashSet::new(),
            non_advancing_seen_events: HashSet::new(),
            low_rv_allowlist: HashMap::new(),
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
            key: Some((namespace, name)),
        });
    }

    pub fn allow_low_rv_for_key(&mut self, namespace: Option<String>, name: String, after_rv: i64) {
        if after_rv <= 0 {
            return;
        }
        self.low_rv_allowlist.insert((namespace, name), after_rv);
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.replay_once_from(self.accepted_rv).await
    }

    pub async fn next_event(&mut self) -> Result<E, WatchCursorError> {
        loop {
            if let Some(event) = self.pop_pending_event() {
                return Ok(event);
            }

            if self.replay_needed {
                self.replay_needed = false;
                let since_rv = self.replay_resume_rv.take().unwrap_or(self.accepted_rv);
                self.replay_once_from(since_rv).await?;
                continue;
            }

            match self.signal_rx.recv().await {
                Ok(signal) => {
                    if let Some(since_rv) = self.matching_signal_replay_since(&signal) {
                        self.replay_once_from(since_rv).await?;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    self.replay_needed = true;
                }
                Err(RecvError::Closed) => return Err(WatchCursorError::Closed),
            }
        }
    }

    async fn replay_once_from(&mut self, since_rv: i64) -> Result<usize, WatchCursorError> {
        let limit = self.window.limit();
        let replay = self
            .replay_source
            .replay_since_checked(since_rv, limit)
            .await
            .map_err(WatchCursorError::Replay)?;
        match replay {
            WatchReplayRead::Events(events) => {
                let event_count = events.len();
                let max_rv = events
                    .iter()
                    .filter_map(ReplayCursorEvent::resource_version)
                    .max();
                self.replay_needed = event_count == limit.get();
                self.replay_resume_rv = self.replay_needed.then_some(max_rv.unwrap_or(since_rv));
                self.pending.extend(events);
                Ok(event_count)
            }
            WatchReplayRead::Expired => Err(WatchCursorError::Expired),
        }
    }

    fn pop_pending_event(&mut self) -> Option<E> {
        while let Some(event) = self.pending.pop_front() {
            let Some(rv) = event.resource_version() else {
                continue;
            };
            let marker = Self::event_marker(&event, rv);
            if rv < self.accepted_rv {
                if self.event_was_seen(&marker) {
                    continue;
                }
                if !self.low_rv_allowed(&event, rv) {
                    continue;
                }
            }
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

    fn matching_signal_replay_since(&self, signal: &WatchSignal) -> Option<i64> {
        if !self.topics.contains(&signal.topic) {
            return None;
        }
        let mut replay_since: Option<i64> = None;
        for advance in &signal.advances {
            if !self.scope.matches_namespace(advance.namespace.as_deref()) {
                continue;
            }
            let since = if advance.high_rv > self.accepted_rv {
                Some(self.accepted_rv)
            } else {
                self.low_rv_replay_floor(advance.high_rv)
            };
            if let Some(since) = since {
                replay_since = Some(replay_since.map_or(since, |current| current.min(since)));
            }
        }
        replay_since
    }

    fn event_matches(&self, event: &E) -> bool {
        let Some(topic) = event.topic() else {
            return false;
        };
        self.topics.contains(&topic) && self.scope.matches_namespace(event.namespace())
    }

    fn low_rv_replay_floor(&self, high_rv: i64) -> Option<i64> {
        self.low_rv_allowlist
            .values()
            .filter(|after_rv| high_rv > **after_rv)
            .copied()
            .min()
    }

    fn low_rv_allowed(&self, event: &E, rv: i64) -> bool {
        let Some(key) = event.key() else {
            return false;
        };
        self.low_rv_allowlist
            .get(&key)
            .is_some_and(|after_rv| rv > *after_rv)
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
