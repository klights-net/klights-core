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

pub trait ReplayCursorEvent: Clone + Send + Sync + 'static {
    fn resource_version(&self) -> Option<i64>;
    fn topic(&self) -> Option<WatchTopic>;
    fn namespace(&self) -> Option<&str>;
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
    processed_position: WatchReplayPosition,
    covered_position_after_pending: Option<WatchReplayPosition>,
    pending: VecDeque<PositionedWatchEvent<E>>,
    window: WindowPolicy,
    replay_needed: bool,
}

impl<E, S> SignalReplayCursorCore<E, S>
where
    E: ReplayCursorEvent,
    S: SignalReplayCursorSource<E>,
{
    pub fn new_at_position(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topics: Vec<WatchTopic>,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        replay_position: WatchReplayPosition,
        window: WindowPolicy,
    ) -> Self {
        Self {
            signal_rx: signal_rx.into(),
            replay_source,
            topics: topics.into_iter().collect(),
            scope,
            accepted_rv,
            replay_position,
            processed_position: replay_position,
            covered_position_after_pending: None,
            pending: VecDeque::new(),
            window,
            replay_needed: false,
        }
    }

    pub fn accepted_rv(&self) -> i64 {
        self.accepted_rv
    }

    pub fn accept_event(&mut self, rv: i64) {
        self.advance_processed_rv(rv);
    }

    /// Durable replay position proven safe through events already returned (or
    /// filtered) by this cursor. Unlike `replay_position`, this never exposes
    /// read-ahead rows that remain pending delivery.
    pub fn processed_position(&self) -> WatchReplayPosition {
        self.processed_position
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
                // Keep draining durable history until a read is empty.  A
                // positioned backend may return a short page even when rows
                // remain (for example when target/selector filtering or a
                // backend-side scan boundary under-fills the LIMIT).  Using
                // `event_count == limit` would then incorrectly hand control
                // back to the signal receiver and stall a list/watch handoff
                // until an unrelated future write.  An empty page is the
                // authoritative exhaustion marker; the next read is still
                // bounded and event-driven, and it avoids any idle polling.
                self.replay_needed = event_count > 0;
                if event_count == 0 {
                    self.processed_position = replay.next_position;
                    self.covered_position_after_pending = None;
                } else {
                    self.covered_position_after_pending = Some(replay.next_position);
                }
                self.pending.extend(replay.events);
                Ok(event_count)
            }
            PositionedWatchReplayRead::Expired => Err(WatchCursorError::Expired),
        }
    }

    fn pop_pending_event(&mut self) -> Option<E> {
        while let Some(positioned) = self.pending.pop_front() {
            let mut processed = positioned.position;
            if processed.event_id
                < self
                    .processed_position
                    .resource_version_filter_through_event_id
            {
                processed.resource_version = self.processed_position.resource_version;
                processed.resource_version_filter_through_event_id = self
                    .processed_position
                    .resource_version_filter_through_event_id;
            }
            self.processed_position = processed;
            if self.pending.is_empty()
                && let Some(covered) = self.covered_position_after_pending.take()
            {
                self.processed_position = covered;
            }
            let event = positioned.event;
            let Some(rv) = event.resource_version() else {
                continue;
            };
            if !self.event_matches(&event) {
                self.advance_processed_rv(rv);
                continue;
            }
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
        if rv > self.accepted_rv {
            self.accepted_rv = rv;
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
}
