use super::signal_replay_cursor_core::SignalReplayCursorCore;
use super::{
    WatchCursorError, WatchDeliveryScope, WatchEvent, WatchEventFilter, WatchReplaySource,
    WindowPolicy,
};
use crate::datastore::WatchReplayPosition;
use klights_watch::{WatchSignalReceiver, WatchTopic};

pub struct SignalWatchCursor<S>
where
    S: WatchReplaySource,
{
    core: SignalReplayCursorCore<WatchEvent, S>,
    event_filter: WatchEventFilter,
}

impl<S: WatchReplaySource> SignalWatchCursor<S> {
    pub fn new(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topic: WatchTopic,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        window: WindowPolicy,
    ) -> Self {
        Self::new_many(
            signal_rx,
            replay_source,
            vec![topic],
            scope,
            accepted_rv,
            window,
        )
    }

    pub fn new_many(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topics: Vec<WatchTopic>,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        window: WindowPolicy,
    ) -> Self {
        Self::new_many_at_position(
            signal_rx,
            replay_source,
            topics,
            scope,
            accepted_rv,
            WatchReplayPosition::from_resource_version(accepted_rv),
            window,
        )
    }

    pub fn new_many_at_position(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topics: Vec<WatchTopic>,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        replay_position: WatchReplayPosition,
        window: WindowPolicy,
    ) -> Self {
        Self {
            core: SignalReplayCursorCore::new_at_position(
                signal_rx,
                replay_source,
                topics,
                scope,
                accepted_rv,
                replay_position,
                window,
            ),
            event_filter: WatchEventFilter::new(),
        }
    }

    pub fn with_event_filter(mut self, event_filter: WatchEventFilter) -> Self {
        self.event_filter = event_filter;
        self
    }

    pub fn accepted_rv(&self) -> i64 {
        self.core.accepted_rv()
    }

    pub fn accept_event(&mut self, rv: i64) {
        self.core.accept_event(rv);
    }

    pub fn processed_position(&self) -> WatchReplayPosition {
        self.core.processed_position()
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.core.prime_replay_or_expired().await
    }

    pub async fn next_event(&mut self) -> Result<WatchEvent, WatchCursorError> {
        loop {
            let event = self.core.next_event().await?;
            if self.event_filter.matches(&event) {
                return Ok(event);
            }
        }
    }
}
