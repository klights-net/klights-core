use super::signal_replay_cursor_core::SignalReplayCursorCore;
use super::{
    WatchCursorError, WatchDeliveryScope, WatchEvent, WatchReplaySource, WatchSignalReceiver,
    WatchTopic, WindowPolicy,
};

pub struct SignalWatchCursor<S>
where
    S: WatchReplaySource,
{
    core: SignalReplayCursorCore<WatchEvent, S>,
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
        Self {
            core: SignalReplayCursorCore::new(
                signal_rx,
                replay_source,
                topics,
                scope,
                accepted_rv,
                window,
            ),
        }
    }

    pub fn accepted_rv(&self) -> i64 {
        self.core.accepted_rv()
    }

    pub fn accept_event(&mut self, rv: i64) {
        self.core.accept_event(rv);
    }

    pub fn mark_delivered(&mut self, rv: i64) {
        self.core.mark_delivered(rv);
    }

    pub fn mark_filtered_for_key(&mut self, namespace: Option<String>, name: String, rv: i64) {
        self.core.mark_filtered_for_key(namespace, name, rv);
    }

    pub fn allow_low_rv_for_key(&mut self, namespace: Option<String>, name: String, after_rv: i64) {
        self.core.allow_low_rv_for_key(namespace, name, after_rv);
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.core.prime_replay_or_expired().await
    }

    pub async fn next_event(&mut self) -> Result<WatchEvent, WatchCursorError> {
        self.core.next_event().await
    }
}
