use super::signal_replay_cursor_core::SignalReplayCursorCore;
use super::{
    WatchCursorError, WatchDeliveryScope, WatchEvent, WatchEventFilter, WatchReplaySource,
    WatchSignalReceiver, WatchTopic, WindowPolicy,
};

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
        Self {
            core: SignalReplayCursorCore::new(
                signal_rx,
                replay_source,
                topics,
                scope,
                accepted_rv,
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
        loop {
            let event = self.core.next_event().await?;
            if self.event_filter.matches(&event) {
                return Ok(event);
            }
            self.mark_filtered_event(&event);
        }
    }

    fn mark_filtered_event(&mut self, event: &WatchEvent) {
        let Some(rv) = event.resource_version() else {
            return;
        };
        let Some(metadata) = event.object.get("metadata") else {
            return;
        };
        let Some(name) = metadata.get("name").and_then(|name| name.as_str()) else {
            return;
        };
        let namespace = metadata
            .get("namespace")
            .and_then(|namespace| namespace.as_str())
            .map(str::to_string);
        self.mark_filtered_for_key(namespace, name.to_string(), rv);
    }
}
