//! Watch cursor and bootstrap logic.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

use super::WatchReceiver;
use super::events::WatchEvent;
use super::replay::{WatchCursorError, WatchReplaySource};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchEventFilter {
    field_selectors: Vec<TargetFieldSelector>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TargetFieldSelector {
    api_version: String,
    kind: String,
    field_selector: String,
}

impl WatchEventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_field_selector(
        mut self,
        api_version: impl Into<String>,
        kind: impl Into<String>,
        field_selector: impl Into<String>,
    ) -> Self {
        self.field_selectors.push(TargetFieldSelector {
            api_version: api_version.into(),
            kind: kind.into(),
            field_selector: field_selector.into(),
        });
        self
    }

    pub fn matches(&self, event: &WatchEvent) -> bool {
        if self.field_selectors.is_empty() {
            return true;
        }
        let Some(kind) = event.object.get("kind").and_then(|kind| kind.as_str()) else {
            return true;
        };
        let api_version = event
            .object
            .get("apiVersion")
            .and_then(|api_version| api_version.as_str());

        for selector in &self.field_selectors {
            if selector.kind != kind {
                continue;
            }
            if api_version.is_some_and(|actual| actual != selector.api_version) {
                continue;
            }
            if !event.matches_field_selector(Some(selector.field_selector.as_str())) {
                return false;
            }
        }
        true
    }
}

pub struct WatchBootstrap<S> {
    rx: WatchReceiver,
    replay_source: S,
    start_rv: i64,
    filter: WatchEventFilter,
}

impl<S: WatchReplaySource> WatchBootstrap<S> {
    pub fn new(rx: impl Into<WatchReceiver>, replay_source: S, start_rv: i64) -> Self {
        Self {
            rx: rx.into(),
            replay_source,
            start_rv,
            filter: WatchEventFilter::new(),
        }
    }

    pub fn with_event_filter(mut self, filter: WatchEventFilter) -> Self {
        self.filter = filter;
        self
    }

    pub fn into_cursor(self) -> WatchCursor<S> {
        WatchCursor::new(self.rx, self.replay_source, self.start_rv).with_event_filter(self.filter)
    }
}

pub struct WatchCursor<S> {
    rx: WatchReceiver,
    replay_source: S,
    floor_rv: i64,
    last_rv: i64,
    seen_rvs: HashSet<i64>,
    seen_order: VecDeque<i64>,
    low_rv_allowlist: HashMap<(Option<String>, String), i64>,
    pending: VecDeque<WatchEvent>,
    pending_replay_floor_rv: Option<i64>,
    filter: WatchEventFilter,
    /// Set when replay is required but hasn't succeeded yet.
    replay_required: bool,
    /// Current backoff duration for replay retry.
    replay_backoff: Duration,
}

pub const INITIAL_REPLAY_BACKOFF: Duration = Duration::from_millis(10);
pub const MAX_REPLAY_BACKOFF: Duration = Duration::from_secs(30);
const RECENT_SEEN_RV_CAPACITY: usize = 32_768;

impl<S: WatchReplaySource> WatchCursor<S> {
    pub fn new(rx: impl Into<WatchReceiver>, replay_source: S, last_rv: i64) -> Self {
        Self {
            rx: rx.into(),
            replay_source,
            floor_rv: last_rv,
            last_rv,
            seen_rvs: HashSet::new(),
            seen_order: VecDeque::new(),
            low_rv_allowlist: HashMap::new(),
            pending: VecDeque::new(),
            pending_replay_floor_rv: None,
            filter: WatchEventFilter::new(),
            replay_required: false,
            replay_backoff: INITIAL_REPLAY_BACKOFF,
        }
    }

    pub fn with_event_filter(mut self, filter: WatchEventFilter) -> Self {
        self.filter = filter;
        self
    }

    /// Highest resourceVersion this cursor has actually observed/delivered.
    /// A watch BOOKMARK must not advertise an rv beyond this, or a resuming
    /// client can skip events the cursor had not yet delivered.
    pub fn high_water_rv(&self) -> i64 {
        self.last_rv.max(self.floor_rv)
    }

    /// Mark an rv as already delivered to the client by an out-of-band path
    /// (e.g. a label-selector watch that emitted existing matches as ADDED
    /// from a baseline list before this cursor took over live delivery). The
    /// cursor then dedupes the matching live/replayed event instead of
    /// re-delivering it. Used together with a floor set BELOW the baseline's
    /// collection rv so establishment-window events are not skipped.
    pub fn mark_delivered(&mut self, rv: i64) {
        if rv <= 0 {
            return;
        }
        if self.seen_rvs.insert(rv) {
            self.seen_order.push_back(rv);
            while self.seen_order.len() > RECENT_SEEN_RV_CAPACITY {
                if let Some(oldest) = self.seen_order.pop_front() {
                    self.seen_rvs.remove(&oldest);
                }
            }
        }
        self.last_rv = self.last_rv.max(rv);
    }

    /// Allow live events below the global floor for a specific object, but
    /// only when they are newer than the object's baseline resourceVersion.
    ///
    /// Selector watches establish membership with a baseline list. In the
    /// replicated path a later broadcast for a baseline object can carry an RV
    /// below the collection RV the client used to start the watch. The normal
    /// global floor would skip that transition, leaving the client believing
    /// the baseline object is still active. This per-object exception keeps the
    /// request RV as the replay floor while allowing those post-baseline live
    /// transitions through.
    pub fn allow_low_rv_for_key(&mut self, namespace: Option<String>, name: String, after_rv: i64) {
        if after_rv <= 0 {
            return;
        }
        self.low_rv_allowlist.insert((namespace, name), after_rv);
    }

    pub async fn next_event(
        &mut self,
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
    ) -> std::result::Result<WatchEvent, WatchCursorError> {
        loop {
            if let Some(event) = self.pop_pending_event() {
                return Ok(event);
            }

            // If a previous call propagated a replay failure, pace the retry
            // before touching the datastore again. Caller may drop the future
            // (HTTP request cancellation) during the sleep.
            if self.replay_required {
                let _ = task_supervisor
                    .sleep("watch_cursor_replay_backoff", self.replay_backoff)
                    .await;
                let since = self.replay_since_rv();
                if self.replay_gap_detected(since).await {
                    return Err(WatchCursorError::Expired);
                }
                match self.replay_source.replay_since(since).await {
                    Ok(replay) => {
                        self.replay_required = false;
                        self.replay_backoff = INITIAL_REPLAY_BACKOFF;
                        self.queue_replay(replay);
                        continue;
                    }
                    Err(e) => {
                        self.replay_backoff = (self.replay_backoff * 2).min(MAX_REPLAY_BACKOFF);
                        return Err(WatchCursorError::Replay(e));
                    }
                }
            }

            match self.rx.recv().await {
                Ok(event) => {
                    if self.should_skip(&event) {
                        self.log_skipped_added("live", &event);
                        continue;
                    }
                    self.observe(&event);
                    if !self.filter.matches(&event) {
                        continue;
                    }
                    return Ok(event);
                }
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(
                        target: "klights::watch_diag",
                        lagged = n,
                        floor_rv = self.floor_rv,
                        last_rv = self.last_rv,
                        "watch cursor lagged; recovering via replay"
                    );
                    // We definitively missed live events. If the durable
                    // replay window no longer reaches back to our cursor, the
                    // gap is unrecoverable — surface Expired so the HTTP watch
                    // returns 410 Gone and the client reflector relists,
                    // rather than silently skipping the missed events.
                    let since = self.replay_since_rv();
                    if self.replay_gap_detected(since).await {
                        return Err(WatchCursorError::Expired);
                    }
                    match self.replay_source.replay_since(since).await {
                        Ok(replay) => {
                            self.queue_replay(replay);
                            continue;
                        }
                        Err(e) => {
                            self.replay_required = true;
                            return Err(WatchCursorError::Replay(e));
                        }
                    }
                }
                Err(RecvError::Closed) => {
                    return Err(WatchCursorError::Closed);
                }
            }
        }
    }

    pub async fn prime_replay(&mut self) -> anyhow::Result<usize> {
        let replay = self
            .replay_source
            .replay_since(self.replay_since_rv())
            .await?;
        let replay_len = replay.len();
        self.queue_replay(replay);
        Ok(replay_len)
    }

    /// Prime the initial catch-up replay for an HTTP watch, distinguishing an
    /// unrecoverable window gap from a transient replay error. When the
    /// requested resume `resourceVersion` predates the oldest retained watch
    /// event, returns [`WatchCursorError::Expired`] so the caller emits
    /// `410 Gone` and the client reflector relists (Kubernetes "too old
    /// resource version" semantics). Other replay failures surface as
    /// [`WatchCursorError::Replay`].
    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        let since = self.replay_since_rv();
        if self.replay_gap_detected(since).await {
            return Err(WatchCursorError::Expired);
        }
        let replay = self
            .replay_source
            .replay_since(since)
            .await
            .map_err(WatchCursorError::Replay)?;
        let replay_len = replay.len();
        self.queue_replay(replay);
        Ok(replay_len)
    }

    /// True when `since_rv` precedes the durable replay window, i.e. the
    /// oldest retained watch event has a higher RV than the next one the
    /// cursor would replay. In that case the intervening events were trimmed
    /// and cannot be recovered. A read error or empty window reports no gap
    /// (fall through to normal replay) so transient datastore hiccups do not
    /// spuriously expire healthy watches.
    async fn replay_gap_detected(&self, since_rv: i64) -> bool {
        match self.replay_source.earliest_retained_rv().await {
            Ok(Some(earliest)) => since_rv + 1 < earliest,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    /// Get the next event from the watch cursor, with automatic replay retry on failure.
    ///
    /// This method is designed for internal controllers (kubelet, node_subnet, etc.)
    /// that require event-driven convergence and must not miss events due to transient
    /// replay failures.
    ///
    /// # Behavior
    ///
    /// - Returns pending events immediately if available.
    /// - If `replay_required` is true (from a prior Lagged error), attempts replay
    ///   with bounded exponential backoff.
    /// - Does not consume live broadcast events while replay is required, ensuring
    ///   event ordering is restored from the persisted watch_events table first.
    /// - On replay success, resets backoff and drains pending events.
    /// - On replay failure, logs a warning, sleeps with exponential backoff, and retries.
    /// - Respects the cancellation token: returns `Ok(None)` within 100ms of cancellation.
    ///
    /// # Arguments
    ///
    /// * `cancel` - CancellationToken to signal graceful shutdown
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event))` - Next event to process
    /// - `Ok(None)` - Cancellation was requested
    /// - `Err(WatchCursorError::Closed)` - Broadcast channel was closed
    pub async fn next_event_recovering(
        &mut self,
        cancel: &CancellationToken,
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
    ) -> Result<Option<WatchEvent>, WatchCursorError> {
        loop {
            if let Some(event) = self.pop_pending_event() {
                return Ok(Some(event));
            }

            if self.replay_required {
                match self
                    .replay_source
                    .replay_since(self.replay_since_rv())
                    .await
                {
                    Ok(replay) => {
                        self.replay_required = false;
                        self.replay_backoff = INITIAL_REPLAY_BACKOFF;
                        self.queue_replay(replay);
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "watch replay failed (backoff: {:?}): {:#}",
                            self.replay_backoff,
                            e
                        );
                        tokio::select! {
                            _ = cancel.cancelled() => return Ok(None),
                            _ = task_supervisor.sleep(
                                "watch_cursor_replay_backoff",
                                self.replay_backoff,
                            ) => {}
                        }
                        let next = (self.replay_backoff * 2).min(MAX_REPLAY_BACKOFF);
                        self.replay_backoff = next;
                        continue;
                    }
                }
            }

            match self.rx.recv().await {
                Ok(event) => {
                    if self.should_skip(&event) {
                        continue;
                    }
                    self.observe(&event);
                    if !self.filter.matches(&event) {
                        continue;
                    }
                    return Ok(Some(event));
                }
                Err(RecvError::Lagged(_)) => {
                    self.replay_required = true;
                    continue;
                }
                Err(RecvError::Closed) => {
                    return Err(WatchCursorError::Closed);
                }
            }
        }
    }

    fn pop_pending_event(&mut self) -> Option<WatchEvent> {
        while let Some(event) = self.pending.pop_front() {
            if self.should_skip(&event) {
                self.log_skipped_added("replay", &event);
                continue;
            }
            self.observe(&event);
            if !self.filter.matches(&event) {
                continue;
            }
            return Some(event);
        }
        self.apply_pending_replay_floor_if_drained();
        None
    }

    fn queue_replay(&mut self, replay: Vec<WatchEvent>) {
        let replay_max_rv = replay.iter().filter_map(WatchEvent::resource_version).max();
        self.pending.extend(replay);
        if let Some(rv) = replay_max_rv {
            self.pending_replay_floor_rv =
                Some(self.pending_replay_floor_rv.map_or(rv, |old| old.max(rv)));
        }
    }

    fn apply_pending_replay_floor_if_drained(&mut self) {
        if !self.pending.is_empty() {
            return;
        }
        if let Some(rv) = self.pending_replay_floor_rv.take() {
            self.floor_rv = self.floor_rv.max(rv);
        }
    }

    /// Diagnostic: an ADDED event being dropped by the cursor (rv <= floor or
    /// already seen) means a watcher resuming from before that object's
    /// creation will never observe its ADDED — the exact failure behind the
    /// flaky `[sig-apps] Deployment should run the lifecycle` conformance test
    /// under high parallel load. Logged at WARN so it surfaces without a
    /// debug build.
    fn log_skipped_added(&self, phase: &str, event: &WatchEvent) {
        let Some(rv) = event.resource_version() else {
            return;
        };
        // Only the dangerous case: the floor moved past an event this cursor
        // never delivered. The benign duplicate-delivery dedup (rv already in
        // seen_rvs, e.g. catch-up + live both carry it) is correct and silent.
        if self.seen_rvs.contains(&rv) || rv > self.floor_rv {
            return;
        }
        tracing::warn!(
            target: "klights::watch_diag",
            phase,
            event_type = %event.event_type,
            dropped_rv = rv,
            floor_rv = self.floor_rv,
            last_rv = self.last_rv,
            kind = event.object.get("kind").and_then(|k| k.as_str()).unwrap_or(""),
            namespace = event
                .object
                .pointer("/metadata/namespace")
                .and_then(|n| n.as_str())
                .unwrap_or(""),
            name = event
                .object
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or(""),
            "watch cursor dropped an undelivered event (floor advanced past it)"
        );
    }

    fn should_skip(&self, event: &WatchEvent) -> bool {
        let Some(rv) = event.resource_version() else {
            return false;
        };
        if self.seen_rvs.contains(&rv) {
            return true;
        }
        if rv > self.floor_rv {
            return false;
        }
        let Some(key) = event_key(event) else {
            return true;
        };
        self.low_rv_allowlist
            .get(&key)
            .is_none_or(|after_rv| rv <= *after_rv)
    }

    fn observe(&mut self, event: &WatchEvent) {
        if let Some(rv) = event.resource_version() {
            if self.seen_rvs.insert(rv) {
                self.seen_order.push_back(rv);
                while self.seen_order.len() > RECENT_SEEN_RV_CAPACITY {
                    if let Some(oldest) = self.seen_order.pop_front() {
                        self.seen_rvs.remove(&oldest);
                    }
                }
            }
            self.last_rv = self.last_rv.max(rv);
        }
    }

    fn replay_since_rv(&self) -> i64 {
        // Live broadcasts are emitted after DB commit by the async caller that
        // performed the write, so delivery can be out of RV order. A higher RV
        // observed live does not prove every lower RV reached this cursor,
        // especially when API-level selectors discard non-matching events after
        // the cursor has observed them. Replay from the durable floor and rely
        // on exact-RV dedupe to suppress already delivered events.
        self.floor_rv
    }

    #[cfg(test)]
    pub fn replay_backoff(&self) -> Duration {
        self.replay_backoff
    }
}

fn event_key(event: &WatchEvent) -> Option<(Option<String>, String)> {
    let name = event
        .object
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)?;
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    Some((namespace, name))
}
