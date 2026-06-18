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
    ordered_replay: bool,
    /// Set when replay is required but hasn't succeeded yet.
    replay_required: bool,
    /// Current backoff duration for replay retry.
    replay_backoff: Duration,
    /// Lowest RV this cursor may replay from to recover events the delivery
    /// floor advanced past. Anchored to the watch's start floor (it never
    /// advances), so a non-dense durable replay -- a broadcast event whose
    /// `watch_events` row lagged or was lost under multinode apply stress --
    /// can be re-fetched and the event recovered instead of silently dropped.
    recovery_floor_rv: i64,
    /// Namespace the durable replay covers, so floor-drop recovery is gated to
    /// events that replay could actually contain. The live broadcast for
    /// built-in kinds is cluster-wide (e.g. all `v1/Pod`), but a namespaced
    /// watch replays only its own namespace; an out-of-namespace event with
    /// `rv <= floor` is legitimately absent from that replay and must be
    /// ignored, not surfaced as Expired. `None` means the replay is cluster- or
    /// all-namespace-scoped (matches every event).
    replay_namespace: Option<String>,
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
            ordered_replay: false,
            replay_required: false,
            replay_backoff: INITIAL_REPLAY_BACKOFF,
            recovery_floor_rv: last_rv,
            replay_namespace: None,
        }
    }

    pub fn with_event_filter(mut self, filter: WatchEventFilter) -> Self {
        self.filter = filter;
        self
    }

    /// Confine floor-drop recovery to events the durable replay could contain.
    /// Pass the watch's namespace (`Some(ns)` for a namespaced watch, `None` for
    /// a cluster-scoped or all-namespace watch). The live broadcast for built-in
    /// kinds is cluster-wide while a namespaced replay covers only its namespace,
    /// so an out-of-namespace event below the floor must be ignored rather than
    /// misread as an unrecoverable gap.
    pub fn with_replay_namespace(mut self, namespace: Option<String>) -> Self {
        self.replay_namespace = namespace;
        self
    }

    /// Preserve client-facing Kubernetes watch ordering by replaying durable
    /// history before emitting a live event that jumped past a positive
    /// processed floor. RV-less watches have no continuity contract from RV 0,
    /// so they keep the default live behavior until a replay/list floor exists.
    /// Internal controllers may intentionally use the default recovery behavior,
    /// which can accept late lower-RV live events.
    pub fn with_ordered_replay(mut self) -> Self {
        self.ordered_replay = self.floor_rv > 0;
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
                    if self.ordered_replay && self.live_event_requires_ordered_replay(&event) {
                        let since = self.replay_since_rv();
                        if self.replay_gap_detected(since).await {
                            return Err(WatchCursorError::Expired);
                        }
                        match self.replay_source.replay_since(since).await {
                            Ok(replay) => {
                                self.queue_ordered_replay_with_live(replay, event);
                                continue;
                            }
                            Err(e) => {
                                self.replay_required = true;
                                return Err(WatchCursorError::Replay(e));
                            }
                        }
                    }
                    if self.should_skip(&event) {
                        // The canary case: the floor advanced past an event this
                        // cursor never delivered (rv <= floor AND not in seen).
                        // That means the durable replay was non-dense -- a
                        // broadcast event whose watch_events row lagged or was
                        // lost under multinode apply stress. Recover it via
                        // re-replay, or surface Expired (410) so the client
                        // relists -- never silently drop the broadcast event a
                        // reflector is waiting on (the Guestbook readiness
                        // stall). Benign dedup (rv in seen) stays a silent skip,
                        // and only events inside the replay's namespace scope are
                        // candidates (the broadcast is cluster-wide; an
                        // out-of-namespace event is legitimately absent from a
                        // namespaced replay and must be ignored, not expired).
                        if let Some(rv) = event.resource_version()
                            && rv > self.recovery_floor_rv
                            && !self.seen_rvs.contains(&rv)
                            && self.event_in_replay_scope(&event)
                        {
                            match self.recover_floor_drop(rv).await {
                                Ok(()) => continue,
                                Err(WatchCursorError::Expired) => {
                                    tracing::warn!(
                                        target: "klights::watch_diag",
                                        dropped_rv = rv,
                                        floor_rv = self.floor_rv,
                                        last_rv = self.last_rv,
                                        kind = event
                                            .object
                                            .get("kind")
                                            .and_then(|k| k.as_str())
                                            .unwrap_or(""),
                                        "watch cursor expired: live event absent from durable replay (unrecoverable gap)"
                                    );
                                    return Err(WatchCursorError::Expired);
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        self.log_skipped_added("live", &event);
                        continue;
                    }
                    self.observe_live(&event);
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
    /// and cannot be recovered.
    ///
    /// A read **error** is treated as a detected gap (fail closed → `Expired`
    /// → HTTP 410 → reflector relists): when the server cannot confirm the
    /// retained window still covers the cursor, it must never silently advance
    /// its delivery floor past possibly-undelivered in-scope events (which
    /// `should_skip` would then drop). Returning 410 on a transient datastore
    /// hiccup is the Kubernetes-correct outcome — clients relist routinely —
    /// whereas the previous fail-open (`Err => false`) is what let the lossy
    /// Guestbook readiness transition be dropped silently. An genuinely empty
    /// window (`Ok(None)`) still reports no gap so fresh/quiet watches and the
    /// `WatchReplaySource` default stay non-expiring.
    async fn replay_gap_detected(&self, since_rv: i64) -> bool {
        match self.replay_source.earliest_retained_rv().await {
            Ok(Some(earliest)) => since_rv + 1 < earliest,
            Ok(None) => false,
            Err(_) => true,
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

    fn queue_ordered_replay_with_live(&mut self, replay: Vec<WatchEvent>, live: WatchEvent) {
        let live_rv = live.resource_version();
        self.queue_replay(replay);
        self.pending.push_back(live);
        if let Some(rv) = live_rv {
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

    /// Whether `event` falls inside the namespace scope the durable replay
    /// covers. The live broadcast for built-in kinds is cluster-wide, but a
    /// namespaced watch replays only its own namespace; an out-of-namespace
    /// event below the floor is legitimately absent from that replay and must
    /// not be treated as a recoverable gap.
    fn event_in_replay_scope(&self, event: &WatchEvent) -> bool {
        let Some(namespace) = &self.replay_namespace else {
            return true;
        };
        event
            .object
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            == Some(namespace.as_str())
    }

    /// Recover a live event the delivery floor advanced past because the durable
    /// replay was non-dense -- a broadcast event whose `watch_events` row lagged
    /// or was lost under multinode raft/outbox apply stress (the Guestbook
    /// scoped-watch readiness stall: `watch_diag` "cursor dropped an
    /// undelivered event (floor advanced past it)").
    ///
    /// Re-replay from the (never-advancing) recovery floor to catch rows that
    /// landed after the original non-dense replay. If the dropped event's RV is
    /// now retained, re-queue the dense replay -- drain delivers the recovered
    /// event (already-delivered RVs are deduped via `seen_rvs`). If it is still
    /// genuinely absent, the gap is unrecoverable: surface `Expired` so the HTTP
    /// watch returns 410 and the client relists, rather than silently dropping
    /// the broadcast event the reflector is waiting on.
    ///
    /// The caller MUST gate this on [`event_in_replay_scope`] so an
    /// out-of-namespace event (absent from a namespaced replay by design) is
    /// ignored instead of expiring the watch.
    async fn recover_floor_drop(
        &mut self,
        dropped_rv: i64,
    ) -> std::result::Result<(), WatchCursorError> {
        let replay = self
            .replay_source
            .replay_since(self.recovery_floor_rv)
            .await
            .map_err(WatchCursorError::Replay)?;
        if !replay
            .iter()
            .any(|event| event.resource_version() == Some(dropped_rv))
        {
            return Err(WatchCursorError::Expired);
        }
        // Reached only when pending is empty (this runs from the live arm of
        // next_event, which follows a draining pop_pending_event). Drop the
        // floor back to the recovery floor so the re-queued events (rv >
        // recovery_floor, including dropped_rv) are not should_skip'd;
        // seen_rvs dedups the already-delivered ones and the floor is restored
        // to the replay max when the re-queue drains.
        self.floor_rv = self.recovery_floor_rv;
        self.pending_replay_floor_rv = None;
        self.queue_replay(replay);
        Ok(())
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

    fn live_event_requires_ordered_replay(&self, event: &WatchEvent) -> bool {
        let Some(rv) = event.resource_version() else {
            return false;
        };
        if rv <= self.floor_rv + 1 || self.seen_rvs.contains(&rv) {
            return false;
        }
        let Some(key) = event_key(event) else {
            return true;
        };
        self.low_rv_allowlist
            .get(&key)
            .is_none_or(|after_rv| rv > *after_rv)
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

    fn observe_live(&mut self, event: &WatchEvent) {
        self.observe(event);
        if self.ordered_replay
            && let Some(rv) = event.resource_version()
            && rv == self.floor_rv + 1
        {
            self.floor_rv = rv;
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

/// Whether `replay` densely reconstructs the ordered gap `(floor_rv, live_rv]`.
///
/// The persisted `watch_events` log carries exactly one row per committed
/// resourceVersion, so the gap is reconstructible iff the replay supplies every
/// integer RV in `floor_rv+1 .. live_rv`. A missing integer means the window
/// was pruned below the gap or the read returned a partial result (e.g. a
/// transient datastore error under transport stress): the gap is unfillable and
/// the caller must expire (410) rather than advance its delivery floor past
/// events it never emitted. `live_rv == None` (a bookmark/RV-less event) carries
/// no gap contract.
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
