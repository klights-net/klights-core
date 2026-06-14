use crate::api::watch_event_to_table;
use crate::datastore::CatchUpResource;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreHandle, WatchTarget};
use crate::label_selector::LabelSelector;
use crate::watch::{
    EventType, WatchContentType, WatchCursor, WatchCursorError, WatchEvent, WatchReceiver,
    WatchTopic,
};
use axum::body::Body;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchCatchUpMode {
    NamespacedScoped,
    ClusterOnly,
}

/// Upper bound on how long a watch/list blocks waiting for the serving
/// node's datastore to catch up to the requested resourceVersion before
/// proceeding best-effort. Generous enough to absorb cross-node raft
/// replication lag on a healthy LAN without stalling a client when a node
/// is genuinely partitioned.
pub const READ_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(5);

/// Block until the serving node's datastore has applied changes up to at
/// least `target_rv`, so a watch/list resumed from a resourceVersion
/// minted by another node — e.g. a cluster-wide LIST served by the raft
/// leader, followed by a namespaced WATCH served locally on a follower —
/// is not answered from stale follower state. This is klights' equivalent
/// of the Kubernetes watch-cache `waitUntilFreshAndBlock` freshness
/// guarantee.
///
/// Pure event-driven: every applied write broadcasts a watch event that
/// advances the resource version, so we subscribe once and wake on those
/// events instead of polling. Bounded by [`READ_FRESHNESS_TIMEOUT`]; on
/// timeout we proceed best-effort (the live broadcast and replay paths
/// still converge once the node catches up) rather than failing the
/// request. On the leader — and any node already caught up — this is a
/// single resource-version read and returns immediately.
pub async fn wait_until_datastore_fresh(
    db: &DatastoreHandle,
    target_rv: i64,
    topic: WatchTopic,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) {
    if target_rv <= 0 {
        return;
    }
    // Subscribe BEFORE the first freshness check so an advance landing
    // between the check and the wait is still observed (no lost wakeup).
    let mut fresh_rx = WatchReceiver::from_receiver(db.subscribe_watch(topic));
    if db.get_current_resource_version().await.unwrap_or(0) >= target_rv {
        return;
    }
    let sleep = task_supervisor.sleep("watch_read_freshness_wait", READ_FRESHNESS_TIMEOUT);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => {
                tracing::warn!(
                    target_rv,
                    "watch read-freshness wait timed out; serving best-effort from local state"
                );
                return;
            }
            recv = fresh_rx.recv() => match recv {
                Ok(event) => {
                    // Any applied write with rv >= target proves the
                    // monotonic resource-version counter has reached the
                    // target — no DB round-trip needed on the hot path.
                    if event.resource_version().is_some_and(|rv| rv >= target_rv) {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // A burst of writes overflowed our buffer; re-check the
                    // authoritative counter directly.
                    if db.get_current_resource_version().await.unwrap_or(0) >= target_rv {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}

pub fn object_matches_field_selector(object: &Value, field_selector: Option<&str>) -> bool {
    crate::watch::value_matches_field_selector(object, field_selector)
}

pub fn watch_event_key(event: &WatchEvent) -> Option<(Option<String>, String)> {
    let name = event
        .object
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .map(ToString::to_string)?;
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    Some((namespace, name))
}

/// Extract the membership key for a resource returned by `list_resources`,
/// using the JSON metadata as the source of truth.
///
/// The live broadcast path identifies events by `(metadata.namespace,
/// metadata.name)` extracted from the JSON object (see `watch_event_key`).
/// The baseline membership set must use the SAME extraction or cluster-scoped
/// kinds that the storage layer mis-classifies as namespaced (and back-fills
/// `Resource.namespace = Some("default")`) end up with a key the live event
/// can never match — which silently rewrites every MODIFIED into ADDED and
/// breaks K8s "API operations" conformance for those kinds.
pub fn resource_to_seen_key(resource: &crate::datastore::Resource) -> (Option<String>, String) {
    let namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    (namespace, resource.name.clone())
}

pub fn apply_selector_transition_event(
    mut event: WatchEvent,
    matches_selector: bool,
    matched_keys: &mut HashSet<(Option<String>, String)>,
) -> Option<WatchEvent> {
    if event.event_type == crate::watch::EventType::Bookmark {
        return Some(event);
    }
    let key = watch_event_key(&event)?;
    // Helper: rewrite `event.event_type` AND invalidate any pre-encoded
    // payload. The broadcaster stamps the in-flight event_type into
    // `encoded_payload` at publish time. `serialize_watch_event_line`
    // short-circuits to those cached bytes when present, so any in-memory
    // mutation here without invalidation would land on the wire with the
    // pre-transition type — exactly the regression behind sonobuoy
    // `should observe an object deletion if it stops meeting the
    // requirements of the selector`.
    fn rewrite_event_type(event: &mut WatchEvent, new_type: crate::watch::EventType) {
        if event.event_type != new_type {
            event.event_type = new_type;
            event.encoded_payload = None;
        }
    }
    match event.event_type {
        crate::watch::EventType::Deleted => {
            if matched_keys.remove(&key) || matches_selector {
                Some(event)
            } else {
                None
            }
        }
        crate::watch::EventType::Added | crate::watch::EventType::Modified => {
            if matches_selector {
                if matched_keys.insert(key) {
                    rewrite_event_type(&mut event, crate::watch::EventType::Added);
                }
                Some(event)
            } else if matched_keys.remove(&key) {
                rewrite_event_type(&mut event, crate::watch::EventType::Deleted);
                Some(event)
            } else {
                None
            }
        }
        _ => {
            if matches_selector {
                Some(event)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
pub struct WatchEncodeReuseContext<'a> {
    pub event: &'a WatchEvent,
    pub table_format: bool,
    pub protobuf: bool,
    pub selector_transitioned: bool,
}

#[cfg(test)]
pub fn can_reuse_encoded_watch_payload(ctx: &WatchEncodeReuseContext<'_>) -> bool {
    match ctx.event.encoded_payload {
        Some(ref payload) if payload.content_type == WatchContentType::Json => {
            !ctx.table_format
                && !ctx.protobuf
                && !ctx.selector_transitioned
                && ctx.event.event_type != EventType::Bookmark
        }
        _ => false,
    }
}

pub fn serialize_watch_event_line(event: WatchEvent, kind: &str, table_format: bool) -> Vec<u8> {
    if let Some(ref payload) = event.encoded_payload
        && !table_format
        && payload.content_type == WatchContentType::Json
        && event.event_type != EventType::Bookmark
    {
        let mut buf = Vec::with_capacity(payload.bytes.len() + 1);
        buf.extend_from_slice(&payload.bytes);
        buf.push(b'\n');
        return buf;
    }
    let event = if table_format {
        watch_event_to_table(event, kind)
    } else {
        event
    };
    let mut json = serde_json::to_vec(&event).unwrap_or_default();
    json.push(b'\n');
    json
}

/// Serialize a mid-stream watch failure as a proper `ERROR` watch event:
/// `{"type":"ERROR","object":<metav1.Status>}`. client-go's `StreamWatcher`
/// decodes every frame as `{type, object}` and cannot consume a bare Status.
pub fn serialize_watch_status_line(code: u16, reason: &str, message: &str) -> Vec<u8> {
    let mut json = serde_json::to_vec(&serde_json::json!({
        "type": "ERROR",
        "object": {
            "apiVersion": "v1",
            "kind": "Status",
            "metadata": {},
            "status": "Failure",
            "code": code,
            "reason": reason,
            "message": message,
        }
    }))
    .unwrap_or_default();
    json.push(b'\n');
    json
}

pub async fn spawn_bookmark_tick_stream(
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    task_name: impl Into<String>,
) -> mpsc::Receiver<()> {
    let task_name = task_name.into();
    let sleep_name = format!("{task_name}_sleep");
    let (tick_tx, tick_rx) = mpsc::channel(4);
    let task_supervisor_for_wait = task_supervisor.clone();
    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Timer,
            task_name.clone(),
            async move {
                loop {
                    if tick_tx.send(()).await.is_err() {
                        break;
                    }
                    if task_supervisor_for_wait
                        .sleep(sleep_name.clone(), Duration::from_secs(60))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("failed to spawn bookmark timer task {}: {}", task_name, err);
    }
    tick_rx
}

/// Spawn the bookmark tick timer only when the watch requested bookmarks via
/// `?allowWatchBookmarks=true`. Otherwise no supervised task, no channel, no
/// permit are created — `recv_bookmark_tick` parks forever via `pending()`.
pub async fn maybe_spawn_bookmark_tick_stream(
    enabled: bool,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    task_name: impl Into<String>,
) -> Option<mpsc::Receiver<()>> {
    if !enabled {
        return None;
    }
    Some(spawn_bookmark_tick_stream(task_supervisor, task_name).await)
}

/// Receive the next bookmark tick. When the receiver is `None` (bookmarks
/// disabled), the future never resolves — combine with the other arms in a
/// `tokio::select!` and add `if send_bookmarks` so the disabled case never
/// observes a tick.
pub async fn recv_bookmark_tick(rx: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn bookmark_rv_for_watch_scope(
    has_scope_filter: bool,
    cursor_high_water_rv: i64,
    last_delivered_scoped_rv: i64,
) -> i64 {
    if has_scope_filter {
        last_delivered_scoped_rv
    } else {
        cursor_high_water_rv
    }
}

/// Resolve the resourceVersion a periodic watch BOOKMARK must carry.
///
/// Shared by every client-facing watch builder -- `build_label_selector_watch_stream`
/// for built-in kinds and the custom-resource watch in `custom_resources.rs` --
/// so the scoped-bookmark invariant lives in exactly one place.
///
/// A BOOKMARK promises the client: "you have received every event for this
/// watch's scope with rv <= bookmark_rv; you may resume from it." The serving
/// cursor can observe higher RVs that this HTTP watch later filters out by
/// namespace, label, or field selector, so a *scoped* watch must bookmark only
/// the highest RV it has actually emitted for its scope
/// (`last_delivered_scoped_rv`) -- otherwise client-go reconnects from the
/// too-high bookmark and skips still-undelivered in-scope events (the flaky
/// `[sig-cli] Kubectl client Guestbook application ... readiness-timeout` and
/// the `repro_scoped_watch_bookmark.py` oracle).
///
/// A selector-free watch bookmarks the cursor's full high-water RV; when even
/// that is 0 (a quiet, freshly-established watch that has observed nothing)
/// this falls back to a fresh collection snapshot read so the client still gets
/// a valid, advancing resume point.
/// Inputs shared by every periodic-watch-BOOKMARK emission site, bundled so the
/// shared resolver stays under clippy's argument limit and call sites read by
/// named field.
pub(crate) struct PeriodicBookmarkContext<'a> {
    pub db: &'a DatastoreHandle,
    pub api_version: &'a str,
    pub kind: &'a str,
    pub watch_namespace: Option<&'a str>,
    pub label_selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    pub requested_rv: i64,
    pub has_scope_filter: bool,
    pub cursor_high_water_rv: i64,
    pub last_delivered_scoped_rv: i64,
}

/// Resolve the resourceVersion a periodic watch BOOKMARK must carry.
///
/// Shared by every client-facing watch builder -- `build_label_selector_watch_stream`
/// for built-in kinds and the custom-resource watch in `custom_resources.rs` --
/// so the scoped-bookmark invariant lives in exactly one place.
///
/// A BOOKMARK promises the client: "you have received every event for this
/// watch's scope with rv <= bookmark_rv; you may resume from it." The serving
/// cursor can observe higher RVs that this HTTP watch later filters out by
/// namespace, label, or field selector, so a *scoped* watch must bookmark only
/// the highest RV it has actually emitted for its scope
/// (`last_delivered_scoped_rv`) -- otherwise client-go reconnects from the
/// too-high bookmark and skips still-undelivered in-scope events (the flaky
/// `[sig-cli] Kubectl client Guestbook application ... readiness-timeout` and
/// the `repro_scoped_watch_bookmark.py` oracle).
///
/// A selector-free watch bookmarks the cursor's full high-water RV; when even
/// that is 0 (a quiet, freshly-established watch that has observed nothing)
/// this falls back to a fresh collection snapshot read so the client still gets
/// a valid, advancing resume point.
pub(crate) async fn resolve_periodic_bookmark_rv(ctx: PeriodicBookmarkContext<'_>) -> i64 {
    let PeriodicBookmarkContext {
        db,
        api_version,
        kind,
        watch_namespace,
        label_selector,
        field_selector,
        requested_rv,
        has_scope_filter,
        cursor_high_water_rv,
        last_delivered_scoped_rv,
    } = ctx;
    let mut rv = bookmark_rv_for_watch_scope(
        has_scope_filter,
        cursor_high_water_rv,
        last_delivered_scoped_rv,
    );
    if has_scope_filter && cursor_high_water_rv > rv {
        tracing::warn!(
            target: "klights::watch_diag",
            api_version = %api_version,
            kind = %kind,
            namespace = watch_namespace.unwrap_or(""),
            label_selector = label_selector.unwrap_or(""),
            field_selector = field_selector.unwrap_or(""),
            requested_rv,
            bookmark_rv = rv,
            cursor_high_water_rv,
            "scoped watch bookmark held at delivered scoped rv"
        );
    }
    if rv <= 0 && !has_scope_filter {
        rv = db
            .list_resources(
                api_version,
                kind,
                watch_namespace,
                crate::datastore::ResourceListQuery::new(None, None, Some(1), None),
            )
            .await
            .map(|list| list.resource_version)
            .unwrap_or(0);
    }
    rv
}

pub async fn maybe_spawn_watch_timeout_stream(
    timeout_seconds: Option<u64>,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    task_name: impl Into<String>,
) -> Option<mpsc::Receiver<()>> {
    let timeout_seconds = timeout_seconds?;
    let (timeout_tx, timeout_rx) = mpsc::channel(1);
    let task_name = task_name.into();
    let task_supervisor_for_wait = task_supervisor.clone();
    let sleep_name = format!("{task_name}_sleep");
    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Timer,
            task_name.clone(),
            async move {
                if task_supervisor_for_wait
                    .sleep(sleep_name, Duration::from_secs(timeout_seconds))
                    .await
                    .is_ok()
                {
                    let _ = timeout_tx.send(()).await;
                }
            },
        )
        .await
    {
        tracing::warn!("failed to spawn watch timeout task {}: {}", task_name, err);
    }
    Some(timeout_rx)
}

pub async fn recv_watch_timeout(rx: &mut Option<mpsc::Receiver<()>>) -> Option<()> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

pub struct LabelSelectorWatchStreamRequest<'a> {
    pub db: DatastoreHandle,
    pub rx: WatchReceiver,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub api_version: &'a str,
    pub kind: String,
    pub watch_namespace: Option<String>,
    pub requested_rv: i64,
    /// For an rv-less (`resourceVersion=""`) label/field-selector watch, the
    /// global resourceVersion captured BEFORE this watch subscribed to the
    /// broadcast bus. The live-delivery floor is anchored to this instead of
    /// the post-subscribe baseline-list collection rv, so an object created in
    /// the establishment window (after subscribe, before the baseline list)
    /// is still delivered live rather than skipped as `rv <= floor`. 0 when
    /// not applicable (resume watch, send_initial_events, or selector-less).
    pub rv_less_floor: i64,
    pub send_initial_events: bool,
    pub send_bookmarks: bool,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub table_format: bool,
    pub catch_up_mode: WatchCatchUpMode,
    pub timeout_seconds: Option<u64>,
}

pub fn build_label_selector_watch_stream(request: LabelSelectorWatchStreamRequest<'_>) -> Body {
    let LabelSelectorWatchStreamRequest {
        db,
        rx,
        task_supervisor,
        api_version,
        kind,
        watch_namespace,
        requested_rv,
        rv_less_floor: _rv_less_floor,
        send_initial_events,
        send_bookmarks,
        label_selector,
        field_selector,
        table_format,
        catch_up_mode,
        timeout_seconds,
    } = request;

    let api_version = api_version.to_string();
    let field_selector = field_selector.filter(|selector| !selector.trim().is_empty());
    let parsed_label_selector = label_selector
        .as_deref()
        .filter(|selector| !selector.trim().is_empty())
        .map(LabelSelector::parse)
        .transpose()
        .map_err(|err| format!("Invalid label selector: {err}"));

    let stream = async_stream::stream! {
        let parsed_label_selector = match parsed_label_selector.as_ref() {
            Ok(parsed) => parsed,
            Err(err) => {
                yield Ok::<_, std::convert::Infallible>(serialize_watch_status_line(
                    400,
                    "BadRequest",
                    err,
                ));
                return;
            }
        };
        let has_label_selector = parsed_label_selector.is_some();
        let has_selector = has_label_selector || field_selector.is_some();
        let has_scope_filter = watch_namespace.is_some() || has_selector;

        // Read-freshness: when resuming from a resourceVersion, ensure this
        // node's datastore has applied up to that point before serving. A
        // follower can receive a WATCH whose resourceVersion was minted by
        // the leader (the conformance "service status lifecycle" flow lists
        // cluster-wide on the leader, then watches a namespace), and serving
        // the catch-up query against not-yet-applied follower state would
        // miss events. Event-driven and bounded; a no-op on a fresh node.
        wait_until_datastore_fresh(
            &db,
            requested_rv,
            WatchTopic::new(&api_version, &kind),
            &task_supervisor,
        )
        .await;

        // Track highest RV seen in initial/catch-up phases. The floor must
        // come from the request/list semantics supplied by the handler, not
        // from a post-subscribe `current_rv` read inside the stream; that old
        // high-watermark path can skip an event already buffered by the
        // receiver during watch establishment.
        let mut initial_list_rv = requested_rv;
        let mut last_delivered_scoped_rv = requested_rv;

        if !send_initial_events && requested_rv > 0 {
            // If the resume point predates the retained watch-event window, the
            // catch-up below (current-state of modified resources) cannot
            // replay deletions that have aged out — the client would silently
            // keep phantom entries. Per Kubernetes "too old resource version"
            // semantics, answer 410 Gone so the reflector performs a fresh
            // list+watch instead.
            if let Ok(Some(earliest)) = db.earliest_watch_event_rv().await
                && requested_rv + 1 < earliest {
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_line(
                        410,
                        "Expired",
                        "too old resource version: requested resourceVersion is older than the watch history window",
                    ));
                    return;
                }
            let missed = match catch_up_mode {
                WatchCatchUpMode::NamespacedScoped => {
                    db.list_resources_modified_since(
                        &api_version,
                        &kind,
                        watch_namespace.clone().as_deref(),
                        requested_rv,
                    )
                    .await
                }
                WatchCatchUpMode::ClusterOnly => {
                    db.list_cluster_resources_modified_since(&api_version, &kind, requested_rv)
                        .await
                }
            };

            if let Ok(missed) = missed {
                for catchup in missed {
                    let resource = catchup.resource.clone();
                    if resource.resource_version <= initial_list_rv {
                        if catchup.event_type.as_ref() == "ADDED" {
                            tracing::warn!(
                                target: "klights::watch_diag",
                                kind = %kind,
                                namespace = watch_namespace.as_deref().unwrap_or(""),
                                name = %resource.name,
                                added_rv = resource.resource_version,
                                requested_rv,
                                initial_list_rv,
                                "catch-up dropped an ADDED event (rv <= resume floor)"
                            );
                        }
                        continue;
                    }
                    initial_list_rv = initial_list_rv.max(resource.resource_version);
                    let event = CatchUpResource {
                        resource: catchup.resource,
                        event_type: catchup.event_type,
                    }
                    .into_watch_event();
                    if !event.matches_filter_parsed(
                        &kind,
                        watch_namespace.as_deref(),
                        parsed_label_selector.as_ref(),
                    ) || !event.matches_field_selector(field_selector.as_deref()) {
                        continue;
                    }
                    if let Some(rv) = event.resource_version() {
                        last_delivered_scoped_rv = last_delivered_scoped_rv.max(rv);
                    }
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                        event,
                        &kind,
                        table_format,
                    ));
                }
            }
        }

        // Track resources visible to this watcher for label-selector-aware transitions.
        // If a MODIFIED event enters selector view -> send ADDED.
        // If a visible resource leaves selector view -> send DELETED.
        // Keyed by (namespace, name) to avoid collisions across namespaces
        // in cluster-wide watches.
        let mut seen_resources: HashSet<(Option<String>, String)> = HashSet::new();
        // rvs already emitted to the client as ADDED from the baseline list
        // below; used to seed the cursor so the (intentionally lower) live
        // floor does not re-deliver them.
        let mut baseline_delivered_rvs: Vec<i64> = Vec::new();
        let mut baseline_low_rv_allowlist: Vec<((Option<String>, String), i64)> = Vec::new();

        // Label-selector watches need a current membership baseline. For
        // resourceVersion-less selector watches, Kubernetes-compatible clients
        // such as the ServiceAccount lifecycle conformance test expect existing
        // matching objects to be delivered as ADDED. Selector-free watches keep
        // the default no-replay behavior.
        if has_selector && !send_initial_events
            && let Ok(baseline) = db
                .list_resources(&api_version, &kind, watch_namespace.clone().as_deref(), crate::datastore::ResourceListQuery::new(label_selector.clone().as_deref(), field_selector.as_deref(), None, None))
                .await
            {
                for resource in baseline.items {
                    let key = resource_to_seen_key(&resource);
                    seen_resources.insert(key.clone());
                    if requested_rv <= 0 {
                        baseline_delivered_rvs.push(resource.resource_version);
                        last_delivered_scoped_rv =
                            last_delivered_scoped_rv.max(resource.resource_version);
                        let event = CatchUpResource {
                            resource,
                            event_type: std::borrow::Cow::Borrowed("ADDED"),
                        }
                        .into_watch_event();
                        yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                            event,
                            &kind,
                            table_format,
                        ));
                    } else {
                        baseline_low_rv_allowlist.push((key, resource.resource_version));
                    }
                }
                // For rv-less selector watches, keep the live cursor floor at
                // 0. The baseline items just emitted are deduped by exact RV
                // below, so a numeric floor is unnecessary; using either the
                // pre-subscribe global RV or the post-subscribe collection RV
                // can drop a genuinely live ADDED whose replicated commit is
                // broadcast after watch establishment with a lower RV.
            }

        if send_initial_events {
            let initial_list = db
                .list_resources(&api_version, &kind, watch_namespace.clone().as_deref(), crate::datastore::ResourceListQuery::new(label_selector.clone().as_deref(), field_selector.as_deref(), None, None))
                .await;

            let mut last_rv = requested_rv;
            if let Ok(list) = initial_list {
                for resource in list.items {
                    if requested_rv > 0 && resource.resource_version <= requested_rv {
                        continue;
                    }

                    last_rv = last_rv.max(resource.resource_version);
                    if has_selector {
                        seen_resources.insert(resource_to_seen_key(&resource));
                    }
                    let event = CatchUpResource {
                        resource,
                        event_type: std::borrow::Cow::Borrowed("ADDED"),
                    }
                    .into_watch_event();
                    if let Some(rv) = event.resource_version() {
                        last_delivered_scoped_rv = last_delivered_scoped_rv.max(rv);
                    }
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                        event,
                        &kind,
                        table_format,
                    ));
                }
                // Anchor the initial-events-end bookmark (and the live-event
                // floor) to the collection's snapshot resourceVersion, not
                // just `max(item rv)`. K8s requires the `initial-events-end`
                // bookmark to report the resourceVersion at which the initial
                // list was taken so a WatchList client can resume from it. For
                // an EMPTY initial list (e.g. a label-selector informer over a
                // fresh namespace, which is exactly the `[sig-scheduling]
                // LimitRange ... defaults` conformance flow) `max(item rv)` is
                // absent, leaving the bookmark/floor at the stale
                // `requested_rv` (0 when the client sends `resourceVersion=""`)
                // — an invalid resume point. This is the WatchList sibling of
                // the complete-list snapshot-RV fix on the plain list path.
                last_rv = last_rv.max(list.resource_version);
            }

            initial_list_rv = last_rv;
            last_delivered_scoped_rv = last_delivered_scoped_rv.max(last_rv);

            let bookmark_event =
                WatchEvent::bookmark_initial_events_end(last_rv, &api_version, &kind);
            yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                bookmark_event,
                &kind,
                table_format,
            ));
        }

        let replay_target = match (catch_up_mode, watch_namespace.clone()) {
            (WatchCatchUpMode::NamespacedScoped, Some(ns)) => {
                WatchTarget::namespaced_in_namespace(api_version.clone(), kind.clone(), ns)
            }
            (WatchCatchUpMode::NamespacedScoped, None) => {
                WatchTarget::namespaced(api_version.clone(), kind.clone())
            }
            (WatchCatchUpMode::ClusterOnly, _) => WatchTarget::cluster(api_version.clone(), kind.clone()),
        };
        let replay_source = DatastoreWatchReplaySource::new(db.clone(), vec![replay_target]);
        let mut cursor = WatchCursor::new(rx, replay_source, initial_list_rv.max(requested_rv));
        // Dedup the baseline matches already emitted as ADDED so the
        // (intentionally lower) live floor does not re-deliver them.
        for rv in baseline_delivered_rvs {
            cursor.mark_delivered(rv);
        }
        for ((namespace, name), after_rv) in baseline_low_rv_allowlist {
            cursor.allow_low_rv_for_key(namespace, name, after_rv);
        }
        if requested_rv > 0 {
            match cursor.prime_replay_or_expired().await {
                Ok(_) => {}
                Err(WatchCursorError::Expired) => {
                    // Resume point predates the retained watch-event window;
                    // tell the client to relist (HTTP 410 Gone semantics).
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_line(
                        410,
                        "Expired",
                        "too old resource version: requested resourceVersion is older than the watch history window",
                    ));
                    return;
                }
                Err(err) => {
                    tracing::warn!("Initial watch replay failed for {}: {:#?}", kind, err);
                }
            }
        }

        let bookmark_task_name = format!("watch_stream_bookmarks_{}_{}", api_version, kind);
        let mut bookmark_ticks = maybe_spawn_bookmark_tick_stream(
            send_bookmarks,
            task_supervisor.clone(),
            bookmark_task_name,
        )
        .await;
        let timeout_task_name = format!("watch_stream_timeout_{}_{}", api_version, kind);
        let mut timeout_tick = maybe_spawn_watch_timeout_stream(
            timeout_seconds,
            task_supervisor.clone(),
            timeout_task_name,
        )
        .await;

        loop {
            tokio::select! {
                Some(()) = recv_watch_timeout(&mut timeout_tick) => {
                    break;
                }
                result = cursor.next_event(&task_supervisor) => {
                    let event = match result {
                        Ok(event) => event,
                        Err(WatchCursorError::Replay(err)) => {
                            match watch_namespace.as_deref() {
                                Some(ns) => tracing::warn!("Watch replay failed for {}/{}: {:#}", ns, kind, err),
                                None => tracing::warn!("Watch replay failed for {}: {:#}", kind, err),
                            }
                            continue;
                        }
                        Err(WatchCursorError::Expired) => {
                            // The live stream fell behind and the missed
                            // events have aged out of the replay window. Emit
                            // 410 Gone so the client reflector relists instead
                            // of silently missing events (e.g. pod deletions).
                            yield Ok::<_, std::convert::Infallible>(serialize_watch_status_line(
                                410,
                                "Expired",
                                "too old resource version: watch fell behind the history window",
                            ));
                            break;
                        }
                        Err(WatchCursorError::Closed) => {
                            tracing::debug!("Watch broadcast channel closed");
                            break;
                        }
                    };

                    if has_selector {
                        let matches = event.matches_filter_parsed(&kind, watch_namespace.as_deref(), parsed_label_selector.as_ref())
                            && event.matches_field_selector(field_selector.as_deref());
                        // Canary: a live MODIFIED for an object the watcher
                        // believes it has never seen is rewritten to ADDED
                        // (object absent from `seen_resources`); a MODIFIED that
                        // no longer matches is rewritten to a synthetic DELETED.
                        // Both silently swallow the MODIFIED a client may be
                        // waiting for — the exact signature of the flaky
                        // `[sig-auth] ServiceAccounts ... lifecycle` failure
                        // (`failed to find MODIFIED event`). The floor-skip
                        // canary in WatchCursor cannot see this — the rewrite
                        // happens after the cursor delivered the event. Logged
                        // at WARN under the same target so it surfaces without a
                        // debug build. Legitimate selector transitions (a label
                        // genuinely changing in/out of view) also log here; the
                        // signal is a rewrite for an object whose selector
                        // membership did not actually change.
                        let pre_transition_type = event.event_type;
                        let transition_key = matches!(
                            pre_transition_type,
                            EventType::Modified | EventType::Deleted
                        )
                        .then(|| watch_event_key(&event))
                        .flatten();
                        if let Some(transitioned) = apply_selector_transition_event(
                            event,
                            matches,
                            &mut seen_resources,
                        ) {
                            if let Some(rv) = transitioned.resource_version() {
                                last_delivered_scoped_rv = last_delivered_scoped_rv.max(rv);
                            }
                            if pre_transition_type == EventType::Modified
                                && transitioned.event_type != EventType::Modified
                                && let Some((ns, name)) = transition_key.as_ref()
                            {
                                tracing::warn!(
                                    target: "klights::watch_diag",
                                    kind = %kind,
                                    namespace = ns.as_deref().unwrap_or(""),
                                    name = %name,
                                    rewritten_to = %transitioned.event_type,
                                    matches_selector = matches,
                                    rv = transitioned.resource_version().unwrap_or(0),
                                    "selector watch rewrote a live MODIFIED (client awaiting MODIFIED may miss it)"
                                );
                            }
                            yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                                transitioned,
                                &kind,
                                table_format,
                            ));
                        }
                    } else if event.matches_filter_parsed(&kind, watch_namespace.as_deref(), parsed_label_selector.as_ref())
                        && event.matches_field_selector(field_selector.as_deref()) {
                        if let Some(rv) = event.resource_version() {
                            last_delivered_scoped_rv = last_delivered_scoped_rv.max(rv);
                        }
                        yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                            event,
                            &kind,
                            table_format,
                        ));
                    }
                }
                Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                    let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
                        db: &db,
                        api_version: &api_version,
                        kind: &kind,
                        watch_namespace: watch_namespace.as_deref(),
                        label_selector: label_selector.as_deref(),
                        field_selector: field_selector.as_deref(),
                        requested_rv,
                        has_scope_filter,
                        cursor_high_water_rv: cursor.high_water_rv(),
                        last_delivered_scoped_rv,
                    })
                    .await;
                    let event = WatchEvent::bookmark_typed(rv, &api_version, &kind);
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_event_line(
                        event,
                        &kind,
                        table_format,
                    ));
                }
            }
        }
    };

    Body::from_stream(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
    use std::sync::Arc;

    #[test]
    fn watch_status_line_is_wrapped_as_error_event() {
        let line = serialize_watch_status_line(410, "Expired", "too old resource version");
        assert_eq!(line.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&line).unwrap();
        // client-go StreamWatcher requires {type, object}, not a bare Status.
        assert_eq!(value["type"], "ERROR");
        assert_eq!(value["object"]["kind"], "Status");
        assert_eq!(value["object"]["code"], 410);
        assert_eq!(value["object"]["reason"], "Expired");
        assert_eq!(value["object"]["status"], "Failure");
    }

    #[test]
    fn catchup_resource_event_type_uses_static_literal_for_added() {
        // The watch hot path constructs CatchUpResource per event during
        // initial-list. Holding event_type as Cow<'static, str> avoids the
        // per-event String allocation when the literal "ADDED" is reused.
        // Confirm the static literal flows through unchanged (no deep copy).
        let resource = crate::datastore::Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: Some("default".into()),
            name: "p1".into(),
            uid: "uid-p1".into(),
            resource_version: 1,
            data: std::sync::Arc::new(serde_json::json!({"metadata": {"name": "p1"}})),
        };
        let event = CatchUpResource {
            resource,
            event_type: std::borrow::Cow::Borrowed("ADDED"),
        };
        match &event.event_type {
            std::borrow::Cow::Borrowed(s) => assert_eq!(*s, "ADDED"),
            std::borrow::Cow::Owned(_) => panic!("static literal must stay borrowed"),
        }
        let watch_event = event.into_watch_event();
        assert_eq!(watch_event.event_type, EventType::Added);
    }

    #[test]
    fn selector_bookmark_rv_stays_at_delivered_scope_frontier() {
        assert_eq!(
            bookmark_rv_for_watch_scope(true, 91, 42),
            42,
            "selector watch bookmarks must not advertise unrelated RVs observed by the cursor"
        );
    }

    #[test]
    fn selector_free_bookmark_rv_uses_cursor_frontier() {
        assert_eq!(
            bookmark_rv_for_watch_scope(false, 91, 42),
            91,
            "selector-free watches can bookmark the cursor's full high-water RV"
        );
    }

    /// Regression guard for the custom-resource watch builder, which used to mint
    /// every periodic BOOKMARK from `db.list_resources(...).resource_version` --
    /// the GLOBAL storage snapshot RV. Out-of-scope churn (other namespaces or
    /// labels) pushed that global RV far past the last in-scope event the watch
    /// had actually delivered, so client-go resumed from the bookmark and skipped
    /// still-undelivered in-scope events (the flaky `[sig-cli] Kubectl Guestbook
    /// ... readiness-timeout` and the `repro_scoped_watch_bookmark.py` oracle).
    /// A scoped watch must bookmark only the highest RV it has emitted for its
    /// scope, ignoring both the cursor high-water and a fresh collection read.
    #[tokio::test]
    async fn resolve_periodic_bookmark_rv_scoped_anchors_to_delivered_frontier() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        // Seed unrelated objects so a naive "collection RV" read would return a
        // large global value; the scoped resolver must NOT touch it.
        for i in 0..10 {
            ds.create_resource(
                "v1",
                "ConfigMap",
                Some("noise"),
                &format!("n{i}"),
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": format!("n{i}"), "namespace": "noise"}
                }),
            )
            .await
            .unwrap();
        }
        let collection_rv = handle.get_current_resource_version().await.unwrap();
        assert!(
            collection_rv > 1,
            "test fixture: global RV must be non-trivial, got {collection_rv}"
        );

        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &handle,
            api_version: "v1",
            kind: "ConfigMap",
            watch_namespace: Some("watched"),
            label_selector: Some("tier=frontend"),
            field_selector: None,
            requested_rv: 1,
            has_scope_filter: true,
            cursor_high_water_rv: collection_rv,
            last_delivered_scoped_rv: 1,
        })
        .await;
        assert_eq!(
            rv, 1,
            "scoped watch bookmark must stay at the delivered scope frontier (1), \
             not the global cursor/collection RV ({collection_rv})"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_rv_selector_free_uses_cursor_high_water() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &handle,
            api_version: "v1",
            kind: "ConfigMap",
            watch_namespace: None,
            label_selector: None,
            field_selector: None,
            requested_rv: 1,
            has_scope_filter: false,
            cursor_high_water_rv: 500,
            last_delivered_scoped_rv: 42,
        })
        .await;
        assert_eq!(
            rv, 500,
            "selector-free watch may bookmark the cursor's full high-water RV"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_rv_selector_free_falls_back_to_collection_when_zero() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        ds.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "seed",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "seed", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
        let collection_rv = handle.get_current_resource_version().await.unwrap();

        // A selector-free watch that has observed nothing yet (quiet,
        // freshly established) must still emit a valid, advancing resume point.
        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &handle,
            api_version: "v1",
            kind: "ConfigMap",
            watch_namespace: None,
            label_selector: None,
            field_selector: None,
            requested_rv: 0,
            has_scope_filter: false,
            cursor_high_water_rv: 0,
            last_delivered_scoped_rv: 0,
        })
        .await;
        assert_eq!(
            rv, collection_rv,
            "selector-free watch with no observed RV falls back to a fresh collection snapshot RV"
        );
    }

    #[tokio::test]
    async fn read_freshness_wait_is_noop_when_zero_or_already_fresh() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        // resourceVersion 0 / unset: nothing to wait for.
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            wait_until_datastore_fresh(&handle, 0, WatchTopic::new("v1", "Pod"), &supervisor),
        )
        .await
        .expect("zero target must return immediately");

        // Already at/above the current rv: return without blocking.
        let cur = handle.get_current_resource_version().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            wait_until_datastore_fresh(&handle, cur, WatchTopic::new("v1", "Pod"), &supervisor),
        )
        .await
        .expect("already-fresh target must return immediately");
        let _ = ds;
    }

    #[tokio::test]
    async fn read_freshness_wait_wakes_on_applied_write() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let base = handle.get_current_resource_version().await.unwrap();
        let target = base + 1;

        let waiter = wait_until_datastore_fresh(
            &handle,
            target,
            WatchTopic::new("v1", "ConfigMap"),
            &supervisor,
        );
        let writer = async {
            // Let the waiter subscribe and run its initial check first so
            // we exercise the event-driven wakeup, not the fast path.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            ds.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "freshness-cm",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "freshness-cm", "namespace": "default"}
                }),
            )
            .await
            .unwrap();
        };

        // Must complete well under READ_FRESHNESS_TIMEOUT: if the wait
        // missed the broadcast it would block to the 5s best-effort cap and
        // this 1s bound would fire.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(waiter, writer)
        })
        .await
        .expect("freshness wait must wake on the applied write");

        assert!(handle.get_current_resource_version().await.unwrap() >= target);
    }

    fn make_event(event_type: EventType, namespace: Option<&str>, name: &str) -> WatchEvent {
        let mut obj = serde_json::json!({"metadata": {"name": name}});
        if let Some(ns) = namespace {
            obj["metadata"]["namespace"] = serde_json::Value::String(ns.to_string());
        }
        WatchEvent {
            event_type,
            object: Arc::new(obj),
            encoded_payload: None,
        }
    }

    #[test]
    fn apply_selector_transition_distinguishes_same_name_different_namespace() {
        let mut matched_keys: HashSet<(Option<String>, String)> = HashSet::new();

        // ADDED a/shared matching selector
        let result = apply_selector_transition_event(
            make_event(EventType::Added, Some("a"), "shared"),
            true,
            &mut matched_keys,
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().event_type, EventType::Added);

        // ADDED b/shared matching selector — must NOT collide with a/shared
        let result = apply_selector_transition_event(
            make_event(EventType::Added, Some("b"), "shared"),
            true,
            &mut matched_keys,
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().event_type, EventType::Added);
        assert_eq!(matched_keys.len(), 2);

        // MODIFIED a/shared stops matching -> DELETED for a/shared only
        let result = apply_selector_transition_event(
            make_event(EventType::Modified, Some("a"), "shared"),
            false,
            &mut matched_keys,
        );
        assert!(result.is_some());
        let ev = result.unwrap();
        assert_eq!(ev.event_type, EventType::Deleted);
        assert_eq!(matched_keys.len(), 1);
        assert!(!matched_keys.contains(&(Some("a".into()), "shared".into())));
        assert!(matched_keys.contains(&(Some("b".into()), "shared".into())));

        // MODIFIED b/shared still matches -> plain MODIFIED, not ADDED
        let result = apply_selector_transition_event(
            make_event(EventType::Modified, Some("b"), "shared"),
            true,
            &mut matched_keys,
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().event_type, EventType::Modified);
    }

    #[test]
    fn apply_selector_transition_then_serialize_reports_post_transition_type() {
        // Production bug behind sonobuoy "[sig-api-machinery] Watchers should
        // observe an object deletion if it stops meeting the requirements of
        // the selector":
        //
        // The broadcaster pre-encodes the WatchEvent's JSON bytes at publish
        // time, stamping the event type into `encoded_payload`. When a
        // subsequent live MODIFIED event arrives whose object no longer
        // matches the selector, `apply_selector_transition_event` correctly
        // mutates `event.event_type` to Deleted in memory — but it leaves
        // the cached `encoded_payload` intact. `serialize_watch_event_line`
        // short-circuits to the cached bytes for non-bookmark JSON events,
        // so the client sees `"type":"MODIFIED"` on the wire even though
        // the in-memory event_type is Deleted. The earlier per-helper unit
        // tests asserted on event_type only and missed this.
        //
        // Drive the full transition+serialize pipeline and assert the
        // serialized output matches the post-transition type.
        use crate::watch::{EventType, WatchContentType, encode_watch_payload};
        let mut matched_keys: HashSet<(Option<String>, String)> = HashSet::new();
        // Seed prior match so the relabel event triggers the Modified→Deleted
        // branch.
        matched_keys.insert((Some("watch-9".into()), "cm".into()));

        let mut relabel = make_event(EventType::Modified, Some("watch-9"), "cm");
        relabel.object = Arc::new(serde_json::json!({
            "kind": "ConfigMap",
            "metadata": {"name": "cm", "namespace": "watch-9", "labels": {"k": "stops-matching"}},
        }));
        // Mirror production: the broadcaster pre-encodes the wire JSON, so
        // the cached bytes carry `"type":"MODIFIED"`.
        relabel.encoded_payload = encode_watch_payload(&relabel, WatchContentType::Json).ok();
        assert!(relabel.encoded_payload.is_some());

        let transitioned = apply_selector_transition_event(relabel, false, &mut matched_keys)
            .expect("selector transition must emit a synthetic event");
        assert_eq!(transitioned.event_type, EventType::Deleted);

        let wire = serialize_watch_event_line(transitioned, "ConfigMap", false);
        let wire_str = std::str::from_utf8(&wire).unwrap();
        assert!(
            wire_str.contains("\"type\":\"DELETED\""),
            "serialized wire bytes must report the post-transition type DELETED, got: {wire_str}"
        );
        assert!(
            !wire_str.contains("\"type\":\"MODIFIED\""),
            "stale MODIFIED type leaked from cached encoded_payload: {wire_str}"
        );
    }

    #[test]
    fn field_selector_transition_then_serialize_reports_synthetic_deleted() {
        use crate::watch::{EventType, WatchContentType, encode_watch_payload};
        let mut matched_keys: HashSet<(Option<String>, String)> = HashSet::new();
        matched_keys.insert((Some("default".into()), "pod-a".into()));

        let mut event = make_event(EventType::Modified, Some("default"), "pod-a");
        event.object = Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod-a", "namespace": "default"},
            "spec": {"nodeName": "node-b"},
        }));
        event.encoded_payload = encode_watch_payload(&event, WatchContentType::Json).ok();
        assert!(
            !event.matches_field_selector(Some("spec.nodeName=node-a")),
            "test event must leave the field selector"
        );

        let transitioned = apply_selector_transition_event(event, false, &mut matched_keys)
            .expect("field selector transition must emit synthetic delete");
        assert_eq!(transitioned.event_type, EventType::Deleted);
        assert!(
            !matched_keys.contains(&(Some("default".into()), "pod-a".into())),
            "synthetic delete must evict the prior field-selector match"
        );

        let wire = serialize_watch_event_line(transitioned, "Pod", false);
        let wire_str = std::str::from_utf8(&wire).unwrap();
        assert!(
            wire_str.contains("\"type\":\"DELETED\""),
            "wire event must expose synthetic DELETED after field-selector transition, got: {wire_str}"
        );
        assert!(
            !wire_str.contains("\"type\":\"MODIFIED\""),
            "cached MODIFIED payload must be invalidated for field-selector transition: {wire_str}"
        );
    }

    /// Helpers for the resource_to_seen_key/watch_event_key parity tests below.
    fn make_resource(
        kind: &str,
        api_version: &str,
        stored_namespace: Option<&str>,
        data_namespace: Option<&str>,
        name: &str,
    ) -> crate::datastore::Resource {
        let mut metadata = serde_json::json!({"name": name});
        if let Some(ns) = data_namespace {
            metadata["namespace"] = serde_json::Value::String(ns.to_string());
        }
        crate::datastore::Resource {
            id: 0,
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: stored_namespace.map(str::to_string),
            name: name.into(),
            uid: "uid".into(),
            resource_version: 1,
            data: std::sync::Arc::new(serde_json::json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": metadata,
            })),
        }
    }

    fn make_event_from_resource(
        event_type: EventType,
        resource: &crate::datastore::Resource,
    ) -> WatchEvent {
        WatchEvent {
            event_type,
            object: resource.data.clone(),
            encoded_payload: None,
        }
    }

    /// Regression guard for the sonobuoy "should support …​ API operations"
    /// failures (IngressClass, ValidatingAdmissionPolicy,
    /// ValidatingAdmissionPolicyBinding). The storage layer mis-classifies
    /// these cluster-scoped kinds as namespaced and back-fills
    /// `Resource.namespace = Some("default")`, but the broadcast event JSON
    /// has no `metadata.namespace` (cluster-scoped → omitted by
    /// `hydrate_watch_event_data`). The baseline-insert key MUST match the
    /// key the live broadcast path will produce, otherwise the first PATCH
    /// rewrites MODIFIED→ADDED and the conformance test fails.
    #[test]
    fn resource_to_seen_key_matches_watch_event_key_for_misclassified_cluster_scoped() {
        let resource = make_resource(
            "IngressClass",
            "networking.k8s.io/v1",
            Some("default"), // storage row was mis-classified into namespaced table
            None,            // but the JSON metadata has no namespace
            "ic1",
        );
        let baseline_key = resource_to_seen_key(&resource);
        let event = make_event_from_resource(EventType::Modified, &resource);
        let live_key = watch_event_key(&event).expect("event must yield key");
        assert_eq!(
            baseline_key, live_key,
            "baseline insert key and live event key must agree so MODIFIED stays MODIFIED"
        );
        assert_eq!(baseline_key, (None, "ic1".into()));
    }

    /// Cluster-scoped kinds the storage layer classifies correctly
    /// (FlowSchema, PriorityLevelConfiguration, Node, etc.) must keep
    /// producing `(None, name)` keys on both sides.
    #[test]
    fn resource_to_seen_key_matches_watch_event_key_for_correctly_classified_cluster_scoped() {
        let resource = make_resource(
            "FlowSchema",
            "flowcontrol.apiserver.k8s.io/v1",
            None,
            None,
            "fs1",
        );
        let baseline_key = resource_to_seen_key(&resource);
        let event = make_event_from_resource(EventType::Modified, &resource);
        let live_key = watch_event_key(&event).expect("event must yield key");
        assert_eq!(baseline_key, live_key);
        assert_eq!(baseline_key, (None, "fs1".into()));
    }

    /// Namespaced kinds must keep producing `(Some(ns), name)` keys on both
    /// sides — the fix must not regress the same-name-different-namespace
    /// guard exercised by `apply_selector_transition_distinguishes_*`.
    #[test]
    fn resource_to_seen_key_matches_watch_event_key_for_namespaced() {
        let resource = make_resource("ConfigMap", "v1", Some("ns-a"), Some("ns-a"), "cm-shared");
        let baseline_key = resource_to_seen_key(&resource);
        let event = make_event_from_resource(EventType::Modified, &resource);
        let live_key = watch_event_key(&event).expect("event must yield key");
        assert_eq!(baseline_key, live_key);
        assert_eq!(baseline_key, (Some("ns-a".into()), "cm-shared".into()));
    }

    /// Cluster-wide namespaced watch: two namespaces hold a same-named
    /// resource. Both baselines must produce distinct keys so a MODIFIED on
    /// one does not appear as ADDED on the other watcher's view.
    #[test]
    fn resource_to_seen_key_preserves_namespace_partitioning_for_same_name() {
        let a = make_resource("ConfigMap", "v1", Some("a"), Some("a"), "shared");
        let b = make_resource("ConfigMap", "v1", Some("b"), Some("b"), "shared");
        let ka = resource_to_seen_key(&a);
        let kb = resource_to_seen_key(&b);
        assert_ne!(ka, kb, "namespace must partition same-name resources");
        assert_eq!(ka, (Some("a".into()), "shared".into()));
        assert_eq!(kb, (Some("b".into()), "shared".into()));
    }

    /// End-to-end regression guard at the helper layer: simulate the failing
    /// IngressClass sonobuoy flow against `apply_selector_transition_event`.
    /// Baseline insert uses `resource_to_seen_key` (post-fix), live event
    /// uses `watch_event_key`. A subsequent MODIFIED must stay MODIFIED.
    #[test]
    fn selector_transition_keeps_modified_after_baseline_for_misclassified_cluster_scoped() {
        let mut matched_keys: HashSet<(Option<String>, String)> = HashSet::new();
        let baseline = make_resource(
            "IngressClass",
            "networking.k8s.io/v1",
            Some("default"),
            None,
            "ic1",
        );
        matched_keys.insert(resource_to_seen_key(&baseline));

        let live = make_event_from_resource(EventType::Modified, &baseline);
        let result = apply_selector_transition_event(live, true, &mut matched_keys)
            .expect("modified must be delivered, not swallowed");
        assert_eq!(
            result.event_type,
            EventType::Modified,
            "live MODIFIED after baseline must NOT be rewritten to ADDED"
        );
    }

    #[test]
    fn apply_selector_transition_cluster_scoped_uses_none_namespace() {
        let mut matched_keys: HashSet<(Option<String>, String)> = HashSet::new();

        // Cluster-scoped resource (no namespace)
        let result = apply_selector_transition_event(
            make_event(EventType::Added, None, "my-node"),
            true,
            &mut matched_keys,
        );
        assert!(result.is_some());
        assert!(matched_keys.contains(&(None, "my-node".into())));

        // Namespaced resource with same name must be separate
        let result = apply_selector_transition_event(
            make_event(EventType::Added, Some("default"), "my-node"),
            true,
            &mut matched_keys,
        );
        assert!(result.is_some());
        assert_eq!(matched_keys.len(), 2);
    }

    #[tokio::test]
    async fn disabled_bookmark_tick_source_spawns_no_timer_task() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let _ticks =
            maybe_spawn_bookmark_tick_stream(false, supervisor.clone(), "disabled_bookmarks_test")
                .await;

        assert!(
            supervisor
                .active_tasks(Some(TaskCategory::Timer))
                .is_empty(),
            "watches without allowWatchBookmarks must not spawn timer work"
        );
        assert_eq!(
            supervisor.managed_task_count(),
            0,
            "no managed task entries should leak when bookmarks are disabled"
        );
    }

    #[tokio::test]
    async fn enabled_bookmark_tick_source_spawns_timer_task() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let _ticks =
            maybe_spawn_bookmark_tick_stream(true, supervisor.clone(), "enabled_bookmarks_test")
                .await;

        // The timer task must be visible as an active Timer-category task.
        let active = supervisor.active_tasks(Some(TaskCategory::Timer));
        assert!(
            active
                .iter()
                .any(|t| t.name.contains("enabled_bookmarks_test")),
            "watches with allowWatchBookmarks must spawn the bookmark timer (active: {:?})",
            active
        );
    }

    #[tokio::test]
    async fn recv_bookmark_tick_with_none_parks_indefinitely() {
        // When bookmarks are disabled, the watch select arm calls
        // recv_bookmark_tick(&mut None). It must park forever — otherwise the
        // select arm would wake up unexpectedly and either dispatch a stale
        // bookmark or busy-loop.
        let mut rx: Option<mpsc::Receiver<()>> = None;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            recv_bookmark_tick(&mut rx),
        )
        .await;
        assert!(
            result.is_err(),
            "recv_bookmark_tick must NOT resolve within 100ms when receiver is None; got: {result:?}"
        );
    }

    #[tokio::test]
    async fn recv_bookmark_tick_with_some_resolves_when_sender_emits() {
        // Sanity-check the Some branch: when the channel sender emits,
        // recv_bookmark_tick resolves to Some(()).
        let (tx, rx) = mpsc::channel::<()>(1);
        let mut rx_opt = Some(rx);
        tx.send(()).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            recv_bookmark_tick(&mut rx_opt),
        )
        .await;
        assert!(
            matches!(result, Ok(Some(()))),
            "expected Ok(Some(())), got: {result:?}"
        );
    }

    #[tokio::test]
    async fn recv_bookmark_tick_inside_select_loses_race_when_disabled() {
        // The realistic scenario: two select arms compete and the bookmark
        // arm (with rx=None) must never win. Stage a competitor that wakes
        // after 20ms and verify the bookmark arm doesn't race ahead of it.
        let mut rx: Option<mpsc::Receiver<()>> = None;
        let won_by_bookmarks = tokio::time::timeout(std::time::Duration::from_millis(200), async {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => false,
                _ = recv_bookmark_tick(&mut rx) => true,
            }
        })
        .await
        .expect("select must complete within 200ms — competitor wins after 20ms");
        assert!(
            !won_by_bookmarks,
            "the disabled bookmark arm must NOT win the select race"
        );
    }

    #[test]
    fn watch_json_event_reuses_encoded_bytes_for_identical_subscribers() {
        let pending = crate::datastore::create_pending_watch_event(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            1,
            "ADDED",
            serde_json::json!({"metadata": {"name": "p1"}}),
        );
        let event1 = pending.event.clone();
        let event2 = pending.event.clone();

        let p1 = event1
            .encoded_payload
            .as_ref()
            .expect("must have pre-encoded payload");
        let p2 = event2
            .encoded_payload
            .as_ref()
            .expect("must have pre-encoded payload");

        assert_eq!(p1.content_type, WatchContentType::Json);
        assert_eq!(
            p1.bytes.as_ptr(),
            p2.bytes.as_ptr(),
            "cloned events must share backing memory"
        );

        let line1 = serialize_watch_event_line(event1, "Pod", false);
        let line2 = serialize_watch_event_line(event2, "Pod", false);
        assert_eq!(
            line1, line2,
            "identical subscribers must produce identical output"
        );

        let expected: serde_json::Value =
            serde_json::from_slice(&line1[..line1.len() - 1]).unwrap();
        assert_eq!(expected["type"], "ADDED");
        assert_eq!(expected["object"]["metadata"]["name"], "p1");
    }

    #[test]
    fn watch_table_and_normal_subscribers_do_not_share_wrong_payload() {
        let pending = crate::datastore::create_pending_watch_event(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            1,
            "ADDED",
            serde_json::json!({"metadata": {"name": "p1"}}),
        );

        let ctx_json = WatchEncodeReuseContext {
            event: &pending.event,
            table_format: false,
            protobuf: false,
            selector_transitioned: false,
        };
        assert!(can_reuse_encoded_watch_payload(&ctx_json));

        let ctx_table = WatchEncodeReuseContext {
            event: &pending.event,
            table_format: true,
            protobuf: false,
            selector_transitioned: false,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_table));

        let ctx_protobuf = WatchEncodeReuseContext {
            event: &pending.event,
            table_format: false,
            protobuf: true,
            selector_transitioned: false,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_protobuf));

        let ctx_transitioned = WatchEncodeReuseContext {
            event: &pending.event,
            table_format: false,
            protobuf: false,
            selector_transitioned: true,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_transitioned));

        let json_line = serialize_watch_event_line(pending.event.clone(), "Pod", false);
        let table_line = serialize_watch_event_line(pending.event, "Pod", true);
        assert_ne!(json_line, table_line, "table and JSON output must differ");
    }

    #[test]
    fn bookmark_event_remains_per_subscriber() {
        let bookmark = WatchEvent::bookmark_typed(42, "v1", "Pod");
        assert!(
            bookmark.encoded_payload.is_none(),
            "bookmarks must not carry pre-encoded payload"
        );

        let ctx = WatchEncodeReuseContext {
            event: &bookmark,
            table_format: false,
            protobuf: false,
            selector_transitioned: false,
        };
        assert!(
            !can_reuse_encoded_watch_payload(&ctx),
            "bookmark events must never be reused"
        );
    }
}
