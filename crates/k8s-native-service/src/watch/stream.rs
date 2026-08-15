use super::event::{EventType, WatchContentType, WatchEvent};
use crate::AppError;
use axum::body::Body;
use axum::http::HeaderMap;
use klights_kube_protobuf::{AcceptValue, ResponseFormat};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Exact server-side Table projection supplied by the outer API composition.
/// Watch framing remains native-owned while the shared Kubernetes printers
/// retain their single owner.
pub type WatchTableRenderer = fn(WatchEvent, &str, time::OffsetDateTime) -> WatchEvent;

pub type WatchSourceWaitFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
pub type WatchSourceListFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    klights_leader_api::ResourceListResult,
                    klights_leader_api::ResourceQueryError,
                >,
            > + Send
            + 'a,
    >,
>;

pub trait WatchStreamSource: Send + Sync {
    fn wait_until_fresh<'a>(
        &'a self,
        target_rv: i64,
        api_version: &'a str,
        kind: &'a str,
        task_supervisor: &'a klights_supervisor::TaskSupervisor,
    ) -> WatchSourceWaitFuture<'a>;
    fn list_watch_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        scope: klights_leader_api::ResourceListScope,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> WatchSourceListFuture<'a>;
    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_>;
}

impl<T> WatchStreamSource for Arc<T>
where
    T: WatchStreamSource + ?Sized,
{
    fn wait_until_fresh<'a>(
        &'a self,
        target_rv: i64,
        api_version: &'a str,
        kind: &'a str,
        task_supervisor: &'a klights_supervisor::TaskSupervisor,
    ) -> WatchSourceWaitFuture<'a> {
        self.as_ref()
            .wait_until_fresh(target_rv, api_version, kind, task_supervisor)
    }

    fn list_watch_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        scope: klights_leader_api::ResourceListScope,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> WatchSourceListFuture<'a> {
        self.as_ref().list_watch_resources(
            api_version,
            kind,
            scope,
            label_selector,
            field_selector,
            limit,
        )
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        self.as_ref().watch_resources(request)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchStreamFormat {
    Json,
    Protobuf,
}

impl WatchStreamFormat {
    pub fn content_type(self) -> &'static str {
        match self {
            WatchStreamFormat::Json => "application/json",
            WatchStreamFormat::Protobuf => "application/vnd.kubernetes.protobuf;stream=watch",
        }
    }
}

pub fn negotiate_watch_stream_format(
    headers: &HeaderMap,
    protobuf_supported: bool,
) -> Result<WatchStreamFormat, AppError> {
    let format = klights_kube_protobuf::negotiate_watch_response(
        headers.get_all("accept").into_iter().map(|value| {
            value
                .to_str()
                .map_or(AcceptValue::Invalid, AcceptValue::Text)
        }),
        protobuf_supported,
    )
    .map_err(|error| AppError::NotAcceptable(error.to_string()))?;
    Ok(match format {
        ResponseFormat::Json => WatchStreamFormat::Json,
        ResponseFormat::Protobuf => WatchStreamFormat::Protobuf,
    })
}

pub fn protobuf_watch_supported_for_request(
    api_version: &str,
    kind: &str,
    table_format: bool,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> bool {
    if table_format {
        return false;
    }
    let has_selector = label_selector.is_some_and(|selector| !selector.trim().is_empty())
        || field_selector.is_some_and(|selector| !selector.trim().is_empty());
    if has_selector {
        klights_kube_protobuf::supports_protobuf_resource(api_version, kind)
    } else {
        klights_kube_protobuf::supports_raw_json_protobuf_resource(api_version, kind)
            || klights_kube_protobuf::supports_protobuf_resource(api_version, kind)
    }
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
    db: &(impl WatchStreamSource + ?Sized),
    target_rv: i64,
    api_version: &str,
    kind: &str,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) {
    db.wait_until_fresh(target_rv, api_version, kind, task_supervisor)
        .await;
}

pub fn object_matches_field_selector(object: &Value, field_selector: Option<&str>) -> bool {
    super::event::value_matches_field_selector(object, field_selector)
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

pub fn serialize_watch_event_line_at(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    now: time::OffsetDateTime,
) -> Vec<u8> {
    let event = if table_format {
        fallback_watch_event_to_table_at(event, kind, now)
    } else {
        event
    };
    serialize_watch_event_line_without_table(event)
}

fn fallback_watch_event_to_table_at(
    event: WatchEvent,
    _kind: &str,
    _now: time::OffsetDateTime,
) -> WatchEvent {
    let resource_version = event
        .object
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .unwrap_or("0");
    let object = if event.event_type == EventType::Bookmark {
        serde_json::json!({
            "apiVersion": "meta.k8s.io/v1",
            "kind": "Table",
            "metadata": {
                "resourceVersion": resource_version,
                "annotations": event.object.pointer("/metadata/annotations").cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            },
            "rows": [],
        })
    } else {
        let name = event
            .object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("");
        let created_at = event
            .object
            .pointer("/metadata/creationTimestamp")
            .and_then(Value::as_str)
            .unwrap_or("");
        serde_json::json!({
            "apiVersion": "meta.k8s.io/v1",
            "kind": "Table",
            "metadata": {"resourceVersion": resource_version},
            "rows": [{"cells": [name, created_at], "object": event.object}],
        })
    };
    WatchEvent {
        event_type: event.event_type,
        object: Arc::new(object),
        encoded_payload: None,
    }
}

pub fn serialize_watch_event_line_without_table(event: WatchEvent) -> Vec<u8> {
    if let Some(ref payload) = event.encoded_payload
        && payload.content_type == WatchContentType::Json
        && event.event_type != EventType::Bookmark
    {
        let mut buf = Vec::with_capacity(payload.bytes.len() + 1);
        buf.extend_from_slice(&payload.bytes);
        buf.push(b'\n');
        return buf;
    }
    let mut json = serde_json::to_vec(&event).unwrap_or_default();
    json.push(b'\n');
    json
}

#[cfg(test)]
pub fn serialize_watch_event_line(event: WatchEvent, kind: &str, table_format: bool) -> Vec<u8> {
    serialize_watch_event_line_at(event, kind, table_format, time::OffsetDateTime::now_utc())
}

pub fn serialize_watch_event_frame(event: &WatchEvent, kind: &str) -> anyhow::Result<Vec<u8>> {
    let object_kind = event
        .object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or(kind);
    let raw = if object_kind == "Status" {
        let raw = klights_kube_protobuf::encode_status_protobuf(&event.object)?;
        klights_kube_protobuf::wrap_protobuf_resource_envelope("v1", "Status", raw)?
    } else {
        let api_version = event
            .object
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        let raw = klights_kube_protobuf::encode_protobuf_resource(object_kind, &event.object)?;
        klights_kube_protobuf::wrap_protobuf_resource_envelope(api_version, object_kind, raw)?
    };
    let event_type = event.event_type.to_string();
    Ok(klights_kube_protobuf::encode_watch_event_frame(
        &event_type,
        raw,
    ))
}

#[cfg(test)]
pub fn serialize_watch_event_for_stream(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
) -> Vec<u8> {
    match try_serialize_watch_event_for_stream(event, kind, table_format, stream_format) {
        Ok(frame) | Err(frame) => frame,
    }
}

pub fn try_serialize_watch_event_for_stream_at(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
    now: time::OffsetDateTime,
) -> Result<Vec<u8>, Vec<u8>> {
    try_serialize_watch_event_for_stream_at_with_renderer(
        event,
        kind,
        table_format,
        stream_format,
        now,
        None,
    )
}

fn try_serialize_watch_event_for_stream_at_with_renderer(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
    now: time::OffsetDateTime,
    table_renderer: Option<WatchTableRenderer>,
) -> Result<Vec<u8>, Vec<u8>> {
    let event = if table_format {
        table_renderer.unwrap_or(fallback_watch_event_to_table_at)(event, kind, now)
    } else {
        event
    };
    match stream_format {
        WatchStreamFormat::Json => Ok(serialize_watch_event_line_at(event, kind, false, now)),
        WatchStreamFormat::Protobuf => match serialize_watch_event_frame(&event, kind) {
            Ok(frame) => Ok(frame),
            Err(err) => Err(serialize_watch_status_for_stream(
                stream_format,
                500,
                "InternalError",
                &format!("failed to encode protobuf watch event: {err}"),
            )),
        },
    }
}

#[cfg(test)]
pub fn try_serialize_watch_event_for_stream(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
) -> Result<Vec<u8>, Vec<u8>> {
    try_serialize_watch_event_for_stream_at(
        event,
        kind,
        table_format,
        stream_format,
        time::OffsetDateTime::now_utc(),
    )
}

/// Adapt one transport-neutral positioned event into the existing Kubernetes
/// JSON/protobuf watch codecs. The durable resume position remains session
/// state and is intentionally not added to either Kubernetes wire format.
pub fn serialize_positioned_watch_event_for_stream_at(
    event: klights_leader_api::ResourceEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
    now: time::OffsetDateTime,
) -> Result<Vec<u8>, Vec<u8>> {
    serialize_positioned_watch_event_for_stream_at_with_renderer(
        event,
        kind,
        table_format,
        stream_format,
        now,
        None,
    )
}

fn serialize_positioned_watch_event_for_stream_at_with_renderer(
    event: klights_leader_api::ResourceEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
    now: time::OffsetDateTime,
    table_renderer: Option<WatchTableRenderer>,
) -> Result<Vec<u8>, Vec<u8>> {
    let event = WatchEvent {
        event_type: match event.event_type() {
            klights_leader_api::WatchEventType::Added => EventType::Added,
            klights_leader_api::WatchEventType::Modified => EventType::Modified,
            klights_leader_api::WatchEventType::Deleted => EventType::Deleted,
            klights_leader_api::WatchEventType::Bookmark => EventType::Bookmark,
            klights_leader_api::WatchEventType::Error => EventType::Error,
        },
        object: event.resource().data.clone(),
        encoded_payload: None,
    };
    try_serialize_watch_event_for_stream_at_with_renderer(
        event,
        kind,
        table_format,
        stream_format,
        now,
        table_renderer,
    )
}

#[cfg(test)]
pub fn serialize_positioned_watch_event_for_stream(
    event: klights_leader_api::ResourceEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
) -> Result<Vec<u8>, Vec<u8>> {
    serialize_positioned_watch_event_for_stream_at(
        event,
        kind,
        table_format,
        stream_format,
        time::OffsetDateTime::now_utc(),
    )
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

pub fn serialize_watch_status_for_stream(
    stream_format: WatchStreamFormat,
    code: u16,
    reason: &str,
    message: &str,
) -> Vec<u8> {
    match stream_format {
        WatchStreamFormat::Json => serialize_watch_status_line(code, reason, message),
        WatchStreamFormat::Protobuf => {
            let event = WatchEvent::from_type(
                "ERROR",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "metadata": {},
                    "status": "Failure",
                    "code": code,
                    "reason": reason,
                    "message": message,
                }),
            );
            serialize_watch_event_frame(&event, "Status").unwrap_or_default()
        }
    }
}

pub async fn spawn_bookmark_tick_stream(
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    task_name: impl Into<String>,
) -> mpsc::Receiver<()> {
    let task_name = task_name.into();
    let sleep_name = format!("{task_name}_sleep");
    let (tick_tx, tick_rx) = mpsc::channel(4);
    let task_supervisor_for_wait = task_supervisor.clone();
    if let Err(err) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Timer,
            task_name.clone(),
            async move {
                loop {
                    if tick_tx.send(()).await.is_err() {
                        break;
                    }
                    tokio::select! {
                        _ = tick_tx.closed() => break,
                        result = task_supervisor_for_wait.sleep(
                            sleep_name.clone(),
                            Duration::from_secs(60),
                        ) => {
                            if result.is_err() {
                                break;
                            }
                        }
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
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
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

pub fn bookmark_rv_for_watch_scope(
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
pub(crate) struct PeriodicBookmarkContext<'a, S: WatchStreamSource + ?Sized> {
    pub db: &'a S,
    pub api_version: &'a str,
    pub kind: &'a str,
    pub watch_namespace: Option<&'a str>,
    pub scope: klights_leader_api::ResourceListScope,
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
pub(crate) async fn resolve_periodic_bookmark_rv<S: WatchStreamSource + ?Sized>(
    ctx: PeriodicBookmarkContext<'_, S>,
) -> i64 {
    let PeriodicBookmarkContext {
        db,
        api_version,
        kind,
        watch_namespace,
        scope,
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
            .list_watch_resources(api_version, kind, scope, None, None, Some(1))
            .await
            .map(|list| list.resource_version())
            .unwrap_or(0);
    }
    rv
}

pub async fn maybe_spawn_watch_timeout_stream(
    timeout_seconds: Option<u64>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    task_name: impl Into<String>,
) -> Option<mpsc::Receiver<()>> {
    let timeout_seconds = timeout_seconds?;
    let (timeout_tx, timeout_rx) = mpsc::channel(1);
    let task_name = task_name.into();
    let task_supervisor_for_wait = task_supervisor.clone();
    let sleep_name = format!("{task_name}_sleep");
    if let Err(err) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Timer,
            task_name.clone(),
            async move {
                tokio::select! {
                    _ = timeout_tx.closed() => {},
                    result = task_supervisor_for_wait.sleep(
                        sleep_name,
                        Duration::from_secs(timeout_seconds),
                    ) => {
                        if result.is_ok() {
                            let _ = timeout_tx.send(()).await;
                        }
                    }
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

pub struct LabelSelectorWatchStreamRequest<'a, S: WatchStreamSource> {
    pub source: S,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub api_version: &'a str,
    pub kind: String,
    pub watch_namespace: Option<String>,
    pub scope: klights_leader_api::ResourceListScope,
    pub requested_rv: i64,
    pub send_initial_events: bool,
    pub send_bookmarks: bool,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub table_format: bool,
    pub table_renderer: WatchTableRenderer,
    pub stream_format: WatchStreamFormat,
    pub timeout_seconds: Option<u64>,
    pub emit_initial_state_for_resource_version_zero: bool,
    pub wall_clock: Arc<dyn klights_auth::clock::Clock>,
}

pub async fn build_label_selector_watch_stream<S: WatchStreamSource + 'static>(
    request: LabelSelectorWatchStreamRequest<'_, S>,
) -> Body {
    let LabelSelectorWatchStreamRequest {
        source,
        task_supervisor,
        api_version,
        kind,
        watch_namespace,
        scope,
        requested_rv,
        send_initial_events,
        send_bookmarks,
        label_selector,
        field_selector,
        table_format,
        table_renderer,
        stream_format,
        timeout_seconds,
        emit_initial_state_for_resource_version_zero,
        wall_clock,
    } = request;
    let api_version = api_version.to_string();
    wait_until_datastore_fresh(&source, requested_rv, &api_version, &kind, &task_supervisor).await;

    let has_selector = label_selector
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        || field_selector
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    let emit_baseline = send_initial_events
        || requested_rv <= 0 && (has_selector || emit_initial_state_for_resource_version_zero);
    let mut start_position = None;
    let mut last_delivered_rv = requested_rv;
    let mut initial_frames = Vec::new();
    if emit_baseline {
        let baseline_now = wall_clock.now();
        let list = match source
            .list_watch_resources(
                &api_version,
                &kind,
                scope.clone(),
                label_selector.as_deref(),
                field_selector.as_deref(),
                None,
            )
            .await
        {
            Ok(list) => list,
            Err(error) => {
                tracing::warn!(?error, "positioned watch baseline LIST failed");
                return single_watch_frame_body(serialize_watch_status_for_stream(
                    stream_format,
                    500,
                    "InternalError",
                    "failed to establish watch baseline",
                ));
            }
        };
        let Some(position) = list.watch_replay_position() else {
            return single_watch_frame_body(serialize_watch_status_for_stream(
                stream_format,
                500,
                "InternalError",
                "watch baseline did not provide an atomic replay position",
            ));
        };
        start_position = Some(position);
        last_delivered_rv = last_delivered_rv.max(list.resource_version());
        for resource in list.into_items() {
            let event = WatchEvent::added((*resource.data).clone());
            match try_serialize_watch_event_for_stream_at_with_renderer(
                event,
                &kind,
                table_format,
                stream_format,
                baseline_now,
                Some(table_renderer),
            ) {
                Ok(frame) => initial_frames.push(frame),
                Err(frame) => return single_watch_frame_body(frame),
            }
        }
        if send_initial_events {
            let bookmark =
                WatchEvent::bookmark_initial_events_end(last_delivered_rv, &api_version, &kind);
            match try_serialize_watch_event_for_stream_at_with_renderer(
                bookmark,
                &kind,
                table_format,
                stream_format,
                baseline_now,
                Some(table_renderer),
            ) {
                Ok(frame) => initial_frames.push(frame),
                Err(frame) => return single_watch_frame_body(frame),
            }
        }
    }

    let start_resource_version = if requested_rv == 0
        && !emit_initial_state_for_resource_version_zero
        && !send_initial_events
    {
        None
    } else {
        Some(requested_rv)
    };
    let watch_request = match klights_leader_api::WatchRequest::try_new_with_scope(
        api_version.clone(),
        kind.clone(),
        watch_namespace.clone(),
        scope.clone(),
        label_selector.clone(),
        field_selector.clone(),
        start_resource_version,
        start_position,
    ) {
        Ok(request) => request,
        Err(error) => {
            return single_watch_frame_body(serialize_watch_status_for_stream(
                stream_format,
                400,
                "BadRequest",
                &error.to_string(),
            ));
        }
    };
    let mut positioned_stream = match source.watch_resources(watch_request).await {
        Ok(stream) => stream,
        Err(error) => {
            return single_watch_frame_body(serialize_positioned_watch_error_for_stream(
                &error,
                stream_format,
            ));
        }
    };

    let stream = async_stream::stream! {
        for frame in initial_frames {
            yield Ok::<_, std::convert::Infallible>(frame);
        }
        let mut bookmark_ticks = maybe_spawn_bookmark_tick_stream(
            send_bookmarks,
            task_supervisor.clone(),
            format!("watch_stream_bookmarks_{api_version}_{kind}"),
        )
        .await;
        let mut timeout_tick = maybe_spawn_watch_timeout_stream(
            timeout_seconds,
            task_supervisor,
            format!("watch_stream_timeout_{api_version}_{kind}"),
        )
        .await;
        let has_scope_filter = watch_namespace.is_some() || has_selector;
        loop {
            tokio::select! {
                Some(()) = recv_watch_timeout(&mut timeout_tick) => break,
                next = futures::StreamExt::next(&mut positioned_stream) => {
                    let Some(next) = next else { break; };
                    let event = match next {
                        Ok(event) => event,
                        Err(error) => {
                            yield Ok::<_, std::convert::Infallible>(
                                serialize_positioned_watch_error_for_stream(&error, stream_format),
                            );
                            break;
                        }
                    };
                    last_delivered_rv = last_delivered_rv.max(event.resource().resource_version);
                    match serialize_positioned_watch_event_for_stream_at_with_renderer(
                        event,
                        &kind,
                        table_format,
                        stream_format,
                        wall_clock.now(),
                        Some(table_renderer),
                    ) {
                        Ok(frame) => yield Ok::<_, std::convert::Infallible>(frame),
                        Err(frame) => {
                            yield Ok::<_, std::convert::Infallible>(frame);
                            break;
                        }
                    }
                }
                Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                    let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
                        db: &source,
                        api_version: &api_version,
                        kind: &kind,
                        watch_namespace: watch_namespace.as_deref(),
                        scope: scope.clone(),
                        label_selector: label_selector.as_deref(),
                        field_selector: field_selector.as_deref(),
                        requested_rv,
                        has_scope_filter,
                        cursor_high_water_rv: last_delivered_rv,
                        last_delivered_scoped_rv: last_delivered_rv,
                    }).await;
                    let bookmark = WatchEvent::bookmark_typed(rv, &api_version, &kind);
                    match try_serialize_watch_event_for_stream_at_with_renderer(
                        bookmark,
                        &kind,
                        table_format,
                        stream_format,
                        wall_clock.now(),
                        Some(table_renderer),
                    ) {
                        Ok(frame) => yield Ok::<_, std::convert::Infallible>(frame),
                        Err(frame) => {
                            yield Ok::<_, std::convert::Infallible>(frame);
                            break;
                        }
                    }
                }
            }
        }
    };
    Body::from_stream(stream)
}

fn single_watch_frame_body(frame: Vec<u8>) -> Body {
    Body::from_stream(futures::stream::once(async move {
        Ok::<_, std::convert::Infallible>(frame)
    }))
}

fn serialize_positioned_watch_error_for_stream(
    error: &klights_leader_api::LeaderWatchError,
    stream_format: WatchStreamFormat,
) -> Vec<u8> {
    let (code, reason) = match error {
        klights_leader_api::LeaderWatchError::ReplayExpired { .. } => (410, "Expired"),
        klights_leader_api::LeaderWatchError::InvalidRequest { .. }
        | klights_leader_api::LeaderWatchError::MismatchedEvent { .. }
        | klights_leader_api::LeaderWatchError::UnknownEventType { .. } => (400, "BadRequest"),
        klights_leader_api::LeaderWatchError::Timeout
        | klights_leader_api::LeaderWatchError::Cancelled => (504, "Timeout"),
        _ => (500, "InternalError"),
    };
    serialize_watch_status_for_stream(stream_format, code, reason, &error.to_string())
}
#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use klights_cluster_core::WatchReplayPosition;
    use klights_kube_protobuf as k8s_pb;
    use klights_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
    use prost::Message;
    use serde_json::json;

    struct DecodedProtobufWatchFrame {
        event_type: String,
        inner_api_version: String,
        inner_kind: String,
        inner_raw: Vec<u8>,
    }

    fn decode_k8s_protobuf_watch_frames(chunks: &[Vec<u8>]) -> Vec<DecodedProtobufWatchFrame> {
        let mut pending = Vec::new();
        let mut frames = Vec::new();
        for chunk in chunks {
            pending.extend_from_slice(chunk);
            while pending.len() >= 4 {
                let frame_len = u32::from_be_bytes(pending[0..4].try_into().unwrap()) as usize;
                let frame_end = 4 + frame_len;
                if pending.len() < frame_end {
                    break;
                }
                let event = k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(
                    &pending[4..frame_end],
                )
                .expect("raw protobuf WatchEvent");
                let object_raw = event.object.and_then(|object| object.raw).unwrap();
                assert_eq!(&object_raw[..4], b"k8s\0");
                let inner = klights_kube_protobuf::Unknown::decode(&object_raw[4..]).unwrap();
                let type_meta = inner.type_meta.unwrap();
                frames.push(DecodedProtobufWatchFrame {
                    event_type: event.r#type.unwrap_or_default(),
                    inner_api_version: type_meta.api_version,
                    inner_kind: type_meta.kind,
                    inner_raw: inner.raw,
                });
                pending.drain(..frame_end);
            }
        }
        assert!(
            pending.is_empty(),
            "all protobuf bytes must form complete frames"
        );
        frames
    }

    fn split_watch_bytes(bytes: &[u8]) -> Vec<Vec<u8>> {
        let widths = [1usize, 2, 5, 3, 8];
        let mut chunks = Vec::new();
        let mut offset = 0;
        let mut width = 0;
        while offset < bytes.len() {
            let end = (offset + widths[width % widths.len()]).min(bytes.len());
            chunks.push(bytes[offset..end].to_vec());
            offset = end;
            width += 1;
        }
        chunks
    }

    fn decode_split_json_watch_lines(chunks: &[Vec<u8>]) -> Vec<Value> {
        let mut pending = Vec::new();
        let mut events = Vec::new();
        for chunk in chunks {
            pending.extend_from_slice(chunk);
            while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                events.push(serde_json::from_slice(&pending[..end]).unwrap());
                pending.drain(..=end);
            }
        }
        assert!(pending.is_empty());
        events
    }

    #[derive(Clone)]
    struct FallibleWatchSource {
        establish_error: Option<klights_leader_api::LeaderWatchError>,
        events:
            Vec<Result<klights_leader_api::ResourceEvent, klights_leader_api::LeaderWatchError>>,
    }

    impl WatchStreamSource for FallibleWatchSource {
        fn wait_until_fresh<'a>(
            &'a self,
            _target_rv: i64,
            _api_version: &'a str,
            _kind: &'a str,
            _task_supervisor: &'a TaskSupervisor,
        ) -> WatchSourceWaitFuture<'a> {
            Box::pin(async {})
        }

        fn list_watch_resources<'a>(
            &'a self,
            _api_version: &'a str,
            _kind: &'a str,
            _scope: klights_leader_api::ResourceListScope,
            _label_selector: Option<&'a str>,
            _field_selector: Option<&'a str>,
            _limit: Option<i64>,
        ) -> WatchSourceListFuture<'a> {
            Box::pin(async { panic!("live-watch fixture must not perform a baseline LIST") })
        }

        fn watch_resources(
            &self,
            _request: klights_leader_api::WatchRequest,
        ) -> klights_leader_api::LeaderWatchFuture<'_> {
            let establish_error = self.establish_error.clone();
            let events = self.events.clone();
            Box::pin(async move {
                if let Some(error) = establish_error {
                    return Err(error);
                }
                Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                    futures::stream::iter(events),
                ))
            })
        }
    }

    #[tokio::test]
    async fn positioned_watch_establishment_failure_emits_error_and_terminates() {
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            source: FallibleWatchSource {
                establish_error: Some(klights_leader_api::LeaderWatchError::unavailable(
                    "history unavailable",
                )),
                events: Vec::new(),
            },
            task_supervisor: Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            api_version: "v1",
            kind: "ConfigMap".to_string(),
            watch_namespace: None,
            scope: klights_leader_api::ResourceListScope::AllNamespaces,
            requested_rv: 0,
            send_initial_events: false,
            send_bookmarks: false,
            label_selector: None,
            field_selector: None,
            table_format: false,
            table_renderer: fallback_watch_event_to_table_at,
            stream_format: WatchStreamFormat::Json,
            timeout_seconds: None,
            emit_initial_state_for_resource_version_zero: false,
            wall_clock: Arc::new(klights_auth::clock::SystemClock),
        })
        .await;

        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["type"], "ERROR");
        assert_eq!(value["object"]["code"], 500);
    }

    #[tokio::test]
    async fn positioned_watch_pull_failure_emits_one_error_and_terminates() {
        let event = klights_leader_api::ResourceEvent::try_new(
            klights_leader_api::WatchEventType::Added,
            klights_cluster_core::Resource::from_data_lossy(Arc::new(json!({
                "apiVersion":"v1","kind":"ConfigMap",
                "metadata":{"name":"cm","resourceVersion":"1"}
            }))),
            None,
        )
        .unwrap();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            source: FallibleWatchSource {
                establish_error: None,
                events: vec![
                    Ok(event),
                    Err(klights_leader_api::LeaderWatchError::unavailable(
                        "history unavailable",
                    )),
                ],
            },
            task_supervisor: Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            api_version: "v1",
            kind: "ConfigMap".to_string(),
            watch_namespace: None,
            scope: klights_leader_api::ResourceListScope::AllNamespaces,
            requested_rv: 0,
            send_initial_events: false,
            send_bookmarks: false,
            label_selector: None,
            field_selector: None,
            table_format: false,
            table_renderer: fallback_watch_event_to_table_at,
            stream_format: WatchStreamFormat::Json,
            timeout_seconds: None,
            emit_initial_state_for_resource_version_zero: false,
            wall_clock: Arc::new(klights_auth::clock::SystemClock),
        })
        .await;
        let mut stream = body.into_data_stream();
        let first: Value = serde_json::from_slice(&stream.next().await.unwrap().unwrap()).unwrap();
        let terminal: Value =
            serde_json::from_slice(&stream.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(first["type"], "ADDED");
        assert_eq!(terminal["type"], "ERROR");
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn phase17c_watch_terminal_status_preserves_json_protobuf_parity() {
        for (code, reason, message) in [
            (410, "Expired", "too old resource version"),
            (504, "Timeout", "watch timed out"),
            (500, "InternalError", "watch history unavailable"),
        ] {
            let json =
                serialize_watch_status_for_stream(WatchStreamFormat::Json, code, reason, message);
            let json: Value = serde_json::from_slice(&json).unwrap();
            assert_eq!(json["type"], "ERROR");
            assert_eq!(json["object"]["kind"], "Status");
            assert_eq!(json["object"]["code"], code);

            let protobuf = serialize_watch_status_for_stream(
                WatchStreamFormat::Protobuf,
                code,
                reason,
                message,
            );
            let frames = decode_k8s_protobuf_watch_frames(&[protobuf]);
            assert_eq!(frames[0].event_type, "ERROR");
            assert_eq!(frames[0].inner_api_version, "v1");
            assert_eq!(frames[0].inner_kind, "Status");
            let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
                frames[0].inner_raw.as_slice(),
            )
            .unwrap();
            assert_eq!(status.code, Some(i32::from(code)));
            assert_eq!(status.reason.as_deref(), Some(reason));
            assert_eq!(status.message.as_deref(), Some(message));
        }
    }

    #[test]
    fn positioned_sequence_and_terminal_errors_have_json_protobuf_parity_across_split_chunks() {
        let objects = [
            (
                klights_leader_api::WatchEventType::Added,
                "ADDED",
                json!({
                    "apiVersion":"v1","kind":"ConfigMap",
                    "metadata":{"name":"cm","namespace":"default","resourceVersion":"71"}
                }),
            ),
            (
                klights_leader_api::WatchEventType::Bookmark,
                "BOOKMARK",
                json!({
                    "apiVersion":"v1","kind":"ConfigMap",
                    "metadata":{"resourceVersion":"72"}
                }),
            ),
        ];
        let mut json_wire = Vec::new();
        let mut protobuf_wire = Vec::new();
        for (index, (event_type, _, object)) in objects.iter().enumerate() {
            let resource =
                klights_cluster_core::Resource::from_data_lossy(Arc::new(object.clone()));
            let position = WatchReplayPosition {
                resource_version: resource.resource_version,
                event_id: 200 + index as i64,
                resource_version_filter_through_event_id: 0,
            };
            let event =
                klights_leader_api::ResourceEvent::try_new(*event_type, resource, Some(position))
                    .unwrap();
            assert_eq!(event.resume_position(), Some(position));
            json_wire.extend(
                serialize_positioned_watch_event_for_stream(
                    event.clone(),
                    "ConfigMap",
                    false,
                    WatchStreamFormat::Json,
                )
                .unwrap(),
            );
            protobuf_wire.extend(
                serialize_positioned_watch_event_for_stream(
                    event,
                    "ConfigMap",
                    false,
                    WatchStreamFormat::Protobuf,
                )
                .unwrap(),
            );
        }

        let json = decode_split_json_watch_lines(&split_watch_bytes(&json_wire));
        let protobuf = decode_k8s_protobuf_watch_frames(&split_watch_bytes(&protobuf_wire));
        for (index, (_, expected_type, _)) in objects.iter().enumerate() {
            assert_eq!(json[index]["type"], *expected_type);
            assert_eq!(protobuf[index].event_type, *expected_type);
            assert_eq!(
                protobuf[index].inner_kind,
                json[index]["object"]["kind"].as_str().unwrap()
            );
            assert!(json[index].get("resumePosition").is_none());
        }

        for (error, expected_code) in [
            (
                klights_leader_api::LeaderWatchError::ReplayExpired {
                    accepted_resource_version: 72,
                },
                410,
            ),
            (
                klights_leader_api::LeaderWatchError::unavailable("history unavailable"),
                500,
            ),
        ] {
            let json = serialize_positioned_watch_error_for_stream(&error, WatchStreamFormat::Json);
            let protobuf =
                serialize_positioned_watch_error_for_stream(&error, WatchStreamFormat::Protobuf);
            let json = decode_split_json_watch_lines(&split_watch_bytes(&json));
            let protobuf = decode_k8s_protobuf_watch_frames(&split_watch_bytes(&protobuf));
            assert_eq!(json[0]["object"]["code"], expected_code);
            let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
                protobuf[0].inner_raw.as_slice(),
            )
            .unwrap();
            assert_eq!(status.code, Some(expected_code));
        }
    }

    async fn wait_for_timer_task_exit(supervisor: &TaskSupervisor, task_name: &str) {
        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if supervisor
                    .active_tasks(Some(TaskCategory::Timer))
                    .iter()
                    .all(|task| !task.name.contains(task_name))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping a watch timer receiver must promptly stop its task");
    }

    #[tokio::test]
    async fn dropping_bookmark_receiver_promptly_cancels_timer_task() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let mut ticks =
            spawn_bookmark_tick_stream(supervisor.clone(), "dropped_bookmark_timer").await;
        ticks.recv().await.expect("initial bookmark tick");
        drop(ticks);
        wait_for_timer_task_exit(&supervisor, "dropped_bookmark_timer").await;
    }

    #[tokio::test]
    async fn dropping_timeout_receiver_promptly_cancels_timer_task() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let timeout = maybe_spawn_watch_timeout_stream(
            Some(3_600),
            supervisor.clone(),
            "dropped_watch_timeout",
        )
        .await
        .expect("timeout receiver");
        drop(timeout);
        wait_for_timer_task_exit(&supervisor, "dropped_watch_timeout").await;
    }
}
