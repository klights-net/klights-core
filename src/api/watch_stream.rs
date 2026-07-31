use crate::api::watch_event::{EventType, WatchContentType, WatchEvent};
use crate::api::{AppError, watch_event_to_table_at};
use axum::body::Body;
use axum::http::HeaderMap;
#[cfg(test)]
use klights_kube_protobuf as k8s_pb;
use klights_kube_protobuf::{AcceptValue, ResponseFormat};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
        namespace: Option<&'a str>,
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
        namespace: Option<&'a str>,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> WatchSourceListFuture<'a> {
        self.as_ref().list_watch_resources(
            api_version,
            kind,
            namespace,
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
    crate::api::watch_event::value_matches_field_selector(object, field_selector)
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
        watch_event_to_table_at(event, kind, now)
    } else {
        event
    };
    serialize_watch_event_line_without_table(event)
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
pub fn serialize_raw_watch_event_frame(
    event: &klights_cluster_store::DurableRawWatchEvent,
) -> anyhow::Result<Vec<u8>> {
    let raw = klights_kube_protobuf::encode_protobuf_resource_from_json_bytes(
        &event.api_version,
        &event.kind,
        &event.object_json,
    )?;
    let raw = klights_kube_protobuf::wrap_protobuf_resource_envelope(
        &event.api_version,
        &event.kind,
        raw,
    )?;
    Ok(klights_kube_protobuf::encode_watch_event_frame(
        event.event_type.as_ref(),
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
    match stream_format {
        WatchStreamFormat::Json => Ok(serialize_watch_event_line_at(
            event,
            kind,
            table_format,
            now,
        )),
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
    try_serialize_watch_event_for_stream_at(event, kind, table_format, stream_format, now)
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

#[cfg(test)]
pub fn serialize_raw_watch_event_line(
    event: &klights_cluster_store::DurableRawWatchEvent,
) -> Vec<u8> {
    let event_type = event.event_type.as_ref();
    let mut line = Vec::with_capacity(event_type.len() + event.object_json.len() + 23);
    line.extend_from_slice(br#"{"type":""#);
    line.extend_from_slice(event_type.as_bytes());
    line.extend_from_slice(br#"","object":"#);
    line.extend_from_slice(&event.object_json);
    line.extend_from_slice(b"}\n");
    line
}

#[cfg(test)]
pub fn serialize_raw_watch_event_for_stream(
    event: &klights_cluster_store::DurableRawWatchEvent,
    stream_format: WatchStreamFormat,
) -> Vec<u8> {
    match try_serialize_raw_watch_event_for_stream(event, stream_format) {
        Ok(frame) | Err(frame) => frame,
    }
}

#[cfg(test)]
pub fn try_serialize_raw_watch_event_for_stream(
    event: &klights_cluster_store::DurableRawWatchEvent,
    stream_format: WatchStreamFormat,
) -> Result<Vec<u8>, Vec<u8>> {
    match stream_format {
        WatchStreamFormat::Json => Ok(serialize_raw_watch_event_line(event)),
        WatchStreamFormat::Protobuf => match serialize_raw_watch_event_frame(event) {
            Ok(frame) => Ok(frame),
            Err(err) => Err(serialize_watch_status_for_stream(
                stream_format,
                500,
                "InternalError",
                &format!("failed to encode raw protobuf watch event: {err}"),
            )),
        },
    }
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
pub(crate) struct PeriodicBookmarkContext<'a, S: WatchStreamSource + ?Sized> {
    pub db: &'a S,
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
pub(crate) async fn resolve_periodic_bookmark_rv<S: WatchStreamSource + ?Sized>(
    ctx: PeriodicBookmarkContext<'_, S>,
) -> i64 {
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
            .list_watch_resources(api_version, kind, watch_namespace, None, None, Some(1))
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
    pub requested_rv: i64,
    pub send_initial_events: bool,
    pub send_bookmarks: bool,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub table_format: bool,
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
        requested_rv,
        send_initial_events,
        send_bookmarks,
        label_selector,
        field_selector,
        table_format,
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
                watch_namespace.as_deref(),
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
            match try_serialize_watch_event_for_stream_at(
                event,
                &kind,
                table_format,
                stream_format,
                baseline_now,
            ) {
                Ok(frame) => initial_frames.push(frame),
                Err(frame) => return single_watch_frame_body(frame),
            }
        }
        if send_initial_events {
            let bookmark =
                WatchEvent::bookmark_initial_events_end(last_delivered_rv, &api_version, &kind);
            match try_serialize_watch_event_for_stream_at(
                bookmark,
                &kind,
                table_format,
                stream_format,
                baseline_now,
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
    let watch_request = match klights_leader_api::WatchRequest::try_new(
        api_version.clone(),
        kind.clone(),
        watch_namespace.clone(),
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
                    match serialize_positioned_watch_event_for_stream_at(
                        event,
                        &kind,
                        table_format,
                        stream_format,
                        wall_clock.now(),
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
                        label_selector: label_selector.as_deref(),
                        field_selector: field_selector.as_deref(),
                        requested_rv,
                        has_scope_filter,
                        cursor_high_water_rv: last_delivered_rv,
                        last_delivered_scoped_rv: last_delivered_rv,
                    }).await;
                    let bookmark = WatchEvent::bookmark_typed(rv, &api_version, &kind);
                    match try_serialize_watch_event_for_stream_at(
                        bookmark,
                        &kind,
                        table_format,
                        stream_format,
                        wall_clock.now(),
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
    use bytes::Bytes;
    use futures::StreamExt;
    use klights_cluster_core::WatchReplayPosition;
    use klights_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
    use prost::Message;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn datastore_watch_source(
        db: &crate::datastore::sqlite::Datastore,
        handle: &crate::datastore::DatastoreHandle,
    ) -> crate::watch_stream_adapter::DatastoreWatchStreamAdapter {
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(db);
        crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
            handle.clone(),
            crate::watch_commit_observation_adapter::test_signal_source(handle),
            crate::positioned_watch_adapter::for_test(&passive_reads, handle.clone()),
        )
    }

    #[derive(Clone)]
    struct FiniteWatchSource {
        events: Vec<klights_leader_api::ResourceEvent>,
    }

    impl WatchStreamSource for FiniteWatchSource {
        fn wait_until_fresh<'a>(
            &'a self,
            _target_rv: i64,
            _api_version: &'a str,
            _kind: &'a str,
            _task_supervisor: &'a klights_supervisor::TaskSupervisor,
        ) -> WatchSourceWaitFuture<'a> {
            Box::pin(async {})
        }

        fn list_watch_resources<'a>(
            &'a self,
            _api_version: &'a str,
            _kind: &'a str,
            _namespace: Option<&'a str>,
            _label_selector: Option<&'a str>,
            _field_selector: Option<&'a str>,
            _limit: Option<i64>,
        ) -> WatchSourceListFuture<'a> {
            Box::pin(async { panic!("finite live-watch fixture must not perform a baseline LIST") })
        }

        fn watch_resources(
            &self,
            _request: klights_leader_api::WatchRequest,
        ) -> klights_leader_api::LeaderWatchFuture<'_> {
            let events = self.events.clone();
            Box::pin(async move {
                Ok(klights_leader_api::WatchStream::unpositioned_test_stream(
                    futures::stream::iter(events.into_iter().map(Ok)),
                ))
            })
        }
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
            _task_supervisor: &'a klights_supervisor::TaskSupervisor,
        ) -> WatchSourceWaitFuture<'a> {
            Box::pin(async {})
        }

        fn list_watch_resources<'a>(
            &'a self,
            _api_version: &'a str,
            _kind: &'a str,
            _namespace: Option<&'a str>,
            _label_selector: Option<&'a str>,
            _field_selector: Option<&'a str>,
            _limit: Option<i64>,
        ) -> WatchSourceListFuture<'a> {
            Box::pin(async {
                panic!("fallible live-watch fixture must not perform a baseline LIST")
            })
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

    struct AdvancingClock {
        base: time::OffsetDateTime,
        reads: AtomicUsize,
    }

    impl klights_auth::clock::Clock for AdvancingClock {
        fn now(&self) -> time::OffsetDateTime {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            self.base + time::Duration::seconds(10 + 60 * read as i64)
        }
    }

    #[tokio::test]
    async fn table_watch_reads_wall_clock_for_each_live_event() {
        let creation = "2026-07-28T12:00:00Z";
        let events = [1, 2]
            .into_iter()
            .map(|resource_version| {
                let resource =
                    klights_cluster_core::Resource::from_data_lossy(Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": format!("pod-{resource_version}"),
                            "namespace": "default",
                            "uid": format!("uid-{resource_version}"),
                            "resourceVersion": resource_version.to_string(),
                            "creationTimestamp": creation
                        },
                        "spec": {"containers": [{"name": "main", "image": "busybox"}]},
                        "status": {"phase": "Running"}
                    })));
                klights_leader_api::ResourceEvent::try_new(
                    klights_leader_api::WatchEventType::Added,
                    resource,
                    None,
                )
                .unwrap()
            })
            .collect();
        let base =
            time::OffsetDateTime::parse(creation, &time::format_description::well_known::Rfc3339)
                .unwrap();
        let wall_clock: Arc<dyn klights_auth::clock::Clock> = Arc::new(AdvancingClock {
            base,
            reads: AtomicUsize::new(0),
        });
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            source: FiniteWatchSource { events },
            task_supervisor: supervisor,
            api_version: "v1",
            kind: "Pod".to_string(),
            watch_namespace: Some("default".to_string()),
            requested_rv: 0,
            send_initial_events: false,
            send_bookmarks: false,
            label_selector: None,
            field_selector: None,
            table_format: true,
            stream_format: WatchStreamFormat::Json,
            timeout_seconds: None,
            emit_initial_state_for_resource_version_zero: false,
            wall_clock,
        })
        .await;

        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let frames: Vec<serde_json::Value> = bytes
            .split(|byte| *byte == b'\n')
            .filter(|frame| !frame.is_empty())
            .map(|frame| serde_json::from_slice(frame).unwrap())
            .collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["object"]["rows"][0]["cells"][4], "10s");
        assert_eq!(frames[1]["object"]["rows"][0]["cells"][4], "1m");
    }

    /// A protobuf watch frame decoded through the full Kubernetes envelope
    /// contract, exactly as client-go's protobuf stream reader consumes it.
    struct DecodedProtobufWatchFrame {
        event_type: String,
        inner_api_version: String,
        inner_kind: String,
        inner_raw: Vec<u8>,
    }

    /// Kubernetes protobuf watch-frame decoder shared by every watch-stream
    /// test.
    ///
    /// client-go removes the four-byte frame length and passes the remaining
    /// bytes to Kubernetes' raw protobuf stream serializer, which decodes a
    /// bare `WatchEvent`. The nested `WatchEvent.object.raw` remains a normal
    /// `k8s\0` resource envelope for the embedded object decoder.
    fn decode_k8s_protobuf_watch_frames(chunks: &[Vec<u8>]) -> Vec<DecodedProtobufWatchFrame> {
        let mut pending = Vec::new();
        let mut frames = Vec::new();

        for chunk in chunks {
            pending.extend_from_slice(chunk);
            while pending.len() >= 4 {
                let frame_len = u32::from_be_bytes(
                    pending[0..4]
                        .try_into()
                        .expect("frame length prefix must be 4 bytes"),
                ) as usize;
                let frame_end = 4 + frame_len;
                if pending.len() < frame_end {
                    break;
                }

                let payload = &pending[4..frame_end];
                let pb_event =
                    k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(payload)
                        .expect("frame payload must decode as a raw protobuf WatchEvent");
                let object_raw = pb_event
                    .object
                    .and_then(|object| object.raw)
                    .expect("WatchEvent must carry an enveloped object RawExtension");
                assert_eq!(
                    &object_raw[..4],
                    b"k8s\0",
                    "inner watch object must begin with the k8s\\0 envelope magic",
                );
                let inner = klights_kube_protobuf::Unknown::decode(&object_raw[4..])
                    .expect("inner object payload must decode as runtime.Unknown");
                let inner_type_meta = inner
                    .type_meta
                    .as_ref()
                    .expect("inner object envelope must carry type metadata");
                frames.push(DecodedProtobufWatchFrame {
                    event_type: pb_event.r#type.unwrap_or_default(),
                    inner_api_version: inner_type_meta.api_version.clone(),
                    inner_kind: inner_type_meta.kind.clone(),
                    inner_raw: inner.raw.clone(),
                });
                pending.drain(0..frame_end);
            }
        }

        frames
    }

    /// The stream decoder must reject the runtime.Unknown outer envelope used
    /// by normal protobuf objects. client-go's watch stream serializer expects
    /// the length-prefixed payload itself to be a bare `WatchEvent`.
    #[test]
    fn raw_watch_decoder_rejects_outer_enveloped_watch_event() {
        let bare = serialize_watch_event_frame(
            &WatchEvent::added(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cm1", "resourceVersion": "1"}
            })),
            "ConfigMap",
        )
        .unwrap();
        let outer = klights_kube_protobuf::wrap_protobuf_resource_envelope(
            "v1",
            "WatchEvent",
            bare[4..].to_vec(),
        )
        .unwrap();
        let mut malformed = Vec::with_capacity(4 + outer.len());
        malformed.extend_from_slice(&(outer.len() as u32).to_be_bytes());
        malformed.extend(outer);

        let result = std::panic::catch_unwind(|| decode_k8s_protobuf_watch_frames(&[malformed]));
        assert!(
            result.is_err(),
            "raw watch decoder must reject an outer-enveloped WatchEvent"
        );
    }

    #[test]
    fn watch_stream_negotiation_respects_q_value_ordering_and_ties() {
        let mut headers = HeaderMap::new();

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0.8, application/json;q=0.8, */*;q=0.8"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf,
        );

        headers.insert(
            "accept",
            "application/json;q=0.8, application/vnd.kubernetes.protobuf;q=0.8"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json,
        );

        headers.insert(
            "accept",
            "application/*;q=0.9, application/json;q=0.9, application/vnd.kubernetes.protobuf;q=0.9"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json,
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0.2, application/json;q=0.8"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json,
        );
    }

    #[test]
    fn serialize_watch_bookmark_event_is_length_prefixed_protobuf() {
        let frame = serialize_watch_event_for_stream(
            WatchEvent::bookmark_initial_events_end(99, "v1", "ConfigMap"),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        assert!(frame.len() > 4);
        assert_eq!(
            frame.len() as u64 - 4,
            u32::from_be_bytes(frame[0..4].try_into().expect("frame length prefix")) as u64,
        );

        let frames = decode_k8s_protobuf_watch_frames(&[frame]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event_type, "BOOKMARK");
        assert_eq!(frames[0].inner_api_version, "v1");
        assert_eq!(frames[0].inner_kind, "ConfigMap");
        let decoded =
            k8s_pb::api::core::v1::ConfigMap::decode(frames[0].inner_raw.as_slice()).unwrap();
        assert_eq!(
            decoded
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.annotations.get("k8s.io/initial-events-end")),
            Some(&"true".to_string()),
        );
    }

    #[test]
    fn serialize_watch_status_is_length_prefixed_error_protobuf() {
        let frame = serialize_watch_status_for_stream(
            WatchStreamFormat::Protobuf,
            410,
            "Expired",
            "too old resource version",
        );
        assert!(frame.len() > 4);
        assert_eq!(
            frame.len() as u64 - 4,
            u32::from_be_bytes(frame[0..4].try_into().expect("frame length prefix")) as u64,
        );

        let frames = decode_k8s_protobuf_watch_frames(&[frame]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event_type, "ERROR");
        assert_eq!(frames[0].inner_api_version, "v1");
        assert_eq!(frames[0].inner_kind, "Status");
        let decoded = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
            frames[0].inner_raw.as_slice(),
        )
        .unwrap();
        assert_eq!(decoded.code, Some(410));
        assert_eq!(decoded.reason, Some("Expired".to_string()));
    }

    #[test]
    fn protobuf_watch_encode_failure_returns_terminal_error_frame() {
        let event = WatchEvent::from_type(
            "ADDED",
            serde_json::json!({
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "metadata": {"name": "w1", "resourceVersion": "7"}
            }),
        );

        let frame = try_serialize_watch_event_for_stream(
            event,
            "Widget",
            false,
            WatchStreamFormat::Protobuf,
        )
        .expect_err("unsupported protobuf resource should be terminal");
        let frames = decode_k8s_protobuf_watch_frames(&[frame]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event_type, "ERROR");
        assert_eq!(frames[0].inner_kind, "Status");
        let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
            frames[0].inner_raw.as_slice(),
        )
        .unwrap();
        assert_eq!(status.code, Some(500));
        assert_eq!(status.reason.as_deref(), Some("InternalError"));
    }

    #[test]
    fn raw_selectorless_protobuf_frame_encodes_enveloped_objects() {
        let cases: &[(&str, &str, &[u8])] = &[
            (
                "v1",
                "ConfigMap",
                br#"{
                    "apiVersion":"v1",
                    "kind":"ConfigMap",
                    "metadata":{
                        "namespace":"default",
                        "name":"cm1",
                        "resourceVersion":"7"
                    },
                    "data":{"k":"v"}
                }"#,
            ),
            (
                "v1",
                "Pod",
                br#"{
                    "apiVersion":"v1",
                    "kind":"Pod",
                    "metadata":{
                        "namespace":"default",
                        "name":"pod1",
                        "resourceVersion":"7"
                    },
                    "spec":{"containers":[{"name":"main","image":"busybox"}]},
                    "status":{"phase":"Pending"}
                }"#,
            ),
            (
                "v1",
                "Service",
                br#"{
                    "apiVersion":"v1",
                    "kind":"Service",
                    "metadata":{
                        "namespace":"default",
                        "name":"svc1",
                        "resourceVersion":"7"
                    },
                    "spec":{"ports":[{"port":80,"protocol":"TCP"}]},
                    "status":{"loadBalancer":{}}
                }"#,
            ),
            (
                "v1",
                "Event",
                br#"{
                    "apiVersion":"v1",
                    "kind":"Event",
                    "metadata":{
                        "namespace":"default",
                        "name":"event1",
                        "resourceVersion":"7"
                    },
                    "involvedObject":{"apiVersion":"v1","kind":"Pod","name":"pod1","namespace":"default"},
                    "message":"created",
                    "reason":"Created",
                    "type":"Normal"
                }"#,
            ),
        ];

        for (api_version, kind, object_json) in cases {
            let row = klights_cluster_store::DurableRawWatchEvent {
                api_version: (*api_version).to_string(),
                kind: (*kind).to_string(),
                namespace: Some("default".to_string()),
                name: format!("{}1", kind.to_ascii_lowercase()),
                resource_version: 7,
                event_type: std::borrow::Cow::Borrowed("ADDED"),
                object_json: Bytes::from_static(object_json),
            };

            let frame = serialize_raw_watch_event_for_stream(&row, WatchStreamFormat::Protobuf);
            assert_ne!(frame.first(), Some(&b'{'));
            let frames = decode_k8s_protobuf_watch_frames(&[frame]);
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].event_type, "ADDED");
            assert_eq!(frames[0].inner_api_version, *api_version);
            assert_eq!(frames[0].inner_kind, *kind);
        }
    }

    #[test]
    fn decode_length_prefixed_watch_frames_is_boundary_agnostic() {
        let added = serialize_watch_event_for_stream(
            WatchEvent::from_type(
                "ADDED",
                serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"default","resourceVersion":"1"}}),
            ),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        let bookmark = serialize_watch_event_for_stream(
            WatchEvent::bookmark_typed(2, "v1", "ConfigMap"),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        let all = [added.as_slice(), bookmark.as_slice()].concat();
        let frames = decode_k8s_protobuf_watch_frames(&[
            all[0..3].to_vec(),
            all[3..all.len() - 5].to_vec(),
            all[all.len() - 5..].to_vec(),
        ]);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].event_type, "ADDED");
        assert_eq!(frames[1].event_type, "BOOKMARK");
    }

    #[test]
    fn protobuf_watch_event_types_table_drive_through_raw_stream_frames() {
        // Table-drive ADDED, MODIFIED, DELETED, BOOKMARK, and ERROR through the
        // same Kubernetes envelope decoder. Each frame must carry the outer
        // watched-resource group/version WatchEvent envelope and a correctly typed inner
        // resource/Status envelope.
        let added = WatchEvent::from_type(
            "ADDED",
            serde_json::json!({
                "apiVersion":"v1","kind":"ConfigMap",
                "metadata":{"name":"cm","namespace":"default","resourceVersion":"3"},
                "data":{"k":"v"}
            }),
        );
        let modified = WatchEvent::from_type(
            "MODIFIED",
            serde_json::json!({
                "apiVersion":"v1","kind":"ConfigMap",
                "metadata":{"name":"cm","namespace":"default","resourceVersion":"4"},
                "data":{"k":"v2"}
            }),
        );
        let deleted = WatchEvent::from_type(
            "DELETED",
            serde_json::json!({
                "apiVersion":"v1","kind":"ConfigMap",
                "metadata":{"name":"cm","namespace":"default","resourceVersion":"5"},
                "data":{"k":"v2"}
            }),
        );
        let bookmark = WatchEvent::bookmark_typed(6, "v1", "ConfigMap");

        let frames = [
            serialize_watch_event_for_stream(
                added,
                "ConfigMap",
                false,
                WatchStreamFormat::Protobuf,
            ),
            serialize_watch_event_for_stream(
                modified,
                "ConfigMap",
                false,
                WatchStreamFormat::Protobuf,
            ),
            serialize_watch_event_for_stream(
                deleted,
                "ConfigMap",
                false,
                WatchStreamFormat::Protobuf,
            ),
            serialize_watch_event_for_stream(
                bookmark,
                "ConfigMap",
                false,
                WatchStreamFormat::Protobuf,
            ),
            serialize_watch_status_for_stream(WatchStreamFormat::Protobuf, 410, "Expired", "gone"),
        ];
        let decoded = decode_k8s_protobuf_watch_frames(&frames);
        assert_eq!(decoded.len(), 5);
        let event_types: Vec<&str> = decoded.iter().map(|f| f.event_type.as_str()).collect();
        assert_eq!(
            event_types,
            ["ADDED", "MODIFIED", "DELETED", "BOOKMARK", "ERROR"]
        );
        for frame in &decoded {
            assert_eq!(frame.inner_api_version, "v1");
        }
        assert_eq!(decoded[0].inner_kind, "ConfigMap");
        assert_eq!(decoded[3].inner_kind, "ConfigMap");
        assert_eq!(decoded[4].inner_kind, "Status");
        let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
            decoded[4].inner_raw.as_slice(),
        )
        .unwrap();
        assert_eq!(status.code, Some(410));
    }

    #[test]
    fn raw_selectorless_protobuf_event_types_use_raw_outer_watch_event() {
        let object_json = serde_json::json!({
            "apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"namespace":"default","name":"cm1","resourceVersion":"7"},
            "data":{"k":"v"}
        })
        .to_string();
        for event_type in ["ADDED", "MODIFIED", "DELETED"] {
            let row = klights_cluster_store::DurableRawWatchEvent {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm1".to_string(),
                resource_version: 7,
                event_type: std::borrow::Cow::Borrowed(event_type),
                object_json: Bytes::from(object_json.clone().into_bytes()),
            };
            let frame = serialize_raw_watch_event_for_stream(&row, WatchStreamFormat::Protobuf);
            let decoded = decode_k8s_protobuf_watch_frames(&[frame]);
            assert_eq!(
                decoded.len(),
                1,
                "{event_type} must produce one raw WatchEvent frame"
            );
            assert_eq!(decoded[0].event_type, event_type);
            assert_eq!(decoded[0].inner_api_version, "v1");
            assert_eq!(decoded[0].inner_kind, "ConfigMap");
            let cm =
                k8s_pb::api::core::v1::ConfigMap::decode(decoded[0].inner_raw.as_slice()).unwrap();
            assert_eq!(
                cm.metadata.as_ref().and_then(|m| m.name.as_deref()),
                Some("cm1")
            );
        }
    }

    #[test]
    fn raw_protobuf_replay_encodes_conformance_service_and_vap_rows() {
        let cases = [
            (
                "v1",
                "Service",
                Some("services-8388"),
                "test-service-c6wqx",
                "ADDED",
                serde_json::json!({
                    "apiVersion":"v1","kind":"Service",
                    "metadata":{"creationTimestamp":"2026-07-13T19:55:39.826925457Z","generateName":"","generation":1,"labels":{"test-service-static":"true"},"name":"test-service-c6wqx","namespace":"services-8388","resourceVersion":"254","uid":"a3afcfc1-d98b-4432-9a40-448f406292de"},
                    "spec":{"clusterIP":"10.51.0.3","clusterIPs":["10.51.0.3"],"externalName":"","ipFamilies":["IPv4"],"ipFamilyPolicy":"SingleStack","ports":[{"name":"http","nodePort":30000,"port":80,"protocol":"TCP","targetPort":80}],"sessionAffinity":"None","type":"LoadBalancer"},
                    "status":{"loadBalancer":{"ingress":[]}}
                }),
            ),
            (
                "admissionregistration.k8s.io/v1",
                "ValidatingAdmissionPolicy",
                None,
                "e2e-example-vap-hdyic",
                "MODIFIED",
                serde_json::json!({
                    "apiVersion":"admissionregistration.k8s.io/v1","kind":"ValidatingAdmissionPolicy",
                    "metadata":{"annotations":{"patched":"true"},"creationTimestamp":"2026-07-13T19:55:40.461194107Z","generateName":"e2e-example-vap-","generation":2,"labels":{"example-e2e-vap-label":"rp4xtt7j"},"name":"e2e-example-vap-hdyic","resourceVersion":"258","uid":"21b10aad-751c-4ab7-b465-15f2c987a56d"},
                    "spec":{"failurePolicy":"Ignore","matchConstraints":{"resourceRules":[{"apiGroups":["apps"],"apiVersions":["v1"],"operations":["CREATE"],"resources":["deployments"]}]},"validations":[{"expression":"object.spec.replicas <= 100"}]},
                    "status":{"typeChecking":{"expressionWarnings":[]}}
                }),
            ),
        ];

        for (api_version, kind, namespace, name, event_type, object) in cases {
            let row = klights_cluster_store::DurableRawWatchEvent {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                name: name.to_string(),
                resource_version: object["metadata"]["resourceVersion"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                event_type: std::borrow::Cow::Borrowed(event_type),
                object_json: Bytes::from(serde_json::to_vec(&object).unwrap()),
            };
            let frame = try_serialize_raw_watch_event_for_stream(&row, WatchStreamFormat::Protobuf)
                .unwrap_or_else(|_| panic!("{kind} replay row must encode as protobuf"));
            let decoded = decode_k8s_protobuf_watch_frames(&[frame]);
            assert_eq!(decoded[0].event_type, event_type, "{kind}");
            assert_eq!(decoded[0].inner_kind, kind, "{kind}");

            let parsed_event = WatchEvent::from_type(event_type, object);
            let parsed_frame = try_serialize_watch_event_for_stream(
                parsed_event,
                kind,
                false,
                WatchStreamFormat::Protobuf,
            )
            .unwrap_or_else(|frame| {
                let decoded = decode_k8s_protobuf_watch_frames(&[frame]);
                panic!(
                    "{kind} parsed replay row must encode as protobuf, got {}",
                    String::from_utf8_lossy(&decoded[0].inner_raw)
                )
            });
            let parsed_decoded = decode_k8s_protobuf_watch_frames(&[parsed_frame]);
            assert_eq!(parsed_decoded[0].event_type, event_type, "{kind}");
            assert_eq!(parsed_decoded[0].inner_kind, kind, "{kind}");
        }
    }

    #[test]
    fn protobuf_watch_frames_split_across_chunks_decode_by_declared_length() {
        // Framing must depend on the declared frame length, not body chunk
        // boundaries: coalesce two frames and split them at arbitrary offsets.
        let first = serialize_watch_event_for_stream(
            WatchEvent::from_type(
                "ADDED",
                serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"a","namespace":"default","resourceVersion":"1"}}),
            ),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        let second = serialize_watch_event_for_stream(
            WatchEvent::bookmark_typed(2, "v1", "ConfigMap"),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        let combined = [first.as_slice(), second.as_slice()].concat();
        let split = combined.len() / 2;
        let decoded = decode_k8s_protobuf_watch_frames(&[
            combined[..split].to_vec(),
            combined[split..].to_vec(),
        ]);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].event_type, "ADDED");
        assert_eq!(decoded[1].event_type, "BOOKMARK");
    }

    #[test]
    fn default_client_go_accept_header_yields_raw_protobuf_watch_stream() {
        // A default client-go Accept header must negotiate protobuf and the
        // resulting frame must be consumable by the Kubernetes envelope
        // decoder without forcing JSON.
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf, application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf,
        );
        let frame = serialize_watch_event_for_stream(
            WatchEvent::from_type(
                "ADDED",
                serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"default","resourceVersion":"9"}}),
            ),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        );
        let decoded = decode_k8s_protobuf_watch_frames(&[frame]);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].event_type, "ADDED");
        assert_eq!(decoded[0].inner_kind, "ConfigMap");
    }

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
    fn watch_stream_negotiation_honors_q_values_and_json_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0.4, application/json;q=0.8"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert(
            "accept",
            "application/json;q=0.2, application/vnd.kubernetes.protobuf;q=0.9"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf, application/json"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, false).unwrap(),
            WatchStreamFormat::Json
        );
    }

    #[test]
    fn watch_stream_negotiation_considers_repeated_accept_headers() {
        let mut headers = HeaderMap::new();
        headers.append(
            "accept",
            "application/vnd.kubernetes.protobuf".parse().unwrap(),
        );
        headers.append("accept", "application/json".parse().unwrap());

        assert_eq!(
            negotiate_watch_stream_format(&headers, false).unwrap(),
            WatchStreamFormat::Json
        );
    }

    #[test]
    fn watch_stream_negotiation_honors_explicit_q0_exclusions() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json;q=0, */*;q=1".parse().unwrap());
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, false),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0, */*;q=1"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;Q=0, application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert(
            "accept",
            "application/json;Q=0, application/vnd.kubernetes.protobuf;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0.8junk, application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=1., application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q=0., application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert("accept", "application/json;Q=0, */*;q=0".parse().unwrap());
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;stream=watch;q=1"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Protobuf
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;stream=wrong;q=1, application/json;q=0.5"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            negotiate_watch_stream_format(&headers, true).unwrap(),
            WatchStreamFormat::Json
        );

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;stream=wrong;q=1"
                .parse()
                .unwrap(),
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert(
            "accept",
            "application/json;stream=wrong;q=1".parse().unwrap(),
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;q".parse().unwrap(),
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf;stream"
                .parse()
                .unwrap(),
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));
    }

    #[test]
    fn watch_stream_negotiation_rejects_when_no_supported_media_remains() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/vnd.kubernetes.protobuf".parse().unwrap(),
        );
        assert!(matches!(
            negotiate_watch_stream_format(&headers, false),
            Err(AppError::NotAcceptable(_))
        ));

        headers.insert("accept", "application/xml".parse().unwrap());
        assert!(matches!(
            negotiate_watch_stream_format(&headers, true),
            Err(AppError::NotAcceptable(_))
        ));
    }

    #[test]
    fn protobuf_watch_support_uses_typed_codec_for_selectorless_requests() {
        assert!(
            protobuf_watch_supported_for_request("v1", "ConfigMap", false, None, None),
            "selectorless ConfigMap protobuf watches can use the raw replay encoder"
        );
        assert!(
            protobuf_watch_supported_for_request("v1", "Pod", false, None, None),
            "selectorless Pod protobuf watches must be allowed when a typed protobuf codec exists"
        );
        assert!(
            protobuf_watch_supported_for_request("v1", "Service", false, None, None),
            "selectorless Service protobuf watches must be allowed when a typed protobuf codec exists"
        );
        assert!(
            protobuf_watch_supported_for_request("v1", "Event", false, None, None),
            "selectorless Event protobuf watches must be allowed when a typed protobuf codec exists"
        );
        assert!(
            protobuf_watch_supported_for_request("v1", "Pod", false, Some("app=guestbook"), None),
            "selector Pod protobuf watches already need parsed selector inspection"
        );
        assert!(
            !protobuf_watch_supported_for_request("v1", "ConfigMap", true, None, None),
            "Table watch output remains JSON"
        );
    }

    #[tokio::test]
    async fn positioned_watch_establishment_failure_emits_error_and_terminates() {
        for table_format in [false, true] {
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
                source: FallibleWatchSource {
                    establish_error: Some(klights_leader_api::LeaderWatchError::unavailable(
                        "history unavailable",
                    )),
                    events: Vec::new(),
                },
                task_supervisor: supervisor,
                api_version: "v1",
                kind: "ConfigMap".to_string(),
                watch_namespace: None,
                requested_rv: 0,
                send_initial_events: false,
                send_bookmarks: false,
                label_selector: None,
                field_selector: None,
                table_format,
                stream_format: WatchStreamFormat::Json,
                timeout_seconds: None,
                emit_initial_state_for_resource_version_zero: false,
                wall_clock: Arc::new(klights_auth::clock::SystemClock),
            })
            .await;

            let bytes = tokio::time::timeout(
                Duration::from_secs(1),
                axum::body::to_bytes(body, usize::MAX),
            )
            .await
            .expect("failed initial replay must terminate instead of waiting for a signal")
            .expect("watch error body should be readable");
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["type"], "ERROR", "table_format={table_format}");
            assert_eq!(value["object"]["code"], 500, "table_format={table_format}");
            assert_eq!(value["object"]["reason"], "InternalError");
        }
    }

    #[tokio::test]
    async fn positioned_watch_pull_failure_emits_one_error_and_terminates() {
        for table_format in [false, true] {
            let event = klights_leader_api::ResourceEvent::try_new(
                klights_leader_api::WatchEventType::Added,
                klights_cluster_core::Resource::from_data_lossy(Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": "established",
                        "resourceVersion": "1"
                    }
                }))),
                None,
            )
            .unwrap();
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
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
                task_supervisor: supervisor,
                api_version: "v1",
                kind: "ConfigMap".to_string(),
                watch_namespace: None,
                requested_rv: 0,
                send_initial_events: false,
                send_bookmarks: false,
                label_selector: None,
                field_selector: None,
                table_format,
                stream_format: WatchStreamFormat::Json,
                timeout_seconds: None,
                emit_initial_state_for_resource_version_zero: false,
                wall_clock: Arc::new(klights_auth::clock::SystemClock),
            })
            .await;
            let mut stream = body.into_data_stream();

            let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("initial replay should establish the live watch")
                .expect("initial replay should emit one event")
                .expect("initial replay frame should be readable");
            let first: serde_json::Value = serde_json::from_slice(&first).unwrap();
            assert_eq!(first["type"], "ADDED", "table_format={table_format}");

            let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("live replay failure must not park awaiting another signal")
                .expect("live replay failure should emit an ERROR")
                .expect("watch ERROR frame should be readable");
            let terminal: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
            assert_eq!(terminal["type"], "ERROR", "table_format={table_format}");
            assert_eq!(terminal["object"]["code"], 500);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .expect("watch must terminate after its ERROR frame")
                    .is_none(),
                "table_format={table_format}"
            );
        }
    }

    #[test]
    fn catch_up_failure_status_line_forces_client_relist() {
        let line = serialize_positioned_watch_error_for_stream(
            &klights_leader_api::LeaderWatchError::ReplayExpired {
                accepted_resource_version: 37,
            },
            WatchStreamFormat::Json,
        );
        let value: serde_json::Value = serde_json::from_slice(&line).unwrap();

        assert_eq!(value["type"], "ERROR");
        assert_eq!(value["object"]["code"], 410);
        assert_eq!(value["object"]["reason"], "Expired");
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
        let watch_source = datastore_watch_source(&ds, &handle);
        assert!(
            collection_rv > 1,
            "test fixture: global RV must be non-trivial, got {collection_rv}"
        );

        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &watch_source,
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
    async fn resolve_periodic_bookmark_rv_keeps_absent_exact_name_scope_open() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        ds.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "noise",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "noise", "namespace": "default"}
            }),
        )
        .await
        .unwrap();
        let snapshot_rv = handle.get_current_resource_version().await.unwrap();
        let watch_source = datastore_watch_source(&ds, &handle);

        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &watch_source,
            api_version: "v1",
            kind: "ConfigMap",
            watch_namespace: Some("default"),
            label_selector: None,
            field_selector: Some("metadata.name=missing"),
            requested_rv: snapshot_rv - 1,
            has_scope_filter: true,
            cursor_high_water_rv: snapshot_rv,
            last_delivered_scoped_rv: snapshot_rv - 1,
        })
        .await;

        assert_eq!(
            rv,
            snapshot_rv - 1,
            "an exact-name collection watch over an absent object must remain open for a future create"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_rv_selector_free_uses_cursor_high_water() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let watch_source = datastore_watch_source(&ds, &handle);
        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &watch_source,
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
        let watch_source = datastore_watch_source(&ds, &handle);

        // A selector-free watch that has observed nothing yet (quiet,
        // freshly established) must still emit a valid, advancing resume point.
        let rv = resolve_periodic_bookmark_rv(PeriodicBookmarkContext {
            db: &watch_source,
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
        let watch_source = datastore_watch_source(&ds, &handle);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        // resourceVersion 0 / unset: nothing to wait for.
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            wait_until_datastore_fresh(&watch_source, 0, "v1", "Pod", &supervisor),
        )
        .await
        .expect("zero target must return immediately");

        // Already at/above the current rv: return without blocking.
        let cur = handle.get_current_resource_version().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            wait_until_datastore_fresh(&watch_source, cur, "v1", "Pod", &supervisor),
        )
        .await
        .expect("already-fresh target must return immediately");
        let _ = ds;
    }

    #[tokio::test]
    async fn read_freshness_wait_wakes_on_applied_write() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let watch_source = datastore_watch_source(&ds, &handle);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let base = handle.get_current_resource_version().await.unwrap();
        let target = base + 1;

        let waiter =
            wait_until_datastore_fresh(&watch_source, target, "v1", "ConfigMap", &supervisor);
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
            supervisor.active_tasks(None).len(),
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
        let pending = crate::datastore::create_staged_post_commit(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            1,
            "ADDED",
            serde_json::json!({"metadata": {"name": "p1"}}),
        );
        let event = crate::datastore::staged_test_event(&pending).unwrap();
        let event1 = event.clone();
        let event2 = event;

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
    fn raw_watch_json_line_wraps_stored_object_bytes_without_reparse() {
        let row = klights_cluster_store::DurableRawWatchEvent {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "p1".to_string(),
            resource_version: 9,
            event_type: std::borrow::Cow::Borrowed("MODIFIED"),
            object_json: bytes::Bytes::from_static(
                br#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p1","namespace":"default","resourceVersion":"9"}}"#,
            ),
        };

        let line = serialize_raw_watch_event_line(&row);
        let decoded: serde_json::Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(decoded["type"], "MODIFIED");
        assert_eq!(decoded["object"]["metadata"]["name"], "p1");
        assert_eq!(decoded["object"]["metadata"]["resourceVersion"], "9");
    }

    #[test]
    fn watch_table_and_normal_subscribers_do_not_share_wrong_payload() {
        let pending = crate::datastore::create_staged_post_commit(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            1,
            "ADDED",
            serde_json::json!({"metadata": {"name": "p1"}}),
        );
        let event = crate::datastore::staged_test_event(&pending).unwrap();

        let ctx_json = WatchEncodeReuseContext {
            event: &event,
            table_format: false,
            protobuf: false,
            selector_transitioned: false,
        };
        assert!(can_reuse_encoded_watch_payload(&ctx_json));

        let ctx_table = WatchEncodeReuseContext {
            event: &event,
            table_format: true,
            protobuf: false,
            selector_transitioned: false,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_table));

        let ctx_protobuf = WatchEncodeReuseContext {
            event: &event,
            table_format: false,
            protobuf: true,
            selector_transitioned: false,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_protobuf));

        let ctx_transitioned = WatchEncodeReuseContext {
            event: &event,
            table_format: false,
            protobuf: false,
            selector_transitioned: true,
        };
        assert!(!can_reuse_encoded_watch_payload(&ctx_transitioned));

        let json_line = serialize_watch_event_line(event.clone(), "Pod", false);
        let table_line = serialize_watch_event_line(event, "Pod", true);
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

    #[test]
    fn production_positioned_watch_has_json_protobuf_and_grpc_parity() {
        let position = WatchReplayPosition {
            resource_version: 73,
            event_id: 109,
            resource_version_filter_through_event_id: 0,
        };
        let resource = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "positioned",
                "namespace": "default",
                "uid": "uid-positioned",
                "resourceVersion": "73"
            },
            "data": {"key": "value"}
        })))
        .expect("valid ConfigMap");
        let event = klights_leader_api::ResourceEvent::try_new(
            klights_leader_api::WatchEventType::Modified,
            resource,
            Some(position),
        )
        .expect("valid positioned event");

        let json = serialize_positioned_watch_event_for_stream(
            event.clone(),
            "ConfigMap",
            false,
            WatchStreamFormat::Json,
        )
        .expect("JSON delivery");
        let decoded_json: serde_json::Value =
            serde_json::from_slice(&json).expect("JSON watch event");
        assert_eq!(decoded_json["type"], "MODIFIED");
        assert_eq!(decoded_json["object"]["metadata"]["name"], "positioned");
        assert_eq!(decoded_json["object"]["metadata"]["resourceVersion"], "73");

        let protobuf = serialize_positioned_watch_event_for_stream(
            event.clone(),
            "ConfigMap",
            false,
            WatchStreamFormat::Protobuf,
        )
        .expect("protobuf delivery");
        let frames = decode_k8s_protobuf_watch_frames(&[protobuf]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event_type, "MODIFIED");
        assert_eq!(frames[0].inner_api_version, "v1");
        assert_eq!(frames[0].inner_kind, "ConfigMap");
        let decoded = k8s_pb::api::core::v1::ConfigMap::decode(frames[0].inner_raw.as_slice())
            .expect("ConfigMap payload");
        let metadata = decoded.metadata.expect("metadata");
        assert_eq!(metadata.name.as_deref(), Some("positioned"));
        assert_eq!(metadata.resource_version.as_deref(), Some("73"));

        let grpc = klights_leader_rpc::server::resource_to_proto(event.resource());
        assert_eq!(grpc.api_version, "v1");
        assert_eq!(grpc.kind, "ConfigMap");
        assert_eq!(grpc.namespace.as_deref(), Some("default"));
        assert_eq!(grpc.name, "positioned");
        assert_eq!(grpc.resource_version, 73);
        let grpc_object: serde_json::Value =
            serde_json::from_slice(&grpc.data_json).expect("gRPC JSON resource");
        assert_eq!(grpc_object, decoded_json["object"]);

        assert_eq!(event.resume_position(), Some(position));
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

    fn decode_split_json_watch_lines(chunks: &[Vec<u8>]) -> Vec<serde_json::Value> {
        let mut pending = Vec::new();
        let mut events = Vec::new();
        for chunk in chunks {
            pending.extend_from_slice(chunk);
            while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                events.push(
                    serde_json::from_slice(&pending[..end]).expect("fragmented JSON watch line"),
                );
                pending.drain(..=end);
            }
        }
        assert!(
            pending.is_empty(),
            "every JSON watch event must end in a newline"
        );
        events
    }

    #[test]
    fn positioned_sequence_and_terminal_errors_have_json_protobuf_parity_across_split_chunks() {
        let cases = [
            (
                klights_leader_api::WatchEventType::Added,
                "ADDED",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "added", "namespace": "default", "resourceVersion": "71"},
                    "data": {"state": "added"}
                }),
            ),
            (
                klights_leader_api::WatchEventType::Modified,
                "MODIFIED",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "modified", "namespace": "default", "resourceVersion": "72"},
                    "data": {"state": "modified"}
                }),
            ),
            (
                klights_leader_api::WatchEventType::Deleted,
                "DELETED",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": "deleted", "namespace": "default", "resourceVersion": "73"},
                    "data": {"state": "deleted"}
                }),
            ),
            (
                klights_leader_api::WatchEventType::Bookmark,
                "BOOKMARK",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"resourceVersion": "74"}
                }),
            ),
            (
                klights_leader_api::WatchEventType::Error,
                "ERROR",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Status", "metadata": {},
                    "status": "Failure", "code": 409, "reason": "Conflict", "message": "conflict"
                }),
            ),
        ];
        let mut json_wire = Vec::new();
        let mut protobuf_wire = Vec::new();
        let mut expected_positions = Vec::new();
        for (index, (event_type, _, object)) in cases.iter().enumerate() {
            let resource =
                klights_cluster_core::Resource::from_data_lossy(Arc::new(object.clone()));
            let position = WatchReplayPosition {
                resource_version: resource.resource_version,
                event_id: 200 + index as i64,
                resource_version_filter_through_event_id: 0,
            };
            let event =
                klights_leader_api::ResourceEvent::try_new(*event_type, resource, Some(position))
                    .expect("valid positioned event");
            expected_positions.push(event.resume_position());
            json_wire.extend(
                serialize_positioned_watch_event_for_stream(
                    event.clone(),
                    event.resource().kind.as_str(),
                    false,
                    WatchStreamFormat::Json,
                )
                .expect("JSON event"),
            );
            protobuf_wire.extend(
                serialize_positioned_watch_event_for_stream(
                    event.clone(),
                    event.resource().kind.as_str(),
                    false,
                    WatchStreamFormat::Protobuf,
                )
                .expect("protobuf event"),
            );
            assert_eq!(event.resume_position(), Some(position));
        }

        let json_events = decode_split_json_watch_lines(&split_watch_bytes(&json_wire));
        let protobuf_events = decode_k8s_protobuf_watch_frames(&split_watch_bytes(&protobuf_wire));
        assert_eq!(json_events.len(), cases.len());
        assert_eq!(protobuf_events.len(), cases.len());
        for (index, (_, expected_type, _)) in cases.iter().enumerate() {
            assert_eq!(json_events[index]["type"], *expected_type);
            assert_eq!(protobuf_events[index].event_type, *expected_type);
            let json_object = &json_events[index]["object"];
            assert_eq!(
                protobuf_events[index].inner_api_version,
                json_object["apiVersion"].as_str().unwrap_or_default(),
            );
            assert_eq!(
                protobuf_events[index].inner_kind,
                json_object["kind"].as_str().unwrap_or_default(),
            );
            if protobuf_events[index].inner_kind == "Status" {
                // Status is encoded through its dedicated meta/v1 protobuf
                // codec and is intentionally absent from the generic resource
                // registry. Decode the actual Kubernetes semantic type so the
                // parity check does not discard every field after metadata.
                let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
                    protobuf_events[index].inner_raw.as_slice(),
                )
                .expect("protobuf Status payload");
                assert_eq!(status.status.as_deref(), json_object["status"].as_str());
                assert_eq!(status.message.as_deref(), json_object["message"].as_str());
                assert_eq!(status.reason.as_deref(), json_object["reason"].as_str());
                assert_eq!(status.code.map(i64::from), json_object["code"].as_i64());
            } else {
                let envelope = klights_kube_protobuf::wrap_protobuf_resource_envelope(
                    &protobuf_events[index].inner_api_version,
                    &protobuf_events[index].inner_kind,
                    protobuf_events[index].inner_raw.clone(),
                )
                .expect("resource envelope");
                let protobuf_object = klights_kube_protobuf::decode_protobuf(&envelope)
                    .expect("decoded protobuf object");
                assert_eq!(
                    protobuf_object["metadata"]["resourceVersion"],
                    json_object["metadata"]["resourceVersion"],
                );
                assert_eq!(
                    protobuf_object["metadata"]["name"],
                    json_object["metadata"]["name"],
                );
            }
            assert!(json_events[index].get("resumePosition").is_none());
        }
        assert!(
            expected_positions
                .into_iter()
                .all(|position| position.is_some())
        );

        for (error, expected_code, expected_reason) in [
            (
                klights_leader_api::LeaderWatchError::ReplayExpired {
                    accepted_resource_version: 73,
                },
                410,
                "Expired",
            ),
            (
                klights_leader_api::LeaderWatchError::unavailable("history unavailable"),
                500,
                "InternalError",
            ),
        ] {
            let json = serialize_positioned_watch_error_for_stream(&error, WatchStreamFormat::Json);
            let protobuf =
                serialize_positioned_watch_error_for_stream(&error, WatchStreamFormat::Protobuf);
            let json = decode_split_json_watch_lines(&split_watch_bytes(&json));
            let protobuf = decode_k8s_protobuf_watch_frames(&split_watch_bytes(&protobuf));
            assert_eq!(json[0]["type"], "ERROR");
            assert_eq!(json[0]["object"]["code"], expected_code);
            assert_eq!(json[0]["object"]["reason"], expected_reason);
            assert_eq!(protobuf[0].event_type, "ERROR");
            let status = k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(
                protobuf[0].inner_raw.as_slice(),
            )
            .expect("protobuf Status");
            assert_eq!(status.code, Some(expected_code));
            assert_eq!(status.reason.as_deref(), Some(expected_reason));
        }
    }
}
