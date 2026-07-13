use crate::api::watch_session::{WatchSessionBootstrap, WatchSessionConfig, WatchSessionEvent};
use crate::api::{AppError, watch_event_to_table};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{CatchUpResource, RawWatchEvent};
use crate::datastore::{
    DatastoreBackendWatchStore, DatastoreHandle, RawWatchReplayStore, SnapshotAtRv,
    WatchReplayAnchorStore, WatchReplayPosition, WatchTarget,
};
use crate::label_selector::LabelSelector;
use crate::watch::{
    EventType, RawSignalWatchCursor, WatchContentType, WatchCursorError, WatchDeliveryScope,
    WatchEvent, WatchSignalReceiver, WatchTopic,
};
#[cfg(test)]
use crate::watch::{event_key as watch_event_key, resource_key as resource_to_seen_key};
use axum::body::Body;
use axum::http::HeaderMap;
use k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent as PbWatchEvent;
use k8s_pb::apimachinery::pkg::runtime::RawExtension;
use prost::Message;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchCatchUpMode {
    NamespacedScoped,
    ClusterOnly,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptedWatchMedia {
    Json,
    Protobuf,
    ApplicationWildcard,
    Any,
    Unsupported,
}

#[derive(Clone, Copy, Debug)]
struct AcceptedWatchFormat {
    media: AcceptedWatchMedia,
    quality_millis: u16,
    order: usize,
}

#[derive(Clone, Copy, Debug)]
struct CandidateWatchFormat {
    format: WatchStreamFormat,
    quality_millis: u16,
    order: usize,
    preference: usize,
}

pub fn negotiate_watch_stream_format(
    headers: &HeaderMap,
    protobuf_supported: bool,
) -> Result<WatchStreamFormat, AppError> {
    if headers.get("accept").is_none() {
        return Ok(WatchStreamFormat::Json);
    }
    let mut accepted = Vec::new();
    let mut order = 0usize;
    for accept in headers.get_all("accept") {
        let Ok(accept) = accept.to_str() else {
            continue;
        };
        for part in accept.split(',') {
            let mut segments = part.split(';').map(str::trim);
            let media = segments.next().unwrap_or_default().to_ascii_lowercase();
            if media.is_empty() {
                continue;
            }
            let mut quality_millis = 1000;
            for parameter in segments {
                if let Some((name, value)) = parameter.split_once('=')
                    && name.trim().eq_ignore_ascii_case("q")
                {
                    quality_millis = parse_accept_quality_millis(value);
                }
            }
            let media = match media.as_str() {
                "application/json" => AcceptedWatchMedia::Json,
                "application/vnd.kubernetes.protobuf" => AcceptedWatchMedia::Protobuf,
                "application/*" => AcceptedWatchMedia::ApplicationWildcard,
                "*/*" => AcceptedWatchMedia::Any,
                _ => AcceptedWatchMedia::Unsupported,
            };
            accepted.push(AcceptedWatchFormat {
                media,
                quality_millis,
                order,
            });
            order += 1;
        }
    }

    let mut candidates = Vec::new();
    if let Some((quality_millis, order)) =
        best_watch_accept_match(WatchStreamFormat::Json, &accepted)
        && quality_millis > 0
    {
        candidates.push(CandidateWatchFormat {
            format: WatchStreamFormat::Json,
            quality_millis,
            order,
            preference: 0,
        });
    }
    if protobuf_supported
        && let Some((quality_millis, order)) =
            best_watch_accept_match(WatchStreamFormat::Protobuf, &accepted)
        && quality_millis > 0
    {
        candidates.push(CandidateWatchFormat {
            format: WatchStreamFormat::Protobuf,
            quality_millis,
            order,
            preference: 1,
        });
    }
    candidates.sort_by_key(|candidate| {
        (
            std::cmp::Reverse(candidate.quality_millis),
            candidate.order,
            candidate.preference,
        )
    });
    if let Some(candidate) = candidates.first() {
        return Ok(candidate.format);
    }
    Err(AppError::NotAcceptable(
        "no supported watch stream media type requested".to_string(),
    ))
}

fn best_watch_accept_match(
    format: WatchStreamFormat,
    accepted: &[AcceptedWatchFormat],
) -> Option<(u16, usize)> {
    accepted
        .iter()
        .filter_map(|accepted| {
            let specificity = match (format, accepted.media) {
                (WatchStreamFormat::Json, AcceptedWatchMedia::Json)
                | (WatchStreamFormat::Protobuf, AcceptedWatchMedia::Protobuf) => 2,
                (_, AcceptedWatchMedia::ApplicationWildcard) => 1,
                (_, AcceptedWatchMedia::Any) => 0,
                _ => return None,
            };
            Some((
                specificity,
                accepted.quality_millis,
                std::cmp::Reverse(accepted.order),
            ))
        })
        .max()
        .map(|(_specificity, quality_millis, std::cmp::Reverse(order))| (quality_millis, order))
}

fn parse_accept_quality_millis(value: &str) -> u16 {
    let value = value.trim();
    if value == "1" || value == "1.0" || value == "1.00" || value == "1.000" {
        return 1000;
    }
    if value == "0" {
        return 0;
    }
    let Some(fraction) = value.strip_prefix("0.") else {
        return 0;
    };
    let mut millis = 0u16;
    for (idx, byte) in fraction.bytes().take(3).enumerate() {
        if !byte.is_ascii_digit() {
            break;
        }
        let place = match idx {
            0 => 100,
            1 => 10,
            _ => 1,
        };
        millis += u16::from(byte - b'0') * place;
    }
    millis
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
        crate::protobuf::supports_protobuf_resource(api_version, kind)
    } else {
        crate::protobuf::supports_raw_json_protobuf_resource(api_version, kind)
            || crate::protobuf::supports_protobuf_resource(api_version, kind)
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
    let mut fresh_rx = db.subscribe_watch_signals(topic);
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
                Ok(signal) => {
                    // Any applied write with rv >= target proves the
                    // monotonic resource-version counter has reached the
                    // target — no DB round-trip needed on the hot path.
                    if signal
                        .advances
                        .iter()
                        .any(|advance| advance.high_rv >= target_rv)
                    {
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

pub fn serialize_watch_event_frame(event: &WatchEvent, kind: &str) -> anyhow::Result<Vec<u8>> {
    let object_kind = event
        .object
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or(kind);
    let raw = if object_kind == "Status" {
        let raw = crate::protobuf::encode_status_protobuf(&event.object)?;
        crate::protobuf::wrap_protobuf_resource_envelope("v1", "Status", raw)?
    } else {
        let api_version = event
            .object
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        let raw = crate::protobuf::encode_protobuf_resource(object_kind, &event.object)?;
        crate::protobuf::wrap_protobuf_resource_envelope(api_version, object_kind, raw)?
    };
    let pb_event = PbWatchEvent {
        r#type: Some(event.event_type.to_string()),
        object: Some(RawExtension { raw: Some(raw) }),
    };
    let event_bytes = pb_event.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + event_bytes.len());
    frame.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
    frame.extend(event_bytes);
    Ok(frame)
}

pub fn serialize_raw_watch_event_frame(event: &RawWatchEvent) -> anyhow::Result<Vec<u8>> {
    let raw = crate::protobuf::encode_protobuf_resource_from_json_bytes(
        &event.api_version,
        &event.kind,
        &event.object_json,
    )?;
    let raw =
        crate::protobuf::wrap_protobuf_resource_envelope(&event.api_version, &event.kind, raw)?;
    let pb_event = PbWatchEvent {
        r#type: Some(event.event_type.to_string()),
        object: Some(RawExtension { raw: Some(raw) }),
    };
    let event_bytes = pb_event.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + event_bytes.len());
    frame.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
    frame.extend(event_bytes);
    Ok(frame)
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

pub fn try_serialize_watch_event_for_stream(
    event: WatchEvent,
    kind: &str,
    table_format: bool,
    stream_format: WatchStreamFormat,
) -> Result<Vec<u8>, Vec<u8>> {
    match stream_format {
        WatchStreamFormat::Json => Ok(serialize_watch_event_line(event, kind, table_format)),
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

pub fn serialize_raw_watch_event_line(event: &RawWatchEvent) -> Vec<u8> {
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
    event: &RawWatchEvent,
    stream_format: WatchStreamFormat,
) -> Vec<u8> {
    match try_serialize_raw_watch_event_for_stream(event, stream_format) {
        Ok(frame) | Err(frame) => frame,
    }
}

pub fn try_serialize_raw_watch_event_for_stream(
    event: &RawWatchEvent,
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

/// Convert any live cursor failure into the watch protocol's terminal frame.
/// A closed signal channel ends cleanly; replay failures and expired history
/// require the client to reconnect rather than parking with an undelivered row.
pub(crate) fn serialize_live_watch_cursor_error(error: &WatchCursorError) -> Option<Vec<u8>> {
    match error {
        WatchCursorError::Replay(_) => Some(serialize_watch_status_line(
            500,
            "InternalError",
            "failed to replay live watch events",
        )),
        WatchCursorError::Expired => Some(serialize_watch_status_line(
            410,
            "Expired",
            "too old resource version: watch fell behind the history window",
        )),
        WatchCursorError::Closed => None,
    }
}

pub(crate) fn serialize_live_watch_cursor_error_for_stream(
    error: &WatchCursorError,
    stream_format: WatchStreamFormat,
) -> Option<Vec<u8>> {
    match error {
        WatchCursorError::Replay(_) => Some(serialize_watch_status_for_stream(
            stream_format,
            500,
            "InternalError",
            "failed to replay live watch events",
        )),
        WatchCursorError::Expired => Some(serialize_watch_status_for_stream(
            stream_format,
            410,
            "Expired",
            "too old resource version: watch fell behind the history window",
        )),
        WatchCursorError::Closed => None,
    }
}

#[cfg(test)]
fn serialize_watch_catch_up_failure_status_line() -> Vec<u8> {
    serialize_watch_status_line(
        410,
        "Expired",
        "too old resource version: unable to replay watch catch-up; relist required",
    )
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeriodicBookmarkDecision {
    Bookmark(i64),
    Expired,
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
pub(crate) async fn resolve_periodic_bookmark_decision(
    ctx: PeriodicBookmarkContext<'_>,
) -> PeriodicBookmarkDecision {
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
        if label_selector.is_none()
            && exact_metadata_name_field_selector(field_selector, watch_namespace).is_some()
            && let Ok(list) = db
                .list_resources(
                    api_version,
                    kind,
                    watch_namespace,
                    crate::datastore::ResourceListQuery::new(None, field_selector, Some(1), None),
                )
                .await
            && list.items.is_empty()
            && list.resource_version > rv
        {
            tracing::warn!(
                target: "klights::watch_diag",
                api_version = %api_version,
                kind = %kind,
                namespace = watch_namespace.unwrap_or(""),
                field_selector = field_selector.unwrap_or(""),
                requested_rv,
                bookmark_rv = rv,
                snapshot_rv = list.resource_version,
                cursor_high_water_rv,
                "scoped exact-name watch expired because selected object is absent and cursor advanced"
            );
            return PeriodicBookmarkDecision::Expired;
        }
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
    PeriodicBookmarkDecision::Bookmark(rv)
}

fn exact_metadata_name_field_selector<'a>(
    field_selector: Option<&'a str>,
    watch_namespace: Option<&str>,
) -> Option<&'a str> {
    let selector = field_selector?;
    let mut name = None;
    for part in crate::label_selector::split_selector(selector) {
        let (field, value) = part.split_once('=')?;
        if part.contains("!=") {
            return None;
        }
        let field = field.trim();
        let value = value.trim();
        if value.starts_with('=') {
            return None;
        }
        match field {
            "metadata.name" if !value.is_empty() => name = Some(value),
            "metadata.namespace" if watch_namespace.is_some_and(|namespace| namespace == value) => {
            }
            _ => return None,
        }
    }
    name
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
    pub watch_anchor: Arc<dyn WatchReplayAnchorStore>,
    pub signal_rx: WatchSignalReceiver,
    pub replay_start_position: WatchReplayPosition,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
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
    pub catch_up_mode: WatchCatchUpMode,
    pub timeout_seconds: Option<u64>,
    pub emit_initial_state_for_resource_version_zero: bool,
}

/// Capture the durable replay boundary before subscribing to signal wakeups.
/// Writes in the narrow boundary-to-subscribe interval remain visible through
/// positioned replay, so correctness never depends on observing their signal.
pub fn watch_replay_anchor_from_backend(db: &DatastoreHandle) -> Arc<dyn WatchReplayAnchorStore> {
    Arc::new(DatastoreBackendWatchStore::new(db.clone()))
}

pub async fn subscribe_watch_handoff(
    watch_anchor: &dyn WatchReplayAnchorStore,
    db: &DatastoreHandle,
    topics: Vec<WatchTopic>,
    requested_rv: i64,
) -> Result<(WatchSignalReceiver, WatchReplayPosition), AppError> {
    let handoff_position = watch_anchor
        .current_watch_replay_position()
        .await
        .map_err(|err| {
            AppError::InternalError(format!(
                "failed to capture durable watch establishment position: {err}"
            ))
        })?;
    let replay_start_position = if requested_rv <= 0 {
        handoff_position
    } else {
        WatchReplayPosition::from_resource_version_through_event_id(
            requested_rv,
            handoff_position.event_id,
        )
    };
    let signal_rx = WatchSignalReceiver::new(
        topics
            .into_iter()
            .map(|topic| db.subscribe_watch_signals(topic))
            .collect(),
    );
    Ok((signal_rx, replay_start_position))
}

pub fn build_label_selector_watch_stream(request: LabelSelectorWatchStreamRequest<'_>) -> Body {
    let LabelSelectorWatchStreamRequest {
        db,
        watch_anchor,
        signal_rx,
        replay_start_position,
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
        catch_up_mode,
        timeout_seconds,
        emit_initial_state_for_resource_version_zero,
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
                yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                    stream_format,
                    400,
                    "BadRequest",
                    err,
                ));
                return;
            }
        };
        let has_label_selector = parsed_label_selector.is_some();
        let has_selector = has_label_selector || field_selector.is_some();
        let use_raw_selectorless_stream =
            !has_selector
                && !table_format
                && (stream_format == WatchStreamFormat::Json
                    || crate::protobuf::supports_raw_json_protobuf_resource(&api_version, &kind));
        let replay_target = match (catch_up_mode, watch_namespace.clone()) {
            (WatchCatchUpMode::NamespacedScoped, Some(ns)) => {
                WatchTarget::namespaced_in_namespace(api_version.clone(), kind.clone(), ns)
            }
            (WatchCatchUpMode::NamespacedScoped, None) => {
                WatchTarget::namespaced(api_version.clone(), kind.clone())
            }
            (WatchCatchUpMode::ClusterOnly, _) => {
                WatchTarget::cluster(api_version.clone(), kind.clone())
            }
        };
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

        // Client/bookmark RV progress is independent from the durable replay
        // boundary captured before signal subscription. Initial and catch-up
        // delivery may advance this scoped RV without moving replay past an
        // event that landed during establishment.
        let mut session_bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
            requested_rv,
            has_selector,
        });
        session_bootstrap.set_replay_start_position(replay_start_position);

        // Track resources visible to this watcher for label-selector-aware transitions.
        // If a MODIFIED event enters selector view -> send ADDED.
        // If a visible resource leaves selector view -> send DELETED.
        // Keyed by (namespace, name) to avoid collisions across namespaces
        // in cluster-wide watches.

        // Label-selector watches need a current membership baseline. For
        // resourceVersion-less selector watches, Kubernetes-compatible clients
        // such as the ServiceAccount lifecycle conformance test expect existing
        // matching objects to be delivered as ADDED. Explicit
        // resourceVersion=0 has the same current-state replay semantics even
        // without selectors; omitted resourceVersion still starts from now.
        if (has_selector || emit_initial_state_for_resource_version_zero) && !send_initial_events {
            let baseline_query = || crate::datastore::ResourceListQuery::new(
                label_selector.as_deref(),
                field_selector.as_deref(),
                None,
                None,
            );
            let baseline = if requested_rv > 0 {
                match watch_anchor
                    .snapshot_resources_at_position(
                        std::slice::from_ref(&replay_target),
                        label_selector.as_deref(),
                        field_selector.as_deref(),
                        replay_start_position,
                    )
                    .await
                {
                    Ok(SnapshotAtRv::List(list)) => Ok(list),
                    Ok(SnapshotAtRv::Current) => db
                        .list_resources(
                            &api_version,
                            &kind,
                            watch_namespace.as_deref(),
                            baseline_query(),
                        )
                        .await,
                    Ok(SnapshotAtRv::Expired) => {
                        yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                            stream_format,
                            410,
                            "Expired",
                            "too old resource version: selector membership snapshot expired",
                        ));
                        return;
                    }
                    Err(err) => Err(err),
                }
            } else {
                db.list_resources(
                    &api_version,
                    &kind,
                    watch_namespace.as_deref(),
                    baseline_query(),
                )
                .await
            };
            let baseline = match baseline {
                Ok(baseline) => baseline,
                Err(err) => {
                    tracing::warn!(error = %err, "watch selector baseline LIST failed");
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                        stream_format,
                        500,
                        "InternalError",
                        "failed to establish watch selector baseline",
                    ));
                    return;
                }
            };
            if requested_rv <= 0
                && let Some(position) = baseline.watch_replay_position
            {
                session_bootstrap.set_replay_start_position(position);
            }
            for resource in baseline.items {
                if requested_rv <= 0 {
                    let event = CatchUpResource::added(resource).into_watch_event();
                    let line = match try_serialize_watch_event_for_stream(
                        event.clone(),
                        &kind,
                        table_format,
                        stream_format,
                    ) {
                        Ok(line) => line,
                        Err(line) => {
                            yield Ok::<_, std::convert::Infallible>(line);
                            return;
                        }
                    };
                    session_bootstrap.record_baseline_event(&event);
                    yield Ok::<_, std::convert::Infallible>(line);
                } else {
                    let event = CatchUpResource::added(resource).into_watch_event();
                    session_bootstrap.record_baseline_event(&event);
                }
            }
            // Baseline delivery advances scoped/bookmark RV independently
            // from the exact durable position captured with the LIST.
        }

        if send_initial_events {
            let initial_list = db
                .list_resources(&api_version, &kind, watch_namespace.clone().as_deref(), crate::datastore::ResourceListQuery::new(label_selector.clone().as_deref(), field_selector.as_deref(), None, None))
                .await;

            let list = match initial_list {
                Ok(list) => list,
                Err(err) => {
                    tracing::warn!(error = %err, "WatchList snapshot LIST failed");
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                        stream_format,
                        500,
                        "InternalError",
                        "failed to establish WatchList snapshot",
                    ));
                    return;
                }
            };
            if let Some(position) = list.watch_replay_position {
                session_bootstrap.set_replay_start_position(position);
            }
            let mut last_rv = requested_rv.max(list.resource_version);
            for resource in list.items {
                let resource_rv = resource.resource_version;
                let event = CatchUpResource::added(resource).into_watch_event();
                let line = match try_serialize_watch_event_for_stream(
                    event.clone(),
                    &kind,
                    table_format,
                    stream_format,
                ) {
                    Ok(line) => line,
                    Err(line) => {
                        yield Ok::<_, std::convert::Infallible>(line);
                        return;
                    }
                };
                last_rv = last_rv.max(resource_rv);
                session_bootstrap.record_baseline_event(&event);
                yield Ok::<_, std::convert::Infallible>(line);
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

            session_bootstrap.observe_snapshot_rv(last_rv);

            let bookmark_event =
                WatchEvent::bookmark_initial_events_end(last_rv, &api_version, &kind);
            match try_serialize_watch_event_for_stream(
                bookmark_event,
                &kind,
                table_format,
                stream_format,
            ) {
                Ok(line) => yield Ok::<_, std::convert::Infallible>(line),
                Err(line) => {
                    yield Ok::<_, std::convert::Infallible>(line);
                    return;
                }
            }
        }

        let replay_source = DatastoreWatchReplaySource::new(
            std::sync::Arc::new(crate::datastore::DatastoreBackendWatchStore::new(db.clone())),
            vec![replay_target.clone()],
        );
        let topic = WatchTopic::new(&api_version, &kind);
        let delivery_scope = match (catch_up_mode, watch_namespace.clone()) {
            (WatchCatchUpMode::ClusterOnly, _) => WatchDeliveryScope::Cluster,
            (WatchCatchUpMode::NamespacedScoped, Some(ns)) => WatchDeliveryScope::Namespaced(ns),
            (WatchCatchUpMode::NamespacedScoped, None) => WatchDeliveryScope::NamespacedAll,
        };
        if use_raw_selectorless_stream {
            let mut cursor = RawSignalWatchCursor::new_at_position(
                signal_rx,
                Arc::new(DatastoreBackendWatchStore::new(db.clone()))
                    as Arc<dyn RawWatchReplayStore>,
                vec![replay_target.clone()],
                topic.clone(),
                delivery_scope.clone(),
                session_bootstrap.cursor_floor(),
                session_bootstrap.replay_start_position(),
            );
            match cursor.prime_replay_or_expired().await {
                    Ok(_) => {}
                    Err(WatchCursorError::Expired) => {
                        yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                            stream_format,
                            410,
                            "Expired",
                            "too old resource version: requested resourceVersion is older than the watch history window",
                        ));
                        return;
                    }
                    Err(err) => {
                        tracing::warn!("Initial raw watch replay failed for {}: {:#?}", kind, err);
                        yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                            stream_format,
                            500,
                            "InternalError",
                            "failed to establish initial watch replay",
                        ));
                        return;
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
                    result = cursor.next_event() => {
                        let event = match result {
                            Ok(event) => event,
                            Err(err) => {
                                match watch_namespace.as_deref() {
                                    Some(ns) => tracing::warn!("Raw watch terminated for {}/{}: {:#?}", ns, kind, err),
                                    None => tracing::warn!("Raw watch terminated for {}: {:#?}", kind, err),
                                }
                                if let Some(line) = serialize_live_watch_cursor_error_for_stream(&err, stream_format) {
                                    yield Ok::<_, std::convert::Infallible>(line);
                                }
                                break;
                            }
                        };

                        let rv = event.resource_version;
                        match try_serialize_raw_watch_event_for_stream(&event, stream_format) {
                            Ok(line) => {
                                yield Ok::<_, std::convert::Infallible>(line);
                                cursor.accept_event(rv);
                                session_bootstrap.observe_delivered_rv(rv);
                            }
                            Err(line) => {
                                yield Ok::<_, std::convert::Infallible>(line);
                                break;
                            }
                        }
                    }
                    Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                        let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
                            db: &db,
                            api_version: &api_version,
                            kind: &kind,
                            watch_namespace: watch_namespace.as_deref(),
                            label_selector: label_selector.as_deref(),
                            field_selector: field_selector.as_deref(),
                            requested_rv,
                            has_scope_filter,
                            cursor_high_water_rv: cursor.accepted_rv(),
                            last_delivered_scoped_rv: session_bootstrap.last_delivered_scoped_rv(),
                        })
                        .await;
                        match decision {
                            PeriodicBookmarkDecision::Bookmark(rv) => {
                                let event = WatchEvent::bookmark_typed(rv, &api_version, &kind);
                                match try_serialize_watch_event_for_stream(
                                    event,
                                    &kind,
                                    table_format,
                                    stream_format,
                                ) {
                                    Ok(line) => yield Ok::<_, std::convert::Infallible>(line),
                                    Err(line) => {
                                        yield Ok::<_, std::convert::Infallible>(line);
                                        break;
                                    }
                                }
                            }
                            PeriodicBookmarkDecision::Expired => {
                                yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                                    stream_format,
                                    410,
                                    "Expired",
                                    "too old resource version: exact-name watch scope is absent and the watch cursor advanced",
                                ));
                                break;
                            }
                        }
                    }
                }
            }
            return;
        }
        let mut session = session_bootstrap.establish_many(
            signal_rx,
            replay_source,
            vec![topic],
            delivery_scope,
        );
        match session.prime_replay_or_expired().await {
                Ok(_) => {}
                Err(WatchCursorError::Expired) => {
                    // Resume point predates the retained watch-event window;
                    // tell the client to relist (HTTP 410 Gone semantics).
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                        stream_format,
                        410,
                        "Expired",
                        "too old resource version: requested resourceVersion is older than the watch history window",
                    ));
                    return;
                }
                Err(err) => {
                    tracing::warn!("Initial watch replay failed for {}: {:#?}", kind, err);
                    yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                        stream_format,
                        500,
                        "InternalError",
                        "failed to establish initial watch replay",
                    ));
                    return;
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
                result = session.next_event() => {
                    let event = match result {
                        Ok(event) => event,
                        Err(err) => {
                            match watch_namespace.as_deref() {
                                Some(ns) => tracing::warn!("Watch terminated for {}/{}: {:#?}", ns, kind, err),
                                None => tracing::warn!("Watch terminated for {}: {:#?}", kind, err),
                            }
                            if let Some(line) = serialize_live_watch_cursor_error_for_stream(&err, stream_format) {
                                yield Ok::<_, std::convert::Infallible>(line);
                            }
                            break;
                        }
                    };

                    let matches = event.matches_filter_parsed(&kind, watch_namespace.as_deref(), parsed_label_selector.as_ref())
                        && event.matches_field_selector(field_selector.as_deref());
                    if has_selector {
                        let source_rv = event.resource_version();
                        match session.classify_event(event, matches) {
                            WatchSessionEvent::Deliver(transitioned) => {
                                match try_serialize_watch_event_for_stream(
                                    transitioned,
                                    &kind,
                                    table_format,
                                    stream_format,
                                ) {
                                    Ok(line) => {
                                        yield Ok::<_, std::convert::Infallible>(line);
                                        if let Some(rv) = source_rv {
                                            session.accept_delivered_rv(rv);
                                        }
                                    }
                                    Err(line) => {
                                        yield Ok::<_, std::convert::Infallible>(line);
                                        break;
                                    }
                                }
                            }
                            WatchSessionEvent::Filtered => {}
                        }
                    } else {
                        match session.classify_event(event, matches) {
                            WatchSessionEvent::Deliver(event) => {
                                let rv = event.resource_version();
                                match try_serialize_watch_event_for_stream(
                                    event,
                                    &kind,
                                    table_format,
                                    stream_format,
                                ) {
                                    Ok(line) => {
                                        yield Ok::<_, std::convert::Infallible>(line);
                                        if let Some(rv) = rv {
                                            session.accept_delivered_rv(rv);
                                        }
                                    }
                                    Err(line) => {
                                        yield Ok::<_, std::convert::Infallible>(line);
                                        break;
                                    }
                                }
                            }
                            WatchSessionEvent::Filtered => {}
                        }
                    }
                }
                Some(()) = recv_bookmark_tick(&mut bookmark_ticks), if send_bookmarks => {
                    let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
                        db: &db,
                        api_version: &api_version,
                        kind: &kind,
                        watch_namespace: watch_namespace.as_deref(),
                        label_selector: label_selector.as_deref(),
                        field_selector: field_selector.as_deref(),
                        requested_rv,
                        has_scope_filter,
                        cursor_high_water_rv: session.accepted_rv(),
                        last_delivered_scoped_rv: session.last_delivered_scoped_rv(),
                    })
                    .await;
                    match decision {
                        PeriodicBookmarkDecision::Bookmark(rv) => {
                            let event = WatchEvent::bookmark_typed(rv, &api_version, &kind);
                            match try_serialize_watch_event_for_stream(
                                event,
                                &kind,
                                table_format,
                                stream_format,
                            ) {
                                Ok(line) => yield Ok::<_, std::convert::Infallible>(line),
                                Err(line) => {
                                    yield Ok::<_, std::convert::Infallible>(line);
                                    break;
                                }
                            }
                        }
                        PeriodicBookmarkDecision::Expired => {
                            yield Ok::<_, std::convert::Infallible>(serialize_watch_status_for_stream(
                                stream_format,
                                410,
                                "Expired",
                                "too old resource version: exact-name watch scope is absent and the watch cursor advanced",
                            ));
                            break;
                        }
                    }
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
    use crate::watch::{SelectorMembership, WatchSignal};
    use bytes::Bytes;
    use futures::StreamExt;
    use prost::Message;
    use std::sync::Arc;

    fn apply_selector_transition_event(
        event: WatchEvent,
        matches_selector: bool,
        membership: &mut SelectorMembership,
    ) -> Option<WatchEvent> {
        membership.transition(event, matches_selector)
    }

    fn decode_length_prefixed_watch_events(
        chunks: &[Vec<u8>],
    ) -> Vec<k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent> {
        let mut pending = Vec::new();
        let mut events = Vec::new();

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
                events.push(
                    k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(payload)
                        .unwrap_or_else(|_| panic!("frame should decode as a protobuf WatchEvent")),
                );
                pending.drain(0..frame_end);
            }
        }

        events
    }

    fn decode_watch_object_envelope(
        event: k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent,
    ) -> crate::protobuf::Unknown {
        let raw = event
            .object
            .and_then(|object| object.raw)
            .expect("protobuf WatchEvent must carry RawExtension bytes");
        assert_eq!(&raw[..4], b"k8s\0");
        crate::protobuf::Unknown::decode(&raw[4..]).unwrap()
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

        let event =
            k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(&frame[4..]).unwrap();
        assert_eq!(event.r#type.as_deref(), Some("BOOKMARK"));

        let envelope = decode_watch_object_envelope(event);
        assert_eq!(
            envelope
                .type_meta
                .as_ref()
                .map(|type_meta| (type_meta.api_version.as_str(), type_meta.kind.as_str())),
            Some(("v1", "ConfigMap"))
        );
        let decoded = k8s_pb::api::core::v1::ConfigMap::decode(envelope.raw.as_slice()).unwrap();
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

        let event =
            k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(&frame[4..]).unwrap();
        assert_eq!(event.r#type.as_deref(), Some("ERROR"));

        let envelope = decode_watch_object_envelope(event);
        assert_eq!(
            envelope
                .type_meta
                .as_ref()
                .map(|type_meta| (type_meta.api_version.as_str(), type_meta.kind.as_str())),
            Some(("v1", "Status"))
        );
        let decoded =
            k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(envelope.raw.as_slice())
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
        let decoded =
            k8s_pb::apimachinery::pkg::apis::meta::v1::WatchEvent::decode(&frame[4..]).unwrap();
        assert_eq!(decoded.r#type.as_deref(), Some("ERROR"));
        let envelope = decode_watch_object_envelope(decoded);
        let status =
            k8s_pb::apimachinery::pkg::apis::meta::v1::Status::decode(envelope.raw.as_slice())
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
            let row = RawWatchEvent {
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
            let events = decode_length_prefixed_watch_events(&[frame]);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].r#type.as_deref(), Some("ADDED"));
            let envelope = decode_watch_object_envelope(events.into_iter().next().unwrap());
            assert_eq!(
                envelope
                    .type_meta
                    .as_ref()
                    .map(|type_meta| (type_meta.api_version.as_str(), type_meta.kind.as_str())),
                Some((*api_version, *kind))
            );
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
        let frames = decode_length_prefixed_watch_events(&[
            all[0..3].to_vec(),
            all[3..all.len() - 5].to_vec(),
            all[all.len() - 5..].to_vec(),
        ]);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].r#type.as_deref(), Some("ADDED"));
        assert_eq!(frames[1].r#type.as_deref(), Some("BOOKMARK"));
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

        headers.insert("accept", "application/json;Q=0, */*;q=0".parse().unwrap());
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
    async fn initial_replay_failure_emits_error_and_terminates_raw_and_parsed_streams() {
        for table_format in [false, true] {
            let db: DatastoreHandle = Arc::new(
                crate::datastore::redb::RedbDatastore::new_in_memory()
                    .await
                    .unwrap(),
            );
            let topic = WatchTopic::new("v1", "ConfigMap");
            let signal_rx = WatchSignalReceiver::new(vec![db.subscribe_watch_signals(topic)]);
            db.close();
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
                watch_anchor: watch_replay_anchor_from_backend(&db),
                db,
                signal_rx,
                replay_start_position: WatchReplayPosition {
                    resource_version: 0,
                    event_id: 1,
                    resource_version_filter_through_event_id: 0,
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
                catch_up_mode: WatchCatchUpMode::NamespacedScoped,
                timeout_seconds: None,
                emit_initial_state_for_resource_version_zero: false,
            });

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
    async fn live_replay_failure_emits_one_error_and_terminates_raw_and_parsed_streams() {
        for table_format in [false, true] {
            let db: DatastoreHandle = Arc::new(
                crate::datastore::redb::RedbDatastore::new_in_memory()
                    .await
                    .unwrap(),
            );
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "established",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": "established"}
                }),
            )
            .await
            .unwrap();
            let (signal_tx, signal_rx) = broadcast::channel(4);
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
                watch_anchor: watch_replay_anchor_from_backend(&db),
                db: db.clone(),
                signal_rx: signal_rx.into(),
                replay_start_position: WatchReplayPosition::default(),
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
                catch_up_mode: WatchCatchUpMode::NamespacedScoped,
                timeout_seconds: None,
                emit_initial_state_for_resource_version_zero: false,
            });
            let mut stream = body.into_data_stream();

            let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("initial replay should establish the live watch")
                .expect("initial replay should emit one event")
                .expect("initial replay frame should be readable");
            let first: serde_json::Value = serde_json::from_slice(&first).unwrap();
            assert_eq!(first["type"], "ADDED", "table_format={table_format}");

            db.close();
            let wake = WatchEvent::modified(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "wake",
                    "resourceVersion": "2"
                }
            }));
            signal_tx
                .send(WatchSignal::from_event(&wake).unwrap())
                .unwrap();

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
        let line = serialize_watch_catch_up_failure_status_line();
        let value: serde_json::Value = serde_json::from_slice(&line).unwrap();

        assert_eq!(value["type"], "ERROR");
        assert_eq!(value["object"]["code"], 410);
        assert_eq!(value["object"]["reason"], "Expired");
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
        let event = CatchUpResource::added(resource);
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

    #[test]
    fn exact_metadata_name_field_selector_requires_exact_identity_scope() {
        assert_eq!(
            exact_metadata_name_field_selector(Some("metadata.name=pod-a"), Some("default")),
            Some("pod-a")
        );
        assert_eq!(
            exact_metadata_name_field_selector(
                Some("metadata.namespace=default,metadata.name=pod-a"),
                Some("default")
            ),
            Some("pod-a")
        );
        assert_eq!(
            exact_metadata_name_field_selector(Some("metadata.name!=pod-a"), Some("default")),
            None
        );
        assert_eq!(
            exact_metadata_name_field_selector(Some("metadata.name==pod-a"), Some("default")),
            None
        );
        assert_eq!(
            exact_metadata_name_field_selector(
                Some("metadata.name=pod-a,status.phase=Running"),
                Some("default")
            ),
            None
        );
        assert_eq!(
            exact_metadata_name_field_selector(
                Some("metadata.namespace=other,metadata.name=pod-a"),
                Some("default")
            ),
            None
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
    async fn resolve_periodic_bookmark_decision_scoped_anchors_to_delivered_frontier() {
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

        let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
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
            decision,
            PeriodicBookmarkDecision::Bookmark(1),
            "scoped watch bookmark must stay at the delivered scope frontier (1), \
             not the global cursor/collection RV ({collection_rv})"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_decision_expires_absent_exact_name_scope() {
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

        let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
            db: &handle,
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
            decision,
            PeriodicBookmarkDecision::Expired,
            "an exact-name watch over an absent object must not emit endless same-rv bookmarks once the cursor advances"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_decision_selector_free_uses_cursor_high_water() {
        let (ds, handle) = crate::datastore::sqlite::test_support::in_memory_with_handle().await;
        let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
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
            decision,
            PeriodicBookmarkDecision::Bookmark(500),
            "selector-free watch may bookmark the cursor's full high-water RV"
        );
        let _ = ds;
    }

    #[tokio::test]
    async fn resolve_periodic_bookmark_decision_selector_free_falls_back_to_collection_when_zero() {
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
        let decision = resolve_periodic_bookmark_decision(PeriodicBookmarkContext {
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
            decision,
            PeriodicBookmarkDecision::Bookmark(collection_rv),
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
        let mut matched_keys = SelectorMembership::default();

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
        let mut matched_keys = SelectorMembership::default();
        // Seed prior match so the relabel event triggers the Modified→Deleted
        // branch.
        matched_keys.record_event(&make_event(EventType::Added, Some("watch-9"), "cm"));

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
        let mut matched_keys = SelectorMembership::default();
        matched_keys.record_event(&make_event(EventType::Added, Some("default"), "pod-a"));

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
        let mut matched_keys = SelectorMembership::default();
        let baseline = make_resource(
            "IngressClass",
            "networking.k8s.io/v1",
            Some("default"),
            None,
            "ic1",
        );
        matched_keys.replace_from_resources(std::slice::from_ref(&baseline));

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
        let mut matched_keys = SelectorMembership::default();

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
    fn raw_watch_json_line_wraps_stored_object_bytes_without_reparse() {
        let row = crate::datastore::RawWatchEvent {
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
