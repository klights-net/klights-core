use serde_json::Value;
use std::time::Duration;
use tracing::Level;

use crate::watch::WatchEvent;

const WATCH_REPLAY_DECODE_WARN_MS: u128 = 10;
const LOG_APPLY_DECODE_WARN_MS: u128 = 25;
const LOG_APPLY_COMMIT_WARN_MS: u128 = 50;
const LARGE_JSON_WARN_BYTES: usize = 512 * 1024;

pub struct NoopResourceWrite<'a> {
    pub operation: &'a str,
    pub api_version: &'a str,
    pub kind: &'a str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    pub uid: &'a str,
    pub resource_version: i64,
    pub reason: &'a str,
}

pub fn log_noop_resource_write(entry: NoopResourceWrite<'_>) {
    let NoopResourceWrite {
        operation,
        api_version,
        kind,
        namespace,
        name,
        uid,
        resource_version,
        reason,
    } = entry;
    tracing::info!(
        target: "klights::datastore::noop_update",
        operation = %operation,
        api_version = %api_version,
        kind = %kind,
        namespace = namespace.unwrap_or(""),
        name = %name,
        uid = %uid,
        resource_version,
        reason = %reason,
        "skipped no-op datastore write"
    );
}

pub fn log_watch_event_broadcast(event: &WatchEvent) {
    if !tracing::enabled!(target: "klights::datastore::watch_event", Level::DEBUG) {
        return;
    }

    let object = event.object.as_ref();
    let metadata = object.get("metadata").unwrap_or(&Value::Null);
    tracing::debug!(
        target: "klights::datastore::watch_event",
        event_type = %event.event_type,
        api_version = value_str(object.get("apiVersion")),
        kind = value_str(object.get("kind")),
        namespace = value_str(metadata.get("namespace")),
        name = value_str(metadata.get("name")),
        uid = value_str(metadata.get("uid")),
        resource_version = value_str(metadata.get("resourceVersion")),
        generation = value_i64(metadata.get("generation")),
        status_phase = value_str(object.pointer("/status/phase")),
        status_observed_generation = value_i64(object.pointer("/status/observedGeneration")),
        "broadcasting datastore watch event"
    );
}

pub struct SlowWatchReplayDecode<'a> {
    pub elapsed: Duration,
    pub data_len: usize,
    pub api_version: &'a str,
    pub kind: &'a str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    pub resource_version: i64,
    pub event_type: &'a str,
}

pub fn log_slow_watch_replay_decode(entry: SlowWatchReplayDecode<'_>) {
    let SlowWatchReplayDecode {
        elapsed,
        data_len,
        api_version,
        kind,
        namespace,
        name,
        resource_version,
        event_type,
    } = entry;
    if !should_log_slow_path(
        elapsed,
        data_len,
        WATCH_REPLAY_DECODE_WARN_MS,
        LARGE_JSON_WARN_BYTES,
    ) {
        return;
    }
    tracing::warn!(
        target: "klights::datastore::slowdown",
        operation = "watch_replay_decode",
        elapsed_ms = elapsed.as_millis(),
        data_len,
        api_version,
        kind,
        namespace = namespace.unwrap_or(""),
        name,
        resource_version,
        event_type,
        "slow datastore JSON decode"
    );
}

pub fn log_slow_log_apply_decode(
    format: &str,
    elapsed: Duration,
    data_len: usize,
    resource_version: i64,
    mutation_count: usize,
) {
    if !should_log_slow_path(
        elapsed,
        data_len,
        LOG_APPLY_DECODE_WARN_MS,
        LARGE_JSON_WARN_BYTES,
    ) {
        return;
    }
    tracing::warn!(
        target: "klights::datastore::slowdown",
        operation = "log_apply_decode",
        format,
        elapsed_ms = elapsed.as_millis(),
        data_len,
        resource_version,
        mutation_count,
        "slow log_apply decode"
    );
}

pub fn log_slow_log_apply_commit(
    elapsed: Duration,
    resource_version: i64,
    mutation_count: usize,
    pending_watch_events: usize,
    emit_watch_events: bool,
    raft_authoritative: bool,
) {
    if !should_log_slow_path(elapsed, 0, LOG_APPLY_COMMIT_WARN_MS, usize::MAX) {
        return;
    }
    tracing::warn!(
        target: "klights::datastore::slowdown",
        operation = "log_apply_commit",
        elapsed_ms = elapsed.as_millis(),
        resource_version,
        mutation_count,
        pending_watch_events,
        emit_watch_events,
        raft_authoritative,
        "slow log_apply commit"
    );
}

fn should_log_slow_path(
    elapsed: Duration,
    data_len: usize,
    slow_ms: u128,
    large_bytes: usize,
) -> bool {
    elapsed.as_millis() >= slow_ms || data_len >= large_bytes
}

fn value_str(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or("")
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}

#[cfg(test)]
mod tests {
    use super::should_log_slow_path;
    use std::time::Duration;

    #[test]
    fn slow_path_logging_respects_time_and_size_thresholds() {
        assert!(!should_log_slow_path(
            Duration::from_millis(9),
            511 * 1024,
            10,
            512 * 1024
        ));
        assert!(should_log_slow_path(
            Duration::from_millis(10),
            1,
            10,
            512 * 1024
        ));
        assert!(should_log_slow_path(
            Duration::from_millis(1),
            512 * 1024,
            10,
            512 * 1024
        ));
    }
}
