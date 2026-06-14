use serde_json::Value;
use tracing::Level;

use crate::watch::WatchEvent;

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

fn value_str(value: Option<&Value>) -> &str {
    value.and_then(Value::as_str).unwrap_or("")
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}
