//! Root-independent diagnostics shared by SQLite mutation packets.

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
    tracing::info!(
        target: "klights::datastore::noop_update",
        operation = %entry.operation,
        api_version = %entry.api_version,
        kind = %entry.kind,
        namespace = entry.namespace.unwrap_or(""),
        name = %entry.name,
        uid = %entry.uid,
        resource_version = entry.resource_version,
        reason = %entry.reason,
        "skipped no-op datastore write"
    );
}
