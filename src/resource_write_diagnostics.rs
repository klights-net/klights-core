pub(crate) struct NoopResourceWrite<'a> {
    pub(crate) operation: &'a str,
    pub(crate) api_version: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) name: &'a str,
    pub(crate) uid: &'a str,
    pub(crate) resource_version: i64,
    pub(crate) reason: &'a str,
}

pub(crate) fn log_noop_resource_write(entry: NoopResourceWrite<'_>) {
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
