pub use crate::bootstrap::native_api_composition::support::{TestAppState, build_test_app_state};
pub use serde_json::json;

pub fn fixed_mirror_time() -> chrono::DateTime<chrono::Utc> {
    chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 8, 14, 12, 0, 0)
        .single()
        .expect("fixed mirror time is valid")
}

pub struct ServiceEndpointBatchReconcileRequest<'a> {
    pub service_name: &'a str,
    pub service_uid: &'a str,
    pub namespace: &'a str,
    pub selector: Option<&'a serde_json::Value>,
    pub service_ports: Option<&'a serde_json::Value>,
    pub publish_not_ready: bool,
}

pub async fn reconcile_endpoints(
    state: &TestAppState,
    service_name: &str,
    namespace: &str,
    selector: Option<&serde_json::Value>,
    ports: Option<&serde_json::Value>,
    publish_not_ready: bool,
) -> anyhow::Result<()> {
    state
        .reconcile_endpoints(service_name, namespace, selector, ports, publish_not_ready)
        .await
}

pub async fn reconcile_endpointslice(
    state: &TestAppState,
    service_name: &str,
    service_uid: &str,
    namespace: &str,
    selector: Option<&serde_json::Value>,
    ports: Option<&serde_json::Value>,
) -> anyhow::Result<()> {
    state
        .reconcile_endpointslice(service_name, service_uid, namespace, selector, ports)
        .await
}

pub async fn reconcile_service_endpoints_batch(
    state: &TestAppState,
    request: ServiceEndpointBatchReconcileRequest<'_>,
) -> anyhow::Result<()> {
    state
        .reconcile_service_endpoint_batch(
            request.service_name,
            request.service_uid,
            request.namespace,
            request.selector,
            request.service_ports,
            request.publish_not_ready,
        )
        .await
}

pub async fn mirror_endpoints_to_endpointslice(
    state: &TestAppState,
    endpoints: &serde_json::Value,
    mirrored_at: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    state
        .mirror_endpoint_fixture_at(endpoints, mirrored_at)
        .await
}
