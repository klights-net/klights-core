use klights_controllers::endpoints::*;

use serde_json::json;

fn controller_store(
    db: &crate::datastore::sqlite::Datastore,
) -> crate::controller_runtime_adapter::RootControllerLeaderPort {
    crate::controller_runtime_adapter::RootControllerLeaderPort::new(std::sync::Arc::new(
        db.clone(),
    ))
}

async fn mirror_endpoints_to_endpointslice(
    db: &(impl EndpointReconcileStore + ?Sized),
    endpoints: &serde_json::Value,
) -> anyhow::Result<()> {
    mirror_endpoints_to_endpointslice_at(db, endpoints, chrono::Utc::now()).await
}

/// Regression for P0-E2E-20260424-03: targetPort=0 (Go int32 zero value from client-go
/// when targetPort is not explicitly set) must fall back to the service port number.
mod endpoints_reconcile_tests;
mod endpointslice_reconcile_tests;
mod target_port_resolution_tests;
