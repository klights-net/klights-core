use super::*;

use serde_json::json;

/// Regression for P0-E2E-20260424-03: targetPort=0 (Go int32 zero value from client-go
/// when targetPort is not explicitly set) must fall back to the service port number.
mod endpoints_reconcile_tests;
mod endpointslice_reconcile_tests;
mod target_port_resolution_tests;
