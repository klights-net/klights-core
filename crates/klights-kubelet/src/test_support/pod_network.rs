//! Focused Pod network assignment support for integration tests.

use std::sync::Arc;

use crate::pod_repository::{
    PodNetworkAssignment, PodNetworkAssignmentQuery, PodNetworkAssignmentRequest,
};

/// Test-only adapter exposing exactly the kubelet network assignment query.
#[derive(Clone)]
pub struct PodNetworkTestPorts {
    assignment: Arc<dyn PodNetworkAssignmentQuery>,
}

impl PodNetworkTestPorts {
    pub fn new(assignment: Arc<dyn PodNetworkAssignmentQuery>) -> Self {
        Self { assignment }
    }

    pub async fn read_pod_network_assignment(
        &self,
        request: PodNetworkAssignmentRequest,
    ) -> anyhow::Result<PodNetworkAssignment> {
        self.assignment
            .read_pod_network_assignment(request)
            .await
            .map_err(anyhow::Error::new)
    }
}
