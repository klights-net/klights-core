use serde::{Deserialize, Serialize};

use klights_cluster_core::{PodEndpointEffect, Resource, StorageCommandRejectionCode};

/// Kubernetes-visible mutation returned by a committed cluster-store apply.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AppliedMutation {
    Resource(Resource),
}

/// Neutral result of applying one committed storage command.
///
/// OpenRaft uses this as its response type, but the passive datastore also
/// returns it; ownership therefore belongs to the focused cluster-store
/// contract rather than either implementation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageCommandResult {
    pub applied_rv: Option<i64>,
    pub error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_code: Option<StorageCommandRejectionCode>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub public_resource_changed: bool,
    pub applied_mutation: Option<AppliedMutation>,
    /// Ephemeral local handoff from committed apply to the leader-side
    /// side-effect dispatcher. It is never serialized into a Raft response.
    #[serde(skip)]
    pod_endpoint_effect: PodEndpointEffect,
}

impl StorageCommandResult {
    pub fn new(
        applied_rv: Option<i64>,
        error_message: Option<String>,
        rejection_code: Option<StorageCommandRejectionCode>,
        public_resource_changed: bool,
        applied_mutation: Option<AppliedMutation>,
        pod_endpoint_effect: PodEndpointEffect,
    ) -> Self {
        Self {
            applied_rv,
            error_message,
            rejection_code,
            public_resource_changed,
            applied_mutation,
            pod_endpoint_effect,
        }
    }

    pub fn pod_endpoint_effect(&self) -> PodEndpointEffect {
        self.pod_endpoint_effect
    }
}

const fn is_false(value: &bool) -> bool {
    !*value
}
