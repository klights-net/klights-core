//! Dormant resource-service RPC identities for the future shared dispatcher.
//!
//! These records characterize transport equivalence only. Production dispatch
//! remains on the existing focused leader ports during the crate refactor.

use klights_types::operation::OperationId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceServiceSemanticOperation {
    pub rpc_method: &'static str,
    pub operation: OperationId,
}

pub const RESOURCE_SERVICE_SEMANTIC_OPERATIONS: [ResourceServiceSemanticOperation; 4] = [
    ResourceServiceSemanticOperation {
        rpc_method: "klights.replication.Replication/GetResource",
        operation: OperationId::ResourceGet,
    },
    ResourceServiceSemanticOperation {
        rpc_method: "klights.replication.Replication/ListResources",
        operation: OperationId::ResourceList,
    },
    ResourceServiceSemanticOperation {
        rpc_method: "klights.replication.Replication/SubmitResourceCommand",
        operation: OperationId::ResourceCommand,
    },
    ResourceServiceSemanticOperation {
        rpc_method: "klights.replication.Replication/WatchResources",
        operation: OperationId::ResourceWatch,
    },
];

#[cfg(test)]
mod tests {
    use super::RESOURCE_SERVICE_SEMANTIC_OPERATIONS;

    #[test]
    fn every_resource_service_rpc_has_one_closed_operation_identity() {
        assert_eq!(
            RESOURCE_SERVICE_SEMANTIC_OPERATIONS
                .map(|entry| (entry.rpc_method, entry.operation.as_str())),
            [
                (
                    "klights.replication.Replication/GetResource",
                    "resource.get",
                ),
                (
                    "klights.replication.Replication/ListResources",
                    "resource.list",
                ),
                (
                    "klights.replication.Replication/SubmitResourceCommand",
                    "resource.command",
                ),
                (
                    "klights.replication.Replication/WatchResources",
                    "resource.watch",
                ),
            ]
        );
    }
}
