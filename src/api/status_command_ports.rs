use klights_cluster_core::{ResourcePreconditions, StorageCommand};
use klights_leader_api::{LeaderResourceCommand, ResourceCommandRequest, ResourceCommandResult};

use crate::api::AppError;

pub(crate) async fn update_resource_status(
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    status: serde_json::Value,
    preconditions: ResourcePreconditions,
) -> Result<klights_cluster_core::Resource, AppError> {
    let request = ResourceCommandRequest::try_new(StorageCommand::UpdateStatus {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        status,
        expected_rv: preconditions.resource_version,
        preconditions,
        observed_status_stamp: None,
    })?;
    match command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?
    {
        ResourceCommandResult::Resource(resource) => Ok(resource),
        ResourceCommandResult::Ack { .. } => Err(AppError::InternalError(
            "resource status update returned an acknowledgement without a resource".to_string(),
        )),
    }
}
