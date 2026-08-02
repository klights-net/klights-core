//! Focused generic resource command submission.
//!
//! The HTTP orchestration owns Kubernetes policy and response mapping. Durable
//! command/result and CAS semantics remain owned by cluster-core/leader-api.

use klights_cluster_core::{ResourcePatchRequest, ResourcePreconditions, StorageCommand};
use klights_leader_api::{
    LeaderResourceCommand, LeaderResourceQuery, ResourceCommandRequest, ResourceCommandResult,
};

use crate::AppError;

fn resource_result(
    result: ResourceCommandResult,
    operation: &'static str,
) -> Result<klights_cluster_core::Resource, AppError> {
    match result {
        ResourceCommandResult::Resource(resource) => Ok(resource),
        ResourceCommandResult::Ack { .. } => Err(AppError::InternalError(format!(
            "{operation} returned an acknowledgement without a resource"
        ))),
    }
}

pub async fn create_namespace(
    command: &dyn LeaderResourceCommand,
    name: &str,
    data: serde_json::Value,
) -> Result<klights_cluster_core::Resource, AppError> {
    let request = ResourceCommandRequest::try_new(StorageCommand::CreateNamespace {
        name: name.to_string(),
        data,
    })?;
    let result = command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?;
    resource_result(result, "namespace create")
}

pub async fn update_namespace(
    command: &dyn LeaderResourceCommand,
    name: &str,
    data: serde_json::Value,
    expected_resource_version: i64,
) -> Result<klights_cluster_core::Resource, AppError> {
    let request = ResourceCommandRequest::try_new(StorageCommand::UpdateNamespace {
        name: name.to_string(),
        data,
        expected_rv: expected_resource_version,
    })?;
    let result = command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?;
    resource_result(result, "namespace update")
}

pub async fn delete_namespace(
    command: &dyn LeaderResourceCommand,
    name: &str,
) -> Result<(), AppError> {
    let request = ResourceCommandRequest::try_new(StorageCommand::DeleteNamespace {
        name: name.to_string(),
    })?;
    match command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?
    {
        ResourceCommandResult::Ack { .. } => Ok(()),
        ResourceCommandResult::Resource(_) => Err(AppError::InternalError(
            "namespace delete returned a resource instead of an acknowledgement".to_string(),
        )),
    }
}

pub async fn create_non_pod_resource(
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: serde_json::Value,
) -> Result<klights_cluster_core::Resource, AppError> {
    if kind == "Pod" {
        return Err(AppError::Forbidden(
            "generic Pod creation is forbidden; use the Pod API repository path".to_string(),
        ));
    }
    let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        data,
    })?;
    let result = command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?;
    resource_result(result, "resource create")
}

pub async fn update_non_pod_resource(
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: serde_json::Value,
    expected_resource_version: i64,
) -> Result<klights_cluster_core::Resource, AppError> {
    if api_version == "v1" && kind == "Pod" {
        return Err(AppError::Forbidden(
            "generic Pod updates are forbidden; use the Pod API repository path".to_string(),
        ));
    }
    let request = ResourceCommandRequest::try_new(StorageCommand::update_resource(
        api_version,
        kind,
        namespace,
        name,
        data,
        expected_resource_version,
    ))?;
    let result = command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?;
    resource_result(result, "resource update")
}

pub async fn patch_non_pod_resource(
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    request: ResourcePatchRequest,
) -> Result<klights_cluster_core::Resource, AppError> {
    if api_version == "v1" && kind == "Pod" {
        return Err(AppError::Forbidden(
            "generic Pod patch is forbidden; use the Pod API repository path".to_string(),
        ));
    }
    let ResourcePatchRequest {
        patch_kind,
        patch,
        preconditions,
        strict_resource_version,
    } = request;
    let request = ResourceCommandRequest::try_new(StorageCommand::PatchResource {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        patch_kind,
        patch,
        preconditions,
        strict_resource_version,
    })?;
    let result = command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)?;
    resource_result(result, "resource patch")
}

pub async fn delete_non_pod_resource(
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    preconditions: ResourcePreconditions,
) -> Result<ResourceCommandResult, AppError> {
    if api_version == "v1" && kind == "Pod" {
        return Err(AppError::Forbidden(
            "generic Pod deletion is forbidden; use the UID-bound Pod lifecycle actor path"
                .to_string(),
        ));
    }
    let request = ResourceCommandRequest::try_new(StorageCommand::DeleteResource {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(str::to_string),
        name: name.to_string(),
        preconditions,
    })?;
    command
        .submit_resource_command(request)
        .await
        .map_err(AppError::from)
}

pub async fn delete_non_pod_collection(
    query: &dyn LeaderResourceQuery,
    command: &dyn LeaderResourceCommand,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    label_selector: Option<&str>,
) -> Result<(), AppError> {
    let resources = crate::generic_read::list_resources(
        query,
        api_version,
        kind,
        namespace,
        label_selector,
        None,
        None,
        None,
    )
    .await?;
    for resource in resources.into_items() {
        delete_non_pod_resource(
            command,
            api_version,
            kind,
            namespace,
            &resource.name,
            ResourcePreconditions::from_resource(&resource),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use klights_cluster_core::Resource;
    use klights_leader_api::{
        ResourceCommandError, ResourceCommandFuture, ResourceGetRequest, ResourceListRequest,
        ResourceListResult, ResourceQueryFuture,
    };

    use super::*;

    #[derive(Default)]
    struct RecordingCommand {
        submissions: AtomicUsize,
        last_request: Mutex<Option<ResourceCommandRequest>>,
    }

    impl LeaderResourceCommand for RecordingCommand {
        fn submit_resource_command(
            &self,
            request: ResourceCommandRequest,
        ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
            self.submissions.fetch_add(1, Ordering::Relaxed);
            *self.last_request.lock().expect("request lock poisoned") = Some(request);
            Box::pin(async {
                Ok(ResourceCommandResult::try_from_response(
                    klights_cluster_core::StorageResponse::Ack {
                        resource_version: 1,
                    },
                )?)
            })
        }
    }

    struct OneResourceQuery;

    impl LeaderResourceQuery for OneResourceQuery {
        fn get_resource(
            &self,
            _request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            Box::pin(async { Ok(None) })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            Box::pin(async {
                let resource = Resource::try_from_data(std::sync::Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "test-config",
                        "namespace": "default",
                        "uid": "config-uid",
                        "resourceVersion": "7"
                    }
                })))
                .expect("fixture resource must have valid identity");
                ResourceListResult::try_new(vec![resource], 0, None, None, None)
            })
        }
    }

    struct RejectingCommand;

    impl LeaderResourceCommand for RejectingCommand {
        fn submit_resource_command(
            &self,
            _request: ResourceCommandRequest,
        ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
            Box::pin(async { Err(ResourceCommandError::NotLeader) })
        }
    }

    #[tokio::test]
    async fn generic_delete_rejects_pods_before_command_submission() {
        let command = RecordingCommand::default();
        let error = delete_non_pod_resource(
            &command,
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            ResourcePreconditions::default(),
        )
        .await
        .expect_err("generic Pod deletion must fail closed");
        assert!(matches!(error, AppError::Forbidden(_)));
        assert_eq!(command.submissions.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn generic_create_rejects_pods_before_command_submission() {
        let command = RecordingCommand::default();
        let error = create_non_pod_resource(
            &command,
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "test-pod", "namespace": "default"}
            }),
        )
        .await
        .expect_err("generic Pod creation must fail closed");
        assert!(matches!(error, AppError::Forbidden(_)));
        assert_eq!(command.submissions.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn generic_update_and_patch_reject_pods_before_command_submission() {
        let command = RecordingCommand::default();
        let update_error = update_non_pod_resource(
            &command,
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            serde_json::json!({}),
            7,
        )
        .await
        .expect_err("generic Pod update must fail closed");
        assert!(matches!(update_error, AppError::Forbidden(_)));

        let patch_error = patch_non_pod_resource(
            &command,
            "v1",
            "Pod",
            Some("default"),
            "test-pod",
            ResourcePatchRequest::new(
                klights_cluster_core::PatchKind::Merge,
                serde_json::json!({}),
                ResourcePreconditions::default(),
            ),
        )
        .await
        .expect_err("generic Pod patch must fail closed");
        assert!(matches!(patch_error, AppError::Forbidden(_)));
        assert_eq!(command.submissions.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn collection_delete_uses_listed_resource_identity_preconditions() {
        let command = RecordingCommand::default();
        delete_non_pod_collection(
            &OneResourceQuery,
            &command,
            "v1",
            "ConfigMap",
            Some("default"),
            None,
        )
        .await
        .expect("collection delete should submit the listed resource");

        let request = command
            .last_request
            .lock()
            .expect("request lock poisoned")
            .clone()
            .expect("delete command must be recorded");
        let StorageCommand::DeleteResource { preconditions, .. } = request.command() else {
            panic!("collection delete must submit DeleteResource");
        };
        assert_eq!(preconditions.uid.as_deref(), Some("config-uid"));
        assert_eq!(preconditions.resource_version, Some(7));
    }

    #[tokio::test]
    async fn collection_delete_propagates_resource_command_failure() {
        let error = delete_non_pod_collection(
            &OneResourceQuery,
            &RejectingCommand,
            "v1",
            "ConfigMap",
            Some("default"),
            None,
        )
        .await
        .expect_err("collection delete must propagate command failure");
        assert!(matches!(error, AppError::ServiceUnavailable(_)));
    }
}
