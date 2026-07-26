use async_trait::async_trait;

use crate::api::AppError;
use crate::resource_preconditions;
use klights_cluster_core::{Resource, ResourcePreconditions};

pub fn ensure_delete_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), AppError> {
    resource_preconditions::ensure_delete_preconditions_match(resource, preconditions)
        .map_err(AppError::from)
}

#[derive(Debug)]
pub enum DeleteResult {
    HardDeleted(Resource),
    MarkedTerminating(Resource),
    GoneOrUidChanged,
}

#[async_trait]
pub trait DeleteStrategy: Send + Sync {
    async fn load(&self, target: &klights_types::ResourceKey) -> Result<Resource, AppError>;

    async fn execute(
        &self,
        target: &klights_types::ResourceKey,
        resource: Resource,
        intent: &crate::api::mutation::DeleteIntent,
    ) -> Result<DeleteResult, AppError>;
}

pub struct FinalizerAwareDeleteStrategy<'a> {
    pub resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
    pub lifecycle: &'a dyn klights_reconcile_api::FinalizerLifecyclePort,
}

#[async_trait]
impl DeleteStrategy for FinalizerAwareDeleteStrategy<'_> {
    async fn load(&self, target: &klights_types::ResourceKey) -> Result<Resource, AppError> {
        crate::api::resource_query_ports::get_resource(
            self.resource_query,
            &target.api_version,
            &target.kind,
            target.namespace.as_deref(),
            &target.name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} not found", target.kind)))
    }

    async fn execute(
        &self,
        target: &klights_types::ResourceKey,
        resource: Resource,
        intent: &crate::api::mutation::DeleteIntent,
    ) -> Result<DeleteResult, AppError> {
        if !intent.orphan_children
            && intent.propagation_policy == crate::api::mutation::PropagationPolicy::Foreground
        {
            let updated = crate::api::finalizer_delete::mark_foreground_deletion_with_retry(
                self.lifecycle,
                &target.api_version,
                &target.kind,
                target.namespace.as_deref(),
                &target.name,
                resource,
                intent.preconditions.clone(),
            )
            .await?;
            return Ok(DeleteResult::MarkedTerminating(updated));
        }

        let grace_seconds = intent.options._grace_period_seconds.unwrap_or(0);
        match crate::api::finalizer_delete::complete_non_foreground_delete_with_live_recheck(
            self.lifecycle,
            crate::api::finalizer_delete::NonForegroundDeleteRequest {
                target: crate::api::finalizer_delete::ResourceDeleteTarget {
                    api_version: &target.api_version,
                    kind: &target.kind,
                    namespace: target.namespace.as_deref(),
                    name: &target.name,
                },
                initial_resource: resource,
                delete_preconditions: intent.preconditions.clone(),
                orphan_children_before_completion: intent.orphan_children,
                uid_mismatch_is_conflict: intent.uid_mismatch_is_conflict,
                grace_seconds,
            },
        )
        .await?
        {
            crate::api::finalizer_delete::DeleteCompletion::HardDeleted(resource) => {
                Ok(DeleteResult::HardDeleted(resource))
            }
            crate::api::finalizer_delete::DeleteCompletion::MarkedTerminating(resource) => {
                Ok(DeleteResult::MarkedTerminating(resource))
            }
            crate::api::finalizer_delete::DeleteCompletion::GoneOrUidChanged => {
                Ok(DeleteResult::GoneOrUidChanged)
            }
        }
    }
}

pub async fn delete_loaded_with_strategy<S>(
    strategy: &S,
    target: klights_types::ResourceKey,
    resource: Resource,
    intent: &crate::api::mutation::DeleteIntent,
) -> Result<DeleteResult, AppError>
where
    S: DeleteStrategy,
{
    strategy.execute(&target, resource, intent).await
}

pub async fn delete_with_strategy<S>(
    strategy: &S,
    target: klights_types::ResourceKey,
    intent: &crate::api::mutation::DeleteIntent,
) -> Result<DeleteResult, AppError>
where
    S: DeleteStrategy,
{
    let resource = strategy.load(&target).await?;
    delete_loaded_with_strategy(strategy, target, resource, intent).await
}

pub async fn delete_collection_items<S>(
    strategy: &S,
    items: Vec<(klights_types::ResourceKey, Resource)>,
    intent: &crate::api::mutation::DeleteIntent,
) -> Result<Vec<DeleteResult>, AppError>
where
    S: DeleteStrategy,
{
    let mut results = Vec::with_capacity(items.len());
    for (target, resource) in items {
        match delete_loaded_with_strategy(strategy, target, resource, intent).await {
            Ok(result) => results.push(result),
            Err(AppError::NotFound(_)) => results.push(DeleteResult::GoneOrUidChanged),
            Err(err) => return Err(err),
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn resource(uid: &str, resource_version: i64) -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "cm".to_string(),
            uid: uid.to_string(),
            resource_version,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "cm",
                    "namespace": "default",
                    "uid": uid,
                    "resourceVersion": resource_version.to_string(),
                }
            })),
        }
    }

    #[test]
    fn delete_preconditions_match_uid_and_resource_version() {
        let resource = resource("uid-1", 7);
        ensure_delete_preconditions_match(
            &resource,
            &ResourcePreconditions::uid_and_resource_version("uid-1", 7),
        )
        .unwrap();
    }

    #[test]
    fn delete_preconditions_reject_wrong_uid() {
        let resource = resource("uid-1", 7);
        assert!(matches!(
            ensure_delete_preconditions_match(
                &resource,
                &ResourcePreconditions::uid_and_resource_version("other", 7),
            ),
            Err(AppError::Conflict(_))
        ));
    }

    #[test]
    fn delete_preconditions_reject_wrong_resource_version() {
        let resource = resource("uid-1", 7);
        assert!(matches!(
            ensure_delete_preconditions_match(
                &resource,
                &ResourcePreconditions::uid_and_resource_version("uid-1", 8),
            ),
            Err(AppError::Conflict(_))
        ));
    }
}
