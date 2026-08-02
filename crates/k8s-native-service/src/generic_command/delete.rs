//! Generic finalizer-aware delete strategy.

use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_leader_api::{LeaderResourceQuery, ResourceGetRequest, ResourceQueryConsistency};
use klights_types::ResourceKey;

use crate::AppError;

use super::{DeleteIntent, PropagationPolicy, finalizer};

pub fn ensure_delete_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), AppError> {
    if let Some(expected_uid) = preconditions.uid.as_deref()
        && resource.uid != expected_uid
    {
        return Err(AppError::Conflict("UID precondition failed".to_string()));
    }
    if let Some(expected_rv) = preconditions.resource_version
        && resource.resource_version != expected_rv
    {
        return Err(AppError::Conflict(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {}",
            resource.resource_version
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub enum DeleteResult {
    HardDeleted(Resource),
    MarkedTerminating(Resource),
    GoneOrUidChanged,
}

#[async_trait]
pub trait DeleteStrategy: Send + Sync {
    async fn load(&self, target: &ResourceKey) -> Result<Resource, AppError>;
    async fn execute(
        &self,
        target: &ResourceKey,
        resource: Resource,
        intent: &DeleteIntent,
    ) -> Result<DeleteResult, AppError>;
}

pub struct FinalizerAwareDeleteStrategy<'a> {
    pub resource_query: &'a dyn LeaderResourceQuery,
    pub lifecycle: &'a dyn klights_reconcile_api::FinalizerLifecyclePort,
    pub operation_now: chrono::DateTime<chrono::Utc>,
}

#[async_trait]
impl DeleteStrategy for FinalizerAwareDeleteStrategy<'_> {
    async fn load(&self, target: &ResourceKey) -> Result<Resource, AppError> {
        let request =
            ResourceGetRequest::try_new(target.clone(), ResourceQueryConsistency::LeaderFresh)?;
        self.resource_query
            .get_resource(request)
            .await
            .map_err(AppError::from)?
            .ok_or_else(|| AppError::NotFound(format!("{} not found", target.kind)))
    }

    async fn execute(
        &self,
        target: &ResourceKey,
        resource: Resource,
        intent: &DeleteIntent,
    ) -> Result<DeleteResult, AppError> {
        if !intent.orphan_children && intent.propagation_policy == PropagationPolicy::Foreground {
            let updated = finalizer::mark_foreground_deletion_with_retry(
                self.lifecycle,
                &target.api_version,
                &target.kind,
                target.namespace.as_deref(),
                &target.name,
                resource,
                intent.preconditions.clone(),
                self.operation_now,
            )
            .await?;
            return Ok(DeleteResult::MarkedTerminating(updated));
        }

        let grace_seconds = intent.options._grace_period_seconds.unwrap_or(0);
        match finalizer::complete_non_foreground_delete_with_live_recheck(
            self.lifecycle,
            finalizer::NonForegroundDeleteRequest {
                target: finalizer::ResourceDeleteTarget {
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
                operation_now: self.operation_now,
            },
        )
        .await?
        {
            finalizer::DeleteCompletion::HardDeleted(resource) => {
                Ok(DeleteResult::HardDeleted(resource))
            }
            finalizer::DeleteCompletion::MarkedTerminating(resource) => {
                Ok(DeleteResult::MarkedTerminating(resource))
            }
            finalizer::DeleteCompletion::GoneOrUidChanged => Ok(DeleteResult::GoneOrUidChanged),
        }
    }
}

pub async fn delete_loaded_with_strategy<S>(
    strategy: &S,
    target: ResourceKey,
    resource: Resource,
    intent: &DeleteIntent,
) -> Result<DeleteResult, AppError>
where
    S: DeleteStrategy,
{
    strategy.execute(&target, resource, intent).await
}

pub async fn delete_with_strategy<S>(
    strategy: &S,
    target: ResourceKey,
    intent: &DeleteIntent,
) -> Result<DeleteResult, AppError>
where
    S: DeleteStrategy,
{
    let resource = strategy.load(&target).await?;
    delete_loaded_with_strategy(strategy, target, resource, intent).await
}

pub async fn delete_collection_items<S>(
    strategy: &S,
    items: Vec<(ResourceKey, Resource)>,
    intent: &DeleteIntent,
) -> Result<Vec<DeleteResult>, AppError>
where
    S: DeleteStrategy,
{
    let mut results = Vec::with_capacity(items.len());
    for (target, resource) in items {
        match delete_loaded_with_strategy(strategy, target, resource, intent).await {
            Ok(result) => results.push(result),
            Err(AppError::NotFound(_)) => results.push(DeleteResult::GoneOrUidChanged),
            Err(error) => return Err(error),
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn resource(uid: &str, resource_version: i64) -> Resource {
        Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cm",
                "namespace": "default",
                "uid": uid,
                "resourceVersion": resource_version.to_string(),
            }
        })))
        .unwrap()
    }

    #[test]
    fn delete_preconditions_preserve_uid_and_resource_version_conflicts() {
        let resource = resource("uid-1", 7);
        ensure_delete_preconditions_match(
            &resource,
            &ResourcePreconditions::uid_and_resource_version("uid-1", 7),
        )
        .unwrap();
        assert!(matches!(
            ensure_delete_preconditions_match(&resource, &ResourcePreconditions::uid("other")),
            Err(AppError::Conflict(_))
        ));
        assert!(matches!(
            ensure_delete_preconditions_match(
                &resource,
                &ResourcePreconditions::resource_version(8)
            ),
            Err(AppError::Conflict(_))
        ));
    }
}
