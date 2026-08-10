use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_leader_api::{
    CacheReadinessRequest, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
    ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryConsistency,
    ResourceQueryError,
};
use klights_watch::RemoteInformerCache;

use super::ReplicationGrpcClient;

fn cache_scope(request: &ResourceListRequest) -> Result<CacheReadinessRequest, ResourceQueryError> {
    CacheReadinessRequest::try_new(
        request.api_version().to_string(),
        request.kind().to_string(),
        request.namespace().map(str::to_owned),
        request.label_selector().map(str::to_owned),
        request.field_selector().map(str::to_owned),
    )
    .map_err(|error| ResourceQueryError::query_failed(error.to_string()))
}

fn with_consistency(
    request: &ResourceListRequest,
    consistency: ResourceQueryConsistency,
) -> Result<ResourceListRequest, ResourceQueryError> {
    ResourceListRequest::try_new(
        request.api_version().to_string(),
        request.kind().to_string(),
        request.namespace().map(str::to_owned),
        request.label_selector().map(str::to_owned),
        request.field_selector().map(str::to_owned),
        request.limit(),
        request.continue_token().map(str::to_owned),
        consistency,
    )
}

pub(crate) async fn prime_list_scope(
    grpc: Option<&Arc<ReplicationGrpcClient>>,
    cache: &dyn RemoteInformerCache,
    request: &ResourceListRequest,
) -> Result<ResourceListResult, ResourceQueryError> {
    let grpc = grpc
        .ok_or_else(|| ResourceQueryError::retryable("RemoteApiClient missing gRPC transport"))?;
    let result = grpc
        .list_resources_rpc(with_consistency(
            request,
            ResourceQueryConsistency::LeaderFresh,
        )?)
        .await?;
    let position = result.watch_replay_position().unwrap_or_else(|| {
        klights_cluster_core::WatchReplayPosition::from_resource_version(result.resource_version())
    });
    cache
        .replace_scope(request, result.items().to_vec(), position)
        .await
        .map_err(|error| ResourceQueryError::query_failed(error.to_string()))?;
    cache
        .mark_ready(cache_scope(request)?)
        .await
        .map_err(|error| ResourceQueryError::query_failed(error.to_string()))?;
    Ok(result)
}

pub async fn get_resource(
    grpc: Option<&Arc<ReplicationGrpcClient>>,
    cache: &dyn RemoteInformerCache,
    request: ResourceGetRequest,
) -> Result<Option<Resource>, ResourceQueryError> {
    let consistency = request.consistency();
    let key = request.into_key();
    if consistency == ResourceQueryConsistency::LeaderFresh {
        let grpc = grpc.ok_or_else(|| {
            ResourceQueryError::retryable("leader-fresh resource query has no gRPC transport")
        })?;
        let resource = grpc.get_resource_rpc(key.clone()).await?;
        if let Some(resource) = &resource {
            cache.insert(resource.clone()).await;
        }
        return Ok(resource);
    }

    if let Some(resource) = cache.get(&key).await {
        return Ok(Some(resource));
    }
    let request = ResourceListRequest::try_new(
        key.api_version.clone(),
        key.kind.clone(),
        key.namespace.clone(),
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::Cached,
    )?;
    if cache.is_ready(&cache_scope(&request)?).await {
        return Ok(None);
    }
    if grpc.is_some() {
        prime_list_scope(grpc, cache, &request).await?;
        return Ok(cache.get(&key).await);
    }
    Ok(None)
}

pub async fn list_resources(
    grpc: Option<&Arc<ReplicationGrpcClient>>,
    cache: &dyn RemoteInformerCache,
    request: ResourceListRequest,
) -> Result<ResourceListResult, ResourceQueryError> {
    if request.consistency() == ResourceQueryConsistency::LeaderFresh {
        return prime_list_scope(grpc, cache, &request).await;
    }
    let scope = cache_scope(&request)?;
    if cache.is_ready(&scope).await || grpc.is_none() {
        return cache
            .list(&request)
            .await
            .map_err(|error| ResourceQueryError::query_failed(error.to_string()));
    }
    prime_list_scope(grpc, cache, &request).await
}

pub async fn submit_resource_command(
    grpc: Option<&Arc<ReplicationGrpcClient>>,
    request: ResourceCommandRequest,
) -> Result<ResourceCommandResult, ResourceCommandError> {
    let grpc = grpc
        .ok_or_else(|| ResourceCommandError::retryable("RemoteApiClient missing gRPC transport"))?;
    grpc.submit_resource_command_rpc(request).await
}
