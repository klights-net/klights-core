use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_leader_api::{
    CacheReadinessRequest, CustomResourceListIdentity, ResourceCommandError,
    ResourceCommandRequest, ResourceCommandResult, ResourceGetRequest, ResourceListRequest,
    ResourceListResult, ResourceListScope, ResourceQueryConsistency, ResourceQueryError,
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
    let custom_resource_identity = request.custom_resource_identity().map(|identity| {
        (
            identity.group().to_string(),
            identity.plural().to_string(),
            identity.requested_version().to_string(),
        )
    });
    let request = ResourceListRequest::try_new_with_continuation_mode(
        request.api_version().to_string(),
        request.kind().to_string(),
        request.scope().clone(),
        request.label_selector().map(str::to_owned),
        request.field_selector().map(str::to_owned),
        request.limit(),
        request.continue_token().map(str::to_owned),
        request.continuation_mode(),
        consistency,
    )?
    .with_resource_version_match(request.resource_version_match())?;
    match custom_resource_identity {
        Some((group, plural, requested_version)) => request.with_custom_resource_identity(
            CustomResourceListIdentity::try_new(group, plural, requested_version)?,
        ),
        None => Ok(request),
    }
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
    // A remote informer is a live complete-scope cache.  A CRD field selector
    // may internally return a bounded candidate page even for an unbounded
    // public request, and an Exact read is historical; neither may replace or
    // mark the live cache complete.
    let complete_live_scope = request.continuation_mode()
        == klights_leader_api::ResourceListContinuationMode::Initial
        && request.limit().is_none()
        && request.continue_token().is_none()
        && request.custom_resource_identity().is_none()
        && matches!(
            request.resource_version_match(),
            klights_leader_api::ResourceListResourceVersionMatch::Any
        )
        && result.continue_token().is_none()
        && result.candidate_continue_tokens().is_empty();
    if complete_live_scope {
        let position = result.watch_replay_position().ok_or_else(|| {
            ResourceQueryError::corrupt_response(
                "complete live ListResources response omitted its positioned replay boundary",
            )
        })?;
        cache
            .replace_scope(request, result.items().to_vec(), position)
            .await
            .map_err(|error| ResourceQueryError::query_failed(error.to_string()))?;
        cache
            .mark_ready(cache_scope(request)?)
            .await
            .map_err(|error| ResourceQueryError::query_failed(error.to_string()))?;
    }
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
        key.namespace
            .clone()
            .map(ResourceListScope::Namespace)
            .unwrap_or(ResourceListScope::Cluster),
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
    // Informer caches are coherent only for an initial, live, ordinary scope.
    // Historical resourceVersion contracts and custom-resource composition are
    // root-owned reads; routing either through a cache could return a partial
    // forced candidate page or overwrite a live scope with history.
    let direct_leader_read = request.limit().is_some()
        || request.continue_token().is_some()
        || request.continuation_mode() != klights_leader_api::ResourceListContinuationMode::Initial
        || request.custom_resource_identity().is_some()
        || !matches!(
            request.resource_version_match(),
            klights_leader_api::ResourceListResourceVersionMatch::Any
        );
    if direct_leader_read {
        let grpc = grpc.ok_or_else(|| {
            ResourceQueryError::retryable("typed leader LIST has no gRPC transport")
        })?;
        return grpc
            .list_resources_rpc(with_consistency(
                &request,
                ResourceQueryConsistency::LeaderFresh,
            )?)
            .await;
    }
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
