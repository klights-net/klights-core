use anyhow::Result;

use crate::control_plane::client::{ListRequest, legacy_list_response};
use crate::datastore::{ResourceList, WatchReplayPosition};
use klights_leader_api::{CacheReadinessRequest, ResourceListRequest, ResourceQueryConsistency};

pub(super) async fn list(
    cache: &klights_watch::WatchCache,
    request: &ListRequest,
) -> Result<ResourceList> {
    cache
        .list(&focused_list_request(request)?)
        .await
        .map(legacy_list_response)
        .map_err(Into::into)
}

pub(super) async fn replace_scope(
    cache: &klights_watch::WatchCache,
    request: &ListRequest,
    list: ResourceList,
) -> Result<()> {
    let position = list
        .watch_replay_position
        .unwrap_or_else(|| WatchReplayPosition::from_resource_version(list.resource_version));
    cache
        .replace_scope(&focused_list_request(request)?, list.items, position)
        .await
        .map_err(Into::into)
}

pub(super) fn scope_for_request(request: &ListRequest) -> CacheReadinessRequest {
    CacheReadinessRequest::try_new(
        request.api_version.clone(),
        request.kind.clone(),
        request.namespace.clone(),
        request.label_selector.clone(),
        request.field_selector.clone(),
    )
    .expect("legacy LIST request identity was already validated")
}

fn focused_list_request(request: &ListRequest) -> Result<ResourceListRequest> {
    Ok(ResourceListRequest::try_new(
        request.api_version.clone(),
        request.kind.clone(),
        request.namespace.clone(),
        request.label_selector.clone(),
        request.field_selector.clone(),
        request.limit,
        request.continue_token.clone(),
        ResourceQueryConsistency::Cached,
    )?)
}
