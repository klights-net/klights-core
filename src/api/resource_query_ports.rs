use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryConsistency,
};
use klights_types::ResourceKey;

use crate::api::AppError;

pub(crate) async fn get_resource(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<klights_cluster_core::Resource>, AppError> {
    let request = ResourceGetRequest::try_new(
        ResourceKey {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        },
        ResourceQueryConsistency::LeaderFresh,
    )?;
    query.get_resource(request).await.map_err(AppError::from)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn list_resources(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) -> Result<ResourceListResult, AppError> {
    let request = ResourceListRequest::try_new(
        api_version,
        kind,
        namespace.map(str::to_string),
        label_selector.map(str::to_string),
        field_selector.map(str::to_string),
        limit,
        continue_token.map(str::to_string),
        ResourceQueryConsistency::LeaderFresh,
    )?;
    query.list_resources(request).await.map_err(AppError::from)
}

pub(crate) async fn list_all_resources(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> Result<ResourceListResult, AppError> {
    list_resources(query, api_version, kind, namespace, None, None, None, None).await
}
