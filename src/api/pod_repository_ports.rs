use klights_cluster_core::Resource;
use klights_pod_api::{PodGetRequest, PodListRequest, PodQuery, PodRepositoryError};

pub(crate) struct ApiPodList {
    pub(crate) items: Vec<Resource>,
    pub(crate) resource_version: i64,
    pub(crate) continue_token: Option<String>,
    pub(crate) remaining_item_count: Option<i64>,
}

/// API-owned adaptation from HTTP path parameters to the focused Pod query
/// capability. Repository and kubelet implementation types remain outside the
/// API owner.
pub(crate) async fn get_pod(
    query: &dyn PodQuery,
    namespace: &str,
    name: &str,
) -> Result<Option<Resource>, PodRepositoryError> {
    let request = PodGetRequest::try_by_name(namespace, name)?;
    query.get_pod(request).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn list_pods(
    query: &dyn PodQuery,
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) -> Result<ApiPodList, PodRepositoryError> {
    let request = PodListRequest::try_new(
        namespace.map(str::to_owned),
        label_selector.map(str::to_owned),
        field_selector.map(str::to_owned),
        limit,
        continue_token.map(str::to_owned),
    )?;
    let (items, resource_version, continue_token, remaining_item_count) =
        query.list_pods(request).await?.into_parts();
    Ok(ApiPodList {
        items,
        resource_version,
        continue_token,
        remaining_item_count,
    })
}
