use klights_cluster_core::Resource;
use klights_pod_api::{PodGetRequest, PodQuery, PodRepositoryError};

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
