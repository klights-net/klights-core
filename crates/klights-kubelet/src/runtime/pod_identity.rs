use klights_cluster_core::Resource;
use klights_pod_api::{PodGetRequest, PodQuery};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivePodUidCheck {
    Matches,
    Different { live_uid: String },
    Missing,
}

pub async fn get_pod_for_uid(
    query: &dyn PodQuery,
    namespace: &str,
    name: &str,
    pod_uid: &str,
) -> anyhow::Result<Option<Resource>> {
    query
        .get_pod(PodGetRequest::try_by_identity(
            klights_types::PodIdentity::new(namespace, name, pod_uid),
        )?)
        .await
        .map_err(Into::into)
}

pub async fn check_live_pod_uid(
    query: &dyn PodQuery,
    namespace: &str,
    name: &str,
    pod_uid: &str,
) -> anyhow::Result<LivePodUidCheck> {
    let Some(pod) = query
        .get_pod(PodGetRequest::try_by_name(namespace, name)?)
        .await?
    else {
        return Ok(LivePodUidCheck::Missing);
    };
    if pod.uid == pod_uid {
        Ok(LivePodUidCheck::Matches)
    } else {
        Ok(LivePodUidCheck::Different { live_uid: pod.uid })
    }
}
