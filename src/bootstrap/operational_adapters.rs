use std::sync::Arc;

pub(crate) struct ApiClusterStatusMetadata {
    metadata_reads: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
}

impl ApiClusterStatusMetadata {
    pub(crate) fn new(
        metadata_reads: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
    ) -> Arc<Self> {
        Arc::new(Self { metadata_reads })
    }
}

impl klights_leader_api::LeaderClusterStatusMetadata for ApiClusterStatusMetadata {
    fn cluster_status_metadata(&self) -> klights_leader_api::ClusterStatusMetadataFuture<'_> {
        Box::pin(async move {
            let metadata = self
                .metadata_reads
                .read_cluster_metadata()
                .await
                .map_err(|error| {
                    klights_leader_api::ClusterStatusMetadataError::unavailable(error.to_string())
                })?;
            Ok(klights_leader_api::ClusterStatusMetadata {
                cluster_id: metadata.metadata().cluster_id.clone(),
                leader_epoch: metadata.metadata().leader_epoch,
                current_resource_version: metadata.metadata().current_rv,
            })
        })
    }
}

pub(crate) struct ApiPodStartRetryDiagnostics {
    tracker: klights_kubelet::pod_creation_state::PodStartRetryTracker,
}

impl ApiPodStartRetryDiagnostics {
    pub(crate) fn new(
        tracker: klights_kubelet::pod_creation_state::PodStartRetryTracker,
    ) -> Arc<Self> {
        Arc::new(Self { tracker })
    }
}

impl klights_pod_api::PodStartRetryDiagnostics for ApiPodStartRetryDiagnostics {
    fn pending_pod_start_retries(&self) -> klights_pod_api::PodStartRetryDiagnosticsFuture<'_> {
        Box::pin(async move { self.tracker.lock().await.pending_key_pairs() })
    }
}
