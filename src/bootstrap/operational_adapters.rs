use std::sync::Arc;

pub(crate) struct ApiClusterStatusMetadata {
    db: crate::datastore::DatastoreHandle,
}

impl ApiClusterStatusMetadata {
    pub(crate) fn new(db: crate::datastore::DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl klights_leader_api::LeaderClusterStatusMetadata for ApiClusterStatusMetadata {
    fn cluster_status_metadata(&self) -> klights_leader_api::ClusterStatusMetadataFuture<'_> {
        Box::pin(async move {
            let metadata = crate::bootstrap::cluster_meta::read_cluster_metadata(self.db.as_ref())
                .await
                .map_err(|error| {
                    klights_leader_api::ClusterStatusMetadataError::unavailable(error.to_string())
                })?;
            Ok(klights_leader_api::ClusterStatusMetadata {
                cluster_id: metadata.cluster_id,
                leader_epoch: metadata.leader_epoch,
                current_resource_version: metadata.current_rv,
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
