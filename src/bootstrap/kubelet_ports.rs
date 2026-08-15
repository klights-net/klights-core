use std::sync::Arc;
pub(crate) struct RootPodEventSink {
    outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl RootPodEventSink {
    pub fn new(
        outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            outbox,
            resource_query,
            wall_clock,
        }
    }
}

#[async_trait::async_trait]
impl klights_kubelet::runtime::events::PodEventSink for RootPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &klights_kubelet::runtime_types::PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> Result<(), klights_kubelet::runtime::events::PodEventSinkError> {
        let pod = serde_json::json!({
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
            },
        });
        let query =
            crate::bootstrap::composition_adapters::pod_event_adapter::LeaderPodEventQuery::new(
                self.resource_query.as_ref(),
            );
        klights_kubelet::pod_events::emit_pod_event_with_outbox(
            &query,
            self.outbox.as_deref(),
            klights_kubelet::pod_events::PodEventRecord {
                pod: &pod,
                reason,
                message,
                event_type,
                reporting_component,
                reporting_instance: node_name,
                operation_now: self.wall_clock.now_utc(),
            },
        )
        .await
        .map_err(|error| {
            klights_kubelet::runtime::events::PodEventSinkError::unavailable(error.to_string())
        })?;
        Ok(())
    }
}

pub(crate) struct WorkerPodEventSink {
    outbox: Arc<klights_kubelet::node_outbox::Outbox>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl WorkerPodEventSink {
    pub fn new(
        outbox: Arc<klights_kubelet::node_outbox::Outbox>,
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            outbox,
            resource_query,
            wall_clock,
        }
    }
}

#[async_trait::async_trait]
impl klights_kubelet::runtime::events::PodEventSink for WorkerPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &klights_kubelet::runtime_types::PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> Result<(), klights_kubelet::runtime::events::PodEventSinkError> {
        let pod = serde_json::json!({
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
            },
        });
        let query =
            crate::bootstrap::composition_adapters::pod_event_adapter::LeaderPodEventQuery::new(
                self.resource_query.as_ref(),
            );
        klights_kubelet::pod_events::emit_worker_pod_event(
            &query,
            self.outbox.as_ref(),
            klights_kubelet::pod_events::PodEventRecord {
                pod: &pod,
                reason,
                message,
                event_type,
                reporting_component,
                reporting_instance: node_name,
                operation_now: self.wall_clock.now_utc(),
            },
        )
        .await
        .map_err(|error| {
            klights_kubelet::runtime::events::PodEventSinkError::unavailable(error.to_string())
        })?;
        Ok(())
    }
}
