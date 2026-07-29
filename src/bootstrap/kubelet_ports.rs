use std::sync::Arc;

use crate::kubelet::node_heartbeat::{
    NodeHeartbeatClock, NodeHeartbeatEvent, NodeHeartbeatEventFuture, NodeHeartbeatEventSource,
};
use crate::kubelet::pod_watch_source::{PodWatchEvent, PodWatchSource, PodWatchStream};
use futures::StreamExt as _;

pub struct SystemNodeHeartbeatClock {
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl SystemNodeHeartbeatClock {
    pub fn new(wall_clock: Arc<dyn klights_supervisor::WallClock>) -> Self {
        Self { wall_clock }
    }
}

impl NodeHeartbeatClock for SystemNodeHeartbeatClock {
    fn now_microtime(&self) -> String {
        klights_cluster_core::k8s_time::format_microtime(self.wall_clock.now_utc())
    }
}

pub struct LeaderPersistentVolumeEventHandler {
    db: crate::datastore::DatastoreHandle,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

impl LeaderPersistentVolumeEventHandler {
    pub fn new(
        db: crate::datastore::DatastoreHandle,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: std::path::PathBuf,
    ) -> Self {
        Self {
            db,
            is_leader_rx,
            file_process,
            local_path_provisioner_root,
        }
    }

    async fn reconcile_pvc(&self, resource: klights_cluster_core::Resource, event_name: &str) {
        use klights_reconcile_api::PvcReconcileSink;

        let reconcile = crate::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
            self.db.as_ref(),
            &self.file_process,
            &self.local_path_provisioner_root,
        );
        if let Err(error) = reconcile.reconcile_pvc(resource).await {
            tracing::error!(pvc = event_name, error = %error, "failed to reconcile PVC");
        }
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_watch_handlers::PersistentVolumeEventHandler
    for LeaderPersistentVolumeEventHandler
{
    async fn handle_pvc_event(&self, event: &PodWatchEvent, event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || !matches!(
                event.event_type,
                klights_leader_api::WatchEventType::Added
                    | klights_leader_api::WatchEventType::Modified
            )
        {
            return;
        }
        let namespace = event
            .object
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str);
        if let Ok(Some(resource)) = self
            .db
            .get_resource("v1", "PersistentVolumeClaim", namespace, event_name)
            .await
        {
            self.reconcile_pvc(resource, event_name).await;
        }
    }

    async fn handle_pv_event(&self, event: &PodWatchEvent, _event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || event.event_type != klights_leader_api::WatchEventType::Added
        {
            return;
        }
        let Ok(pvcs) = self
            .db
            .list_resources(
                "v1",
                "PersistentVolumeClaim",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
        else {
            return;
        };
        for pvc in pvcs.items {
            if pvc
                .data
                .pointer("/status/phase")
                .and_then(serde_json::Value::as_str)
                != Some("Bound")
            {
                let name = pvc.name.clone();
                self.reconcile_pvc(pvc, &name).await;
            }
        }
    }
}

pub struct DatastorePodSlotAdapter {
    store: crate::datastore::node_local::NodeLocalHandle,
}

impl DatastorePodSlotAdapter {
    pub fn new(store: crate::datastore::node_local::NodeLocalHandle) -> Arc<Self> {
        Arc::new(Self { store })
    }
}

fn slot_state(
    state: crate::datastore::node_local::PodSlotAdmissionState,
) -> klights_node_store::PodSlotAdmissionState {
    match state {
        crate::datastore::node_local::PodSlotAdmissionState::Admitted => {
            klights_node_store::PodSlotAdmissionState::Admitted
        }
        crate::datastore::node_local::PodSlotAdmissionState::Terminating => {
            klights_node_store::PodSlotAdmissionState::Terminating
        }
    }
}

fn observed_pod_version(
    value: i64,
) -> Result<klights_node_store::ObservedPodVersion, klights_node_store::RuntimeWorkError> {
    klights_node_store::ObservedPodVersion::try_new(value)
        .map_err(|error| klights_node_store::RuntimeWorkError::corrupt_data(error.to_string()))
}

impl klights_node_store::PodSlotAdmissionStore for DatastorePodSlotAdapter {
    fn try_admit(
        &self,
        request: klights_node_store::PodSlotAdmissionRequest,
    ) -> klights_node_store::RuntimeWorkFuture<'_, klights_node_store::PodSlotAdmissionResult> {
        Box::pin(async move {
            let (pod, node_name) = request.into_parts();
            match self
                .store
                .pod_slot_try_admit(&pod.namespace, &pod.name, &pod.uid, &node_name)
                .await
                .map_err(|error| {
                    klights_node_store::RuntimeWorkError::persistence_failed(error.to_string())
                })? {
                crate::datastore::node_local::PodSlotAdmissionResult::Admitted {
                    resource_version,
                } => Ok(klights_node_store::PodSlotAdmissionResult::Admitted {
                    observed_pod_version: observed_pod_version(resource_version)?,
                }),
                crate::datastore::node_local::PodSlotAdmissionResult::Blocked {
                    blocking_uid,
                    blocking_node,
                    state,
                    resource_version,
                } => Ok(klights_node_store::PodSlotAdmissionResult::Blocked {
                    blocking_uid,
                    blocking_node,
                    state: slot_state(state),
                    observed_pod_version: observed_pod_version(resource_version)?,
                }),
            }
        })
    }

    fn mark_terminating(
        &self,
        request: klights_node_store::PodSlotAdmissionRequest,
    ) -> klights_node_store::RuntimeWorkFuture<'_, klights_node_store::PodSlotMutationResult> {
        Box::pin(async move {
            let (pod, node_name) = request.into_parts();
            let result = self
                .store
                .pod_slot_mark_terminating(&pod.namespace, &pod.name, &pod.uid, &node_name)
                .await
                .map_err(|error| {
                    klights_node_store::RuntimeWorkError::persistence_failed(error.to_string())
                })?;
            match result {
                crate::datastore::node_local::PodSlotMutationResult::Changed {
                    resource_version,
                } => Ok(klights_node_store::PodSlotMutationResult::Changed {
                    observed_pod_version: observed_pod_version(resource_version)?,
                }),
                crate::datastore::node_local::PodSlotMutationResult::Unchanged {
                    resource_version,
                } => Ok(klights_node_store::PodSlotMutationResult::Unchanged {
                    observed_pod_version: observed_pod_version(resource_version)?,
                }),
            }
        })
    }

    fn clear_if_uid(
        &self,
        request: klights_node_store::PodSlotAdmissionRequest,
    ) -> klights_node_store::RuntimeWorkFuture<'_, klights_node_store::PodSlotClearResult> {
        Box::pin(async move {
            let (pod, _node_name) = request.into_parts();
            let result = self
                .store
                .pod_slot_clear_if_uid(&pod.namespace, &pod.name, &pod.uid)
                .await
                .map_err(|error| {
                    klights_node_store::RuntimeWorkError::persistence_failed(error.to_string())
                })?;
            match result {
                crate::datastore::node_local::PodSlotClearResult::Cleared { resource_version } => {
                    Ok(klights_node_store::PodSlotClearResult::Cleared {
                        observed_pod_version: observed_pod_version(resource_version)?,
                    })
                }
                crate::datastore::node_local::PodSlotClearResult::NotFound => {
                    Ok(klights_node_store::PodSlotClearResult::NotFound)
                }
                crate::datastore::node_local::PodSlotClearResult::UidMismatch {
                    blocking_uid,
                    blocking_node,
                    state,
                    resource_version,
                } => Ok(klights_node_store::PodSlotClearResult::UidMismatch {
                    blocking_uid,
                    blocking_node,
                    state: slot_state(state),
                    observed_pod_version: observed_pod_version(resource_version)?,
                }),
            }
        })
    }
}

struct DatastorePodSlotSubscription {
    receiver: tokio::sync::broadcast::Receiver<crate::datastore::node_local::PodSlotAdmissionEvent>,
}

impl klights_node_store::PodSlotEventSubscription for DatastorePodSlotSubscription {
    fn next_event(
        &mut self,
    ) -> klights_node_store::RuntimeWorkFuture<'_, Option<klights_node_store::PodSlotAdmissionEvent>>
    {
        Box::pin(async move {
            let event = match self.receiver.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(None),
                Err(error) => {
                    return Err(klights_node_store::RuntimeWorkError::retryable(
                        error.to_string(),
                    ));
                }
            };
            let event = match event {
                crate::datastore::node_local::PodSlotAdmissionEvent::Changed {
                    namespace,
                    pod_name,
                    pod_uid,
                    state,
                    resource_version,
                } => klights_node_store::PodSlotAdmissionEvent::Changed {
                    pod: klights_types::PodIdentity::new(&namespace, &pod_name, &pod_uid),
                    state: slot_state(state),
                    observed_pod_version: observed_pod_version(resource_version)?,
                },
                crate::datastore::node_local::PodSlotAdmissionEvent::Cleared {
                    namespace,
                    pod_name,
                    pod_uid,
                    resource_version,
                } => klights_node_store::PodSlotAdmissionEvent::Cleared {
                    pod: klights_types::PodIdentity::new(&namespace, &pod_name, &pod_uid),
                    observed_pod_version: observed_pod_version(resource_version)?,
                },
            };
            Ok(Some(event))
        })
    }
}

impl klights_node_store::PodSlotAdmissionEventSource for DatastorePodSlotAdapter {
    fn subscribe(&self) -> Box<dyn klights_node_store::PodSlotEventSubscription> {
        Box::new(DatastorePodSlotSubscription {
            receiver: self.store.subscribe_pod_slot_admissions(),
        })
    }
}

pub struct DatastorePodWatchSource {
    leader_watch: Arc<dyn klights_leader_api::LeaderWatch>,
    heartbeat_watch: tokio::sync::Mutex<HeartbeatWatchState>,
}

#[derive(Default)]
struct HeartbeatWatchState {
    stream: Option<klights_leader_api::WatchStream>,
    cursor: Option<klights_leader_api::WatchResumeCursor>,
}

impl DatastorePodWatchSource {
    pub fn new(leader_watch: Arc<dyn klights_leader_api::LeaderWatch>) -> Self {
        Self {
            leader_watch,
            heartbeat_watch: tokio::sync::Mutex::new(HeartbeatWatchState::default()),
        }
    }
}

#[async_trait::async_trait]
impl PodWatchSource for DatastorePodWatchSource {
    fn open_pod_manager_watch(
        &self,
        node_name: String,
        recovery: crate::kubelet::pod_watch_source::PodWatchRecoveryPlan,
    ) -> crate::kubelet::pod_watch_source::PodWatchFuture<'_> {
        Box::pin(async move {
            use crate::kubelet::pod_watch_source::{
                PodWatchCheckpoint, PodWatchScope, PodWatchSession, scope_watch_stream,
            };
            let requests = [
                (
                    PodWatchScope::Pod,
                    klights_leader_api::WatchRequest::try_new(
                        "v1",
                        "Pod",
                        None,
                        None,
                        Some(format!("spec.nodeName={node_name}")),
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::PersistentVolumeClaim,
                    klights_leader_api::WatchRequest::try_new(
                        "v1",
                        "PersistentVolumeClaim",
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::PersistentVolume,
                    klights_leader_api::WatchRequest::try_new(
                        "v1",
                        "PersistentVolume",
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::Secret,
                    klights_leader_api::WatchRequest::try_new(
                        "v1", "Secret", None, None, None, None, None,
                    )?,
                ),
                (
                    PodWatchScope::ConfigMap,
                    klights_leader_api::WatchRequest::try_new(
                        "v1",
                        "ConfigMap",
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
                (
                    PodWatchScope::Namespace,
                    klights_leader_api::WatchRequest::try_new(
                        "v1",
                        "Namespace",
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?,
                ),
            ];
            let mut streams = Vec::with_capacity(requests.len());
            let mut checkpoint = PodWatchCheckpoint::default();
            for (scope, request) in requests {
                // A typed replay expiry deliberately omits only that scope's
                // cursor. The focused LeaderWatch implementation then invokes
                // its authoritative fresh establishment/relist kernel; this is
                // not a lenient reuse of an expired scalar RV.
                let request = if recovery.must_relist(scope) {
                    request
                } else if let Some(cursor) = recovery.cursor_for(scope) {
                    request.with_resume_cursor(cursor)?
                } else {
                    request
                };
                let stream = self.leader_watch.watch_resources(request).await?;
                if let Some(cursor) = stream.accepted_cursor() {
                    checkpoint.accept_open_cursor(scope, cursor);
                } else if let Some(cursor) = recovery.cursor_for(scope) {
                    checkpoint.accept_open_cursor(scope, cursor);
                }
                streams.push(scope_watch_stream(scope, stream));
            }
            let stream = futures::stream::select_all(streams);
            Ok(PodWatchSession {
                stream: Box::pin(stream) as PodWatchStream,
                checkpoint,
            })
        })
    }
}

impl crate::api::pod_subresources::logs::PodLogFollowWatchPort for DatastorePodWatchSource {
    fn open_pod_watch(&self) -> klights_leader_api::LeaderWatchFuture<'_> {
        let request =
            klights_leader_api::WatchRequest::try_new("v1", "Pod", None, None, None, None, None)
                .expect("Pod log follow watch identity is valid");
        self.leader_watch.watch_resources(request)
    }
}

impl NodeHeartbeatEventSource for DatastorePodWatchSource {
    fn next_node_event(&self) -> NodeHeartbeatEventFuture<'_> {
        Box::pin(async move {
            let mut heartbeat = self.heartbeat_watch.lock().await;
            if heartbeat.stream.is_none() {
                let request = klights_leader_api::WatchRequest::try_new(
                    "v1", "Node", None, None, None, None, None,
                )
                .expect("Node heartbeat watch identity is valid");
                let request = if let Some(cursor) = heartbeat.cursor {
                    request.with_resume_cursor(cursor)?
                } else {
                    request
                };
                match self.leader_watch.watch_resources(request).await {
                    Ok(stream) => {
                        if let Some(cursor) = stream.accepted_cursor() {
                            heartbeat.cursor = Some(cursor);
                        }
                        heartbeat.stream = Some(stream);
                    }
                    Err(klights_leader_api::LeaderWatchError::ReplayExpired { .. }) => {
                        heartbeat.cursor = None;
                        return Ok(NodeHeartbeatEvent::ReplayExpired);
                    }
                    Err(error) => return Err(anyhow::Error::from(error)),
                }
            }
            let event = heartbeat
                .stream
                .as_mut()
                .expect("heartbeat stream initialized")
                .next()
                .await;
            match event {
                Some(Ok(event)) => {
                    let cursor = heartbeat.cursor.get_or_insert_default();
                    cursor.advance_after_apply(&event)?;
                    if matches!(
                        event.event_type(),
                        klights_leader_api::WatchEventType::Bookmark
                            | klights_leader_api::WatchEventType::Deleted
                    ) || event.resource().kind != "Node"
                    {
                        return Ok(NodeHeartbeatEvent::Other);
                    }
                    let node_name = event.resource().name.as_str();
                    if node_name.is_empty() {
                        return Ok(NodeHeartbeatEvent::Other);
                    }
                    Ok(NodeHeartbeatEvent::NodeChanged {
                        node_name: node_name.to_string(),
                    })
                }
                Some(Err(klights_leader_api::LeaderWatchError::ReplayExpired { .. })) => {
                    heartbeat.stream = None;
                    heartbeat.cursor = None;
                    Ok(NodeHeartbeatEvent::ReplayExpired)
                }
                Some(Err(error)) => {
                    heartbeat.stream = None;
                    Err(anyhow::Error::from(error))
                }
                None => {
                    heartbeat.stream = None;
                    anyhow::bail!("Node heartbeat positioned watch stream closed")
                }
            }
        })
    }
}
pub struct RootPodEventSink {
    outbox: Option<Arc<crate::node_outbox::Outbox>>,
    datastore: crate::datastore::DatastoreHandle,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl RootPodEventSink {
    pub fn new(
        outbox: Option<Arc<crate::node_outbox::Outbox>>,
        datastore: crate::datastore::DatastoreHandle,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            outbox,
            datastore,
            wall_clock,
        }
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::events::PodEventSink for RootPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> Result<(), crate::kubelet::pod_runtime::events::PodEventSinkError> {
        let pod = serde_json::json!({
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
            },
        });
        crate::pod_events::emit_pod_event_with_outbox(
            self.datastore.as_ref(),
            self.outbox.as_deref(),
            crate::pod_events::PodEventRecord {
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
            crate::kubelet::pod_runtime::events::PodEventSinkError::unavailable(error.to_string())
        })?;
        Ok(())
    }
}

pub struct WorkerPodEventSink {
    outbox: Arc<crate::node_outbox::Outbox>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl WorkerPodEventSink {
    pub fn new(
        outbox: Arc<crate::node_outbox::Outbox>,
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
impl crate::kubelet::pod_runtime::events::PodEventSink for WorkerPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> Result<(), crate::kubelet::pod_runtime::events::PodEventSinkError> {
        let pod = serde_json::json!({
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
            },
        });
        crate::pod_events::emit_worker_pod_event(
            self.resource_query.as_ref(),
            self.outbox.as_ref(),
            crate::pod_events::PodEventRecord {
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
            crate::kubelet::pod_runtime::events::PodEventSinkError::unavailable(error.to_string())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod positioned_watch_boundary_tests {
    use super::*;
    use klights_leader_api::{
        LeaderWatch, LeaderWatchFuture, ResourceEvent, WatchEventType, WatchRequest, WatchStream,
    };
    use std::sync::Mutex as StdMutex;

    struct RecordingLeaderWatch {
        requests: StdMutex<Vec<WatchRequest>>,
        events: StdMutex<Option<Vec<ResourceEvent>>>,
    }

    impl RecordingLeaderWatch {
        fn with_events(events: Vec<ResourceEvent>) -> Arc<Self> {
            Arc::new(Self {
                requests: StdMutex::new(Vec::new()),
                events: StdMutex::new(Some(events)),
            })
        }
    }

    impl LeaderWatch for RecordingLeaderWatch {
        fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
            self.requests
                .lock()
                .expect("watch request mutex")
                .push(request);
            let events = self
                .events
                .lock()
                .expect("watch event mutex")
                .take()
                .expect("heartbeat must establish only one stream");
            Box::pin(async move {
                Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::iter(events.into_iter().map(Ok)),
                ))
            })
        }
    }

    fn node_event(name: &str, resource_version: i64) -> ResourceEvent {
        let resource = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "resourceVersion": resource_version.to_string(),
            }
        })))
        .expect("valid Node resource");
        ResourceEvent::try_new(WatchEventType::Modified, resource, None)
            .expect("valid Node watch event")
    }

    #[tokio::test]
    async fn heartbeat_retains_one_positioned_leader_watch_stream() {
        let leader = RecordingLeaderWatch::with_events(vec![
            node_event("node-a", 11),
            node_event("node-b", 12),
        ]);
        let source = DatastorePodWatchSource::new(leader.clone());

        assert_eq!(
            source.next_node_event().await.expect("first Node event"),
            NodeHeartbeatEvent::NodeChanged {
                node_name: "node-a".to_string()
            }
        );
        assert_eq!(
            source.next_node_event().await.expect("second Node event"),
            NodeHeartbeatEvent::NodeChanged {
                node_name: "node-b".to_string()
            }
        );

        let requests = leader.requests.lock().expect("watch request mutex");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].api_version(), "v1");
        assert_eq!(requests[0].kind(), "Node");
        assert_eq!(requests[0].namespace(), None);
        assert_eq!(requests[0].start_resource_version(), None);
        assert_eq!(requests[0].start_watch_replay_position(), None);
    }
}
