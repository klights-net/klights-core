use std::sync::Arc;

use crate::datastore::{CurrentResourceVersionStore, WatchStore};
use crate::datastore_watch_replay_adapter::DatastoreWatchReplaySource;
use crate::kubelet::node_heartbeat::{
    NodeHeartbeatClock, NodeHeartbeatEvent, NodeHeartbeatEventFuture, NodeHeartbeatEventSource,
};
use crate::kubelet::pod_watch_source::{PodWatchEvent, PodWatchSource, PodWatchStream};
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WindowPolicy,
};
use klights_watch::WatchTopic;

struct BoxedWatchReplaySource {
    inner: Arc<dyn crate::watch::WatchReplaySource>,
}

impl BoxedWatchReplaySource {
    fn new(inner: Arc<dyn crate::watch::WatchReplaySource>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl crate::watch::WatchReplaySource for BoxedWatchReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<crate::watch::WatchEvent>> {
        self.inner.replay_since(since_rv).await
    }

    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<klights_watch::WatchReplayRead<crate::watch::WatchEvent>> {
        self.inner.replay_since_checked(since_rv, limit).await
    }

    async fn replay_after_checked(
        &self,
        position: klights_watch::WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<klights_watch::PositionedWatchReplayRead<crate::watch::WatchEvent>> {
        self.inner.replay_after_checked(position, limit).await
    }

    async fn earliest_retained_rv(&self) -> anyhow::Result<Option<i64>> {
        self.inner.earliest_retained_rv().await
    }
}

pub struct SystemNodeHeartbeatClock;

impl NodeHeartbeatClock for SystemNodeHeartbeatClock {
    fn now_microtime(&self) -> String {
        crate::k8s_time::now_microtime()
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
    watch_store: Arc<dyn WatchStore>,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    resource_versions: Arc<dyn CurrentResourceVersionStore>,
    leader_watch: Arc<dyn klights_leader_api::LeaderWatch>,
    heartbeat_cursor: tokio::sync::Mutex<Option<SignalWatchCursor<BoxedWatchReplaySource>>>,
}

impl DatastorePodWatchSource {
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: WatchStore + CurrentResourceVersionStore + klights_leader_api::LeaderWatch + 'static,
        T: klights_watch::WatchSignalSubscribe,
    {
        Self {
            watch_store: store.clone(),
            watch_signals: store.clone(),
            resource_versions: store.clone(),
            leader_watch: store,
            heartbeat_cursor: tokio::sync::Mutex::new(None),
        }
    }

    pub fn new_with_ports(
        watch_store: Arc<dyn WatchStore>,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
        resource_versions: Arc<dyn CurrentResourceVersionStore>,
        leader_watch: Arc<dyn klights_leader_api::LeaderWatch>,
    ) -> Self {
        Self {
            watch_store,
            watch_signals,
            resource_versions,
            leader_watch,
            heartbeat_cursor: tokio::sync::Mutex::new(None),
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
            let mut cursor = self.heartbeat_cursor.lock().await;
            if cursor.is_none() {
                let topic = WatchTopic::new("v1", "Node");
                let replay =
                    BoxedWatchReplaySource::new(Arc::new(DatastoreWatchReplaySource::new(
                        self.watch_store.clone(),
                        vec![crate::datastore::WatchTarget::cluster("v1", "Node")],
                    )));
                let mut next = SignalWatchCursor::new(
                    self.watch_signals.subscribe(topic.clone()),
                    replay,
                    topic,
                    WatchDeliveryScope::Cluster,
                    self.resource_versions
                        .get_current_resource_version()
                        .await
                        .unwrap_or(0),
                    WindowPolicy::default_watch_delivery(),
                );
                if let Err(error) = next.prime_replay_or_expired().await {
                    tracing::warn!(?error, "Node heartbeat initial replay failed");
                }
                *cursor = Some(next);
            }
            let event = cursor
                .as_mut()
                .expect("heartbeat cursor initialized")
                .next_event()
                .await;
            match event {
                Ok(event)
                    if !matches!(event.event_type, EventType::Bookmark | EventType::Deleted)
                        && event.object.get("kind").and_then(|kind| kind.as_str())
                            == Some("Node") =>
                {
                    let Some(node_name) = event
                        .object
                        .pointer("/metadata/name")
                        .and_then(|name| name.as_str())
                    else {
                        return Ok(NodeHeartbeatEvent::Other);
                    };
                    Ok(NodeHeartbeatEvent::NodeChanged {
                        node_name: node_name.to_string(),
                    })
                }
                Ok(_) => Ok(NodeHeartbeatEvent::Other),
                Err(WatchCursorError::Expired) => Ok(NodeHeartbeatEvent::ReplayExpired),
                Err(WatchCursorError::Closed) => {
                    anyhow::bail!("Node heartbeat watch signal channel closed")
                }
                Err(WatchCursorError::Replay(error)) => Err(error),
            }
        })
    }
}
pub struct RootPodEventSink {
    outbox: Option<Arc<crate::node_outbox::Outbox>>,
    datastore: crate::datastore::DatastoreHandle,
}

impl RootPodEventSink {
    pub fn new(
        outbox: Option<Arc<crate::node_outbox::Outbox>>,
        datastore: crate::datastore::DatastoreHandle,
    ) -> Self {
        Self { outbox, datastore }
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
}

impl WorkerPodEventSink {
    pub fn new(
        outbox: Arc<crate::node_outbox::Outbox>,
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self {
            outbox,
            resource_query,
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
            },
        )
        .await
        .map_err(|error| {
            crate::kubelet::pod_runtime::events::PodEventSinkError::unavailable(error.to_string())
        })?;
        Ok(())
    }
}
