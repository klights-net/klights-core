use std::sync::Arc;

use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{CurrentResourceVersionStore, WatchStore};
use crate::kubelet::node_heartbeat::{
    NodeHeartbeatClock, NodeHeartbeatEvent, NodeHeartbeatEventFuture, NodeHeartbeatEventSource,
};
use crate::kubelet::pod_watch_source::{BoxedWatchReplaySource, PodWatchSource};
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WindowPolicy,
};
use klights_watch::WatchTopic;

pub struct SystemNodeHeartbeatClock;

impl NodeHeartbeatClock for SystemNodeHeartbeatClock {
    fn now_microtime(&self) -> String {
        crate::utils::k8s_microtime_now()
    }
}

pub struct LeaderPersistentVolumeEventHandler {
    db: crate::datastore::DatastoreHandle,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    file_process: klights_supervisor::FileProcessExecutor,
}

impl LeaderPersistentVolumeEventHandler {
    pub fn new(
        db: crate::datastore::DatastoreHandle,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self {
            db,
            is_leader_rx,
            file_process,
        }
    }

    async fn reconcile_pvc(&self, resource: klights_cluster_core::Resource, event_name: &str) {
        use klights_reconcile_api::PvcReconcileSink;

        let reconcile = crate::pod_reconcile_adapter::PersistentVolumeReconcileAdapter::new(
            self.db.as_ref(),
            &self.file_process,
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
    async fn handle_pvc_event(&self, event: &crate::watch::WatchEvent, event_name: &str) {
        if !*self.is_leader_rx.borrow()
            || !matches!(
                event.event_type,
                crate::watch::EventType::Added | crate::watch::EventType::Modified
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

    async fn handle_pv_event(&self, event: &crate::watch::WatchEvent, _event_name: &str) {
        if !*self.is_leader_rx.borrow() || event.event_type != crate::watch::EventType::Added {
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
    state: crate::datastore::PodSlotAdmissionState,
) -> klights_node_store::PodSlotAdmissionState {
    match state {
        crate::datastore::PodSlotAdmissionState::Admitted => {
            klights_node_store::PodSlotAdmissionState::Admitted
        }
        crate::datastore::PodSlotAdmissionState::Terminating => {
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
                crate::datastore::PodSlotAdmissionResult::Admitted { resource_version } => {
                    Ok(klights_node_store::PodSlotAdmissionResult::Admitted {
                        observed_pod_version: observed_pod_version(resource_version)?,
                    })
                }
                crate::datastore::PodSlotAdmissionResult::Blocked {
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
                crate::datastore::PodSlotMutationResult::Changed { resource_version } => {
                    Ok(klights_node_store::PodSlotMutationResult::Changed {
                        observed_pod_version: observed_pod_version(resource_version)?,
                    })
                }
                crate::datastore::PodSlotMutationResult::Unchanged { resource_version } => {
                    Ok(klights_node_store::PodSlotMutationResult::Unchanged {
                        observed_pod_version: observed_pod_version(resource_version)?,
                    })
                }
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
                crate::datastore::PodSlotClearResult::Cleared { resource_version } => {
                    Ok(klights_node_store::PodSlotClearResult::Cleared {
                        observed_pod_version: observed_pod_version(resource_version)?,
                    })
                }
                crate::datastore::PodSlotClearResult::NotFound => {
                    Ok(klights_node_store::PodSlotClearResult::NotFound)
                }
                crate::datastore::PodSlotClearResult::UidMismatch {
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
    receiver: tokio::sync::broadcast::Receiver<crate::datastore::PodSlotAdmissionEvent>,
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
                crate::datastore::PodSlotAdmissionEvent::Changed {
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
                crate::datastore::PodSlotAdmissionEvent::Cleared {
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
    resource_versions: Arc<dyn CurrentResourceVersionStore>,
    heartbeat_cursor: tokio::sync::Mutex<Option<SignalWatchCursor<BoxedWatchReplaySource>>>,
}

impl DatastorePodWatchSource {
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: WatchStore + CurrentResourceVersionStore + 'static,
    {
        Self {
            watch_store: store.clone(),
            resource_versions: store,
            heartbeat_cursor: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl PodWatchSource for DatastorePodWatchSource {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_store.subscribe_watch_signals(topic)
    }

    fn replay_source(&self, targets: Vec<klights_watch::WatchTarget>) -> BoxedWatchReplaySource {
        BoxedWatchReplaySource::new(Arc::new(DatastoreWatchReplaySource::new(
            self.watch_store.clone(),
            targets.into_iter().map(datastore_watch_target).collect(),
        )))
    }

    async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.resource_versions.get_current_resource_version().await
    }
}

impl crate::api::pod_subresources::logs::PodLogFollowWatchPort for DatastorePodWatchSource {
    fn subscribe_pod_watch_signals(&self) -> klights_watch::WatchSignalReceiver {
        self.watch_store
            .subscribe_watch_signals(klights_watch::WatchTopic::new("v1", "Pod"))
    }

    fn pod_watch_replay_source(
        &self,
    ) -> crate::api::pod_subresources::logs::PodLogFollowReplaySource {
        crate::api::pod_subresources::logs::PodLogFollowReplaySource::new(Arc::new(
            DatastoreWatchReplaySource::new(
                self.watch_store.clone(),
                vec![crate::datastore::WatchTarget::namespaced("v1", "Pod")],
            ),
        ))
    }

    fn current_resource_version(
        &self,
    ) -> crate::api::pod_subresources::logs::PodLogFollowResourceVersionFuture<'_> {
        Box::pin(async move { self.resource_versions.get_current_resource_version().await })
    }
}

fn datastore_watch_target(target: klights_watch::WatchTarget) -> crate::datastore::WatchTarget {
    match target.scope() {
        klights_watch::WatchTargetScope::Cluster => {
            crate::datastore::WatchTarget::cluster(target.api_version(), target.kind())
        }
        klights_watch::WatchTargetScope::Namespaced(None) => {
            crate::datastore::WatchTarget::namespaced(target.api_version(), target.kind())
        }
        klights_watch::WatchTargetScope::Namespaced(Some(namespace)) => {
            crate::datastore::WatchTarget::namespaced_in_namespace(
                target.api_version(),
                target.kind(),
                namespace,
            )
        }
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
                    self.watch_store.subscribe_watch_signals(topic.clone()),
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
    ) -> anyhow::Result<()> {
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
        .await?;
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
    ) -> anyhow::Result<()> {
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
        .await?;
        Ok(())
    }
}
