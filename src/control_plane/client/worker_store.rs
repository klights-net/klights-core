use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::control_plane::client::{LeaderApiClient, ListRequest, ResourceKey, WatchRequest};
#[cfg(test)]
use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::node_local::NodeLocalHandle;
use crate::datastore::{
    AppliedOutboxRecord, CatchUpResource, DatastoreBackend, ListPageRequest, NodeSubnet, PatchKind,
    PodCleanupIntent, PodEndpointEvent, PodEndpointRow, PodNetworkAllocationLink,
    PodNetworkAllocationPod, PodNetworkAllocationRequest, PodNetworkAllocationSubnet,
    PodNetworkEndpoint, PodSlotAdmissionEvent, PodSlotAdmissionResult, PodWorkqueueEntry,
    PodWorkqueueKind, Resource, ResourceList, ResourcePatchRequest, ResourcePreconditions,
    SandboxRef, WatchTarget, WatchTargetScope,
};
use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::networking::VtepMac;
use crate::watch::{EventType, WatchBus, WatchEvent, WatchSignal, WatchTopic};

const WORKER_WATCH_EVENT_HISTORY_CAPACITY: usize = 32_768;

/// Worker-local compatibility store for legacy kubelet call sites.
///
/// This type deliberately does not open or own `cluster.db`. Cluster resource
/// reads are served through the node/worker cache exposed by `LeaderApiClient`;
/// node-local runtime/network rows are served through `NodeLocalBackend`.
pub struct WorkerStoreAdapter {
    cluster_api: Arc<dyn LeaderApiClient>,
    node_local: NodeLocalHandle,
    watch_bus: Arc<WatchBus>,
    node_name: String,
    current_rv: AtomicI64,
    event_history: Mutex<VecDeque<WatchEvent>>,
    pod_lifecycle_router: Mutex<Option<Arc<PodLifecycleRouter>>>,
}

impl WorkerStoreAdapter {
    pub fn new(
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        node_name: String,
    ) -> Self {
        Self {
            cluster_api,
            node_local,
            watch_bus: Arc::new(WatchBus::new(1024)),
            node_name,
            current_rv: AtomicI64::new(0),
            event_history: Mutex::new(VecDeque::new()),
            pod_lifecycle_router: Mutex::new(None),
        }
    }

    pub fn set_pod_lifecycle_router(&self, router: Arc<PodLifecycleRouter>) {
        *self
            .pod_lifecycle_router
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(router);
    }

    pub async fn start_watch_mirrors(
        self: &Arc<Self>,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<crate::task_supervisor::SupervisedJoinHandle<()>>> {
        let mut handles = Vec::new();
        for req in self.worker_watch_requests() {
            let this = self.clone();
            let cancel = cancel.clone();
            let spawn_supervisor = supervisor.clone();
            let mirror_supervisor = supervisor.clone();
            handles.push(
                spawn_supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Network,
                        "worker_store_watch_mirror",
                        async move {
                            this.run_watch_mirror(req, mirror_supervisor, cancel).await;
                        },
                    )
                    .await?,
            );
        }
        Ok(handles)
    }

    pub fn watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    pub fn watch_topic(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    fn worker_watch_requests(&self) -> Vec<WatchRequest> {
        let mut reqs = vec![WatchRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: Some(format!("spec.nodeName={}", self.node_name)),
            start_resource_version: None,
        }];
        for (api_version, kind, namespace) in [
            ("v1", "Namespace", None),
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
        ] {
            reqs.push(WatchRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: None,
                field_selector: None,
                start_resource_version: None,
            });
        }
        reqs
    }

    async fn run_watch_mirror(
        self: Arc<Self>,
        req: WatchRequest,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let mut next_resource_version = req.start_resource_version;
        // Consecutive failed reconnects; reset to 0 once the stream delivers an
        // event (progress). Drives the shared exponential reconnect backoff so
        // a sustained leader/WAN outage cannot become a fixed-interval
        // reconnect storm across every watch scope.
        let mut reconnect_attempt: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if next_resource_version.is_none() {
                match self.publish_initial_watch_snapshot(&req).await {
                    Ok(resource_version) => {
                        next_resource_version = Some(resource_version);
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "worker store watch mirror initial list failed");
                        if !sleep_before_watch_mirror_reconnect(
                            &supervisor,
                            &cancel,
                            reconnect_attempt,
                        )
                        .await
                        {
                            return;
                        }
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        continue;
                    }
                }
            }

            let mut watch_req = req.clone();
            watch_req.start_resource_version = next_resource_version;
            match self.cluster_api.watch_resources(watch_req).await {
                Ok(mut stream) => {
                    use futures::StreamExt;
                    let mut relist_required = false;
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            event = stream.next() => {
                                match event {
                                    Some(Ok(event)) => {
                                        reconnect_attempt = 0;
                                        if let Some(rv) = event.event.resource_version() {
                                            next_resource_version =
                                                Some(next_resource_version.unwrap_or(0).max(rv));
                                        }
                                        self.publish_watch_from_mirror(event.event).await;
                                    }
                                    Some(Err(err)) => {
                                        if is_watch_window_expired(&err) {
                                            // Replay-window expiration (gRPC
                                            // OUT_OF_RANGE, the K8s "too old
                                            // resource version" / HTTP 410
                                            // contract): the leader GC'd past
                                            // our resume bookmark and the
                                            // in-scope events in the gap are
                                            // gone. Relist from a fresh
                                            // snapshot instead of reconnecting
                                            // at the stale bookmark, which
                                            // would loop on the same
                                            // expiration and never recover the
                                            // missing events.
                                            tracing::info!(
                                                error = %err,
                                                "worker store watch mirror replay window expired; relisting"
                                            );
                                            next_resource_version = None;
                                            reconnect_attempt = 0;
                                            relist_required = true;
                                            break;
                                        }
                                        tracing::warn!(error = %err, "worker store watch mirror failed");
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                    // A relist is recovery, not a retry: skip the reconnect
                    // backoff and immediately re-enter the outer loop, where
                    // next_resource_version == None triggers a fresh LIST.
                    if relist_required {
                        continue;
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "worker store watch mirror could not open stream");
                }
            }
            if !sleep_before_watch_mirror_reconnect(&supervisor, &cancel, reconnect_attempt).await {
                return;
            }
            reconnect_attempt = reconnect_attempt.saturating_add(1);
        }
    }

    async fn publish_initial_watch_snapshot(&self, req: &WatchRequest) -> Result<i64> {
        let list = self
            .cluster_api
            .list_resources(ListRequest {
                api_version: req.api_version.clone(),
                kind: req.kind.clone(),
                namespace: req.namespace.clone(),
                label_selector: req.label_selector.clone(),
                field_selector: req.field_selector.clone(),
                limit: None,
                continue_token: None,
            })
            .await?;
        self.observe_rv(list.resource_version);
        let resource_version = list.resource_version;
        for resource in list.items {
            self.publish_watch_from_mirror(WatchEvent {
                event_type: EventType::Added,
                object: resource.data.clone(),
                encoded_payload: None,
            })
            .await;
        }
        Ok(resource_version)
    }

    async fn publish_watch_from_mirror(&self, event: WatchEvent) {
        let lifecycle_message = self.local_pod_lifecycle_message(&event);
        self.publish_watch(event);
        let Some(message) = lifecycle_message else {
            return;
        };
        let router = self
            .pod_lifecycle_router
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(router) = router else {
            tracing::debug!(
                node = %self.node_name,
                "worker store mirror saw local Pod before lifecycle router was configured"
            );
            return;
        };
        if let Err(err) = router.route(message).await {
            tracing::warn!(
                node = %self.node_name,
                error = %err,
                "worker store mirror failed to route local Pod to lifecycle actor"
            );
        }
    }

    fn publish_watch(&self, event: WatchEvent) {
        if let Some(rv) = event
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(|rv| rv.as_i64().or_else(|| rv.as_str()?.parse::<i64>().ok()))
        {
            self.observe_rv(rv);
        }
        self.record_watch_event(event.clone());
        if let Some(signal) = WatchSignal::from_event(&event) {
            self.watch_bus.publish_signal(signal);
        }
        #[cfg(test)]
        self.watch_bus.publish(event);
    }

    fn local_pod_lifecycle_message(&self, event: &WatchEvent) -> Option<LifecycleMessage> {
        let pod = event.object.as_ref();
        if pod.get("apiVersion").and_then(|value| value.as_str()) != Some("v1")
            || pod.get("kind").and_then(|value| value.as_str()) != Some("Pod")
        {
            return None;
        }
        if pod
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str())
            != Some(self.node_name.as_str())
        {
            return None;
        }
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())?;
        let name = pod
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())?;
        let uid = pod
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .filter(|uid| !uid.trim().is_empty())?;
        let key = PodLifecycleKey::new(namespace, name, uid);
        let resource_version = event.resource_version();
        match event.event_type {
            EventType::Added => Some(LifecycleMessage::WatchAdded {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Modified => Some(LifecycleMessage::WatchModified {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Deleted => Some(LifecycleMessage::WatchDeleted {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Bookmark | EventType::Error => None,
        }
    }

    fn record_watch_event(&self, event: WatchEvent) {
        if event.resource_version().is_none() {
            return;
        }
        let mut history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        history.push_back(event);
        while history.len() > WORKER_WATCH_EVENT_HISTORY_CAPACITY {
            history.pop_front();
        }
    }

    fn historical_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Vec<CatchUpResource> {
        let history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        history
            .iter()
            .filter_map(|event| {
                let rv = event.resource_version()?;
                if rv <= since_rv || !watch_event_matches_targets(event, targets) {
                    return None;
                }
                catchup_resource_from_watch_event(event)
            })
            .collect()
    }

    fn is_pod_resource(api_version: &str, kind: &str) -> bool {
        api_version == "v1" && kind == "Pod"
    }

    fn pod_belongs_to_local_node(&self, resource: &Resource) -> bool {
        resource
            .data
            .pointer("/spec/nodeName")
            .and_then(|node| node.as_str())
            .is_some_and(|node| node == self.node_name)
    }

    fn snapshot_replay_event_type(since_rv: i64) -> &'static str {
        if since_rv > 0 { "MODIFIED" } else { "ADDED" }
    }

    fn local_pod_field_selector(&self, field_selector: Option<&str>) -> String {
        let local_selector = format!("spec.nodeName={}", self.node_name);
        match field_selector
            .map(str::trim)
            .filter(|selector| !selector.is_empty())
        {
            Some(selector)
                if selector
                    .split(',')
                    .any(|part| part.trim() == local_selector) =>
            {
                selector.to_string()
            }
            Some(selector) => format!("{selector},{local_selector}"),
            None => local_selector,
        }
    }

    fn observe_rv(&self, rv: i64) {
        let mut current = self.current_rv.load(Ordering::Relaxed);
        while rv > current {
            match self.current_rv.compare_exchange_weak(
                current,
                rv,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    async fn list_for_target(&self, target: &WatchTarget) -> Result<ResourceList> {
        let namespace = match &target.scope {
            WatchTargetScope::Cluster => None,
            WatchTargetScope::Namespaced(namespace) => namespace.clone(),
        };
        let field_selector = if target.api_version == "v1" && target.kind == "Pod" {
            Some(format!("spec.nodeName={}", self.node_name))
        } else {
            None
        };
        self.cluster_api
            .list_resources(ListRequest {
                api_version: target.api_version.clone(),
                kind: target.kind.clone(),
                namespace,
                label_selector: None,
                field_selector,
                limit: None,
                continue_token: None,
            })
            .await
    }

    fn unsupported<T>(&self, operation: &str) -> Result<T> {
        Err(anyhow!(
            "worker-local store does not support direct cluster datastore operation {operation}"
        ))
    }
}

async fn sleep_before_watch_mirror_reconnect(
    supervisor: &crate::task_supervisor::TaskSupervisor,
    cancel: &tokio_util::sync::CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = supervisor.sleep(
            "worker_store_watch_mirror_reconnect",
            crate::utils::watch_reconnect_delay(attempt),
        ) => true,
    }
}

/// True when a worker watch-stream error is a replay-window expiration
/// (gRPC `OUT_OF_RANGE`, the Kubernetes "too old resource version" / HTTP 410
/// contract). The leader returns it from `replay_watch_events_after` when the
/// durable `watch_events` window no longer covers the worker's resume bookmark;
/// the reflector must relist from a fresh snapshot rather than retry the stale
/// bookmark, which would loop on the same expiration.
///
/// The tonic::Status is carried as the error source by the gRPC client (see
/// `watch_resources_rpc`), so walk the anyhow chain to find and inspect it.
fn is_watch_window_expired(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|s| s.code() == tonic::Code::OutOfRange)
    })
}

fn watch_event_matches_targets(event: &WatchEvent, targets: &[WatchTarget]) -> bool {
    let Some(api_version) = event
        .object
        .get("apiVersion")
        .and_then(|value| value.as_str())
    else {
        return false;
    };
    let Some(kind) = event.object.get("kind").and_then(|value| value.as_str()) else {
        return false;
    };
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str());

    targets.iter().any(|target| {
        if target.api_version != api_version || target.kind != kind {
            return false;
        }
        match &target.scope {
            WatchTargetScope::Cluster => namespace.is_none(),
            WatchTargetScope::Namespaced(Some(target_ns)) => namespace == Some(target_ns.as_str()),
            WatchTargetScope::Namespaced(None) => namespace.is_some(),
        }
    })
}

fn catchup_resource_from_watch_event(event: &WatchEvent) -> Option<CatchUpResource> {
    let api_version = event.object.get("apiVersion")?.as_str()?.to_string();
    let kind = event.object.get("kind")?.as_str()?.to_string();
    let metadata = event.object.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let uid = metadata
        .get("uid")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let resource_version = event.resource_version()?;

    Some(CatchUpResource {
        resource: Resource {
            id: 0,
            api_version,
            kind,
            namespace,
            name,
            uid,
            resource_version,
            data: event.object.clone(),
        },
        event_type: std::borrow::Cow::Owned(event.event_type.to_string()),
    })
}

#[async_trait]
impl DatastoreBackend for WorkerStoreAdapter {
    fn close(&self) {
        self.node_local.close();
    }

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        self.watch_bus.subscribe_many(topics)
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: crate::datastore::PendingWatchEvent) {
        self.publish_watch(pending.event);
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        self.unsupported("apply_raft_log_apply_commit")
    }

    async fn create_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
    ) -> Result<Resource> {
        self.unsupported("create_resource")
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let key = ResourceKey {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        };
        let resource = self.cluster_api.get_resource(key).await?;
        if Self::is_pod_resource(api_version, kind)
            && resource
                .as_ref()
                .is_some_and(|resource| !self.pod_belongs_to_local_node(resource))
        {
            return Ok(None);
        }
        if let Some(resource) = &resource {
            self.observe_rv(resource.resource_version);
        }
        Ok(resource)
    }

    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let field_selector = if Self::is_pod_resource(api_version, kind) {
            Some(self.local_pod_field_selector(field_selector))
        } else {
            field_selector.map(str::to_string)
        };
        // Fetch the full scope from the leader and paginate locally. The worker
        // cache (and the leader datastore) own the authoritative collection, but
        // pagination must be applied exactly once: passing limit/continue_token
        // to the leader *and* re-applying ListPageRequest here would let the
        // local pass clear the leader-provided continue_token (the leader has
        // already truncated to the limit), silently dropping the rest of the
        // collection from a worker LIST. The node/worker cache also returns
        // items in arbitrary (hash-map) order, so sort by name before applying
        // the page so name-based continuation is deterministic and matches the
        // leader's ordering.
        let mut list = self
            .cluster_api
            .list_resources(ListRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: label_selector.map(str::to_string),
                field_selector,
                limit: None,
                continue_token: None,
            })
            .await?;
        self.observe_rv(list.resource_version);
        if page.limit().is_some() || page.continue_token().is_some() {
            list.items.sort_by(|a, b| a.name.cmp(&b.name));
            list = page.apply_to_sorted_resource_list(list);
        }
        Ok(list)
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        let list = self
            .list_resources(
                &api_version,
                &kind,
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        Ok(list
            .items
            .into_iter()
            .map(|resource| {
                (
                    namespaced.then_some(resource.namespace).flatten(),
                    resource.name,
                )
            })
            .collect())
    }

    async fn update_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _expected_rv: i64,
    ) -> Result<Resource> {
        self.unsupported("update_resource")
    }

    async fn update_resource_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.unsupported("update_resource_with_preconditions")
    }

    async fn update_status_only(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _status: Value,
        _expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.unsupported("update_status_only")
    }

    async fn update_status_only_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _status: Value,
        _preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.unsupported("update_status_only_with_preconditions")
    }

    async fn delete_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
    ) -> Result<()> {
        self.unsupported("delete_resource")
    }

    async fn delete_resource_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.unsupported("delete_resource_with_preconditions")
    }

    async fn get_current_resource_version(&self) -> Result<i64> {
        Ok(self.current_rv.load(Ordering::Relaxed))
    }

    async fn create_namespace(&self, _name: &str, _data: Value) -> Result<Resource> {
        self.unsupported("create_namespace")
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        let key = ResourceKey {
            api_version: "v1".to_string(),
            kind: "Namespace".to_string(),
            namespace: None,
            name: name.to_string(),
        };
        let resource = self.cluster_api.get_resource_fresh(key).await?;
        if let Some(resource) = &resource {
            self.observe_rv(resource.resource_version);
        }
        Ok(resource)
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_resources_page(
            "v1",
            "Namespace",
            None,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn update_namespace(
        &self,
        _name: &str,
        _data: Value,
        _expected_rv: i64,
    ) -> Result<Resource> {
        self.unsupported("update_namespace")
    }

    async fn delete_namespace_contents(&self, _name: &str) -> Result<()> {
        self.unsupported("delete_namespace_contents")
    }

    async fn delete_namespace(&self, _name: &str) -> Result<()> {
        self.unsupported("delete_namespace")
    }

    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.node_local
            .enqueue_workqueue(kind, pod, payload, attempt_count, min_delay_ms, last_error)
            .await
    }

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.node_local.peek_workqueue_next_due().await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.node_local.claim_workqueue_due(now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.node_local.complete_workqueue(id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        let pod = crate::pod_identity::PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.node_local
            .enqueue_workqueue(
                row.kind,
                &pod,
                row.payload,
                row.attempt_count.saturating_add(1),
                min_delay_ms,
                Some(error),
            )
            .await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, _error: &str) -> Result<()> {
        self.node_local.complete_workqueue(id).await
    }

    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.node_local
            .admit_pod_runtime(pod_uid, namespace, pod_name, &self.node_name)
            .await?;
        self.node_local.record_sandbox(pod_uid, sandbox_id).await
    }

    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        Ok(self
            .node_local
            .list_pod_runtime_by_namespace(namespace)
            .await?
            .into_iter()
            .find(|row| row.pod_name == pod_name)
            .and_then(|row| row.sandbox_id))
    }

    async fn get_sandbox_for_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        Ok(self
            .node_local
            .get_pod_runtime(pod_uid)
            .await?
            .and_then(|row| row.sandbox_id))
    }

    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        for row in self
            .node_local
            .list_pod_runtime_by_namespace(namespace)
            .await?
            .into_iter()
            .filter(|row| row.pod_name == pod_name)
        {
            self.node_local
                .delete_pod_runtime_for_uid(&row.pod_uid)
                .await?;
        }
        Ok(())
    }

    async fn delete_sandbox_for_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
        _sandbox_id: &str,
    ) -> Result<()> {
        self.node_local.delete_pod_runtime_for_uid(pod_uid).await
    }

    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        self.node_local.delete_network_for_sandbox(sandbox_id).await
    }

    async fn find_owned_resources(
        &self,
        _owner_uid: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn list_resources_by_owner_uid(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        _owner_api_version: &str,
        _owner_name: &str,
        _owner_kind: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let list = self
            .list_resources(
                api_version,
                kind,
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let event_type = Self::snapshot_replay_event_type(since_rv);
        Ok(list
            .items
            .into_iter()
            .filter(|resource| resource.resource_version > since_rv)
            .map(|resource| CatchUpResource {
                resource,
                event_type: std::borrow::Cow::Borrowed(event_type),
            })
            .collect())
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.unsupported("list_cluster_resources")
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let list = self
            .list_resources(
                api_version,
                kind,
                namespace,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let event_type = Self::snapshot_replay_event_type(since_rv);
        Ok(list
            .items
            .into_iter()
            .filter(|resource| resource.resource_version > since_rv)
            .map(|resource| CatchUpResource {
                resource,
                event_type: std::borrow::Cow::Borrowed(event_type),
            })
            .collect())
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.observe_rv(min_rv);
        Ok(self.current_rv.load(Ordering::Relaxed))
    }

    async fn list_namespace_resources(&self, _namespace: &str) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "v1",
                kind,
                Some(namespace),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?
            .items)
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        _namespace: &str,
        _kind: &str,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        Ok(self.list_namespace_resources(namespace).await?.len() as i64)
    }

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let mut events = self.historical_watch_events_since(targets, since_rv);
        let mut seen_rvs: HashSet<i64> = events
            .iter()
            .map(|event| event.resource.resource_version)
            .collect();
        let event_type = Self::snapshot_replay_event_type(since_rv);
        for target in targets {
            let list = self.list_for_target(target).await?;
            self.observe_rv(list.resource_version);
            events.extend(
                list.items
                    .into_iter()
                    .filter(|resource| resource.resource_version > since_rv)
                    .filter(|resource| seen_rvs.insert(resource.resource_version))
                    .map(|resource| CatchUpResource {
                        resource,
                        event_type: std::borrow::Cow::Borrowed(event_type),
                    }),
            );
        }
        events.sort_by_key(|event| event.resource.resource_version);
        Ok(events)
    }

    async fn list_all_watch_events_since(&self, _since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Ok(Vec::new())
    }

    async fn list_deleted_watch_events_since(
        &self,
        _since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Ok(Vec::new())
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.cluster_api
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn update_node_vtep_mac(&self, _node_name: &str, _vtep_mac: &VtepMac) -> Result<()> {
        Ok(())
    }

    async fn update_node_peer_attributes(
        &self,
        _node_name: &str,
        _mode: crate::controllers::annotations::NodePeerMode,
        _hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_node_dataplane(
        &self,
        _metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        self.unsupported("update_node_dataplane")
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        self.cluster_api.get_node_dataplane(node_name).await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.cluster_api
            .list_pod_cleanup_intents_for_node(node_name)
            .await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.cluster_api
            .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.cluster_api.get_node_subnet(node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.cluster_api.list_peer_subnets(my_node_name).await
    }

    async fn delete_node_subnet(&self, _node_name: &str) -> Result<()> {
        Ok(())
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        self.node_local
            .admit_pod_runtime(pod_uid, namespace, pod_name, node_name)
            .await?;
        Ok(PodSlotAdmissionResult::Admitted {
            resource_version: 0,
        })
    }

    async fn pod_slot_mark_terminating(
        &self,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _node_name: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn pod_slot_clear_if_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
        _node_name: &str,
    ) -> Result<()> {
        self.node_local.delete_pod_runtime_for_uid(pod_uid).await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.node_local.subscribe_pod_slot_admissions()
    }

    async fn patch_resource_latest(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _patch_kind: PatchKind,
        _patch: Value,
    ) -> Result<Option<Resource>> {
        self.unsupported("patch_resource_latest")
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        self.unsupported("patch_resource_latest_with_preconditions")
    }

    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        self.node_local.get_network_for_sandbox(sandbox_id).await
    }

    async fn get_pod_network_for_pod(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.node_local.get_network_for_uid(pod_uid).await
    }

    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        self.node_local
            .reserve_ip_and_insert_network(PodNetworkAllocationRequest::new(
                sandbox_id,
                PodNetworkAllocationPod::new(&pod.namespace, &pod.name, &pod.uid),
                PodNetworkAllocationSubnet::new(subnet_base_int, subnet_size),
                PodNetworkAllocationLink::new(veth_host, netns_path),
            ))
            .await
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        Ok(self
            .node_local
            .list_pod_runtime()
            .await?
            .into_iter()
            .filter_map(|row| {
                Some(SandboxRef {
                    namespace: row.namespace,
                    pod_name: row.pod_name,
                    pod_uid: row.pod_uid,
                    sandbox_id: row.sandbox_id?,
                })
            })
            .collect())
    }

    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.node_local.list_networks().await
    }

    async fn watch_events_gc_prunable_count(
        &self,
        _max_rows: i64,
        _batch_cap: i64,
    ) -> Result<usize> {
        Ok(0)
    }

    async fn gc_watch_events(&self, _max_rows: i64, _batch_cap: i64) -> Result<usize> {
        Ok(0)
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        self.node_local.get_endpoint_by_pod_ip(pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.node_local.list_endpoints_all().await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.node_local.subscribe_pod_endpoints()
    }

    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.node_local.get_node_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        self.node_local.set_node_meta(key, value).await
    }

    async fn get_applied_outbox(
        &self,
        _idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        Ok(None)
    }

    async fn insert_applied_outbox(&self, _record: AppliedOutboxRecord) -> Result<bool> {
        Ok(false)
    }

    async fn apply_outbox_transactionally(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "worker-local store does not support leader-side outbox apply".to_string(),
        ))
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "worker-local store does not support leader-side outbox build".to_string(),
        ))
    }

    async fn gc_applied_outbox(&self, _now_ms: i64, _ttl_ms: i64) -> Result<usize> {
        Ok(0)
    }

    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        _command: StorageCommand,
        _meta: CommandMeta,
    ) -> Result<()> {
        self.unsupported("apply_replicated_command")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::local::LocalApiClient;
    use crate::control_plane::client::{
        CacheScope, ConfigMap, Node, Pod, ResourceEvent, Secret, WatchStream,
    };
    use crate::datastore::DatastoreBackend;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    #[test]
    fn is_watch_window_expired_detects_out_of_range_in_error_chain() {
        // OutOfRange (replay window expired) carried as the error source ->
        // relist required. The gRPC client preserves the tonic::Status as the
        // chain source, so it must be found even when wrapped in a context.
        let err = anyhow::Error::from(tonic::Status::out_of_range("expired"))
            .context("gRPC WatchResources stream failed");
        assert!(
            is_watch_window_expired(&err),
            "OutOfRange status must trigger a relist"
        );

        // Any other gRPC code is a transport/processing error, not an
        // expiration: keep the bookmark and reconnect (with backoff).
        let err = anyhow::Error::from(tonic::Status::unavailable("transport gone"))
            .context("gRPC WatchResources stream failed");
        assert!(
            !is_watch_window_expired(&err),
            "non-OutOfRange gRPC errors must not trigger a relist"
        );

        // A plain non-tonic error is not an expiration.
        assert!(
            !is_watch_window_expired(&anyhow!("some other failure")),
            "non-tonic errors must not trigger a relist"
        );
    }

    #[derive(Default)]
    struct HandoffLeaderApi;

    #[async_trait]
    impl LeaderApiClient for HandoffLeaderApi {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            if key.api_version == "v1" && key.kind == "Namespace" && key.name == "fresh-events" {
                return Ok(None);
            }
            if key.api_version == "v1" && key.kind == "Pod" && key.name == "cached-deleted" {
                return Ok(Some(Resource {
                    id: 1,
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "cached-deleted".to_string(),
                    uid: "uid-cached".to_string(),
                    resource_version: 12,
                    data: Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "cached-deleted",
                            "uid": "uid-cached",
                            "resourceVersion": "12"
                        },
                        "spec": {
                            "nodeName": "worker-a",
                            "containers": [{"name": "app", "image": "nginx"}]
                        }
                    })),
                }));
            }
            unreachable!("handoff test does not use get_resource for {key:?}")
        }

        async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
            if key.api_version == "v1" && key.kind == "Namespace" && key.name == "fresh-events" {
                return Ok(Some(Resource {
                    id: 2,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: "fresh-events".to_string(),
                    uid: "uid-fresh-events".to_string(),
                    resource_version: 13,
                    data: Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {
                            "name": "fresh-events",
                            "uid": "uid-fresh-events",
                            "resourceVersion": "13"
                        },
                        "status": {"phase": "Active"}
                    })),
                }));
            }
            if key.api_version == "v1" && key.kind == "Pod" && key.name == "cached-deleted" {
                return Ok(None);
            }
            self.get_resource(key).await
        }

        async fn list_resources(&self, req: ListRequest) -> Result<ResourceList> {
            let resource_version = if req.api_version == "v1" && req.kind == "Pod" {
                assert_eq!(
                    req.field_selector.as_deref(),
                    Some("spec.nodeName=worker-a")
                );
                41
            } else {
                0
            };
            Ok(ResourceList {
                items: Vec::new(),
                resource_version,
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            if req.api_version == "v1" && req.kind == "Pod" {
                assert_eq!(req.start_resource_version, Some(41));
                let event = ResourceEvent {
                    event: WatchEvent::modified(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "bound-during-handoff",
                            "uid": "uid-handoff",
                            "resourceVersion": "42"
                        },
                        "spec": {
                            "nodeName": "worker-a",
                            "containers": [{"name": "app", "image": "nginx"}]
                        },
                        "status": {"phase": "Pending"}
                    })),
                };
                return Ok(Box::pin(futures::stream::once(async move { Ok(event) })));
            }
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
            Ok(())
        }

        async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
            unreachable!("handoff test does not use get_pod")
        }

        async fn get_pod_for_uid(&self, _ns: &str, _name: &str, _uid: &str) -> Result<Option<Pod>> {
            unreachable!("handoff test does not use get_pod_for_uid")
        }

        async fn watch_pods_on_node(&self, _node_name: &str) -> Result<WatchStream<Pod>> {
            unreachable!("handoff test does not use watch_pods_on_node")
        }

        async fn list_pods_on_node(&self, _node_name: &str) -> Result<Vec<Pod>> {
            unreachable!("handoff test does not use list_pods_on_node")
        }

        async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
            unreachable!("handoff test does not use get_configmap")
        }

        async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
            unreachable!("handoff test does not use get_secret")
        }

        async fn get_node(&self, _name: &str) -> Result<Node> {
            unreachable!("handoff test does not use get_node")
        }

        async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
            unreachable!("handoff test does not use watch_node")
        }

        async fn allocate_node_subnet(
            &self,
            _node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            unreachable!("handoff test does not use allocate_node_subnet")
        }

        async fn get_node_subnet(&self, _node_name: &str) -> Result<Option<NodeSubnet>> {
            unreachable!("handoff test does not use get_node_subnet")
        }

        async fn list_peer_subnets(&self, _my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            unreachable!("handoff test does not use list_peer_subnets")
        }

        async fn get_node_dataplane(
            &self,
            _node_name: &str,
        ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
            unreachable!("handoff test does not use get_node_dataplane")
        }

        async fn list_pod_cleanup_intents_for_node(
            &self,
            _node_name: &str,
        ) -> Result<Vec<PodCleanupIntent>> {
            unreachable!("handoff test does not use list_pod_cleanup_intents_for_node")
        }

        async fn delete_pod_cleanup_intent(
            &self,
            _node_name: &str,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _reason: &str,
        ) -> Result<()> {
            unreachable!("handoff test does not use delete_pod_cleanup_intent")
        }

        async fn apply_outbox(
            &self,
            _idempotency_key: &str,
            _operation: crate::kubelet::outbox::payload::OutboxOperation,
            _payload: bytes::Bytes,
        ) -> std::result::Result<
            crate::kubelet::outbox::OutboxApplyResult,
            crate::kubelet::outbox::OutboxApplyError,
        > {
            unreachable!("handoff test does not use apply_outbox")
        }
    }

    #[tokio::test]
    async fn worker_pod_get_uses_worker_cache_not_fresh_leader_state() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pod-get-fresh-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let pod = adapter
            .get_resource("v1", "Pod", Some("default"), "cached-deleted")
            .await
            .expect("fresh pod get should succeed");

        assert_eq!(
            pod.as_ref().map(|resource| resource.uid.as_str()),
            Some("uid-cached"),
            "worker pod get must read the worker cache and avoid a fresh leader unary read"
        );
    }

    #[tokio::test]
    async fn worker_store_pod_events_use_fresh_namespace_state_before_outbox_enqueue() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-event-namespace-fresh-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local.clone(),
            "worker-a".to_string(),
        );
        let outbox = crate::kubelet::outbox::Outbox::new(node_local.clone());
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "fresh-events",
                "name": "sysctl-pod",
                "uid": "uid-sysctl-pod"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "test-container", "image": "busybox"}]
            }
        });

        crate::kubelet::events::emit_pod_event_with_outbox(
            &adapter,
            Some(&outbox),
            crate::kubelet::events::PodEventRecord {
                pod: &pod,
                reason: "Started",
                message: "Started container test-container",
                event_type: "Normal",
                reporting_component: "klights-kubelet",
                reporting_instance: "worker-a",
            },
        )
        .await
        .expect("worker-store event emission should enqueue event");

        let row = node_local
            .claim_next_due_outbox(i64::MAX / 2, 1_000, "event-test")
            .await
            .expect("claim outbox")
            .expect("event outbox row should be enqueued");
        assert_eq!(row.operation, "EventCreate");
        assert_eq!(row.subject_namespace.as_deref(), Some("fresh-events"));
        assert_eq!(row.subject_kind, "Event");
    }

    #[tokio::test]
    async fn worker_pod_lists_are_constrained_to_local_node() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pod-list-local-node-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let list = adapter
            .list_resources_page(
                "v1",
                "Pod",
                Some("default"),
                None,
                None,
                ListPageRequest::unbounded(),
            )
            .await
            .expect("list local pods");

        assert_eq!(list.resource_version, 41);
    }

    #[tokio::test]
    async fn worker_list_page_preserves_continuation_metadata() {
        // Regression: list_resources_page used to pass limit/continue_token to
        // the leader *and* re-apply ListPageRequest locally. The leader-side
        // pagination already truncated the page, so the local re-apply saw a
        // list no longer than the limit and cleared the leader-provided
        // continue_token / remaining_item_count — workers' LIST silently dropped
        // the rest of the collection. Pagination must be applied exactly once.
        let cluster_db = crate::datastore::test_support::in_memory().await;
        for name in ["cm-a", "cm-b", "cm-c"] {
            cluster_db
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    name,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"namespace": "default", "name": name}
                    }),
                )
                .await
                .expect("create configmap");
        }
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pagination-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());

        let first = adapter
            .list_resources_page(
                "v1",
                "ConfigMap",
                Some("default"),
                None,
                None,
                ListPageRequest::try_new(Some(2), None).expect("page request"),
            )
            .await
            .expect("list first page");
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["cm-a", "cm-b"]
        );
        assert_eq!(
            first.continue_token.as_deref(),
            Some("cm-b"),
            "first page must expose a continue token for the remaining item"
        );
        assert_eq!(first.remaining_item_count, Some(1));

        let second = adapter
            .list_resources_page(
                "v1",
                "ConfigMap",
                Some("default"),
                None,
                None,
                ListPageRequest::try_new(Some(2), first.continue_token.clone())
                    .expect("page request"),
            )
            .await
            .expect("list second page");
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["cm-c"]
        );
        assert!(
            second.continue_token.is_none(),
            "final page must not advertise a continue token"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_respects_resume_resource_version() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        for name in ["cm-a", "cm-b", "cm-c"] {
            cluster_db
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    name,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"namespace": "default", "name": name}
                    }),
                )
                .await
                .expect("create configmap");
        }
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-resume-rv-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        let targets = [WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )];
        let limit = std::num::NonZeroUsize::new(3).expect("non-zero limit");

        let first = adapter
            .list_watch_events_since_checked_bounded(&targets, 0, limit)
            .await
            .expect("initial watch replay");
        let crate::datastore::WatchReplayRead::Events(first_events) = first else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(first_events.len(), 3);
        let max_rv = first_events
            .iter()
            .map(|event| event.resource.resource_version)
            .max()
            .expect("initial replay should have a max rv");

        let second = adapter
            .list_watch_events_since_checked_bounded(&targets, max_rv, limit)
            .await
            .expect("resumed watch replay");
        let crate::datastore::WatchReplayRead::Events(second_events) = second else {
            panic!("worker adapter replay should not expire");
        };
        assert!(
            second_events.is_empty(),
            "resumed worker replay must not return resources at or below the resume RV"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_preserves_mirrored_delete_events() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-delete-replay-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());

        crate::datastore::DatastoreBackend::broadcast_watch_event(
            &adapter,
            crate::datastore::create_pending_watch_event(
                "v1",
                "ConfigMap",
                Some("default"),
                "deleted-config",
                42,
                "DELETED",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": "deleted-config",
                        "resourceVersion": "41"
                    },
                    "data": {"data-1": "value-1"}
                }),
            ),
        );

        let replay = adapter
            .list_watch_events_since_checked_bounded(
                &[WatchTarget::namespaced("v1", "ConfigMap")],
                0,
                std::num::NonZeroUsize::new(8).expect("non-zero limit"),
            )
            .await
            .expect("watch replay should succeed");

        let crate::datastore::WatchReplayRead::Events(events) = replay else {
            panic!("worker adapter replay should not expire");
        };
        assert!(
            events.iter().any(|event| {
                event.event_type.as_ref() == "DELETED"
                    && event.resource.kind == "ConfigMap"
                    && event.resource.name == "deleted-config"
                    && event.resource.resource_version == 42
            }),
            "worker watch replay must preserve mirrored DELETED events because deleted resources are absent from snapshot replay"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_marks_resumed_bound_pod_snapshot_changes_modified() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let created = cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-pod",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-pod",
                        "uid": "uid-deadline"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{
                            "name": "pause",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    }
                }),
            )
            .await
            .expect("create pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-resume-pod-modified-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        let targets = [WatchTarget::namespaced_in_namespace("v1", "Pod", "default")];
        let limit = std::num::NonZeroUsize::new(4).expect("non-zero limit");

        let first = adapter
            .list_watch_events_since_checked_bounded(&targets, 0, limit)
            .await
            .expect("initial watch replay");
        let crate::datastore::WatchReplayRead::Events(first_events) = first else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(first_events.len(), 1);
        assert_eq!(first_events[0].event_type.as_ref(), "ADDED");

        cluster_db
            .update_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-pod",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-pod",
                        "uid": "uid-deadline"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "activeDeadlineSeconds": 1,
                        "containers": [{
                            "name": "pause",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    }
                }),
                created.resource_version,
            )
            .await
            .expect("update pod");

        let resumed = adapter
            .list_watch_events_since_checked_bounded(&targets, created.resource_version, limit)
            .await
            .expect("resumed watch replay");
        let crate::datastore::WatchReplayRead::Events(resumed_events) = resumed else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(resumed_events.len(), 1);
        assert_eq!(
            resumed_events[0].event_type.as_ref(),
            "MODIFIED",
            "worker snapshot replay after a resume RV must preserve update semantics"
        );
        assert_eq!(
            resumed_events[0]
                .resource
                .data
                .pointer("/spec/activeDeadlineSeconds")
                .and_then(|value| value.as_i64()),
            Some(1)
        );
    }

    #[tokio::test]
    async fn reads_cluster_objects_through_worker_cache_and_runtime_rows_from_node_local() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "web",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "web",
                        "uid": "uid-1"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    }
                }),
            )
            .await
            .expect("create cluster pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-test",
        )
        .await
        .expect("open node-local");
        let adapter =
            WorkerStoreAdapter::new(cluster_api, node_local.clone(), "worker-a".to_string());

        let pod = adapter
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("get pod through leader api")
            .expect("pod exists");
        assert_eq!(pod.uid, "uid-1");

        adapter
            .record_sandbox("default", "web", "uid-1", "sandbox-1")
            .await
            .expect("record sandbox in node-local store");
        assert_eq!(
            adapter
                .get_sandbox_for_uid("default", "web", "uid-1")
                .await
                .expect("read worker sandbox"),
            Some("sandbox-1".to_string())
        );
        assert_eq!(
            cluster_db
                .get_sandbox_for_uid("default", "web", "uid-1")
                .await
                .expect("cluster runtime lookup must stay empty"),
            None,
            "worker runtime rows must not be written to cluster storage"
        );
    }

    #[tokio::test]
    async fn watch_mirror_publishes_existing_node_pods_on_startup() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "already-bound",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "already-bound",
                        "uid": "uid-bound"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .expect("create cluster pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-bootstrap-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
            .await
            .expect("existing node pod should be replayed into worker watch")
            .expect("watch channel should remain open");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str()),
            Some("already-bound")
        );
        assert_eq!(
            event
                .object
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("worker-a")
        );
    }

    #[tokio::test]
    async fn watch_mirror_publishes_namespace_events_on_startup() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_namespace(
                "terminating-ns",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "terminating-ns",
                        "uid": "ns-uid",
                        "deletionTimestamp": "2026-05-18T20:06:06Z"
                    },
                    "spec": {"finalizers": ["kubernetes"]},
                    "status": {"phase": "Terminating"}
                }),
            )
            .await
            .expect("create terminating namespace");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-namespace-watch-bootstrap-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Namespace"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let event = watch_rx
                    .recv()
                    .await
                    .expect("watch channel should remain open");
                if event.object.get("kind").and_then(|value| value.as_str()) == Some("Namespace") {
                    break event;
                }
            }
        })
        .await
        .expect("terminating namespace should be replayed into worker watch");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|value| value.as_str()),
            Some("terminating-ns")
        );
        assert_eq!(
            event
                .object
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str()),
            Some("2026-05-18T20:06:06Z")
        );
    }

    #[tokio::test]
    async fn worker_store_requeues_node_local_pod_workqueue_failures() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-workqueue-retry-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let pod = crate::pod_identity::PodIdentity::new("default", "stuck", "uid-stuck");
        adapter
            .pod_workqueue_enqueue(
                PodWorkqueueKind::Pod,
                &pod,
                serde_json::json!({"source": "test"}),
                3,
                0,
                None,
            )
            .await
            .expect("enqueue workqueue row");
        let claimed = adapter
            .pod_workqueue_claim_due(i64::MAX)
            .await
            .expect("claim workqueue row")
            .expect("workqueue row exists");

        adapter
            .pod_workqueue_record_failure(claimed, 0, "missed delete")
            .await
            .expect("record worker-local failure");

        let retried = adapter
            .pod_workqueue_claim_due(i64::MAX)
            .await
            .expect("claim retried workqueue row")
            .expect("failure must requeue worker-local pod delete work");
        assert_eq!(retried.kind, PodWorkqueueKind::Pod);
        assert_eq!(retried.namespace, "default");
        assert_eq!(retried.name, "stuck");
        assert_eq!(retried.uid, "uid-stuck");
        assert_eq!(retried.attempt_count, 4);
        assert_eq!(retried.payload, serde_json::json!({"source": "test"}));
    }

    #[tokio::test]
    async fn worker_store_routes_local_pod_watch_to_lifecycle_actor() {
        struct LocalPodLeaderApi;

        #[async_trait]
        impl LeaderApiClient for LocalPodLeaderApi {
            async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
                unreachable!("local pod watch test does not use get_resource for {key:?}")
            }

            async fn list_resources(&self, req: ListRequest) -> Result<ResourceList> {
                Ok(ResourceList {
                    items: Vec::new(),
                    resource_version: if req.api_version == "v1" && req.kind == "Pod" {
                        41
                    } else {
                        0
                    },
                    continue_token: None,
                    remaining_item_count: None,
                })
            }

            async fn watch_resources(
                &self,
                req: WatchRequest,
            ) -> Result<WatchStream<ResourceEvent>> {
                if req.api_version == "v1" && req.kind == "Pod" {
                    if req.start_resource_version != Some(41) {
                        return Ok(Box::pin(futures::stream::pending()));
                    }
                    let events = vec![
                        ResourceEvent {
                            event: WatchEvent::added(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "startable",
                                    "uid": "uid-startable",
                                    "resourceVersion": "42"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Pending"}
                            })),
                        },
                        ResourceEvent {
                            event: WatchEvent::modified(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "terminating",
                                    "uid": "uid-terminating",
                                    "resourceVersion": "43",
                                    "deletionTimestamp": "2026-06-21T02:07:04Z"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Succeeded"}
                            })),
                        },
                    ];
                    return Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))));
                }
                Ok(Box::pin(futures::stream::pending()))
            }

            async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
                Ok(())
            }

            async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
                unreachable!("local pod watch test does not use get_pod")
            }

            async fn get_pod_for_uid(
                &self,
                _ns: &str,
                _name: &str,
                _uid: &str,
            ) -> Result<Option<Pod>> {
                unreachable!("local pod watch test does not use get_pod_for_uid")
            }

            async fn watch_pods_on_node(&self, _node_name: &str) -> Result<WatchStream<Pod>> {
                unreachable!("local pod watch test does not use watch_pods_on_node")
            }

            async fn list_pods_on_node(&self, _node_name: &str) -> Result<Vec<Pod>> {
                unreachable!("local pod watch test does not use list_pods_on_node")
            }

            async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
                Ok(None)
            }

            async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
                Ok(None)
            }

            async fn get_node(&self, name: &str) -> Result<Node> {
                unreachable!("local pod watch test does not use get_node for {name}")
            }

            async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
                Ok(Box::pin(futures::stream::pending()))
            }

            async fn allocate_node_subnet(
                &self,
                node_name: &str,
                _cluster_cidr: &str,
                _node_ip: &str,
            ) -> Result<NodeSubnet> {
                unreachable!("local pod watch test does not allocate subnet for {node_name}")
            }

            async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
                unreachable!("local pod watch test does not get subnet for {node_name}")
            }

            async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
                unreachable!("local pod watch test does not list peer subnets for {my_node_name}")
            }

            async fn get_node_dataplane(
                &self,
                node_name: &str,
            ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
                unreachable!("local pod watch test does not get dataplane for {node_name}")
            }

            async fn apply_outbox(
                &self,
                _idempotency_key: &str,
                _operation: crate::kubelet::outbox::payload::OutboxOperation,
                _payload: bytes::Bytes,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                unreachable!("local pod watch test does not apply outbox")
            }
        }

        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-terminating-pod-watch-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            Arc::new(LocalPodLeaderApi),
            node_local,
            "worker-a".to_string(),
        ));
        let executor = crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor::new();
        let registry = Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(
                    executor.clone()
                        as Arc<
                            dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor,
                        >,
                )),
            ),
        );
        let router = Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry,
                executor.clone()
                    as Arc<dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor>,
            ),
        );
        adapter.set_pod_lifecycle_router(router);

        let cancel = tokio_util::sync::CancellationToken::new();
        let handles = adapter
            .start_watch_mirrors(supervisor, cancel.clone())
            .await
            .expect("start watch mirrors");

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut observed = Vec::new();
        loop {
            observed.extend(executor.take_actions());
            let start_seen = observed.iter().any(|action| {
                matches!(
                    action,
                    crate::kubelet::pod_lifecycle_core::action::PodAction::StartPod {
                        key, ..
                    }
                    | crate::kubelet::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                        key,
                        ..
                    } if key.name == "startable" && key.uid == "uid-startable"
                )
            });
            let stop_seen = observed.iter().any(|action| {
                matches!(
                    action,
                    crate::kubelet::pod_lifecycle_core::action::PodAction::StopPod {
                        key, ..
                    } if key.name == "terminating" && key.uid == "uid-terminating"
                )
            });
            if start_seen && stop_seen {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "local Pod watch events must wake lifecycle actors; observed actions: {observed:?}"
                );
            }
            tokio::task::yield_now().await;
        }
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }
    }

    #[tokio::test]
    async fn watch_mirror_replays_pods_bound_between_initial_list_and_watch() {
        let cluster_api = Arc::new(HandoffLeaderApi);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-handoff-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
            .await
            .expect("pod bound after the initial list should be replayed from list RV")
            .expect("watch channel should remain open");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(event.event_type, crate::watch::EventType::Modified);
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str()),
            Some("bound-during-handoff")
        );
        assert_eq!(
            event
                .object
                .pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("42")
        );
    }
}
