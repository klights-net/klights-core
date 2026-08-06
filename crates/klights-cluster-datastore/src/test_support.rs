//! Narrow fixtures shared by cluster-datastore consumers.

use klights_cluster_core::{LogApplyCommit, LogApplyMutation, Resource, ResourcePreconditions};
use klights_cluster_store::ResourceListOptions;

#[derive(Default)]
struct GcCommitSink;

impl klights_cluster_store::CommitObservationSink for GcCommitSink {
    fn observe(&self, _observations: &[klights_cluster_store::StagedPostCommit]) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct GcOutboxCodec;

impl klights_cluster_store::OutboxResponseCodec for GcOutboxCodec {
    fn encode(&self, response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
        serde_json::to_vec(response).map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

pub(crate) fn gc_commit_sink() -> std::sync::Arc<dyn klights_cluster_store::CommitObservationSink> {
    std::sync::Arc::new(GcCommitSink)
}

pub(crate) fn gc_outbox_codec() -> std::sync::Arc<dyn klights_cluster_store::OutboxResponseCodec> {
    std::sync::Arc::new(GcOutboxCodec)
}

/// Focused SQLite fixture capability for cross-crate GC conformance tests.
///
/// The concrete datastore stays private so consumers cannot acquire a generic
/// cluster-store escape hatch. Pod removals are exposed only as UID-qualified
/// actor finalization or the terminating-unscheduled UID/RV CAS exception.
#[derive(Clone)]
pub struct GcTestStore {
    datastore: crate::sqlite::embedded::Datastore,
}

impl GcTestStore {
    pub async fn open(
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            datastore: crate::sqlite::embedded::Datastore::new_for_gc_test_support(supervisor)
                .await?,
        })
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore.create_namespace(name, value).await
    }

    pub async fn seed_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn observe_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn list_fixtures(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        Ok(self
            .datastore
            .list_resources(api_version, kind, namespace, ResourceListOptions::all())
            .await?
            .items)
    }

    pub async fn remove_non_pod_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()> {
        assert_ne!(
            (api_version, kind),
            ("v1", "Pod"),
            "generic Pod removal is forbidden"
        );
        self.datastore
            .delete_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn update_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_main_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_main_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_fixture_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_status_only(
                api_version,
                kind,
                namespace,
                name,
                status,
                Some(resource_version),
            )
            .await
    }

    pub async fn owned_fixtures(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .find_owned_resources(owner_uid, namespace)
            .await
    }

    pub async fn empty_uid_owned_fixtures(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .find_owned_by_name_kind_empty_uid(owner_api_version, owner_name, owner_kind, namespace)
            .await
    }

    pub async fn finalize_bound_pod_for_actor(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        let live = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("actor-owned Pod is gone"))?;
        let node_name = live
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let terminating = live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let finalizers_pending = live
            .data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty());
        if live.uid != uid || node_name.is_empty() || !terminating || finalizers_pending {
            anyhow::bail!("actor finalization preconditions are not satisfied");
        }
        self.datastore
            .delete_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                ResourcePreconditions::uid_and_resource_version(uid, live.resource_version),
            )
            .await
    }

    /// Mark the exact Pod UID terminating before waking its lifecycle actor.
    pub async fn mark_pod_deleting_for_actor(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<bool> {
        let Some(live) = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
        else {
            return Ok(false);
        };
        if live.uid != uid {
            return Ok(false);
        }
        if live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            return Ok(true);
        }
        let mut marked = live.data.as_ref().clone();
        marked["metadata"]["deletionTimestamp"] = serde_json::json!("2026-01-01T00:00:00Z");
        match self
            .datastore
            .update_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                marked,
                ResourcePreconditions::uid_and_resource_version(uid, live.resource_version),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.to_string().contains("precondition")
                    || error.to_string().contains("conflict") =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn finalize_unscheduled_pod_cas(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        observed_resource_version: i64,
    ) -> anyhow::Result<bool> {
        let Some(live) = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
        else {
            return Ok(false);
        };
        let node_name = live
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let terminating = live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let finalizers_pending = live
            .data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty());
        if live.uid != uid
            || live.resource_version != observed_resource_version
            || !node_name.is_empty()
            || !terminating
            || finalizers_pending
        {
            return Ok(false);
        }
        match self
            .datastore
            .delete_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                ResourcePreconditions::uid_and_resource_version(uid, observed_resource_version),
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(error)
                if error.to_string().contains("precondition")
                    || error.to_string().contains("conflict") =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
}

/// Focused cluster-side fixture for node-delivery integration tests.
#[derive(Clone)]
pub struct NodeDeliveryTestCluster {
    datastore: crate::sqlite::embedded::Datastore,
    node_events: tokio::sync::broadcast::Sender<String>,
}

impl NodeDeliveryTestCluster {
    pub async fn open(
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> anyhow::Result<Self> {
        let (node_events, _) = tokio::sync::broadcast::channel(32);
        Ok(Self {
            datastore: crate::sqlite::embedded::Datastore::new_for_gc_test_support(supervisor)
                .await?,
            node_events,
        })
    }

    pub async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .create_resource(api_version, kind, namespace, name, value)
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn list(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        Ok(self
            .datastore
            .list_resources(api_version, kind, namespace, ResourceListOptions::all())
            .await?
            .items)
    }

    pub async fn update(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource(
                api_version,
                kind,
                namespace,
                name,
                value,
                expected_resource_version,
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn replace_if_current(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        current: &Resource,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                value,
                ResourcePreconditions::from_resource(current),
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn update_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                value,
                preconditions,
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn seed_namespace(&self, name: &str, value: serde_json::Value) -> anyhow::Result<()> {
        self.datastore
            .create_namespace(name, value)
            .await
            .map(|_| ())
    }

    pub async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> anyhow::Result<()> {
        self.datastore
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
            .map(|_| ())
    }

    pub async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.datastore.get_current_resource_version().await
    }

    pub async fn watch_replay_position(
        &self,
    ) -> anyhow::Result<klights_cluster_core::WatchReplayPosition> {
        self.datastore.current_watch_replay_position().await
    }

    pub async fn outbox_stream_watermarks(
        &self,
    ) -> anyhow::Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.datastore.list_outbox_stream_watermarks().await
    }

    pub fn subscribe_node_events(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.node_events.subscribe()
    }

    pub async fn stamp_node_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> anyhow::Result<bool> {
        let mut changed = false;
        if let Some(subnet) = self.datastore.get_node_subnet(node_name).await? {
            changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet.to_string());
        }
        if let Some(metadata) = self.datastore.get_node_dataplane(node_name).await? {
            changed |= klights_types::set_node_dataplane_annotations(
                node,
                &metadata.endpoint.to_string(),
                metadata.mode.as_str(),
                metadata.encryption.as_str(),
                metadata.public_key.as_ref().map(|key| key.as_str()),
                metadata.port,
            );
        }
        Ok(changed)
    }

    pub async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        self.datastore
            .apply_outbox_transactionally_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
    }
}

/// Build the RV-zero live-apply template consumed by passive-store tests.
///
/// Public resource versions are allocated by committed apply, so legacy
/// fixture RVs are deliberately erased before validation.
pub fn test_live_commit(
    candidate_resource_version: i64,
    mut mutations: Vec<LogApplyMutation>,
) -> LogApplyCommit {
    fn clear_nested_resource_version(data: &mut serde_json::Value) {
        if let Some(metadata) = data
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            metadata.remove("resourceVersion");
        }
    }

    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.patch);
            }
            LogApplyMutation::PutNamespace(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PutWatchEvent(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
                if let Some(object) = row.data.get_mut("object") {
                    clear_nested_resource_version(object);
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
    }
    let _ = candidate_resource_version;
    LogApplyCommit::try_new(mutations).expect("test live commit must be an RV-zero template")
}
