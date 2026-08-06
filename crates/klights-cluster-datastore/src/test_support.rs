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
