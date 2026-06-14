//! `PodObjectService` — controller-facing pod object writes
//! (`create_controller_pod`, `delete_pod`, `update_pod_owner_references`,
//! `record_sandbox_id`).
//!
//! Holds `Arc<PodStore>` and a sibling `Arc<PodApiService>` (so that
//! `create_controller_pod` can delegate to the full admission pipeline).
//! Implementations land in Tasks 6 and 14.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::command::StorageCommand;
use crate::datastore::{Resource, ResourcePreconditions};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSubject};
use crate::side_effects::ControllerDispatcherSlot;

use super::api::PodApiService;
use super::store::PodStore;

const SANDBOX_ID_ANNOTATION: &str = "klights.dev/sandbox-id";

pub(super) struct PodObjectService {
    store: Arc<PodStore>,
    api: Arc<PodApiService>,
    controller_dispatcher: ControllerDispatcherSlot,
    outbox: Option<Arc<Outbox>>,
    cluster_api: Option<Arc<dyn LeaderApiClient>>,
}

impl PodObjectService {
    pub(super) fn new(
        store: Arc<PodStore>,
        api: Arc<PodApiService>,
        controller_dispatcher: ControllerDispatcherSlot,
        outbox: Option<Arc<Outbox>>,
        cluster_api: Option<Arc<dyn LeaderApiClient>>,
    ) -> Self {
        Self {
            store,
            api,
            controller_dispatcher,
            outbox,
            cluster_api,
        }
    }

    async fn send_pod_metadata_outbox_command(&self, command: OutboxCommand) -> Result<bool> {
        if self.outbox.is_some() {
            OutboxSendPlanner::new(self.outbox.as_deref())
                .route(command)
                .await?;
            return Ok(true);
        }
        if self.cluster_api.is_some() {
            return Err(anyhow!(
                "outbox is unavailable for node-local queueing; caller must retry after outbox initialization"
            ));
        }
        Ok(false)
    }

    async fn read_current_pod(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
    ) -> Result<Resource> {
        let pod = if let Some(cluster_api) = &self.cluster_api {
            cluster_api.get_pod(ns, name).await?
        } else {
            self.store.get(ns, name).await?
        }
        .ok_or_else(|| anyhow!("Pod not found"))?;
        if let Some(uid) = expected_uid {
            super::ensure_pod_uid_matches(&pod.data, uid, ns, name)?;
        }
        Ok(pod)
    }

    /// Workload-controller-driven Pod create. Delegates to
    /// `PodApiService::api_create_pod` so Deployment/ReplicaSet/StatefulSet/
    /// DaemonSet/Job/RC and other controllers go through the same admission
    /// + quota + defaulting pipeline as the API path. Returns the persisted Resource.
    pub(super) async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        let result = self
            .api
            .api_create_pod(super::types::PodApiCreateRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body: pod,
                dry_run: false,
                run_admission: true,
            })
            .await
            .map_err(|e| anyhow!("{e:?}"))?;
        result
            .resource
            .ok_or_else(|| anyhow!("controller pod create returned dry-run"))
    }

    /// Replace `metadata.ownerReferences` with `owner_refs`, preserving
    /// every other field. Persists with CAS so concurrent metadata writers
    /// cannot silently overwrite ownerReference adoption.
    pub(super) async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let current = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        let cas_rv = current.resource_version;
        self.update_pod_owner_references_inner(ns, name, None, owner_refs, cas_rv, &current)
            .await
    }

    /// UID-gated: fails if the live Pod UID does not match `expected_uid`.
    pub(super) async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let current = self.read_current_pod(ns, name, Some(expected_uid)).await?;
        let cas_rv = current.resource_version;
        self.update_pod_owner_references_inner(
            ns,
            name,
            Some(expected_uid),
            owner_refs,
            cas_rv,
            &current,
        )
        .await
    }

    async fn update_pod_owner_references_inner(
        &self,
        ns: &str,
        name: &str,
        _expected_uid: Option<&str>,
        owner_refs: Vec<Value>,
        cas_rv: i64,
        current: &Resource,
    ) -> Result<Resource> {
        let snapshot = std::sync::Arc::unwrap_or_clone(current.data.clone());
        let mut body: Value = snapshot;
        let metadata = body
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod body is not a JSON object"))?
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let metadata_obj = metadata
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod metadata is not a JSON object"))?;
        metadata_obj.insert("ownerReferences".to_string(), Value::Array(owner_refs));

        let pod_uid = current.uid.as_str();
        let subject_key = format!("v1/Pod/{ns}/{name}/{}", current.uid);
        let enqueued = self
            .send_pod_metadata_outbox_command(OutboxCommand {
                idempotency_key: format!("{}:{}", subject_key, uuid::Uuid::new_v4()),
                operation: OutboxOperation::PodMetadata,
                subject: OutboxSubject {
                    key: subject_key,
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    uid: Some(pod_uid.to_string()),
                },
                pod_uid: pod_uid.to_string(),
                command: StorageCommand::UpdateResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    data: body.clone(),
                    expected_rv: cas_rv,
                    preconditions: ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        resource_version: Some(cas_rv),
                    },
                },
                now_ms: now_ms(),
            })
            .await?;
        if enqueued {
            return Ok(synthetic_resource(current.clone(), body));
        }

        self.store.update(ns, name, body, cas_rv).await
    }

    /// Merge labels into `metadata.labels`, preserving every other field.
    /// Persists with CAS so concurrent metadata writers cannot silently
    /// overwrite label updates.
    pub(super) async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let current = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| anyhow!("Pod not found"))?;
        let cas_rv = current.resource_version;
        self.merge_pod_labels_inner(ns, name, None, labels, cas_rv, &current)
            .await
    }

    /// UID-gated: fails if the live Pod UID does not match `expected_uid`.
    pub(super) async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let current = self.read_current_pod(ns, name, Some(expected_uid)).await?;
        let cas_rv = current.resource_version;
        self.merge_pod_labels_inner(ns, name, Some(expected_uid), labels, cas_rv, &current)
            .await
    }

    async fn merge_pod_labels_inner(
        &self,
        ns: &str,
        name: &str,
        _expected_uid: Option<&str>,
        labels: Vec<(String, String)>,
        cas_rv: i64,
        current: &Resource,
    ) -> Result<Resource> {
        let snapshot = std::sync::Arc::unwrap_or_clone(current.data.clone());
        let previous = snapshot.clone();
        let mut body: Value = snapshot;
        let metadata = body
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod body is not a JSON object"))?
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let metadata_obj = metadata
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod metadata is not a JSON object"))?;
        let labels_value = metadata_obj
            .entry("labels".to_string())
            .or_insert_with(|| json!({}));
        if !labels_value.is_object() {
            *labels_value = json!({});
        }
        let label_obj = labels_value
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod labels are not a JSON object"))?;
        for (key, value) in labels {
            label_obj.insert(key, Value::String(value));
        }

        let pod_uid = current.uid.as_str();
        let subject_key = format!("v1/Pod/{ns}/{name}/{}", current.uid);
        let enqueued = self
            .send_pod_metadata_outbox_command(OutboxCommand {
                idempotency_key: format!("{}:{}", subject_key, uuid::Uuid::new_v4()),
                operation: OutboxOperation::PodMetadata,
                subject: OutboxSubject {
                    key: subject_key,
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    uid: Some(pod_uid.to_string()),
                },
                pod_uid: pod_uid.to_string(),
                command: StorageCommand::UpdateResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    data: body.clone(),
                    expected_rv: cas_rv,
                    preconditions: ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        resource_version: Some(cas_rv),
                    },
                },
                now_ms: now_ms(),
            })
            .await?;
        if enqueued {
            return Ok(synthetic_resource(current.clone(), body));
        }

        let updated = self.store.update(ns, name, body, cas_rv).await?;
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.store.db().as_ref(),
            &self.controller_dispatcher,
        )
        .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository::objects",
                error = %err,
                pod = %name,
                "failed to enqueue Service reconcile after pod label merge"
            );
        }
        Ok(updated)
    }

    /// Set the `klights.dev/sandbox-id` annotation on a pod's metadata,
    /// preserving every other field. Persists with CAS so concurrent
    /// metadata writers cannot silently drop each other's annotations.
    pub(super) async fn record_sandbox_id(
        &self,
        ns: &str,
        name: &str,
        sandbox_id: &str,
    ) -> Result<Resource> {
        self.record_sandbox_id_checked(ns, name, None, sandbox_id)
            .await
    }

    pub(super) async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<Resource> {
        self.record_sandbox_id_checked(ns, name, Some(pod_uid), sandbox_id)
            .await
    }

    async fn record_sandbox_id_checked(
        &self,
        ns: &str,
        name: &str,
        expected_uid: Option<&str>,
        sandbox_id: &str,
    ) -> Result<Resource> {
        let current = self.read_current_pod(ns, name, expected_uid).await?;
        let cas_rv = current.resource_version;
        let mut body: Value = std::sync::Arc::unwrap_or_clone(current.data.clone());

        let metadata = body
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod body is not a JSON object"))?
            .entry("metadata".to_string())
            .or_insert_with(|| json!({}));
        let metadata_obj = metadata
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod metadata is not a JSON object"))?;
        let annotations = metadata_obj
            .entry("annotations".to_string())
            .or_insert_with(|| json!({}));
        let annotations_obj = annotations
            .as_object_mut()
            .ok_or_else(|| anyhow!("Pod annotations is not a JSON object"))?;
        annotations_obj.insert(
            SANDBOX_ID_ANNOTATION.to_string(),
            Value::String(sandbox_id.to_string()),
        );

        let pod_uid = current.uid.as_str();
        let subject_key = format!("v1/Pod/{ns}/{name}/{}", current.uid);
        let enqueued = self
            .send_pod_metadata_outbox_command(OutboxCommand {
                idempotency_key: format!("{}:{}", subject_key, uuid::Uuid::new_v4()),
                operation: OutboxOperation::PodMetadata,
                subject: OutboxSubject {
                    key: subject_key,
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    uid: Some(pod_uid.to_string()),
                },
                pod_uid: pod_uid.to_string(),
                command: StorageCommand::UpdateResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                    data: body.clone(),
                    expected_rv: cas_rv,
                    preconditions: ResourcePreconditions {
                        uid: Some(current.uid.clone()),
                        resource_version: Some(cas_rv),
                    },
                },
                now_ms: now_ms(),
            })
            .await?;
        if enqueued {
            return Ok(synthetic_resource(current.clone(), body));
        }

        self.store.update(ns, name, body, cas_rv).await
    }
}

fn synthetic_resource(mut current: Resource, body: Value) -> Resource {
    current.uid = Resource::uid_from_data(&body);
    current.data = Arc::new(body);
    current
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
