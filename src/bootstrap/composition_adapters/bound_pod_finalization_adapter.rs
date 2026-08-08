//! Root-owned adapters for UID-bound Pod deletion marking and finalization.
//!
//! Kubelet code receives only the transport-neutral `GcPodDeleteSink` and
//! `BoundPodFinalization` capabilities. Command authorship and local-vs-outbox
//! routing stay at the composition root.

use std::sync::Arc;

use klights_cluster_core::StorageCommand;
use klights_pod_api::{
    BoundPodFinalization, BoundPodFinalizationError, BoundPodFinalizationFuture,
    BoundPodFinalizationOutcome, BoundPodFinalizationRequest,
};
use klights_reconcile_api::{
    GcPodDeleteError, GcPodDeleteFuture, GcPodDeleteRequest, GcPodDeleteSink,
};

use crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::LocalBoundPodFinalizationPersistence;
use klights_kubelet::outbox::{
    Outbox, OutboxCommand, OutboxOperation, OutboxSendPlanner, OutboxSubject,
};
use klights_kubelet::pod_repository::store::{ActorPodDeleteObservation, PodStore};

pub(crate) struct RootBoundPodFinalization {
    store: Arc<PodStore>,
    local_persistence: Option<Arc<dyn LocalBoundPodFinalizationPersistence>>,
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<Outbox>>,
    remote_delivery_required: bool,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
}

impl RootBoundPodFinalization {
    pub(crate) fn new(
        store: Arc<PodStore>,
        local_persistence: Option<Arc<dyn LocalBoundPodFinalizationPersistence>>,
        cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        outbox: Option<Arc<Outbox>>,
        remote_delivery_required: bool,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            local_persistence,
            cluster_api,
            outbox,
            remote_delivery_required,
            wall_clock,
        })
    }

    async fn read_remote_live(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.cluster_api
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!("leader Pod query is unavailable for remote bound-Pod finalization")
            })?
            .get_resource(klights_leader_api::pod_get_request(
                namespace,
                name,
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn read_live_for_role(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        if self.remote_delivery_required {
            self.read_remote_live(namespace, name).await
        } else {
            self.store.get(namespace, name).await
        }
    }

    fn outbox_command(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        operation_name: &str,
        command: StorageCommand,
    ) -> OutboxCommand {
        let subject_key = format!("v1/Pod/{namespace}/{name}/{uid}");
        OutboxCommand {
            idempotency_key: format!("{subject_key}:{operation_name}:{}", uuid::Uuid::new_v4()),
            operation: OutboxOperation::PodMetadata,
            subject: OutboxSubject {
                key: subject_key,
                namespace: Some(namespace.to_string()),
                name: name.to_string(),
                uid: Some(uid.to_string()),
            },
            pod_uid: uid.to_string(),
            command,
            now_ms: self.wall_clock.now_ms(),
        }
    }

    fn delete_mark_command(
        namespace: &str,
        name: &str,
        uid: &str,
        live: &klights_cluster_core::Resource,
        operation_now: chrono::DateTime<chrono::Utc>,
    ) -> Result<
        StorageCommand,
        klights_kubelet::pod_repository::delete_deadline::PodDeleteDeadlineError,
    > {
        let plan = klights_kubelet::pod_repository::delete_deadline::plan_pod_delete_deadline(
            &live.data,
            None,
            operation_now,
        )?;
        let mut metadata = serde_json::Map::new();
        for field in [
            "deletionTimestamp",
            "deletionGracePeriodSeconds",
            "generation",
        ] {
            if let Some(value) = plan.body.pointer(&format!("/metadata/{field}")) {
                metadata.insert(field.to_string(), value.clone());
            }
        }
        Ok(StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: serde_json::json!({
                "metadata": metadata
            }),
            preconditions: klights_cluster_core::ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            strict_resource_version: false,
        })
    }
}

pub(crate) fn new_for_root(
    store: Arc<PodStore>,
    local_persistence: Arc<dyn LocalBoundPodFinalizationPersistence>,
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<Outbox>>,
    remote_delivery_required: bool,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> Arc<dyn BoundPodFinalization> {
    RootBoundPodFinalization::new(
        store,
        Some(local_persistence),
        cluster_api,
        outbox,
        remote_delivery_required,
        wall_clock,
    )
}

impl BoundPodFinalization for RootBoundPodFinalization {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            let namespace = identity.namespace;
            let name = identity.name;
            let uid = identity.uid;

            if self.remote_delivery_required {
                let outbox = self.outbox.as_ref().ok_or_else(|| {
                    BoundPodFinalizationError::unavailable(
                        "outbox is unavailable for node-local queueing; caller must retry after outbox initialization",
                    )
                })?;
                let live = self
                    .read_remote_live(&namespace, &name)
                    .await
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
                let observation = self.store.classify_bound_finalization(live.as_ref(), &uid);
                let (node_name, observed_resource_version) = match observation {
                    ActorPodDeleteObservation::Ready {
                        resource_version,
                        node_name,
                    } => (node_name, resource_version),
                    ActorPodDeleteObservation::IdentityChanged => {
                        return Ok(BoundPodFinalizationOutcome::IdentityChanged);
                    }
                    ActorPodDeleteObservation::FinalizersPending => {
                        return Ok(BoundPodFinalizationOutcome::FinalizersPending);
                    }
                    ActorPodDeleteObservation::Retry => {
                        return Ok(BoundPodFinalizationOutcome::Retry);
                    }
                };
                let command = author(
                    namespace.clone(),
                    name.clone(),
                    uid.clone(),
                    node_name,
                    observed_resource_version,
                );
                OutboxSendPlanner::new(Some(outbox.as_ref()))
                    .route(self.outbox_command(
                        &namespace,
                        &name,
                        &uid,
                        "actor-finalize-delete",
                        command,
                    ))
                    .await
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
                return Ok(BoundPodFinalizationOutcome::Accepted);
            }

            self.local_persistence
                .as_ref()
                .ok_or_else(|| {
                    BoundPodFinalizationError::unavailable(
                        "local bound-Pod persistence capability is unavailable",
                    )
                })?
                .finalize_bound_pod(BoundPodFinalizationRequest::try_new(
                    klights_types::PodIdentity::new(&namespace, &name, &uid),
                )?)
                .await
        })
    }
}

pub(crate) fn author(
    namespace: String,
    name: String,
    pod_uid: String,
    node_name: String,
    observed_resource_version: i64,
) -> StorageCommand {
    StorageCommand::FinalizeBoundPod {
        namespace,
        name,
        pod_uid,
        node_name,
        observed_resource_version,
    }
}

impl GcPodDeleteSink for RootBoundPodFinalization {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            let live = self
                .read_live_for_role(&identity.namespace, &identity.name)
                .await
                .map_err(|error| GcPodDeleteError::unavailable(error.to_string()))?
                .ok_or_else(|| GcPodDeleteError::not_found("Pod is already absent"))?;
            if live.uid != identity.uid {
                return Err(GcPodDeleteError::identity_changed(format!(
                    "Pod {}/{} now has UID {}",
                    identity.namespace, identity.name, live.uid
                )));
            }
            if live.data.pointer("/metadata/deletionTimestamp").is_some() {
                return Ok(());
            }
            let outbox = self.outbox.as_ref().ok_or_else(|| {
                GcPodDeleteError::unavailable(
                    "worker Pod delete marking requires the root-owned node outbox adapter",
                )
            })?;
            let command = Self::delete_mark_command(
                &identity.namespace,
                &identity.name,
                &identity.uid,
                &live,
                self.wall_clock.now_utc(),
            )
            .map_err(|error| GcPodDeleteError::unavailable(error.to_string()))?;
            OutboxSendPlanner::new(Some(outbox.as_ref()))
                .route(self.outbox_command(
                    &identity.namespace,
                    &identity.name,
                    &identity.uid,
                    "actor-delete-mark",
                    command,
                ))
                .await
                .map(|_| ())
                .map_err(|error| GcPodDeleteError::unavailable(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::{Value, json};

    #[test]
    fn worker_delete_mark_command_uses_the_canonical_deadline_plan() {
        let operation_now = Utc
            .with_ymd_and_hms(2026, 8, 8, 12, 0, 0)
            .single()
            .expect("fixed operation time");
        let live = klights_cluster_core::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            uid: "uid-a".to_string(),
            resource_version: 41,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "pod-a",
                    "namespace": "default",
                    "uid": "uid-a",
                    "generation": 7,
                    "finalizers": ["example.test/finalizer"]
                },
                "spec": {
                    "nodeName": "node-a",
                    "terminationGracePeriodSeconds": 30,
                    "containers": [{"name": "app", "image": "busybox"}]
                }
            })),
        };

        let command = RootBoundPodFinalization::delete_mark_command(
            "default",
            "pod-a",
            "uid-a",
            &live,
            operation_now,
        )
        .expect("canonical Pod delete mark command");
        let StorageCommand::PatchResource {
            patch,
            preconditions,
            ..
        } = command
        else {
            panic!("delete mark must use a merge patch");
        };

        assert_eq!(
            patch
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str),
            Some("2026-08-08T12:00:30.000000000Z")
        );
        assert_eq!(
            patch
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64),
            Some(30)
        );
        assert_eq!(
            patch
                .pointer("/metadata/generation")
                .and_then(Value::as_i64),
            Some(8)
        );
        assert_eq!(preconditions.uid.as_deref(), Some("uid-a"));
        assert_eq!(preconditions.resource_version, None);
        assert!(
            patch.pointer("/metadata/finalizers").is_none(),
            "the deadline patch must preserve finalizers by omission"
        );
    }
}
