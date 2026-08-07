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
use crate::kubelet::pod_repository::store::{ActorPodDeleteObservation, PodStore};
use klights_kubelet::outbox::{
    Outbox, OutboxCommand, OutboxOperation, OutboxSendPlanner, OutboxSubject,
};

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
    ) -> StorageCommand {
        let grace_period_seconds = live
            .data
            .pointer("/spec/terminationGracePeriodSeconds")
            .and_then(|value| value.as_i64())
            .unwrap_or(30)
            .max(0);
        StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: serde_json::json!({
                "metadata": {
                    "deletionTimestamp": klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now),
                    "deletionGracePeriodSeconds": grace_period_seconds,
                }
            }),
            preconditions: klights_cluster_core::ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            strict_resource_version: false,
        }
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
            );
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
