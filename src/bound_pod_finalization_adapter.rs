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

use crate::kubelet::pod_repository::store::{
    ActorPodDeleteObservation, BoundPodDeleteOutcome, PodStore,
};
use klights_kubelet::outbox::{
    Outbox, OutboxCommand, OutboxOperation, OutboxSendPlanner, OutboxSubject,
};

pub(crate) struct RootBoundPodFinalization {
    store: Arc<PodStore>,
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<Outbox>>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
}

impl RootBoundPodFinalization {
    pub(crate) fn new(
        store: Arc<PodStore>,
        cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        outbox: Option<Arc<Outbox>>,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            cluster_api,
            outbox,
            wall_clock,
        })
    }

    async fn read_live(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        if let Some(cluster_api) = &self.cluster_api {
            return cluster_api
                .get_resource(klights_leader_api::pod_get_request(
                    namespace,
                    name,
                    klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                )?)
                .await
                .map_err(anyhow::Error::new);
        }
        self.store.get(namespace, name).await
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
    cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    outbox: Option<Arc<Outbox>>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> Arc<dyn BoundPodFinalization> {
    RootBoundPodFinalization::new(store, cluster_api, outbox, wall_clock)
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

            if self.cluster_api.is_some() && self.outbox.is_none() {
                return Err(BoundPodFinalizationError::unavailable(
                    "outbox is unavailable for node-local queueing; caller must retry after outbox initialization",
                ));
            }

            if let Some(outbox) = &self.outbox {
                let live = self
                    .read_live(&namespace, &name)
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

            match self
                .store
                .finalize_bound_with_uid(&namespace, &name, &uid)
                .await
            {
                Ok(BoundPodDeleteOutcome::Removed) => Ok(BoundPodFinalizationOutcome::Removed),
                Ok(BoundPodDeleteOutcome::IdentityChanged) => {
                    Ok(BoundPodFinalizationOutcome::IdentityChanged)
                }
                Ok(BoundPodDeleteOutcome::FinalizersPending) => {
                    Ok(BoundPodFinalizationOutcome::FinalizersPending)
                }
                Ok(BoundPodDeleteOutcome::Retry) => Ok(BoundPodFinalizationOutcome::Retry),
                Err(error) => Err(BoundPodFinalizationError::unavailable(error.to_string())),
            }
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
                .read_live(&identity.namespace, &identity.name)
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
