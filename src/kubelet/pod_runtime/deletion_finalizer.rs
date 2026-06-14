use std::sync::Arc;

use crate::controllers::gc::GcPodDeleteSink;
use crate::kubelet::outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSubject};
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_runtime::service::{PodDeletionFinalizeResult, PodRuntimeKey};

fn pod_is_node_lost_terminal(pod: &serde_json::Value) -> bool {
    pod.pointer("/status/phase")
        .and_then(|value| value.as_str())
        == Some("Failed")
        && pod
            .pointer("/status/reason")
            .and_then(|value| value.as_str())
            == Some("NodeLost")
}

fn gc_pod_delete_error_means_gone_or_uid_changed(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.contains("Resource not found")
        || message.contains("Pod not found")
        || message.contains("NotFound")
        || message.contains("UID precondition failed")
        || message.contains("uid precondition")
        || message.contains("precondition failed")
}

fn pod_delete_grace_period_seconds(data: &serde_json::Value) -> i64 {
    data.pointer("/spec/terminationGracePeriodSeconds")
        .and_then(|value| value.as_i64())
        .unwrap_or(30)
        .max(0)
}

/// Actor-owned Pod API-object deletion finalizer port.
/// The production implementation is the only code path allowed to
/// hard-delete a `v1/Pod` datastore row after actor cleanup completes.
#[async_trait::async_trait]
pub trait PodDeletionFinalizer: Send + Sync {
    /// Finalize pod deletion after actor-side runtime cleanup completes.
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult>;
}

/// Production actor-owned Pod deletion finalizer.
///
/// Moves the body of `PodRepository::finalize_pod_deletion_after_actor_cleanup`
/// behind the `PodDeletionFinalizer` trait so the actor-owned hard-delete
/// invariant can be source-guarded.
pub struct RealPodDeletionFinalizer {
    pub store: Arc<PodStore>,
    gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    cluster_api: Option<Arc<dyn crate::control_plane::client::LeaderApiClient>>,
    outbox: Option<Arc<Outbox>>,
    side_effects: Arc<crate::side_effects::SideEffectRegistry>,
    metrics: Arc<crate::side_effects::SideEffectMetrics>,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl RealPodDeletionFinalizer {
    pub fn new(
        store: Arc<PodStore>,
        gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
        cluster_api: Option<Arc<dyn crate::control_plane::client::LeaderApiClient>>,
        outbox: Option<Arc<Outbox>>,
        side_effects: Arc<crate::side_effects::SideEffectRegistry>,
        metrics: Arc<crate::side_effects::SideEffectMetrics>,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            store,
            gc_pod_delete_sink,
            cluster_api,
            outbox,
            side_effects,
            metrics,
            supervisor,
        }
    }

    async fn make_actor_finalize_delete_outbox_command(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> OutboxCommand {
        let subject_key = format!("v1/Pod/{ns}/{name}/{uid}");
        OutboxCommand {
            idempotency_key: format!(
                "{}:actor-finalize-delete:{}",
                subject_key,
                uuid::Uuid::new_v4()
            ),
            operation: crate::kubelet::outbox::payload::OutboxOperation::PodMetadata,
            subject: OutboxSubject {
                key: subject_key,
                namespace: Some(ns.to_string()),
                name: name.to_string(),
                uid: Some(uid.to_string()),
            },
            pod_uid: uid.to_string(),
            command: crate::datastore::command::StorageCommand::DeleteResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some(ns.to_string()),
                name: name.to_string(),
                preconditions: crate::datastore::ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
            },
            now_ms: crate::kubelet::pod_repository::current_epoch_millis(),
        }
    }

    async fn make_actor_delete_mark_outbox_command(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        live: &crate::datastore::Resource,
    ) -> OutboxCommand {
        let subject_key = format!("v1/Pod/{ns}/{name}/{uid}");
        OutboxCommand {
            idempotency_key: format!("{}:actor-delete-mark:{}", subject_key, uuid::Uuid::new_v4()),
            operation: crate::kubelet::outbox::payload::OutboxOperation::PodMetadata,
            subject: OutboxSubject {
                key: subject_key,
                namespace: Some(ns.to_string()),
                name: name.to_string(),
                uid: Some(uid.to_string()),
            },
            pod_uid: uid.to_string(),
            command: crate::datastore::command::StorageCommand::PatchResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some(ns.to_string()),
                name: name.to_string(),
                patch_kind: crate::datastore::PatchKind::Merge,
                patch: serde_json::json!({
                    "metadata": {
                        "deletionTimestamp": crate::utils::k8s_timestamp(),
                        "deletionGracePeriodSeconds": pod_delete_grace_period_seconds(&live.data),
                    }
                }),
                preconditions: crate::datastore::ResourcePreconditions {
                    uid: Some(uid.to_string()),
                    resource_version: None,
                },
            },
            now_ms: crate::kubelet::pod_repository::current_epoch_millis(),
        }
    }

    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let db = self.store.db().clone();
        let pod_reader: Arc<dyn crate::kubelet::pod_repository::PodReader> = self.store.clone();
        let metrics = self.metrics.clone();
        let ns = namespace.to_string();
        drop(self.supervisor.spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            format!("post_write_maintenance/{ns}"),
            async move {
                crate::controllers::pdb::reconcile_pdbs_for_namespace(
                    db.as_ref(),
                    pod_reader.as_ref(),
                    &ns,
                )
                .await;
                if let Err(err) =
                    crate::api::reconcile_namespace_termination(db.as_ref(), &ns, metrics.as_ref())
                        .await
                {
                    tracing::warn!(
                        namespace = %ns,
                        error = ?err,
                        "post-write namespace termination reconcile failed"
                    );
                }
            },
        ));
    }

    async fn delete_status_checkpoint_after_finalization(&self, uid: &str) {
        let Some(outbox) = &self.outbox else {
            return;
        };
        if let Err(err) = outbox.delete_pod_status_checkpoint(uid).await {
            tracing::warn!(
                pod_uid = %uid,
                error = %err,
                "actor-owned Pod finalization failed to delete node-local status checkpoint"
            );
        }
    }
}

#[async_trait::async_trait]
impl PodDeletionFinalizer for RealPodDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        let ns = &key.namespace;
        let name = &key.name;
        let uid = &key.uid;

        let live = if let Some(cluster_api) = &self.cluster_api {
            cluster_api
                .get_resource_fresh(crate::control_plane::client::ResourceKey {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(ns.to_string()),
                    name: name.to_string(),
                })
                .await?
        } else {
            self.store.get(ns, name).await?
        };
        let Some(live) = live else {
            self.delete_status_checkpoint_after_finalization(uid).await;
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        };

        if live.uid != *uid {
            tracing::warn!(
                namespace = %ns,
                pod = %name,
                requested_uid = %uid,
                live_uid = %live.uid,
                "actor-owned Pod finalization ignored stale UID because a replacement Pod exists"
            );
            self.delete_status_checkpoint_after_finalization(uid).await;
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        }

        if live.data.pointer("/metadata/deletionTimestamp").is_none() {
            if pod_is_node_lost_terminal(live.data.as_ref()) {
                tracing::warn!(
                    namespace = %ns,
                    pod = %name,
                    uid = %uid,
                    "actor-owned Pod finalization treated NodeLost terminal Pod as local cleanup"
                );
                self.delete_status_checkpoint_after_finalization(uid).await;
                return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
            }
            tracing::warn!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                "actor-owned Pod finalization reissued UID-bound delete mark for non-terminating Pod"
            );
            if let Some(outbox) = &self.outbox {
                OutboxSendPlanner::new(Some(outbox.as_ref()))
                    .route(
                        self.make_actor_delete_mark_outbox_command(ns, name, uid, &live)
                            .await,
                    )
                    .await?;
                return Ok(PodDeletionFinalizeResult::FinalizersPending);
            }
            match self
                .gc_pod_delete_sink
                .request_gc_pod_delete(ns, name, uid)
                .await
            {
                Ok(()) => return Ok(PodDeletionFinalizeResult::FinalizersPending),
                Err(err) if gc_pod_delete_error_means_gone_or_uid_changed(&err) => {
                    tracing::debug!(
                        namespace = %ns,
                        pod = %name,
                        uid = %uid,
                        error = %err,
                        "actor-owned Pod finalization delete-mark retry found Pod gone or UID changed"
                    );
                    return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
                }
                Err(err) => return Err(err),
            }
        }

        if live
            .data
            .pointer("/metadata/finalizers")
            .and_then(|finalizers| finalizers.as_array())
            .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return Ok(PodDeletionFinalizeResult::FinalizersPending);
        }

        if self.cluster_api.is_some() && self.outbox.is_none() {
            return Err(anyhow::anyhow!(
                "outbox is unavailable for node-local queueing; caller must retry after outbox initialization"
            ));
        }

        if let Some(outbox) = &self.outbox {
            OutboxSendPlanner::new(Some(outbox.as_ref()))
                .route(
                    self.make_actor_finalize_delete_outbox_command(ns, name, uid)
                        .await,
                )
                .await?;
            self.delete_status_checkpoint_after_finalization(uid).await;
            return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
        }

        let deleted_data = live.data.clone();
        match self.store.delete_with_uid(ns, name, uid).await {
            Ok(()) => {}
            Err(err) if crate::datastore::errors::is_conflict_error(&err) => {
                tracing::warn!(
                    namespace = %ns,
                    pod = %name,
                    requested_uid = %uid,
                    error = %err,
                    "actor-owned Pod finalization lost UID race; preserving live Pod"
                );
                self.delete_status_checkpoint_after_finalization(uid).await;
                return Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone);
            }
            Err(err) => return Err(err),
        }
        self.delete_status_checkpoint_after_finalization(uid).await;

        if let Err(err) = crate::controllers::gc::cascade_delete_with_uid(
            self.store.db().as_ref(),
            uid,
            "v1",
            name,
            "Pod",
            Some(ns.to_string()),
            self.gc_pod_delete_sink.as_ref(),
        )
        .await
        {
            self.metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                error = %err,
                "actor-owned Pod finalization cascade delete failed"
            );
        }

        if let Err(err) = crate::controllers::gc::finalize_foreground_owners_after_dependent_delete(
            self.store.db().as_ref(),
            &live,
            self.gc_pod_delete_sink.as_ref(),
        )
        .await
        {
            self.metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                error = %err,
                "actor-owned Pod finalization foreground-owner check failed"
            );
        }

        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_delete(
            &deleted_data,
            self.store.db().as_ref(),
            &self.side_effects.controller_dispatcher_slot(),
        )
        .await
        {
            tracing::debug!(
                target: "klights::kubelet::pod_repository",
                error = %err,
                pod = %name,
                "failed to enqueue Service reconcile after actor-owned pod finalization"
            );
        }

        crate::side_effects::run_hooks_logged(
            &self.side_effects,
            &deleted_data,
            self.store.db().as_ref(),
            &self.metrics,
            "pod_actor_finalize_delete",
        )
        .await;
        self.spawn_post_write_maintenance(ns).await;
        Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
    }
}
