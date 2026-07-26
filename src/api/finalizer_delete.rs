//! Shared finalizer-aware deletion helpers.
//!
//! Non-Pod resources with finalizers are marked terminating first and are
//! removed only after the finalizers drain. Pods are intentionally excluded from
//! hard-delete completion here: the Pod lifecycle actor owns Pod row removal.

use crate::api::*;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{
    FinalizerEffectsRequest, FinalizerLifecycleError, FinalizerLifecyclePort,
    FinalizerOrphanRequest, FinalizerResourceTarget, FinalizerTombstoneDeleteRequest,
    FinalizerUpdateRequest,
};
use std::sync::Arc;

const DELETE_MAX_CONFLICT_RETRIES: usize = 16;
const ORPHAN_FINALIZER: &str = "orphan";

#[derive(Debug)]
pub enum DeleteCompletion {
    HardDeleted(Resource),
    MarkedTerminating(Resource),
    GoneOrUidChanged,
}

pub fn preserve_deletion_timestamp_on_update(current: &Value, updated: &mut Value) {
    let Some(deletion_timestamp) = current
        .pointer("/metadata/deletionTimestamp")
        .filter(|v| !v.is_null())
        .cloned()
    else {
        return;
    };
    let metadata = updated
        .as_object_mut()
        .map(|obj| {
            obj.entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}))
        })
        .and_then(|metadata| metadata.as_object_mut());
    if let Some(metadata) = metadata {
        metadata.insert("deletionTimestamp".to_string(), deletion_timestamp);
    }
}

pub fn ensure_deletion_timestamp(data: &mut Value, grace_seconds: i64) {
    let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };
    if meta
        .get("deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_none_or(str::is_empty)
    {
        meta.insert(
            "deletionTimestamp".to_string(),
            Value::String(crate::utils::k8s_timestamp()),
        );
    }
    meta.entry("deletionGracePeriodSeconds".to_string())
        .or_insert_with(|| serde_json::json!(grace_seconds));
}

fn has_deletion_timestamp(data: &Value) -> bool {
    data.pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty())
}

fn has_finalizer(data: &Value, finalizer: &str) -> bool {
    data.pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .any(|value| value.as_str() == Some(finalizer))
        })
}

fn has_only_orphan_finalizer(data: &Value) -> bool {
    data.pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .filter(|finalizers| !finalizers.is_empty())
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .all(|value| value.as_str() == Some(ORPHAN_FINALIZER))
        })
}

fn add_finalizer(data: &mut Value, finalizer: &'static str) {
    let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };
    let finalizers = meta
        .entry("finalizers".to_string())
        .or_insert_with(|| serde_json::json!([]));
    if let Some(finalizers) = finalizers.as_array_mut()
        && !finalizers
            .iter()
            .any(|value| value.as_str() == Some(finalizer))
    {
        finalizers.push(serde_json::json!(finalizer));
    }
}

fn remove_finalizer(data: &mut Value, finalizer: &str) {
    let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };
    let Some(finalizers) = meta
        .get_mut("finalizers")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    finalizers.retain(|value| value.as_str() != Some(finalizer));
    if finalizers.is_empty() {
        meta.remove("finalizers");
    }
}

fn apply_orphan_deletion_mark(data: &mut Value, grace_seconds: i64) {
    ensure_deletion_timestamp(data, grace_seconds);
    add_finalizer(data, ORPHAN_FINALIZER);
}

fn apply_foreground_deletion_mark(data: &mut Value) {
    ensure_deletion_timestamp(data, 0);
    add_finalizer(data, "foregroundDeletion");
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceDeleteTarget<'a> {
    pub api_version: &'a str,
    pub kind: &'a str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
}

struct DeletionMarkRequest<'a> {
    target: ResourceDeleteTarget<'a>,
    initial_resource: Resource,
    delete_preconditions: ResourcePreconditions,
    grace_seconds: i64,
    apply_mark: fn(&mut Value, i64),
    conflict_label: &'static str,
}

async fn mark_deletion_with_retry(
    lifecycle: &dyn FinalizerLifecyclePort,
    request: DeletionMarkRequest<'_>,
) -> Result<Resource, AppError> {
    let DeletionMarkRequest {
        target:
            ResourceDeleteTarget {
                api_version,
                kind,
                namespace,
                name,
            },
        initial_resource,
        delete_preconditions,
        grace_seconds,
        apply_mark,
        conflict_label,
    } = request;

    let explicit_rv = delete_preconditions.resource_version;
    let expected_uid = delete_preconditions
        .uid
        .clone()
        .unwrap_or_else(|| initial_resource.uid.clone());
    let mut candidate = Some(initial_resource);

    for attempt in 0..=DELETE_MAX_CONFLICT_RETRIES {
        let resource = match candidate.take() {
            Some(resource) => resource,
            None => lifecycle
                .get_resource(FinalizerResourceTarget::try_new(
                    api_version,
                    kind,
                    namespace,
                    name,
                )?)
                .await?
                .ok_or_else(|| AppError::NotFound(format!("{} not found", kind)))?,
        };

        if resource.uid != expected_uid {
            return Err(AppError::Conflict("UID precondition failed".to_string()));
        }
        if let Some(expected_rv) = explicit_rv
            && resource.resource_version != expected_rv
        {
            return Err(AppError::Conflict(format!(
                "resourceVersion precondition failed: expected {expected_rv} got {}",
                resource.resource_version
            )));
        }

        let mut del_data: Value = (*resource.data).clone();
        apply_mark(&mut del_data, grace_seconds);

        let update_preconditions = ResourcePreconditions::uid_and_resource_version(
            &expected_uid,
            resource.resource_version,
        );
        match lifecycle
            .update_resource(FinalizerUpdateRequest {
                target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                data: del_data,
                preconditions: update_preconditions,
            })
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(err)
                if explicit_rv.is_none()
                    && matches!(err, FinalizerLifecycleError::Conflict(_))
                    && attempt < DELETE_MAX_CONFLICT_RETRIES =>
            {
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Err(AppError::Conflict(format!(
        "{conflict_label} conflicted after retries"
    )))
}

pub async fn mark_foreground_deletion_with_retry(
    lifecycle: &dyn FinalizerLifecyclePort,
    api_version: &str,
    kind: &str,
    ns: Option<&str>,
    name: &str,
    initial_resource: Resource,
    delete_preconditions: ResourcePreconditions,
) -> Result<Resource, AppError> {
    mark_deletion_with_retry(
        lifecycle,
        DeletionMarkRequest {
            target: ResourceDeleteTarget {
                api_version,
                kind,
                namespace: ns,
                name,
            },
            initial_resource,
            delete_preconditions,
            grace_seconds: 0,
            apply_mark: |data, _| apply_foreground_deletion_mark(data),
            conflict_label: "foreground delete",
        },
    )
    .await
}

pub struct NonForegroundDeleteRequest<'a> {
    pub target: ResourceDeleteTarget<'a>,
    pub initial_resource: Resource,
    pub delete_preconditions: ResourcePreconditions,
    pub orphan_children_before_completion: bool,
    pub uid_mismatch_is_conflict: bool,
    pub grace_seconds: i64,
}

pub async fn complete_non_foreground_delete_with_live_recheck(
    lifecycle: &dyn FinalizerLifecyclePort,
    request: NonForegroundDeleteRequest<'_>,
) -> Result<DeleteCompletion, AppError> {
    let NonForegroundDeleteRequest {
        target:
            ResourceDeleteTarget {
                api_version,
                kind,
                namespace,
                name,
            },
        initial_resource,
        delete_preconditions,
        orphan_children_before_completion,
        uid_mismatch_is_conflict,
        grace_seconds,
    } = request;

    let explicit_rv = delete_preconditions.resource_version;
    let expected_uid = delete_preconditions
        .uid
        .clone()
        .unwrap_or_else(|| initial_resource.uid.clone());

    for attempt in 0..=DELETE_MAX_CONFLICT_RETRIES {
        let Some(mut resource) = lifecycle
            .get_resource(FinalizerResourceTarget::try_new(
                api_version,
                kind,
                namespace,
                name,
            )?)
            .await?
        else {
            return Ok(DeleteCompletion::GoneOrUidChanged);
        };

        if resource.uid != expected_uid {
            if uid_mismatch_is_conflict {
                return Err(AppError::Conflict("UID precondition failed".to_string()));
            }
            return Ok(DeleteCompletion::GoneOrUidChanged);
        }
        if let Some(expected_rv) = explicit_rv
            && resource.resource_version != expected_rv
        {
            return Err(AppError::Conflict(format!(
                "resourceVersion precondition failed: expected {expected_rv} got {}",
                resource.resource_version
            )));
        }

        if orphan_children_before_completion {
            if !has_deletion_timestamp(&resource.data)
                || !has_finalizer(&resource.data, ORPHAN_FINALIZER)
            {
                let mut del_data: Value = (*resource.data).clone();
                apply_orphan_deletion_mark(&mut del_data, grace_seconds);
                let update_preconditions = ResourcePreconditions::uid_and_resource_version(
                    expected_uid.clone(),
                    resource.resource_version,
                );
                match lifecycle
                    .update_resource(FinalizerUpdateRequest {
                        target: FinalizerResourceTarget::try_new(
                            api_version,
                            kind,
                            namespace,
                            name,
                        )?,
                        data: del_data,
                        preconditions: update_preconditions,
                    })
                    .await
                {
                    Ok(updated) => {
                        resource = updated;
                    }
                    Err(err)
                        if explicit_rv.is_none()
                            && matches!(err, FinalizerLifecycleError::Conflict(_))
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                }
            }

            lifecycle
                .orphan_children(FinalizerOrphanRequest {
                    target: FinalizerResourceTarget::try_new(
                        api_version,
                        kind,
                        namespace,
                        &resource.name,
                    )?,
                    owner_uid: resource.uid.clone(),
                })
                .await?;

            if has_finalizer(&resource.data, ORPHAN_FINALIZER) {
                let mut del_data: Value = (*resource.data).clone();
                remove_finalizer(&mut del_data, ORPHAN_FINALIZER);
                let update_preconditions = ResourcePreconditions::uid_and_resource_version(
                    expected_uid.clone(),
                    resource.resource_version,
                );
                match lifecycle
                    .update_resource(FinalizerUpdateRequest {
                        target: FinalizerResourceTarget::try_new(
                            api_version,
                            kind,
                            namespace,
                            name,
                        )?,
                        data: del_data,
                        preconditions: update_preconditions,
                    })
                    .await
                {
                    Ok(updated) => {
                        resource = updated;
                    }
                    Err(err)
                        if explicit_rv.is_none()
                            && matches!(err, FinalizerLifecycleError::Conflict(_))
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        if orphan_children_before_completion && has_only_orphan_finalizer(&resource.data) {
            if attempt < DELETE_MAX_CONFLICT_RETRIES {
                tracing::debug!(
                    api_version = %api_version,
                    kind = %kind,
                    namespace = ?namespace,
                    name = %name,
                    attempt = attempt,
                    "orphan delete observed only the internal orphan finalizer after completion attempt; retrying"
                );
                continue;
            }
            return Err(AppError::Conflict(
                "orphan delete conflicted after retries".to_string(),
            ));
        }

        let has_finalizers = resource
            .data
            .pointer("/metadata/finalizers")
            .and_then(|f| f.as_array())
            .is_some_and(|a| !a.is_empty());

        if has_finalizers {
            if has_deletion_timestamp(&resource.data) {
                return Ok(DeleteCompletion::MarkedTerminating(resource));
            }
            let mut del_data: Value = (*resource.data).clone();
            ensure_deletion_timestamp(&mut del_data, grace_seconds);
            let update_preconditions = ResourcePreconditions::uid_and_resource_version(
                expected_uid.clone(),
                resource.resource_version,
            );
            match lifecycle
                .update_resource(FinalizerUpdateRequest {
                    target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                    data: del_data,
                    preconditions: update_preconditions,
                })
                .await
            {
                Ok(updated) => return Ok(DeleteCompletion::MarkedTerminating(updated)),
                Err(err)
                    if explicit_rv.is_none()
                        && matches!(err, FinalizerLifecycleError::Conflict(_))
                        && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                {
                    continue;
                }
                Err(err) => return Err(err.into()),
            }
        }

        let delete_preconditions = ResourcePreconditions::uid_and_resource_version(
            resource.uid.clone(),
            resource.resource_version,
        );
        match lifecycle
            .delete_with_tombstone(FinalizerTombstoneDeleteRequest {
                target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                preconditions: delete_preconditions,
                grace_seconds,
            })
            .await
        {
            Ok(deleted) => return Ok(DeleteCompletion::HardDeleted(deleted)),
            Err(err) => {
                if explicit_rv.is_none()
                    && matches!(err, FinalizerLifecycleError::Conflict(_))
                    && attempt < DELETE_MAX_CONFLICT_RETRIES
                {
                    continue;
                }
                let app_error = AppError::from(err);
                match app_error {
                    AppError::NotFound(_) => return Ok(DeleteCompletion::GoneOrUidChanged),
                    other => return Err(other),
                }
            }
        }
    }

    Err(AppError::Conflict(
        "delete conflicted after retries".to_string(),
    ))
}

pub fn ready_to_finalize_after_update(data: &Value) -> bool {
    let has_deletion_timestamp = data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty());
    if !has_deletion_timestamp {
        return false;
    }
    data.pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .is_none_or(|arr| arr.is_empty())
}

pub async fn finalize_after_update_if_ready(
    state: &Arc<AppState>,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    updated: &Resource,
) {
    if api_version == "v1" && kind == "Pod" {
        return;
    }
    if !ready_to_finalize_after_update(&updated.data) {
        return;
    }

    let preconditions = ResourcePreconditions::uid_and_resource_version(
        updated.uid.clone(),
        updated.resource_version,
    );
    match crate::api::resource_command_ports::delete_non_pod_resource(
        state.resource_mutation().resource_command.as_ref(),
        api_version,
        kind,
        namespace,
        name,
        preconditions,
    )
    .await
    {
        Ok(_) => {}
        Err(AppError::NotFound(_) | AppError::Conflict(_)) => return,
        Err(error) => {
            tracing::warn!(
                api_version = %api_version,
                kind = %kind,
                namespace = ?namespace,
                name = %name,
                error = ?error,
                "finalizer-drained hard delete failed"
            );
            return;
        }
    }

    crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
        state,
        api_version,
        kind,
    )
    .await;

    if api_version == "v1" && kind == "Service" {
        state
            .controller_reconcile()
            .service_allocations
            .release_resource(&updated.data);
    }

    if let Err(error) = state
        .resource_mutation()
        .finalizer_lifecycle
        .run_finalized_effects(FinalizerEffectsRequest {
            resource: updated.clone(),
        })
        .await
    {
        tracing::error!(
            namespace = ?namespace,
            name = %updated.name,
            error = %error,
            "finalizer-drained post-delete effects failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::watch::EventType;
    use klights_cluster_core::StorageCommand;

    #[derive(Default)]
    struct RecordingFinalizerPort {
        updates: Mutex<Vec<ResourcePreconditions>>,
    }

    impl FinalizerLifecyclePort for RecordingFinalizerPort {
        fn get_resource(
            &self,
            _target: FinalizerResourceTarget,
        ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, Option<Resource>> {
            Box::pin(async { Ok(None) })
        }

        fn update_resource(
            &self,
            request: FinalizerUpdateRequest,
        ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, Resource> {
            self.updates
                .lock()
                .expect("update record lock poisoned")
                .push(request.preconditions.clone());
            Box::pin(async move {
                let uid = request
                    .data
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Ok(Resource {
                    id: 1,
                    api_version: request.target.api_version().to_string(),
                    kind: request.target.kind().to_string(),
                    namespace: request.target.namespace().map(str::to_string),
                    name: request.target.name().to_string(),
                    uid,
                    resource_version: 8,
                    data: Arc::new(request.data),
                })
            })
        }

        fn delete_with_tombstone(
            &self,
            _request: FinalizerTombstoneDeleteRequest,
        ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, Resource> {
            Box::pin(async {
                Err(FinalizerLifecycleError::Internal(
                    "unexpected tombstone delete".to_string(),
                ))
            })
        }

        fn orphan_children(
            &self,
            _request: FinalizerOrphanRequest,
        ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, ()> {
            Box::pin(async {
                Err(FinalizerLifecycleError::Internal(
                    "unexpected orphan request".to_string(),
                ))
            })
        }

        fn run_finalized_effects(
            &self,
            _request: FinalizerEffectsRequest,
        ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, ()> {
            Box::pin(async {
                Err(FinalizerLifecycleError::Internal(
                    "unexpected effects request".to_string(),
                ))
            })
        }
    }

    fn finalizer_test_resource(api_version: &str, kind: &str) -> Resource {
        Resource {
            id: 1,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: Some("default".to_string()),
            name: "owned".to_string(),
            uid: "owned-uid".to_string(),
            resource_version: 7,
            data: Arc::new(serde_json::json!({
                "apiVersion": api_version,
                "kind": kind,
                "metadata": {
                    "name": "owned",
                    "namespace": "default",
                    "uid": "owned-uid",
                    "resourceVersion": "7"
                }
            })),
        }
    }

    #[tokio::test]
    async fn fake_port_foreground_mark_uses_exact_uid_and_resource_version() {
        let port = RecordingFinalizerPort::default();
        let updated = mark_foreground_deletion_with_retry(
            &port,
            "apps/v1",
            "Deployment",
            Some("default"),
            "owned",
            finalizer_test_resource("apps/v1", "Deployment"),
            ResourcePreconditions::uid("owned-uid"),
        )
        .await
        .expect("foreground deletion mark should update through the port");

        assert_eq!(updated.resource_version, 8);
        assert_eq!(
            port.updates
                .lock()
                .expect("update record lock poisoned")
                .as_slice(),
            &[ResourcePreconditions::uid_and_resource_version(
                "owned-uid",
                7
            )]
        );
    }

    #[tokio::test]
    async fn fake_port_rejects_pod_before_adapter_dispatch() {
        let port = RecordingFinalizerPort::default();
        let error = mark_foreground_deletion_with_retry(
            &port,
            "v1",
            "Pod",
            Some("default"),
            "owned",
            finalizer_test_resource("v1", "Pod"),
            ResourcePreconditions::uid("owned-uid"),
        )
        .await
        .expect_err("generic finalizer lifecycle must reject Pods");

        assert!(matches!(error, AppError::Forbidden(_)));
        assert!(
            port.updates
                .lock()
                .expect("update record lock poisoned")
                .is_empty()
        );
    }

    struct OrphanFinalizerReinjectingProposer {
        inner: crate::datastore::backend::DatastoreHandle,
        reinjected: AtomicBool,
    }

    impl OrphanFinalizerReinjectingProposer {
        fn should_reinject_orphan_finalizer(command: &StorageCommand) -> bool {
            let StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                ..
            } = command
            else {
                return false;
            };
            api_version == "apps/v1"
                && kind == "Deployment"
                && namespace.as_deref() == Some("orphan-raft-race")
                && name == "demo"
                && data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| !value.is_empty())
                && !data
                    .pointer("/metadata/finalizers")
                    .and_then(|value| value.as_array())
                    .is_some_and(|finalizers| {
                        finalizers
                            .iter()
                            .any(|finalizer| finalizer.as_str() == Some("orphan"))
                    })
        }

        async fn apply_inline(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
            if matches!(command, StorageCommand::DeleteResourceWithTombstone { .. }) {
                let commit = self
                    .inner
                    .build_log_apply_commit_for_command(
                        command,
                        crate::node_outbox::payload::OutboxOperation::PodStatus.as_str(),
                        "orphan-race-proposer",
                    )
                    .await?;
                return self.inner.apply_raft_log_apply_commit(commit).await;
            }
            let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                .encode_protobuf()?;
            let key = format!("orphan-race-{}", uuid::Uuid::new_v4());
            let outcome = crate::datastore::raft::state_machine::propose_outbox_on_backend(
                self.inner.as_ref(),
                &key,
                crate::node_outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::from(payload),
                "orphan-race-proposer",
            )
            .await
            .map_err(|err| anyhow::anyhow!("inline propose: {err}"))?;
            Ok(crate::datastore::raft::types::StorageCommandResult {
                applied_rv: outcome.applied_resource_version(),
                error_message: None,
                rejection_code: None,
                public_resource_changed: false,
                applied_mutation: None,
                pod_endpoint_effect: Default::default(),
            })
        }

        async fn reinsert_orphan_finalizer(&self) -> anyhow::Result<()> {
            let current = self
                .inner
                .get_resource("apps/v1", "Deployment", Some("orphan-raft-race"), "demo")
                .await?
                .expect("Deployment must exist before racing stale status write");
            let mut data: serde_json::Value = (*current.data).clone();
            let metadata = data
                .get_mut("metadata")
                .and_then(|value| value.as_object_mut())
                .expect("Deployment metadata must be an object");
            metadata.insert("finalizers".to_string(), json!(["orphan"]));
            data["status"] = json!({"racedStatusWrite": true});
            self.inner
                .update_resource_with_preconditions(
                    "apps/v1",
                    "Deployment",
                    Some("orphan-raft-race"),
                    "demo",
                    data,
                    ResourcePreconditions::from_resource(&current),
                )
                .await?;
            Ok(())
        }
    }

    #[async_trait]
    impl crate::datastore::sequenced::RaftProposal for OrphanFinalizerReinjectingProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
            let should_reinject = Self::should_reinject_orphan_finalizer(&command)
                && !self.reinjected.swap(true, Ordering::SeqCst);
            let result = self.apply_inline(command).await?;
            if should_reinject {
                self.reinsert_orphan_finalizer().await?;
            }
            Ok(result)
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            crate::node_outbox::OutboxApplyResult,
            crate::node_outbox::OutboxApplyError,
        > {
            self.propose_command(command)
                .await
                .map_err(|err| crate::node_outbox::OutboxApplyError::Retryable(err.to_string()))?;
            let applied_rv = self
                .inner
                .get_current_resource_version()
                .await
                .map_err(|err| crate::node_outbox::OutboxApplyError::Retryable(err.to_string()))?;
            Ok(crate::node_outbox::OutboxApplyResult::Applied { applied_rv })
        }
    }

    #[tokio::test]
    async fn orphan_delete_retries_when_raft_race_reintroduces_internal_orphan_finalizer() {
        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        let proposer = Arc::new(OrphanFinalizerReinjectingProposer {
            inner: inner.clone(),
            reinjected: AtomicBool::new(false),
        });
        let db = crate::datastore::sequenced::SequencedDatastore::new(inner, proposer.clone());

        db.create_namespace(
            "orphan-raft-race",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "orphan-raft-race"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("orphan-raft-race"),
            "demo",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "orphan-raft-race",
                    "name": "demo",
                    "uid": "deploy-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": {
                        "metadata": {"labels": {"app": "demo"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("orphan-raft-race"),
            "demo-abc123",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "namespace": "orphan-raft-race",
                    "name": "demo-abc123",
                    "uid": "rs-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "demo",
                        "uid": "deploy-uid",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": {
                        "metadata": {"labels": {"app": "demo"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

        let owner = db
            .get_resource("apps/v1", "Deployment", Some("orphan-raft-race"), "demo")
            .await
            .unwrap()
            .expect("deployment exists");
        let lifecycle =
            crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
                &db,
            );
        let outcome = complete_non_foreground_delete_with_live_recheck(
            &lifecycle,
            NonForegroundDeleteRequest {
                target: ResourceDeleteTarget {
                    api_version: "apps/v1",
                    kind: "Deployment",
                    namespace: Some("orphan-raft-race"),
                    name: "demo",
                },
                initial_resource: owner,
                delete_preconditions: ResourcePreconditions::uid("deploy-uid"),
                orphan_children_before_completion: true,
                uid_mismatch_is_conflict: true,
                grace_seconds: 0,
            },
        )
        .await
        .unwrap();

        assert!(
            proposer.reinjected.load(Ordering::SeqCst),
            "test proposer must have simulated a stale status write that restored the internal orphan finalizer"
        );
        assert!(
            matches!(outcome, DeleteCompletion::HardDeleted(_)),
            "orphan delete must retry internal orphan-finalizer reintroduction and hard-delete the owner, got {outcome:?}"
        );
        assert!(
            db.get_resource("apps/v1", "Deployment", Some("orphan-raft-race"), "demo")
                .await
                .unwrap()
                .is_none(),
            "Deployment must not remain visible with klights' internal orphan finalizer"
        );
        let child = db
            .get_resource(
                "apps/v1",
                "ReplicaSet",
                Some("orphan-raft-race"),
                "demo-abc123",
            )
            .await
            .unwrap()
            .expect("orphaned ReplicaSet must survive");
        assert!(
            child
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|value| value.as_array())
                .is_none_or(|refs| refs.is_empty()),
            "orphaned ReplicaSet must not retain Deployment ownerRef: {:?}",
            child.data
        );
    }

    #[tokio::test]
    async fn orphan_delete_marks_owner_terminating_before_ownerref_removal_and_hard_delete() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_namespace(
            "orphan-race",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "orphan-race"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("orphan-race"),
            "demo",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "orphan-race",
                    "name": "demo",
                    "uid": "deploy-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": {
                        "metadata": {"labels": {"app": "demo"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("orphan-race"),
            "demo-abc123",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "namespace": "orphan-race",
                    "name": "demo-abc123",
                    "uid": "rs-uid",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "name": "demo",
                        "uid": "deploy-uid",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": {
                        "metadata": {"labels": {"app": "demo"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

        let owner = db
            .get_resource("apps/v1", "Deployment", Some("orphan-race"), "demo")
            .await
            .unwrap()
            .expect("deployment exists");
        let mut watch = db.subscribe_watch(klights_watch::WatchTopic::new(
            owner.api_version.as_str(),
            owner.kind.as_str(),
        ));
        let lifecycle =
            crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
                &db,
            );

        let outcome = complete_non_foreground_delete_with_live_recheck(
            &lifecycle,
            NonForegroundDeleteRequest {
                target: ResourceDeleteTarget {
                    api_version: "apps/v1",
                    kind: "Deployment",
                    namespace: Some("orphan-race"),
                    name: "demo",
                },
                initial_resource: owner,
                delete_preconditions: ResourcePreconditions::uid("deploy-uid"),
                orphan_children_before_completion: true,
                uid_mismatch_is_conflict: true,
                grace_seconds: 0,
            },
        )
        .await
        .unwrap();
        assert!(matches!(outcome, DeleteCompletion::HardDeleted(_)));

        let mut events = Vec::new();
        for _ in 0..3 {
            match tokio::time::timeout(Duration::from_millis(200), watch.recv()).await {
                Ok(Ok(event)) => events.push(event),
                _ => break,
            }
        }

        let owner_modified = events.iter().position(|event| {
            event.event_type == EventType::Modified
                && event.object.pointer("/apiVersion").and_then(|v| v.as_str()) == Some("apps/v1")
                && event.object.pointer("/kind").and_then(|v| v.as_str()) == Some("Deployment")
                && event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("demo")
                && event
                    .object
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| !value.is_empty())
                && event
                    .object
                    .pointer("/metadata/finalizers")
                    .and_then(|v| v.as_array())
                    .is_some_and(|finalizers| {
                        finalizers
                            .iter()
                            .any(|finalizer| finalizer.as_str() == Some("orphan"))
                    })
        });
        let owner_deleted = events.iter().position(|event| {
            event.event_type == EventType::Deleted
                && event.object.pointer("/apiVersion").and_then(|v| v.as_str()) == Some("apps/v1")
                && event.object.pointer("/kind").and_then(|v| v.as_str()) == Some("Deployment")
                && event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("demo")
        });

        assert!(
            owner_modified
                .zip(owner_deleted)
                .is_some_and(|(mark, delete)| mark < delete),
            "orphan delete must publish a terminating owner update before the owner delete; events: {:?}",
            events
        );

        let child = db
            .get_resource("apps/v1", "ReplicaSet", Some("orphan-race"), "demo-abc123")
            .await
            .unwrap()
            .expect("orphaned ReplicaSet must survive");
        assert!(
            child
                .data
                .pointer("/metadata/ownerReferences")
                .and_then(|v| v.as_array())
                .is_none_or(|refs| refs.is_empty()),
            "orphaned ReplicaSet must not retain Deployment ownerRef: {:?}",
            child.data
        );
    }

    #[tokio::test]
    async fn non_finalizer_delete_without_finalizers_emits_only_deleted_watch_event() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_namespace(
            "no-finalizer-delete",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "no-finalizer-delete"}
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("no-finalizer-delete"),
            "demo",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "namespace": "no-finalizer-delete",
                    "name": "demo",
                    "uid": "deploy-uid"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": {
                        "metadata": {"labels": {"app": "demo"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

        let owner = db
            .get_resource("apps/v1", "Deployment", Some("no-finalizer-delete"), "demo")
            .await
            .unwrap()
            .expect("deployment exists");
        let mut watch = db.subscribe_watch(klights_watch::WatchTopic::new(
            owner.api_version.as_str(),
            owner.kind.as_str(),
        ));
        let lifecycle =
            crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
                &db,
            );

        let outcome = complete_non_foreground_delete_with_live_recheck(
            &lifecycle,
            NonForegroundDeleteRequest {
                target: ResourceDeleteTarget {
                    api_version: "apps/v1",
                    kind: "Deployment",
                    namespace: Some("no-finalizer-delete"),
                    name: "demo",
                },
                initial_resource: owner,
                delete_preconditions: ResourcePreconditions::uid("deploy-uid"),
                orphan_children_before_completion: false,
                uid_mismatch_is_conflict: true,
                grace_seconds: 0,
            },
        )
        .await
        .unwrap();

        assert!(matches!(outcome, DeleteCompletion::HardDeleted(_)));

        let mut events = Vec::new();
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_millis(200), watch.recv()).await {
                Ok(Ok(event)) => events.push(event),
                _ => break,
            }
        }

        let has_modified = events.iter().any(|event| {
            event.event_type == EventType::Modified
                && event.object.pointer("/apiVersion").and_then(|v| v.as_str()) == Some("apps/v1")
                && event.object.pointer("/kind").and_then(|v| v.as_str()) == Some("Deployment")
                && event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("demo")
                && event
                    .object
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(|v| v.as_str())
                    .is_some_and(|value| !value.is_empty())
        });
        let has_deleted = events.iter().any(|event| {
            event.event_type == EventType::Deleted
                && event.object.pointer("/apiVersion").and_then(|v| v.as_str()) == Some("apps/v1")
                && event.object.pointer("/kind").and_then(|v| v.as_str()) == Some("Deployment")
                && event
                    .object
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    == Some("demo")
        });

        assert!(
            !has_modified,
            "non-finalizer delete must not emit a MODIFIED event"
        );
        assert!(
            has_deleted,
            "non-finalizer delete must emit a DELETED event"
        );
        assert!(
            db.get_resource("apps/v1", "Deployment", Some("no-finalizer-delete"), "demo")
                .await
                .unwrap()
                .is_none()
        );
    }
}
