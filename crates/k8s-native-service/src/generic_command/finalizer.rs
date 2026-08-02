//! Shared finalizer-aware generic deletion mechanics.
//!
//! Pods are rejected by the focused lifecycle contract; bound-Pod row removal
//! remains exclusively actor-owned and the unscheduled CAS exception is not
//! reachable from this module.

use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::{
    FinalizerLifecycleError, FinalizerLifecyclePort, FinalizerOrphanRequest,
    FinalizerResourceTarget, FinalizerTombstoneDeleteRequest, FinalizerUpdateRequest,
};
use serde_json::Value;

use crate::AppError;

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
        .filter(|value| !value.is_null())
        .cloned()
    else {
        return;
    };
    let metadata = updated
        .as_object_mut()
        .map(|object| {
            object
                .entry("metadata".to_string())
                .or_insert_with(|| serde_json::json!({}))
        })
        .and_then(Value::as_object_mut);
    if let Some(metadata) = metadata {
        metadata.insert("deletionTimestamp".to_string(), deletion_timestamp);
    }
}

pub fn ensure_deletion_timestamp_at(
    data: &mut Value,
    grace_seconds: i64,
    now: chrono::DateTime<chrono::Utc>,
) {
    let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    if metadata
        .get("deletionTimestamp")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        metadata.insert(
            "deletionTimestamp".to_string(),
            Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(now)),
        );
    }
    metadata
        .entry("deletionGracePeriodSeconds".to_string())
        .or_insert_with(|| serde_json::json!(grace_seconds));
}

pub fn set_deletion_timestamp_at(data: &mut Value, now: chrono::DateTime<chrono::Utc>) {
    let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    metadata.insert(
        "deletionTimestamp".to_string(),
        Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(now)),
    );
    metadata.insert(
        "deletionGracePeriodSeconds".to_string(),
        serde_json::json!(0),
    );
}

fn has_deletion_timestamp(data: &Value) -> bool {
    data.pointer("/metadata/deletionTimestamp")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

fn has_finalizer(data: &Value, finalizer: &str) -> bool {
    data.pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .any(|value| value.as_str() == Some(finalizer))
        })
}

fn has_only_orphan_finalizer(data: &Value) -> bool {
    data.pointer("/metadata/finalizers")
        .and_then(Value::as_array)
        .filter(|finalizers| !finalizers.is_empty())
        .is_some_and(|finalizers| {
            finalizers
                .iter()
                .all(|value| value.as_str() == Some(ORPHAN_FINALIZER))
        })
}

fn add_finalizer(data: &mut Value, finalizer: &'static str) {
    let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    let finalizers = metadata
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
    let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(finalizers) = metadata.get_mut("finalizers").and_then(Value::as_array_mut) else {
        return;
    };
    finalizers.retain(|value| value.as_str() != Some(finalizer));
    if finalizers.is_empty() {
        metadata.remove("finalizers");
    }
}

fn apply_orphan_deletion_mark(
    data: &mut Value,
    grace_seconds: i64,
    now: chrono::DateTime<chrono::Utc>,
) {
    ensure_deletion_timestamp_at(data, grace_seconds, now);
    add_finalizer(data, ORPHAN_FINALIZER);
}

fn apply_foreground_deletion_mark(data: &mut Value, now: chrono::DateTime<chrono::Utc>) {
    ensure_deletion_timestamp_at(data, 0, now);
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
    apply_mark: fn(&mut Value, i64, chrono::DateTime<chrono::Utc>),
    operation_now: chrono::DateTime<chrono::Utc>,
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
        operation_now,
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
                .ok_or_else(|| AppError::NotFound(format!("{kind} not found")))?,
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

        let mut delete_data = (*resource.data).clone();
        apply_mark(&mut delete_data, grace_seconds, operation_now);
        let update_preconditions = ResourcePreconditions::uid_and_resource_version(
            &expected_uid,
            resource.resource_version,
        );
        match lifecycle
            .update_resource(FinalizerUpdateRequest {
                target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                data: delete_data,
                preconditions: update_preconditions,
            })
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(error)
                if explicit_rv.is_none()
                    && matches!(error, FinalizerLifecycleError::Conflict(_))
                    && attempt < DELETE_MAX_CONFLICT_RETRIES =>
            {
                continue;
            }
            Err(error) => return Err(error.into()),
        }
    }

    Err(AppError::Conflict(format!(
        "{conflict_label} conflicted after retries"
    )))
}

#[allow(clippy::too_many_arguments)]
pub async fn mark_foreground_deletion_with_retry(
    lifecycle: &dyn FinalizerLifecyclePort,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    initial_resource: Resource,
    delete_preconditions: ResourcePreconditions,
    operation_now: chrono::DateTime<chrono::Utc>,
) -> Result<Resource, AppError> {
    mark_deletion_with_retry(
        lifecycle,
        DeletionMarkRequest {
            target: ResourceDeleteTarget {
                api_version,
                kind,
                namespace,
                name,
            },
            initial_resource,
            delete_preconditions,
            grace_seconds: 0,
            apply_mark: |data, _, now| apply_foreground_deletion_mark(data, now),
            operation_now,
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
    pub operation_now: chrono::DateTime<chrono::Utc>,
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
        operation_now,
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
                let mut delete_data = (*resource.data).clone();
                apply_orphan_deletion_mark(&mut delete_data, grace_seconds, operation_now);
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
                        data: delete_data,
                        preconditions: update_preconditions,
                    })
                    .await
                {
                    Ok(updated) => resource = updated,
                    Err(error)
                        if explicit_rv.is_none()
                            && matches!(error, FinalizerLifecycleError::Conflict(_))
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error.into()),
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
                let mut delete_data = (*resource.data).clone();
                remove_finalizer(&mut delete_data, ORPHAN_FINALIZER);
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
                        data: delete_data,
                        preconditions: update_preconditions,
                    })
                    .await
                {
                    Ok(updated) => resource = updated,
                    Err(error)
                        if explicit_rv.is_none()
                            && matches!(error, FinalizerLifecycleError::Conflict(_))
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }

        if orphan_children_before_completion && has_only_orphan_finalizer(&resource.data) {
            if attempt < DELETE_MAX_CONFLICT_RETRIES {
                tracing::debug!(
                    api_version,
                    kind,
                    namespace,
                    name,
                    attempt,
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
            .and_then(Value::as_array)
            .is_some_and(|finalizers| !finalizers.is_empty());
        if has_finalizers {
            if has_deletion_timestamp(&resource.data) {
                return Ok(DeleteCompletion::MarkedTerminating(resource));
            }
            let mut delete_data = (*resource.data).clone();
            ensure_deletion_timestamp_at(&mut delete_data, grace_seconds, operation_now);
            let update_preconditions = ResourcePreconditions::uid_and_resource_version(
                expected_uid.clone(),
                resource.resource_version,
            );
            match lifecycle
                .update_resource(FinalizerUpdateRequest {
                    target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                    data: delete_data,
                    preconditions: update_preconditions,
                })
                .await
            {
                Ok(updated) => return Ok(DeleteCompletion::MarkedTerminating(updated)),
                Err(error)
                    if explicit_rv.is_none()
                        && matches!(error, FinalizerLifecycleError::Conflict(_))
                        && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                {
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let preconditions = ResourcePreconditions::uid_and_resource_version(
            resource.uid.clone(),
            resource.resource_version,
        );
        match lifecycle
            .delete_with_tombstone(FinalizerTombstoneDeleteRequest {
                target: FinalizerResourceTarget::try_new(api_version, kind, namespace, name)?,
                preconditions,
                grace_seconds,
            })
            .await
        {
            Ok(deleted) => return Ok(DeleteCompletion::HardDeleted(deleted)),
            Err(error)
                if explicit_rv.is_none()
                    && matches!(error, FinalizerLifecycleError::Conflict(_))
                    && attempt < DELETE_MAX_CONFLICT_RETRIES =>
            {
                continue;
            }
            Err(error) => match AppError::from(error) {
                AppError::NotFound(_) => return Ok(DeleteCompletion::GoneOrUidChanged),
                other => return Err(other),
            },
        }
    }

    Err(AppError::Conflict(
        "delete conflicted after retries".to_string(),
    ))
}

pub fn ready_to_finalize_after_update(data: &Value) -> bool {
    let has_deletion_timestamp = data
        .pointer("/metadata/deletionTimestamp")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.is_empty());
    has_deletion_timestamp
        && data
            .pointer("/metadata/finalizers")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
}
