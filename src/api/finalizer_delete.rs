//! Shared finalizer-aware deletion helpers.
//!
//! Non-Pod resources with finalizers are marked terminating first and are
//! removed only after the finalizers drain. Pods are intentionally excluded from
//! hard-delete completion here: the Pod lifecycle actor owns Pod row removal.

use crate::api::*;
use crate::datastore::errors::is_conflict_error;
use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
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
    db: &dyn DatastoreBackend,
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
            None => db
                .get_resource(api_version, kind, namespace, name)
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
        match db
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                del_data,
                update_preconditions,
            )
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(err)
                if explicit_rv.is_none()
                    && is_conflict_error(&err)
                    && attempt < DELETE_MAX_CONFLICT_RETRIES =>
            {
                continue;
            }
            Err(err) => return Err(AppError::from(err)),
        }
    }

    Err(AppError::Conflict(format!(
        "{conflict_label} conflicted after retries"
    )))
}

pub async fn mark_foreground_deletion_with_retry(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    ns: Option<&str>,
    name: &str,
    initial_resource: Resource,
    delete_preconditions: ResourcePreconditions,
) -> Result<Resource, AppError> {
    mark_deletion_with_retry(
        db,
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
    db: &dyn DatastoreBackend,
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
        let Some(mut resource) = db.get_resource(api_version, kind, namespace, name).await? else {
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
                match db
                    .update_resource_with_preconditions(
                        api_version,
                        kind,
                        namespace,
                        name,
                        del_data,
                        update_preconditions,
                    )
                    .await
                {
                    Ok(updated) => {
                        resource = updated;
                    }
                    Err(err)
                        if explicit_rv.is_none()
                            && is_conflict_error(&err)
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(err) => return Err(AppError::from(err)),
                }
            }

            controllers::gc::orphan_children(
                db,
                &resource.uid,
                api_version,
                &resource.name,
                kind,
                namespace.map(str::to_string),
            )
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;

            if has_finalizer(&resource.data, ORPHAN_FINALIZER) {
                let mut del_data: Value = (*resource.data).clone();
                remove_finalizer(&mut del_data, ORPHAN_FINALIZER);
                let update_preconditions = ResourcePreconditions::uid_and_resource_version(
                    expected_uid.clone(),
                    resource.resource_version,
                );
                match db
                    .update_resource_with_preconditions(
                        api_version,
                        kind,
                        namespace,
                        name,
                        del_data,
                        update_preconditions,
                    )
                    .await
                {
                    Ok(updated) => {
                        resource = updated;
                    }
                    Err(err)
                        if explicit_rv.is_none()
                            && is_conflict_error(&err)
                            && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
                    Err(err) => return Err(AppError::from(err)),
                }
            }
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
            match db
                .update_resource_with_preconditions(
                    api_version,
                    kind,
                    namespace,
                    name,
                    del_data,
                    update_preconditions,
                )
                .await
            {
                Ok(updated) => return Ok(DeleteCompletion::MarkedTerminating(updated)),
                Err(err)
                    if explicit_rv.is_none()
                        && is_conflict_error(&err)
                        && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                {
                    continue;
                }
                Err(err) => return Err(AppError::from(err)),
            }
        }

        let delete_preconditions = ResourcePreconditions::uid_and_resource_version(
            expected_uid.clone(),
            resource.resource_version,
        );
        match db
            .delete_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                delete_preconditions,
            )
            .await
        {
            Ok(()) => return Ok(DeleteCompletion::HardDeleted(resource)),
            Err(err) => {
                let app_error = AppError::from(err);
                match app_error {
                    AppError::NotFound(_) => return Ok(DeleteCompletion::GoneOrUidChanged),
                    AppError::Conflict(_)
                        if explicit_rv.is_none() && attempt < DELETE_MAX_CONFLICT_RETRIES =>
                    {
                        continue;
                    }
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
    match state
        .db
        .delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
        .await
    {
        Ok(()) => {}
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("Resource not found")
                || msg.contains("precondition")
                || msg.contains("Precondition")
                || msg.contains("Conflict")
            {
                return;
            }
            tracing::warn!(
                api_version = %api_version,
                kind = %kind,
                namespace = ?namespace,
                name = %name,
                error = %e,
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
        crate::controllers::service::release_service_allocations_from_resource(
            state.service_ipam.as_ref(),
            state.nodeport_alloc.as_ref(),
            &updated.data,
        );
    }

    if let Err(e) = controllers::gc::cascade_delete_with_uid(
        state.db.as_ref(),
        &updated.uid,
        api_version,
        &updated.name,
        kind,
        namespace.map(str::to_string),
        state.pod_repository.as_ref() as &dyn crate::controllers::gc::GcPodDeleteSink,
    )
    .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(
            namespace = ?namespace,
            name = %updated.name,
            error = %e,
            "cascade delete after finalizer-drained hard delete failed"
        );
    }

    let _ = state
        .side_effects
        .run_hooks(&updated.data, state.db.as_ref())
        .await;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::watch::EventType;

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
        let mut watch = db.subscribe_watch(crate::watch::WatchTopic::new(
            owner.api_version.as_str(),
            owner.kind.as_str(),
        ));

        let outcome = complete_non_foreground_delete_with_live_recheck(
            &db,
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
}
