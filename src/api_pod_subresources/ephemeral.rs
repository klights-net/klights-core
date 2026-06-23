use super::*;

/// Maximum optimistic-concurrency retries for an ephemeralcontainers
/// read-merge-write. Ephemeral container updates race with kubelet Pod status
/// writes (which bump `resourceVersion`); upstream Kubernetes retries on
/// conflict (`retry.RetryOnConflict`), so we must too — otherwise a benign
/// concurrent status write surfaces as a 500 / `resourceVersion precondition
/// failed` to clients (e2e `Ephemeral Containers should update ...`).
const EPHEMERAL_CONFLICT_MAX_ATTEMPTS: usize = 10;

fn conflict_backoff_ms(attempt: usize) -> u64 {
    // 5,10,20,40,80,100,100,... ms — bounded so a steady status-write storm
    // cannot stall a subresource update for long.
    std::cmp::min(5_u64.saturating_mul(1u64 << attempt), 100)
}

/// Run an optimistic read-merge-write `persist` step with bounded 409-conflict
/// retry. Each invocation of `persist` must perform its own *fresh* read (so
/// it observes the latest `resourceVersion`) before merging and writing.
async fn run_ephemeral_update_with_conflict_retry<P, F>(
    mut persist: P,
    supervisor: &crate::task_supervisor::TaskSupervisor,
    max_attempts: usize,
) -> Result<crate::datastore::Resource, AppError>
where
    P: FnMut() -> F,
    F: std::future::Future<Output = Result<crate::datastore::Resource, AppError>>,
{
    for attempt in 0..max_attempts {
        match persist().await {
            Ok(resource) => return Ok(resource),
            Err(AppError::Conflict(_)) if attempt + 1 < max_attempts => {
                // JUSTIFY: ephemeralcontainers RMW races concurrent kubelet Pod
                // status writes; there is no event signalling a completed status
                // write, so a bounded optimistic-concurrency backoff is the only
                // spec-correct option. Timer work is supervised.
                let _ = supervisor
                    .sleep(
                        "ephemeral_conflict_retry_backoff",
                        std::time::Duration::from_millis(conflict_backoff_ms(attempt)),
                    )
                    .await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(AppError::Conflict(
        "ephemeralcontainers update conflicted too many times; retry the request later".to_string(),
    ))
}

pub async fn get_pod_ephemeral_containers(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let pod_with_rv = crate::api::inject_resource_version(pod.data, pod.resource_version);
    Ok(Json(pod_with_rv))
}

// PUT /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
pub async fn update_pod_ephemeral_containers(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    crate::api::LenientJson(body): crate::api::LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    // Read-merge-write with bounded 409-conflict retry. Each attempt re-reads
    // the Pod (fresh resourceVersion) so a concurrent kubelet status write no
    // longer fails the stale-RV precondition.
    let ns_owned = namespace.clone();
    let name_owned = name.clone();
    let supervisor = state.task_supervisor.clone();
    let persist = move || {
        let state = state.clone();
        let ns = ns_owned.clone();
        let name = name_owned.clone();
        let body = body.clone();
        Box::pin(async move {
            let pod = crate::kubelet::pod_repository::PodReader::get_pod(
                state.pod_repository.as_ref(),
                &ns,
                &name,
            )
            .await?
            .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", ns, name)))?;

            // K8s spec: ephemeral containers are append-only — existing
            // containers cannot be removed or modified. Validation lives in the
            // handler; persistence runs through the repository.
            let merged = if let Some(new_ephemeral_arr) = request_ephemeral_containers(&body) {
                Some(merge_append_only_ephemeral_containers(
                    &pod.data,
                    new_ephemeral_arr,
                    &ns,
                    &name,
                    "PUT",
                ))
            } else {
                tracing::warn!(
                    "ephemeralcontainers PUT {}/{} missing ephemeral containers field; top_keys={:?} spec_keys={:?}",
                    ns,
                    name,
                    body.as_object()
                        .map(|o| o.keys().cloned().collect::<Vec<_>>()),
                    body.get("spec")
                        .and_then(|s| s.as_object())
                        .map(|o| o.keys().cloned().collect::<Vec<_>>())
                );
                None
            };

            persist_ephemeral_containers(&state, &ns, &name, &pod, merged).await
        })
    };
    let updated = run_ephemeral_update_with_conflict_retry(
        persist,
        supervisor.as_ref(),
        EPHEMERAL_CONFLICT_MAX_ATTEMPTS,
    )
    .await?;
    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

// PATCH /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
pub async fn patch_pod_ephemeral_containers(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<Value>, AppError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    let patch_value: Value = serde_json::from_slice(&body)
        .map_err(|e| AppError::BadRequest(format!("Invalid patch body: {}", e)))?;

    // Read-merge-write with bounded 409-conflict retry; each attempt re-reads
    // the Pod (fresh resourceVersion) and re-applies the patch.
    let ns_owned = namespace.clone();
    let name_owned = name.clone();
    let supervisor = state.task_supervisor.clone();
    let persist = move || {
        let state = state.clone();
        let ns = ns_owned.clone();
        let name = name_owned.clone();
        let patch_value = patch_value.clone();
        Box::pin(async move {
            let pod = crate::kubelet::pod_repository::PodReader::get_pod(
                state.pod_repository.as_ref(),
                &ns,
                &name,
            )
            .await?
            .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", ns, name)))?;

            // Apply the patch to the full pod (immutability validation lives in
            // the handler; the repository only persists).
            let patched = crate::api::apply_patch(&pod.data, &patch_value, content_type)?;

            // Extract only spec.ephemeralContainers from the patch result.
            // K8s behavior: append-only by container name; existing ephemeral
            // containers are immutable and cannot be removed/replaced.
            let merged = request_ephemeral_containers(&patched).map(|new_ephemeral_arr| {
                merge_append_only_ephemeral_containers(
                    &pod.data,
                    new_ephemeral_arr,
                    &ns,
                    &name,
                    "PATCH",
                )
            });

            persist_ephemeral_containers(&state, &ns, &name, &pod, merged).await
        })
    };
    let updated = run_ephemeral_update_with_conflict_retry(
        persist,
        supervisor.as_ref(),
        EPHEMERAL_CONFLICT_MAX_ATTEMPTS,
    )
    .await?;
    let result = crate::api::inject_resource_version(updated.data, updated.resource_version);
    Ok(Json(result))
}

/// Merge incoming ephemeral container specs with the pod's existing ones,
/// honouring K8s append-only semantics (existing entries are immutable).
/// Returns the final array to persist.
fn merge_append_only_ephemeral_containers(
    pod_data: &Value,
    new_ephemeral_arr: &[Value],
    namespace: &str,
    name: &str,
    verb: &str,
) -> Vec<Value> {
    let incoming_names: Vec<String> = new_ephemeral_arr
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    let existing: Vec<Value> = pod_data
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let existing_names: std::collections::HashSet<String> = existing
        .iter()
        .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(str::to_string))
        .collect();

    let mut merged = existing;
    for ec in new_ephemeral_arr {
        let cname = ec.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if !existing_names.contains(cname) {
            merged.push(ec.clone());
        }
    }
    let merged_names: Vec<String> = merged
        .iter()
        .map(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    tracing::info!(
        "ephemeralcontainers {} {}/{} incoming={:?} existing_count={} merged={:?}",
        verb,
        namespace,
        name,
        incoming_names,
        existing_names.len(),
        merged_names
    );
    merged
}

/// Persist the merged ephemeral containers list via the repository.
///
/// When `merged` is `None` (no ephemeral containers in the request payload),
/// the call still goes through the repository to bump `resourceVersion`,
/// matching today's behaviour for empty PUT/PATCH bodies. The repository
/// itself bumps `metadata.generation` when the new list grows, so the
/// handler does not need a second write.
async fn persist_ephemeral_containers(
    state: &Arc<AppState>,
    namespace: &str,
    name: &str,
    pod: &crate::datastore::Resource,
    merged: Option<Vec<Value>>,
) -> Result<crate::datastore::Resource, AppError> {
    let to_persist = match merged {
        Some(arr) => arr,
        // Empty request — keep the existing list verbatim so we still bump RV
        // (matches today's behaviour where update_resource was always called).
        None => pod
            .data
            .pointer("/spec/ephemeralContainers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default(),
    };

    crate::kubelet::pod_repository::PodSubresourceWriter::update_ephemeral_containers(
        state.pod_repository.as_ref(),
        namespace,
        name,
        to_persist,
        pod.resource_version,
    )
    .await
    .map_err(|e| {
        // Surface optimistic-concurrency conflicts as Kubernetes 409 Conflict so
        // the bounded retry loop (and clients) can distinguish a transient
        // status-write race from a real internal failure.
        if crate::datastore::errors::is_conflict_error(&e) {
            AppError::Conflict(format!("ephemeralcontainers update conflict: {e}"))
        } else {
            AppError::InternalError(format!("ephemeralcontainers update failed: {e}"))
        }
    })
}

/// Kubernetes subresource clients can send ephemeral container updates in
/// either of these equivalent shapes:
/// 1) `{ "spec": { "ephemeralContainers": [...] } }`
/// 2) `{ "ephemeralContainers": [...] }`
fn request_ephemeral_containers(value: &Value) -> Option<&Vec<Value>> {
    // Preferred canonical forms.
    let direct = value
        .pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .or_else(|| value.get("ephemeralContainers").and_then(|v| v.as_array()));
    if direct.is_some() {
        return direct;
    }

    // Be permissive for client serializer variants.
    if let Some(spec_obj) = value.get("spec").and_then(|s| s.as_object()) {
        for key in [
            "ephemeralcontainers",
            "ephemeral_containers",
            "ephemeralContainers",
        ] {
            if let Some(arr) = spec_obj.get(key).and_then(|v| v.as_array()) {
                return Some(arr);
            }
        }
        for (k, v) in spec_obj {
            if normalize_ephemeral_key(k)
                && let Some(arr) = v.as_array()
            {
                return Some(arr);
            }
        }
    }

    if let Some(top_obj) = value.as_object() {
        for key in [
            "ephemeralcontainers",
            "ephemeral_containers",
            "ephemeralContainers",
        ] {
            if let Some(arr) = top_obj.get(key).and_then(|v| v.as_array()) {
                return Some(arr);
            }
        }
        for (k, v) in top_obj {
            if normalize_ephemeral_key(k)
                && let Some(arr) = v.as_array()
            {
                return Some(arr);
            }
        }
    }

    None
}

fn normalize_ephemeral_key(key: &str) -> bool {
    key.to_ascii_lowercase().replace('_', "") == "ephemeralcontainers"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fake_resource() -> crate::datastore::Resource {
        crate::datastore::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("ns".to_string()),
            name: "p".to_string(),
            uid: "u".to_string(),
            resource_version: 42,
            data: Arc::new(serde_json::json!({})),
        }
    }

    fn test_supervisor() -> crate::task_supervisor::TaskSupervisor {
        crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )
    }

    /// P0 e2e `Ephemeral Containers should update ...`: a concurrent kubelet
    /// Pod status write bumps resourceVersion between our read and write. The
    /// handler must retry on 409 instead of surfacing a 500 / stale-RV
    /// precondition failure.
    #[tokio::test]
    async fn ephemeral_update_retries_on_conflict_then_succeeds() {
        let supervisor = test_supervisor();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_closure = attempts.clone();
        let persist = Box::new(move || {
            let attempts = attempts_for_closure.clone();
            Box::pin(async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(AppError::Conflict(
                        "simulated status-write race (409 Conflict)".to_string(),
                    ))
                } else {
                    Ok(fake_resource())
                }
            })
        });

        let result = run_ephemeral_update_with_conflict_retry(persist, &supervisor, 10).await;
        assert!(
            result.is_ok(),
            "must succeed after retrying transient conflicts"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "must retry exactly twice before succeeding"
        );
    }

    /// Non-conflict errors must NOT be retried — they surface immediately.
    #[tokio::test]
    async fn ephemeral_update_does_not_retry_non_conflict_errors() {
        let supervisor = test_supervisor();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_for_closure = attempts.clone();
        let persist = Box::new(move || {
            let attempts = attempts_for_closure.clone();
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(AppError::InternalError("real failure".to_string()))
            })
        });

        let result = run_ephemeral_update_with_conflict_retry(persist, &supervisor, 10).await;
        assert!(
            matches!(result, Err(AppError::InternalError(_))),
            "non-conflict errors must surface immediately, not retry"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "non-conflict error must not be retried"
        );
    }

    /// After exhausting retries the loop must return 409 Conflict (not 500),
    /// so a client can retry the whole request.
    #[tokio::test]
    async fn ephemeral_update_returns_conflict_after_exhausting_retries() {
        let supervisor = test_supervisor();
        let persist = Box::new(|| {
            Box::pin(async { Err(AppError::Conflict("always conflicts".to_string())) })
        });

        let result = run_ephemeral_update_with_conflict_retry(persist, &supervisor, 3).await;
        assert!(
            matches!(result, Err(AppError::Conflict(_))),
            "exhausted retries must surface as 409 Conflict, not 500"
        );
    }
}
