use crate::api::AppError;
use crate::api::state::AppState;
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;
use crate::controllers::workqueue::ReconcileKey;
use axum::{Extension, Json, extract::State};
use serde_json::json;
use std::sync::Arc;

pub async fn pod_lifecycle_debug_dump(
    State(state): State<Arc<AppState>>,
    Extension(identity): Extension<AuthenticatedIdentity>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Authorize: non-resource URL /debug/klights/pod-lifecycle
    let request = AuthorizationRequest::non_resource("get", "/debug/klights/pod-lifecycle");
    let decision = state.authorizer.authorize(&identity, &request).await;
    if !decision.allowed {
        return Err(AppError::Forbidden(if decision.denied {
            decision.reason
        } else {
            "forbidden: get /debug/klights/pod-lifecycle".to_string()
        }));
    }

    let diag = if let Some(router) = state.pod_lifecycle_router.as_ref() {
        router.diagnostics().await
    } else {
        crate::kubelet::pod_lifecycle_router::PodLifecycleDiagnostics {
            mode: crate::kubelet::pod_lifecycle_router::PodLifecycleRouteMode::Actor,
            actor_states: Vec::new(),
            recent_trace: Vec::new(),
            active_pod_count: 0,
        }
    };

    let actors = diag
        .actor_states
        .into_iter()
        .map(|entry| {
            json!({
                "namespace": entry.namespace,
                "podName": entry.name,
                "uid": entry.uid,
                "state": entry.state,
            })
        })
        .collect::<Vec<_>>();

    let recent_trace = diag
        .recent_trace
        .into_iter()
        .map(|entry| {
            json!({
                "namespace": entry.key.namespace,
                "podName": entry.key.name,
                "uid": entry.key.uid,
                "event": entry.event,
                "resourceVersion": entry.resource_version,
                "sandboxId": entry.sandbox_id,
            })
        })
        .collect::<Vec<_>>();

    let pending_controller_keys = state
        .controller_dispatcher
        .pending_reconcile_keys()
        .await
        .into_iter()
        .map(reconcile_key_to_string)
        .collect::<Vec<_>>();

    let pending_retry_keys = if let Some(retry_state) = state.pod_start_retry_state.as_ref() {
        retry_state.lock().await.pending_key_pairs()
    } else {
        Vec::new()
    };

    let side_effect_failures = state
        .metrics
        .recent_failures()
        .into_iter()
        .map(|entry| {
            json!({
                "apiVersion": entry.api_version,
                "kind": entry.kind,
                "namespace": entry.namespace,
                "name": entry.name,
                "hook": entry.hook,
                "context": entry.context,
                "error": entry.error,
            })
        })
        .collect::<Vec<_>>();

    Ok(Json(serde_json::json!({
        "actors": actors,
        "recentTrace": recent_trace,
        "pendingControllerKeys": pending_controller_keys,
        "pendingRetryKeys": pending_retry_keys,
        "sideEffectFailures": side_effect_failures,
    })))
}

fn reconcile_key_to_string(key: ReconcileKey) -> String {
    match key.namespace {
        Some(namespace) => {
            format!("{}/{}/{}", key.api_version, key.kind, namespace).replace("//", "/")
                + "/"
                + &key.name
        }
        None => format!("{}/{}/{}", key.api_version, key.kind, key.name),
    }
}
