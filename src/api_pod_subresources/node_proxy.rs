use super::*;

/// GET/POST/PUT/PATCH/DELETE /api/v1/nodes/{name}/proxy/{*path}
///
/// Proxies requests to the kubelet API. klights embeds the kubelet so
/// the `/pods` path is served directly from the DB. The node name may
/// include a port suffix ({nodeName}:{port}) which is stripped.
/// Authorization is enforced by the global `authorize_request` middleware.
pub async fn node_proxy_with_path(
    State(state): State<Arc<AppState>>,
    Path((name, proxy_path)): Path<(String, String)>,
) -> Result<Response, AppError> {
    node_proxy_inner(state, &name, &proxy_path).await
}

/// GET/POST/PUT/PATCH/DELETE /api/v1/nodes/{name}/proxy (no trailing path)
pub async fn node_proxy(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Response, AppError> {
    node_proxy_inner(state, &name, "").await
}

/// Strip optional ":port" suffix from node name — Sonobuoy sends "dp:10250".
fn node_name_from_param(param: &str) -> &str {
    if let Some(idx) = param.rfind(':') {
        &param[..idx]
    } else {
        param
    }
}

async fn node_proxy_inner(
    state: Arc<AppState>,
    name_param: &str,
    proxy_path: &str,
) -> Result<Response, AppError> {
    let node_name = node_name_from_param(name_param);

    // Verify the node exists
    let node = state.db.get_resource("v1", "Node", None, node_name).await?;
    if node.is_none() {
        return Err(AppError::NotFound(format!("Node {} not found", node_name)));
    }

    tracing::debug!("nodes/{}/proxy/{}", node_name, proxy_path);

    match proxy_path {
        "pods" | "pods/" => {
            // Return all pods on this node as a kubelet-style v1.PodList.
            // Routes through the pod repository so the v1/Pod read boundary
            // stays inside `PodStore`.
            use crate::kubelet::pod_repository::PodReader;
            let list = state
                .pod_repository
                .list_pods(None, None, None, None, None)
                .await
                .map_err(|e| AppError::InternalError(format!("Failed to list pods: {}", e)))?;

            // Filter pods scheduled to this node
            let items: Vec<Value> = list
                .items
                .into_iter()
                .filter(|r| {
                    r.data
                        .pointer("/spec/nodeName")
                        .and_then(|v| v.as_str())
                        .map(|n| n == node_name)
                        .unwrap_or(false)
                })
                .map(|r| std::sync::Arc::unwrap_or_clone(r.data))
                .collect();

            let response = serde_json::json!({
                "apiVersion": "v1",
                "kind": "PodList",
                "metadata": {
                    // K8s clients require resourceVersion in all list responses.
                    // The Go meta.ListAccessor returns nil when this field is absent,
                    // which propagates as a typed-nil error in reflector callers.
                    "resourceVersion": list.resource_version.to_string(),
                },
                "items": items,
            });
            Ok(Json(response).into_response())
        }
        "metrics" | "metrics/" => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .body(axum::body::Body::from(""))
            .unwrap()
            .into_response()),
        _ => Err(AppError::NotFound(format!(
            "kubelet API path /{} not implemented",
            proxy_path
        ))),
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod node_proxy_tests {
    use super::*;

    #[test]
    fn test_node_name_from_param_strips_port() {
        assert_eq!(node_name_from_param("dp:10250"), "dp");
    }

    #[test]
    fn test_node_name_from_param_no_port() {
        assert_eq!(node_name_from_param("mynode"), "mynode");
    }

    #[test]
    fn test_node_name_from_param_empty() {
        assert_eq!(node_name_from_param(""), "");
    }

    #[tokio::test]
    async fn test_node_proxy_pods_returns_podlist_for_node() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create the node
        db.create_resource(
            "v1",
            "Node",
            None,
            "dp",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "dp"},
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        // Create a pod on this node
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "mypod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "mypod", "namespace": "default"},
                "spec": {"nodeName": "dp", "containers": [{"name": "c1", "image": "nginx"}]},
                "status": {}
            }),
        )
        .await
        .unwrap();

        // Create a pod on a different node (should NOT appear)
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "otherpod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "otherpod", "namespace": "default"},
                "spec": {"nodeName": "othernode", "containers": [{"name": "c1", "image": "nginx"}]},
                "status": {}
            }),
        )
        .await
        .unwrap();

        let result = node_proxy_inner_db(&db, "dp", "pods").await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].pointer("/metadata/name").and_then(|v| v.as_str()),
            Some("mypod")
        );
    }

    #[tokio::test]
    async fn test_node_proxy_pods_with_port_suffix() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create the node
        db.create_resource(
            "v1",
            "Node",
            None,
            "dp",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "dp"},
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        // "dp:10250" should resolve to node "dp"
        assert_eq!(node_name_from_param("dp:10250"), "dp");

        // Node exists — lookup should succeed
        let node = db.get_resource("v1", "Node", None, "dp").await.unwrap();
        assert!(node.is_some());
    }
}

/// Testable inner function that operates on DB directly (no AppState needed).
#[cfg(test)]
async fn node_proxy_inner_db(
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
    proxy_path: &str,
) -> Result<Vec<Value>, AppError> {
    match proxy_path {
        "pods" | "pods/" => {
            let list = db
                .list_resources(
                    "v1",
                    "Pod",
                    None,
                    crate::datastore::ResourceListQuery::all(),
                )
                .await
                .map_err(|e| AppError::InternalError(format!("Failed to list pods: {}", e)))?;

            let items: Vec<Value> = list
                .items
                .into_iter()
                .filter(|r| {
                    r.data
                        .pointer("/spec/nodeName")
                        .and_then(|v| v.as_str())
                        .map(|n| n == node_name)
                        .unwrap_or(false)
                })
                .map(|r| std::sync::Arc::unwrap_or_clone(r.data))
                .collect();
            Ok(items)
        }
        _ => Err(AppError::NotFound(format!(
            "kubelet API path /{} not implemented",
            proxy_path
        ))),
    }
}
