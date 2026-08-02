//! Kubernetes Node proxy HTTP adaptation.

use super::*;

/// GET/POST/PUT/PATCH/DELETE /api/v1/nodes/{name}/proxy/{*path}
///
/// Proxies requests to the kubelet API. klights embeds the kubelet so
/// the `/pods` path is served directly from the DB. The node name may
/// include a port suffix ({nodeName}:{port}) which is stripped.
/// Authorization is enforced by the global `authorize_request` middleware.
pub async fn node_proxy_with_path<S>(
    State(state): State<Arc<S>>,
    Path((name, proxy_path)): Path<(String, String)>,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
    node_proxy_inner(state, &name, &proxy_path).await
}

/// GET/POST/PUT/PATCH/DELETE /api/v1/nodes/{name}/proxy (no trailing path)
pub async fn node_proxy<S>(
    State(state): State<Arc<S>>,
    Path(name): Path<String>,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
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

async fn node_proxy_inner<S>(
    state: Arc<S>,
    name_param: &str,
    proxy_path: &str,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
    let node_name = node_name_from_param(name_param);

    // Verify the node exists
    let node = crate::generic_read::get_resource(
        state.streaming_resource_query(),
        "v1",
        "Node",
        None,
        node_name,
    )
    .await?;
    if node.is_none() {
        return Err(AppError::NotFound(format!("Node {} not found", node_name)));
    }

    tracing::debug!("nodes/{}/proxy/{}", node_name, proxy_path);

    match proxy_path {
        "pods" | "pods/" => {
            // Return all pods on this node as a kubelet-style v1.PodList.
            // Routes through the pod repository so the v1/Pod read boundary
            // stays inside `PodStore`.
            let (items, resource_version, _, _) = state
                .streaming_dependencies()
                .pod_query
                .list_pods(PodListRequest::try_new(None, None, None, None, None)?)
                .await
                .map_err(|e| AppError::InternalError(format!("Failed to list pods: {}", e)))?
                .into_parts();

            // Filter pods scheduled to this node
            let items: Vec<Value> = items
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
                    "resourceVersion": resource_version.to_string(),
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
    use std::sync::{Arc, Mutex};

    use klights_cluster_core::Resource;
    use klights_leader_api::{
        LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
        ResourceQueryFuture,
    };
    use klights_pod_api::{
        PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
        PodRepositoryFuture,
    };

    use super::*;

    struct FakePodQuery {
        items: Vec<Resource>,
        resource_version: i64,
    }

    impl PodQuery for FakePodQuery {
        fn get_pod(&self, _request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
            Box::pin(async { panic!("node proxy must not issue an exact Pod get") })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            let result =
                PodListResult::try_new(self.items.clone(), self.resource_version, None, None)
                    .expect("valid Pod list fixture");
            Box::pin(async move { Ok(result) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<Resource>> {
            Box::pin(async { panic!("node proxy must not issue an owner Pod list") })
        }
    }

    struct FakeNodeQuery {
        requested_names: Mutex<Vec<String>>,
    }

    impl LeaderResourceQuery for FakeNodeQuery {
        fn get_resource(
            &self,
            request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            self.requested_names
                .lock()
                .expect("node query capture lock")
                .push(request.key().name.clone());
            Box::pin(async { Ok(Some(resource("Node", None, "dp", serde_json::json!({})))) })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            Box::pin(async { panic!("node proxy must not issue a generic resource list") })
        }
    }

    struct UnusedAdmission;

    impl ResourceAdmissionPort for UnusedAdmission {
        fn admit(
            &self,
            _request: ResourceAdmissionRequest,
        ) -> crate::generic_command::GenericCommandFuture<'_, Value> {
            Box::pin(async { panic!("node proxy must not invoke admission") })
        }
    }

    struct FakeStreamingState {
        dependencies: StreamingDependencies,
        node_query: Arc<FakeNodeQuery>,
        admission: UnusedAdmission,
    }

    impl StreamingState for FakeStreamingState {
        fn streaming_dependencies(&self) -> &StreamingDependencies {
            &self.dependencies
        }

        fn streaming_resource_query(&self) -> &dyn LeaderResourceQuery {
            self.node_query.as_ref()
        }

        fn streaming_admission(&self) -> &dyn ResourceAdmissionPort {
            &self.admission
        }
    }

    fn resource(kind: &str, namespace: Option<&str>, name: &str, extra: Value) -> Resource {
        let mut data = serde_json::json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": {"name": name},
        });
        if let Some(namespace) = namespace {
            data["metadata"]["namespace"] = Value::String(namespace.to_string());
        }
        if let Some(extra) = extra.as_object() {
            data.as_object_mut()
                .expect("resource fixture object")
                .extend(extra.clone());
        }
        Resource::try_from_data(Arc::new(data)).expect("valid resource fixture")
    }

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
        let pod_query = Arc::new(FakePodQuery {
            items: vec![
                resource(
                    "Pod",
                    Some("default"),
                    "mypod",
                    serde_json::json!({"spec": {"nodeName": "dp"}}),
                ),
                resource(
                    "Pod",
                    Some("default"),
                    "otherpod",
                    serde_json::json!({"spec": {"nodeName": "othernode"}}),
                ),
            ],
            resource_version: 77,
        });
        let node_query = Arc::new(FakeNodeQuery {
            requested_names: Mutex::new(Vec::new()),
        });
        let unavailable = Arc::new(super::super::test_support::UnavailableStreaming);
        let state = Arc::new(FakeStreamingState {
            dependencies: StreamingDependencies::new(
                pod_query,
                None,
                None,
                unavailable,
                Arc::<str>::from("dp"),
                Arc::new(TaskSupervisor::new(Default::default())),
            ),
            node_query: node_query.clone(),
            admission: UnusedAdmission,
        });

        let response = node_proxy_with_path(
            State(state),
            Path(("dp:10250".to_string(), "pods".to_string())),
        )
        .await
        .expect("node PodList response");
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("bounded PodList body");
        let list: Value = serde_json::from_slice(&body).expect("JSON PodList");

        assert_eq!(list["kind"], "PodList");
        assert_eq!(list["metadata"]["resourceVersion"], "77");
        assert_eq!(list["items"].as_array().expect("PodList items").len(), 1);
        assert_eq!(list["items"][0]["metadata"]["name"], "mypod");
        assert_eq!(
            *node_query
                .requested_names
                .lock()
                .expect("node query capture lock"),
            vec!["dp".to_string()],
            "node port suffix must be stripped before the existence query",
        );
    }
}
