use super::{openapi_v2, openapi_v3_discovery_with_crds};
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryFuture,
};
use serde_json::json;

struct EmptyCrdQuery;

impl LeaderResourceQuery for EmptyCrdQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { ResourceListResult::try_new(Vec::new(), 1, None, None, None) })
    }
}

#[tokio::test]
async fn test_openapi_v3_discovery_returns_paths() {
    let response = openapi_v3_discovery_with_crds(&EmptyCrdQuery).await;
    let paths = response["paths"].as_object().expect("OpenAPI v3 paths");
    assert!(paths.contains_key("api/v1"));
    assert!(
        paths["api/v1"]["serverRelativeURL"]
            .as_str()
            .unwrap()
            .starts_with("/openapi/v3/api/v1")
    );
}

#[tokio::test]
async fn test_openapi_v2_returns_swagger() {
    let response = openapi_v2(&EmptyCrdQuery).await;
    assert_eq!(response["swagger"], "2.0");
    assert_eq!(response["info"]["title"], "Kubernetes");
    assert!(response["paths"]["/api/"].is_object());
    assert!(response["definitions"].is_object());
}

#[tokio::test]
async fn test_openapi_v2_includes_builtin_pod_schema_properties() {
    let response = openapi_v2(&EmptyCrdQuery).await;
    let pod = response
        .pointer("/definitions/io.k8s.api.core.v1.Pod")
        .expect("built-in Pod schema");
    assert_eq!(pod["type"], "object");
    assert_eq!(
        pod.pointer("/x-kubernetes-group-version-kind/0"),
        Some(&json!({"group": "", "version": "v1", "kind": "Pod"}))
    );
    let spec_ref = pod
        .pointer("/properties/spec/$ref")
        .or_else(|| pod.pointer("/properties/spec/allOf/0/$ref"))
        .and_then(|value| value.as_str())
        .and_then(|reference| reference.strip_prefix("#/definitions/"))
        .expect("PodSpec reference");
    assert_eq!(
        response.pointer(&format!(
            "/definitions/{spec_ref}/properties/containers/type"
        )),
        Some(&json!("array"))
    );
}
