use super::*;

#[test]
fn aggregated_accept_negotiation_prefers_v2() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::ACCEPT,
        "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
            .parse()
            .unwrap(),
    );
    assert_eq!(wants_aggregated_discovery(&headers), Some("v2"));
}

#[test]
fn apps_discovery_remains_native_and_namespaced() {
    let resources = aggregated_resources_for_group_version("apps", "v1");
    let deployment = resources
        .iter()
        .find(|resource| resource.resource == "deployments")
        .expect("apps/v1 discovery must advertise deployments");
    assert_eq!(deployment.scope, "Namespaced");
    assert_eq!(deployment.response_kind.kind, "Deployment");
}
