use super::*;

#[test]
fn core_discovery_keeps_canonical_short_names() {
    let resources = core_v1_aggregated_resources();
    for (name, short_name) in [("pods", "po"), ("services", "svc"), ("nodes", "no")] {
        let resource = resources
            .iter()
            .find(|resource| resource.resource == name)
            .unwrap_or_else(|| panic!("missing core discovery resource {name}"));
        assert!(
            resource
                .short_names
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|candidate| candidate == short_name)
        );
    }
}

#[tokio::test]
async fn api_versions_remains_kubernetes_v1_document() {
    let response = api_versions(axum::http::HeaderMap::new()).await;
    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["kind"], "APIVersions");
    assert_eq!(payload["versions"], serde_json::json!(["v1"]));
}
