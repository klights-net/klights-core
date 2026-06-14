use super::*;
pub async fn discovery_v1_resources() -> Json<APIResourceList> {
    let standard_verbs = vec![
        "create".to_string(),
        "delete".to_string(),
        "deletecollection".to_string(),
        "get".to_string(),
        "list".to_string(),
        "patch".to_string(),
        "update".to_string(),
        "watch".to_string(),
    ];

    Json(APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "discovery.k8s.io/v1".to_string(),
        resources: vec![APIResource {
            name: "endpointslices".to_string(),
            singular_name: "endpointslice".to_string(),
            namespaced: true,
            kind: "EndpointSlice".to_string(),
            verbs: standard_verbs.clone(),
            short_names: None,
            categories: None,
        }],
    })
}
