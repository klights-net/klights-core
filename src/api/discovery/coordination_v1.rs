use super::*;
pub async fn coordination_v1_resources() -> Json<APIResourceList> {
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
        group_version: "coordination.k8s.io/v1".to_string(),
        resources: vec![APIResource {
            name: "leases".to_string(),
            singular_name: "lease".to_string(),
            namespaced: true,
            kind: "Lease".to_string(),
            verbs: standard_verbs.clone(),
            short_names: None,
            categories: None,
        }],
    })
}
