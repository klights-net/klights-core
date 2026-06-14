use super::*;
pub async fn rbac_v1_resources() -> Json<APIResourceList> {
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
        group_version: "rbac.authorization.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "roles".to_string(),
                singular_name: "role".to_string(),
                namespaced: true,
                kind: "Role".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "rolebindings".to_string(),
                singular_name: "rolebinding".to_string(),
                namespaced: true,
                kind: "RoleBinding".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "clusterroles".to_string(),
                singular_name: "clusterrole".to_string(),
                namespaced: false,
                kind: "ClusterRole".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "clusterrolebindings".to_string(),
                singular_name: "clusterrolebinding".to_string(),
                namespaced: false,
                kind: "ClusterRoleBinding".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
        ],
    })
}
