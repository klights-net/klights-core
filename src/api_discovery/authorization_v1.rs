use super::*;
pub async fn authorization_v1_resources() -> Json<APIResourceList> {
    Json(APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "authorization.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "selfsubjectaccessreviews".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "SelfSubjectAccessReview".to_string(),
                verbs: vec!["create".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "selfsubjectrulesreviews".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "SelfSubjectRulesReview".to_string(),
                verbs: vec!["create".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "subjectaccessreviews".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "SubjectAccessReview".to_string(),
                verbs: vec!["create".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "localsubjectaccessreviews".to_string(),
                singular_name: String::new(),
                namespaced: true,
                kind: "LocalSubjectAccessReview".to_string(),
                verbs: vec!["create".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}
