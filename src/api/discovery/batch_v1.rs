use super::*;
pub async fn batch_v1_resources() -> Json<APIResourceList> {
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
        group_version: "batch/v1".to_string(),
        resources: vec![
            APIResource {
                name: "jobs".to_string(),
                singular_name: "job".to_string(),
                namespaced: true,
                kind: "Job".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: Some(vec!["all".to_string()]),
            },
            APIResource {
                name: "cronjobs".to_string(),
                singular_name: "cronjob".to_string(),
                namespaced: true,
                kind: "CronJob".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["cj".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            // Subresources
            APIResource {
                name: "jobs/status".to_string(),
                singular_name: String::new(),
                namespaced: true,
                kind: "Job".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "cronjobs/status".to_string(),
                singular_name: String::new(),
                namespaced: true,
                kind: "CronJob".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}
