use super::*;

pub async fn metrics_v1beta1_resources() -> Json<APIResourceList> {
    let read_verbs = vec!["get".to_string(), "list".to_string()];

    Json(APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "metrics.k8s.io/v1beta1".to_string(),
        resources: vec![
            APIResource {
                name: "nodes".to_string(),
                singular_name: "node".to_string(),
                namespaced: false,
                kind: "NodeMetrics".to_string(),
                verbs: read_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "pods".to_string(),
                singular_name: "pod".to_string(),
                namespaced: true,
                kind: "PodMetrics".to_string(),
                verbs: read_verbs,
                short_names: None,
                categories: None,
            },
        ],
    })
}
