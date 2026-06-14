use super::*;
pub async fn networking_v1_resources() -> Json<APIResourceList> {
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
        group_version: "networking.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "ingresses".to_string(),
                singular_name: "ingress".to_string(),
                namespaced: true,
                kind: "Ingress".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["ing".to_string()]),
                categories: None,
            },
            // F1-01: NetworkPolicy discovery now matches the OpenAPI side and the
            // newly added CRUD routes.
            APIResource {
                name: "networkpolicies".to_string(),
                singular_name: "networkpolicy".to_string(),
                namespaced: true,
                kind: "NetworkPolicy".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["netpol".to_string()]),
                categories: None,
            },
            APIResource {
                name: "ingressclasses".to_string(),
                singular_name: "ingressclass".to_string(),
                namespaced: false,
                kind: "IngressClass".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            // P0-E2E-20260423-10: ServiceCIDR + IPAddress (GA in v1.31).
            APIResource {
                name: "servicecidrs".to_string(),
                singular_name: "servicecidr".to_string(),
                namespaced: false,
                kind: "ServiceCIDR".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "servicecidrs/status".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "ServiceCIDR".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "ipaddresses".to_string(),
                singular_name: "ipaddress".to_string(),
                namespaced: false,
                kind: "IPAddress".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["ip".to_string()]),
                categories: None,
            },
        ],
    })
}
