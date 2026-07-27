use super::*;
pub async fn certificates_v1_resources() -> Json<APIResourceList> {
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
        group_version: "certificates.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "certificatesigningrequests".to_string(),
                singular_name: "certificatesigningrequest".to_string(),
                namespaced: false,
                kind: "CertificateSigningRequest".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["csr".to_string()]),
                categories: None,
            },
            APIResource {
                name: "certificatesigningrequests/status".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "CertificateSigningRequest".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "certificatesigningrequests/approval".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "CertificateSigningRequest".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}
