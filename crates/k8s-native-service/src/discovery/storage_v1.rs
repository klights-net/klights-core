use super::*;
pub async fn storage_v1_resources() -> Json<APIResourceList> {
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
        group_version: "storage.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "storageclasses".to_string(),
                singular_name: "storageclass".to_string(),
                namespaced: false,
                kind: "StorageClass".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["sc".to_string()]),
                categories: None,
            },
            APIResource {
                name: "volumeattachments".to_string(),
                singular_name: "volumeattachment".to_string(),
                namespaced: false,
                kind: "VolumeAttachment".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "volumeattachments/status".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "VolumeAttachment".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "csinodes".to_string(),
                singular_name: "csinode".to_string(),
                namespaced: false,
                kind: "CSINode".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "csinodes/status".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "CSINode".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "csistoragecapacities".to_string(),
                singular_name: "csistoragecapacity".to_string(),
                namespaced: true,
                kind: "CSIStorageCapacity".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            // P0-E2E-20260423-15 part 1: CSIDriver discovery.
            APIResource {
                name: "csidrivers".to_string(),
                singular_name: "csidriver".to_string(),
                namespaced: false,
                kind: "CSIDriver".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
        ],
    })
}
